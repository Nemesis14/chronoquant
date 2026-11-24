"""Read-only DuckDB access for the API (short-lived connection + retry, blueprint §5.4).

Every request opens a fresh in-memory connection with live / lab / reg (and the
trading journal as ``jrn``) attached READ_ONLY, and closes it afterwards.  A
cross-process writer lock is retried briefly; if it persists the request fails
with ``db_busy`` (HTTP 503) and ``/api/status`` reports ``db.state = "busy"``.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import duckdb
from fastapi import Depends, Query

import utils
from data_handling.store.readonly_query import open_readonly_connection
from worker import assets

logger = logging.getLogger(__name__)

ATTACH_TRIES = 4        # 1 attempt + 3 retries
ATTACH_DELAY = 0.2      # seconds (backoff step)

_state_lock = threading.Lock()
_db_busy = False


class DbBusyError(Exception):
    """The DuckDB files are locked by a writer (sync in progress)."""


def db_state() -> str:
    """Return ``"busy"`` if the last connection attempt hit a lock, else ``"ok"``."""
    return "busy" if _db_busy else "ok"


def _set_busy(value: bool) -> None:
    global _db_busy  # noqa: PLW0603
    with _state_lock:
        _db_busy = value


def journal_db_path() -> str | None:
    """Absolute path of the trading journal DuckDB file, or None when not configured."""
    try:
        return utils._resolve_path(utils.load_trading_config()["db_path"])
    except Exception:
        logger.exception("journal_db_path: trading config unavailable")
        return None


@contextmanager
def ro_connection(asset_id: str | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield a short-lived read-only connection of ``asset_id`` (default asset if None).

    Raises DbBusyError on a persistent lock.
    """
    extra: dict[str, str] = {}
    jpath = journal_db_path()
    if jpath and Path(jpath).exists():
        extra["jrn"] = jpath
    try:
        conn = open_readonly_connection(
            asset_id, tries=ATTACH_TRIES, delay=ATTACH_DELAY, extra_targets=extra
        )
    except duckdb.IOException as exc:
        _set_busy(True)
        logger.warning("read-only connection failed (locked?): %s", exc)
        raise DbBusyError(str(exc)) from exc
    _set_busy(False)
    try:
        yield conn
    finally:
        conn.close()


def get_asset(
    asset: Annotated[
        str | None,
        Query(max_length=64, description="Asset id (config/assets.json); default asset if omitted"),
    ] = None,
) -> str:
    """FastAPI dependency: the requested asset id (``?asset=``), default when omitted; unknown -> 404."""
    try:
        return assets.require_asset(asset)
    except assets.UnknownAssetError as exc:
        from api.errors import not_found  # noqa: PLC0415 - api.errors imports this module

        raise not_found(str(exc)) from exc


AssetDep = Annotated[str, Depends(get_asset)]


def get_conn(asset_id: AssetDep) -> Iterator[duckdb.DuckDBPyConnection]:
    """FastAPI dependency: a per-request read-only connection of the requested asset."""
    with ro_connection(asset_id) as conn:
        yield conn
