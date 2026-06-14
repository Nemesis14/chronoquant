from __future__ import annotations

import io
import logging
import threading
from contextlib import redirect_stdout
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd

import utils
from data_pipeline.sync_features import sync_features
from data_pipeline.sync_ohlcv import sync_ohlcv
from data_pipeline.sync_predictions import sync_predictions
from store.duckdb_query import ohlcv_latest_open_time, ohlcv_row_count
from streamlit_app.dashboard_logging import get_dashboard_logger

INITIAL_SYNC_START = "2017-01-01 00:00:00"

_ASSET_SYNC_LOCKS: dict[str, threading.Lock] = {}
_LOCKS_MUTEX = threading.Lock()


# =============================================================================
# get_sync_lock(asset_id: str | None = None) -> threading.Lock
# =============================================================================
# Purpose:
#  - Return the per-asset sync lock shared by dashboard and trading service
# =============================================================================
def get_sync_lock(asset_id: str | None = None) -> threading.Lock:
    key = asset_id or "default"
    with _LOCKS_MUTEX:
        if key not in _ASSET_SYNC_LOCKS:
            _ASSET_SYNC_LOCKS[key] = threading.Lock()
        return _ASSET_SYNC_LOCKS[key]


@dataclass(frozen=True)
class SyncResult:
    start_time: str
    end_time: str | None
    ohlcv_rows_before: int
    ohlcv_rows_after: int

    @property
    def inserted_ohlcv_rows(self) -> int:
        return max(0, self.ohlcv_rows_after - self.ohlcv_rows_before)


class _LoggerWriter(io.StringIO):
    def __init__(self, logger: logging.Logger) -> None:
        super().__init__()
        self._logger = logger
        self._buffer = ""

    def write(self, value: str) -> int:
        self._buffer += value
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._log_line(line)
        return len(value)

    def flush(self) -> None:
        self._log_line(self._buffer)
        self._buffer = ""

    def _log_line(self, line: str) -> None:
        line = line.strip()
        if line:
            self._logger.info(line)


def run_database_sync(asset_id: str | None = None) -> SyncResult:
    logger   = get_dashboard_logger()
    db_cfg  = utils.load_asset_config(asset_id)["database"]
    db_path = db_cfg["db_path"]

    lock = get_sync_lock(asset_id)
    if not lock.acquire(blocking=False):
        logger.info("Sync already running for asset_id=%s — skipped", asset_id)
        rows = ohlcv_row_count(db_path)
        return SyncResult(
            start_time        = "",
            end_time          = None,
            ohlcv_rows_before = rows,
            ohlcv_rows_after  = rows,
        )

    try:
        return _run_database_sync_locked(asset_id, logger, db_path)
    finally:
        lock.release()


def _run_database_sync_locked(
    asset_id : str | None,
    logger   : logging.Logger,
    db_path  : str,
) -> SyncResult:
    logger.info("Database sync started")
    logger.info("DB path: %s", db_path)

    rows_before    = ohlcv_row_count(db_path)
    last_open_time = ohlcv_latest_open_time(db_path)
    start_time     = _next_open_time(last_open_time) if last_open_time else INITIAL_SYNC_START
    start_ms       = _utc_str_to_ms(start_time)

    logger.info("OHLCV sync from Binance started at %s", start_time)
    _run_with_logged_stdout(sync_ohlcv, start_ms, asset_id=asset_id, logger=logger)

    rows_after       = ohlcv_row_count(db_path)
    latest_open_time = ohlcv_latest_open_time(db_path)
    logger.info(
        "OHLCV rows before=%s after=%s inserted=%s",
        rows_before, rows_after, rows_after - rows_before,
    )

    if not latest_open_time or pd.to_datetime(latest_open_time) < pd.to_datetime(start_time):
        logger.info("No new OHLCV interval found for feature and prediction sync")
        logger.info("Database sync complete")
        return SyncResult(start_time, latest_open_time, rows_before, rows_after)

    logger.info("Feature sync started: %s -> %s", start_time, latest_open_time)
    _run_with_logged_stdout(
        sync_features,
        start_time,
        end_time = latest_open_time,
        asset_id = asset_id,
        logger   = logger,
    )

    logger.info("Prediction sync started: %s -> %s", start_time, latest_open_time)
    _run_with_logged_stdout(
        sync_predictions,
        start_time,
        end_time = latest_open_time,
        asset_id = asset_id,
        logger   = logger,
    )

    logger.info("Database sync complete")
    return SyncResult(start_time, latest_open_time, rows_before, rows_after)


def _run_with_logged_stdout(func, *args, logger: logging.Logger, **kwargs) -> None:
    writer = _LoggerWriter(logger)
    with redirect_stdout(writer):
        func(*args, **kwargs)
    writer.flush()



def _next_open_time(open_time: str) -> str:
    value = pd.to_datetime(open_time, errors="raise") + timedelta(minutes=1)
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _utc_str_to_ms(value: str) -> int:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    dt = datetime.fromisoformat(text)
    dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return int(dt.timestamp() * 1000)


