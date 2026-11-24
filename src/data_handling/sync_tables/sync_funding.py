"""Fetch settled Binance USDT-M funding rates into the live funding_rate table.

Source: ``GET fapi/v1/fundingRate`` (python-binance ``futures_funding_rate``),
one row per settlement.  Incremental and idempotent: fetching starts after the
stored maximum ``funding_ts``; rows already stored are skipped.

Pipeline position (epic_074 / t939)
-----------------------------------
sync_ohlcv  →  sync_funding (worker: only when a settlement is due)

The settlement interval is NOT assumed to be 8h (SOL had 2h/4h phases): the
"due" check uses the gap between the last two stored settlements (min 1h).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import cast

import polars as pl
from binance.client import Client

import utils
from data_handling.store.duckdb_store import ensure_tables, get_connection, insert_funding_rate

logger = logging.getLogger(__name__)

_PAGE_LIMIT        = 1000
_DEFAULT_INTERVAL  = timedelta(hours=8)
_MIN_INTERVAL      = timedelta(hours=1)


# %% Helpers


def next_due(last_two: list[datetime]) -> datetime | None:
    """Expected next settlement time from the last stored settlements (newest first).

    Args:
        last_two : Up to two latest funding_ts values, newest first (naive UTC).

    Returns:
        Next expected settlement (naive UTC), or None when nothing is stored.
    """
    if not last_two:
        return None
    interval = _DEFAULT_INTERVAL
    if len(last_two) >= 2:
        interval = max(last_two[0] - last_two[1], _MIN_INTERVAL)
    return last_two[0] + interval


def _client(asset_id: str | None) -> Client:
    env_cfg = utils.load_env_config()
    with open(env_cfg["api"]["binance_keys_path"], encoding="utf-8") as f:
        keys = json.load(f)
    return Client(keys.get("api_key") or keys.get("key"), keys.get("api_secret") or keys.get("secret"))


def _rows_to_frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame({
        "funding_ts"  : [datetime.fromtimestamp(int(r["fundingTime"]) / 1000, tz=UTC).replace(tzinfo=None) for r in rows],
        "funding_rate": [float(r["fundingRate"]) for r in rows],
        "mark_price"  : [float(r["markPrice"]) if r.get("markPrice") not in (None, "") else None for r in rows],
    }, schema={"funding_ts": pl.Datetime("us"), "funding_rate": pl.Float64, "mark_price": pl.Float64})


# %% Public API


def sync_funding(
    asset_id    : str | None = None,
    start_time  : str | None = None,
    only_if_due : bool = False,
    now         : datetime | None = None,
) -> int:
    """Append new settled funding rates for a futures asset.

    Args:
        asset_id    : Asset key from config/assets.json; uses default if None.
        start_time  : UTC ``YYYY-MM-DD HH:MM:SS`` start when the table is empty
                      (default: earliest OHLCV open_time).  Ignored otherwise.
        only_if_due : Worker mode — skip the Binance call (no network) until the
                      next settlement is expected.
        now         : Override of the current UTC time (tests).

    Returns:
        Number of rows inserted (0 when skipped / nothing new / not a futures asset).
    """
    db_cfg  = utils.load_asset_config(asset_id)["database"]
    if db_cfg.get("market", "spot") != "futures":
        logger.info("sync_funding kihagyva: nem futures asset (%s)", db_cfg.get("asset_id"))
        return 0
    db_path = db_cfg["db_path"]
    symbol  = db_cfg["symbol"]
    now_utc = (now or datetime.now(UTC)).replace(tzinfo=None)

    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        last_two = [
            r[0] for r in conn.execute(
                "SELECT funding_ts FROM funding_rate ORDER BY funding_ts DESC LIMIT 2"
            ).fetchall()
        ]
        if last_two:
            due = next_due(last_two)
            if only_if_due and due is not None and now_utc < due:
                logger.debug("sync_funding: nincs esedekes elszamolas (kovetkezo=%s)", due)
                return 0
            start_ms = utils.utc_str_to_ms(str(last_two[0])[:19]) + 1
        else:
            if start_time is None:
                row = conn.execute("SELECT MIN(open_time) FROM ohlcv").fetchone()
                start_time = str(row[0])[:19] if row and row[0] else utils.INITIAL_SYNC_START
            start_ms = utils.utc_str_to_ms(start_time)

        client   = _client(asset_id)
        inserted = 0
        while True:
            rows = cast(
                "list[dict]",
                client.futures_funding_rate(symbol=symbol, startTime=start_ms, limit=_PAGE_LIMIT),
            )
            if not rows:
                break
            inserted += insert_funding_rate(conn, _rows_to_frame(rows))
            last_ms = int(rows[-1]["fundingTime"])
            if len(rows) < _PAGE_LIMIT or last_ms + 1 <= start_ms:
                break
            start_ms = last_ms + 1
    finally:
        conn.close()

    if inserted:
        logger.info("OK: %d funding sor irva, symbol=%s, db_path=%s", inserted, symbol, db_path)
    return inserted
