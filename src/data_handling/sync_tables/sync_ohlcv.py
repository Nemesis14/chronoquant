#!/usr/bin/env python3
"""Fetch and sync OHLCV klines from Binance into the DuckDB ohlcv table.

Config-driven. OHLCV rows are immutable events keyed by open_time — no business
upsert is performed. Only rows past the stored maximum open_time are written.
"""

import json
import logging
import time

import polars as pl
from binance.client import Client

import utils
from data_handling.store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

logger = logging.getLogger(__name__)

_KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]


# %% Sync

def sync_ohlcv(open_time_ms_from: int, asset_id: str | None = None) -> None:
    """Fetch klines from Binance and append into the DuckDB ohlcv table.

    open_time_ms_from is the requested start, i.e. stored_max_open_time + 1 minute.
    A guard filters any Binance rows with open_time_ms < open_time_ms_from and
    logs a warning — these are stale or duplicate rows that must not be written.

    Args:
        open_time_ms_from : Start time as epoch milliseconds (UTC). Should be
                            stored_max_open_time + 60 000 ms.
        asset_id          : Asset key from config/assets.json. Uses default if None.
    """
    # --- load config ---
    db_cfg   = utils.load_asset_config(asset_id)
    env_cfg  = utils.load_env_config()
    db_path  = db_cfg["database"]["db_path"]
    symbol   = db_cfg["database"]["symbol"]
    market   = db_cfg["database"].get("market", "spot")

    binance_keys = env_cfg["api"]["binance_keys_path"]

    # --- load Binance API keys ---
    with open(binance_keys, encoding="utf-8") as f:
        keys = json.load(f)
    api_key    = keys.get("api_key")    or keys.get("key")
    api_secret = keys.get("api_secret") or keys.get("secret")

    # --- open DuckDB connection ---
    conn = get_connection(db_path)
    ensure_tables(conn)

    # --- fetch klines (paginated) ---
    client         = Client(api_key, api_secret)
    server_ms      = int(client.get_server_time()["serverTime"])
    limit          = 1000
    start_ms       = int(open_time_ms_from)
    inserted_total = 0
    batch_count    = 0
    t_start        = time.monotonic()

    try:
        while True:
            try:
                if market == "futures":
                    rows = client.futures_klines(
                        symbol    = symbol,
                        interval  = Client.KLINE_INTERVAL_1MINUTE,
                        startTime = start_ms,
                        limit     = limit,
                    )
                else:
                    rows = client.get_klines(
                        symbol    = symbol,
                        interval  = Client.KLINE_INTERVAL_1MINUTE,
                        startTime = start_ms,
                        limit     = limit,
                    )
            except Exception:
                logger.exception("Binance klines lehivas sikertelen: symbol=%s start_ms=%d", symbol, start_ms)
                raise

            if not rows:
                break

            # --- build Polars DataFrame (fast type parsing) ---
            pl_df = pl.DataFrame(
                {col: [str(row[i]) for row in rows] for i, col in enumerate(_KLINE_COLUMNS)},
                schema={col: pl.String for col in _KLINE_COLUMNS},
            ).with_columns([
                pl.col("open_time").cast(pl.Int64).alias("open_time_ms"),
                pl.col("close_time").cast(pl.Int64).alias("close_time_ms"),
            ]).filter(pl.col("close_time_ms") < server_ms)

            if pl_df.is_empty():
                break

            # Guard: skip stale rows (open_time_ms < open_time_ms_from)
            stale = pl_df.filter(pl.col("open_time_ms") < open_time_ms_from)
            if stale.height > 0:
                logger.warning(
                    "OHLCV sync guard: %d sor kiszurve (open_time_ms < requested_start %d): %s",
                    stale.height, open_time_ms_from, stale["open_time_ms"].to_list(),
                )
                pl_df = pl_df.filter(pl.col("open_time_ms") >= open_time_ms_from)
            if pl_df.is_empty():
                break

            # Gap check
            first_open_ms = int(pl_df["open_time_ms"][0])
            if first_open_ms > open_time_ms_from and batch_count == 0:
                logger.warning(
                    "OHLCV sync: res detektalt! requested_start=%d, first_row=%d (delta=%d ms)",
                    open_time_ms_from, first_open_ms, first_open_ms - open_time_ms_from,
                )

            numeric_cols = ["open", "high", "low", "close", "volume",
                            "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]
            pl_df = pl_df.with_columns([
                pl.col(c).cast(pl.Float64, strict=False) for c in numeric_cols
            ]).with_columns([
                pl.col("open_time_ms").map_elements(
                    utils.ms_to_utc_str, return_dtype=pl.String
                ).alias("open_time"),
            ]).select(["open_time", "open", "high", "low", "close", "volume",
                       "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"])

            # --- append to DuckDB ohlcv table ---
            appended = insert_ohlcv(conn, pl_df)

            inserted_total += appended
            batch_count    += 1

            last_open_ms = int(rows[-1][0])
            if batch_count == 1 or batch_count % 10 == 0 or len(rows) < limit:
                elapsed = time.monotonic() - t_start
                logger.info(
                    "OHLCV batches=%d appended=%d latest=%s elapsed=%.1fs",
                    batch_count, inserted_total, utils.ms_to_utc_str(last_open_ms), elapsed,
                )

            next_start = last_open_ms + 60000
            if next_start <= start_ms:
                break

            start_ms = next_start
            if len(rows) < limit:
                break

    finally:
        conn.close()

    if inserted_total == 0:
        logger.info("Nincs uj kline adat")
        return

    logger.info("OK: %d kline szinkronizalva, dataset=ohlcv, db_path=%s", inserted_total, db_path)
