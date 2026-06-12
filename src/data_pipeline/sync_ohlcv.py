#!/usr/bin/env python3
"""Fetch and sync OHLCV klines from Binance into daily Parquet partitions.

Config-driven and idempotent by open_time — safe to re-run.
"""

import json
import logging
import os
from store.parquet_store import upsert_partition

import pandas as pd
from binance.client import Client

import utils

logger = logging.getLogger(__name__)

_KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]


# %% Sync

def sync_ohlcv(open_time_ms_from: int, asset_id: str | None = None) -> None:
    """Fetch klines from Binance and upsert into daily Parquet partitions.

    Args:
        open_time_ms_from : Start time as epoch milliseconds (UTC).
        asset_id          : Asset key from config/assets.json. Uses default if None.
    """
    # --- load config ---
    db_cfg   = utils.load_asset_config(asset_id)
    env_cfg  = utils.load_env_config()
    data_dir = db_cfg["database"]["data_dir"]
    symbol   = db_cfg["database"]["symbol"]
    market   = db_cfg["database"].get("market", "spot")

    binance_keys = env_cfg["api"]["binance_keys_path"]

    # --- load Binance API keys ---
    with open(binance_keys, encoding="utf-8") as f:
        keys = json.load(f)
    api_key    = keys.get("api_key")    or keys.get("key")
    api_secret = keys.get("api_secret") or keys.get("secret")

    # --- fetch klines (paginated) ---
    os.makedirs(data_dir, exist_ok=True)

    client         = Client(api_key, api_secret)
    server_ms      = int(client.get_server_time()["serverTime"])
    limit          = 1000
    start_ms       = int(open_time_ms_from)
    inserted_total = 0
    batch_count    = 0

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

        # --- build DataFrame ---
        df = pd.DataFrame(rows, columns=_KLINE_COLUMNS)

        df["open_time_ms"]  = pd.to_numeric(df["open_time"],  errors="coerce").astype("int64")  # type: ignore[union-attr]
        df["close_time_ms"] = pd.to_numeric(df["close_time"], errors="coerce").astype("int64")  # type: ignore[union-attr]
        df = df[df["close_time_ms"] < server_ms].copy()
        if df.empty:
            break

        df["open_time"] = df["open_time_ms"].map(utils.ms_to_utc_str)  # type: ignore[union-attr]

        for col in ["open", "high", "low", "close", "volume",
                    "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df[["open_time", "open", "high", "low", "close", "volume",
                 "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]]

        # --- upsert batch into Parquet ---
        assert isinstance(df, pd.DataFrame)
        upsert_partition(data_dir, "ohlcv", df)

        inserted_total += len(df)
        batch_count    += 1

        last_open_ms = int(rows[-1][0])
        if batch_count == 1 or batch_count % 10 == 0 or len(rows) < limit:
            logger.info(
                "OHLCV batches=%d inserted=%d latest=%s",
                batch_count, inserted_total, utils.ms_to_utc_str(last_open_ms),
            )

        next_start = last_open_ms + 60000
        if next_start <= start_ms:
            break

        start_ms = next_start
        if len(rows) < limit:
            break

    if inserted_total == 0:
        logger.info("Nincs uj kline adat")
        return

    logger.info("OK: %d kline szinkronizalva, dataset=ohlcv, data_dir=%s", inserted_total, data_dir)
