#!/usr/bin/env python3
# =============================================================================
# BCHUSDT 1m sync utility (very small, config-driven, pandas append)
# =============================================================================
# - Single public function: sync_bchusdt_1m_from_ms(open_time_ms_from)
# - Reads config via utils._load_config(), builds DataFrame from Binance klines,
#   then appends to the configured base table using pandas.to_sql(if_exists='append').
# - Function accepts only open_time_ms_from (epoch ms UTC). Minimal and dense.
# =============================================================================

import pandas as pd
import sqlite3
from binance.client import Client

import utils as utils   # _load_config(), ms_to_utc_str(), now_utc_ms()


def sync_bchusdt_1m_from_ms(open_time_ms_from: int) -> None:
    # -------------------------------------------------------------------------
    # Validate input and load configuration
    # -------------------------------------------------------------------------
    if open_time_ms_from is None:
        raise ValueError("open_time_ms_from must be provided (epoch milliseconds)")

    cfg     = utils._load_config()
    db_cfg  = cfg.get("database", {}) or {}
    dev_cfg = db_cfg.get("dev_data", {}) or {}
    ex_cfg  = cfg.get("exchange", {}) or {}

    DB_PATH    = db_cfg.get("db_path")
    TABLE_NAME = dev_cfg.get("table_name") or db_cfg.get("table_name")

    SYMBOL   = ex_cfg.get("symbol")   or dev_cfg.get("symbol")
    INTERVAL = ex_cfg.get("interval") or dev_cfg.get("interval")

    API_KEY    = ex_cfg.get("api_key")
    API_SECRET = ex_cfg.get("api_secret")

    # -------------------------------------------------------------------------
    # Instantiate Binance client
    # -------------------------------------------------------------------------
    client = Client(API_KEY, API_SECRET)

    # -------------------------------------------------------------------------
    # Fetch klines starting from provided ms (no explicit batching here)
    # -------------------------------------------------------------------------
    start_ms = int(open_time_ms_from)
    rows = client.get_klines(
        symbol    = SYMBOL,
        interval  = INTERVAL,
        startTime = start_ms
    )

    # nothing to append
    if not rows:
        print("No klines returned from Binance for the requested start.")
        return

    # -------------------------------------------------------------------------
    # Build DataFrame and normalize columns
    # -------------------------------------------------------------------------
    df = pd.DataFrame(
        rows,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ]
    )

    df["open_time_ms"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
    df["open_time"]    = df["open_time_ms"].apply(lambda ms: utils.ms_to_utc_str(int(ms)))

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[["open_time_ms", "open_time", "open", "high", "low", "close", "volume"]]

    # -------------------------------------------------------------------------
    # Append to DB (pandas will create the table if missing)
    # -------------------------------------------------------------------------
    conn = sqlite3.connect(DB_PATH)
    try:
        df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    finally:
        conn.close()

    # -------------------------------------------------------------------------
    # Summary print
    # -------------------------------------------------------------------------
    print(f"Appended {len(df)} rows into '{TABLE_NAME}' in DB '{DB_PATH}' for start_ms {open_time_ms_from}.")
