#!/usr/bin/env python3
# =============================================================================
# BCHUSDT 1m sync utility (minimal, config-driven)
# - Single public function: sync_bchusdt_1m_from_ms(open_time_ms_from)
# - Reads only the config keys that exist in the provided config (database.dev_data.table_name, database.db_path)
# - Derives SYMBOL and INTERVAL from the dev table_name (e.g. "bchusdt_1m" -> "BCHUSDT", "1m")
# - No 'or' fallbacks, no exchange section lookup.
# =============================================================================

import pandas as pd
import sqlite3
import os, json
from binance.client import Client

import utils  # _load_config(), ms_to_utc_str()

def sync_bchusdt_1m(open_time_ms_from: int) -> None:
    # -------------------------------------------------------------------------
    # Input validation
    # -------------------------------------------------------------------------
    # Ensure the caller provided the epoch milliseconds to start from.
    if open_time_ms_from is None:
        raise ValueError("open_time_ms_from must be provided (epoch milliseconds)")

    config     = utils._load_config()
    DB_PATH    = config["database"]["db_path"]
    TABLE_NAME = config.get("database", {}).get("dev_data", {}).get("table_name")
    SYMBOL     = config.get("database", {}).get("dev_data", {}).get("symbol")
    INTERVAL   = Client.KLINE_INTERVAL_1MINUTE

    # Fixed keys path (not a function parameter)
    keys_path = "C:/connection/binance_keys.json"

    # ---------------------------------------------------------------------
    # Load Binance keys and instantiate client
    # ---------------------------------------------------------------------
    if not os.path.exists(keys_path):
        raise FileNotFoundError(f"Binance keys file not found: {keys_path}")
    with open(keys_path, "r", encoding="utf-8") as f:
        keys = json.load(f)

    API_KEY    = keys.get("api_key")    or keys.get("key")
    API_SECRET = keys.get("api_secret") or keys.get("secret")
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Binance API key/secret missing in keys file")

    # -------------------------------------------------------------------------
    # Instantiate Binance client
    # -------------------------------------------------------------------------
    # Create the client (public endpoints work without keys)
    client = Client(API_KEY, API_SECRET)

    # -------------------------------------------------------------------------
    # Fetch klines from Binance
    # -------------------------------------------------------------------------
    # Request klines starting from the given millisecond timestamp.
    start_ms = int(open_time_ms_from)
    rows     = client.get_klines(symbol=SYMBOL, interval=INTERVAL, startTime=start_ms)

    # If Binance returned nothing, exit early.
    if not rows:
        print("No klines returned from Binance for the requested start.")
        return

    # -------------------------------------------------------------------------
    # Build DataFrame and normalize columns
    # -------------------------------------------------------------------------
    # Construct a DataFrame from the kline rows and coerce types.
    df = pd.DataFrame(
        rows,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ]
    )

    # Convert open time to ms and human-readable string.
    df["open_time_ms"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
    df["open_time"]    = df["open_time_ms"].apply(lambda ms: utils.ms_to_utc_str(int(ms)))

    # Convert numeric columns to proper dtypes.
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Keep only the columns we store.
    df = df[["open_time_ms", "open_time", "open", "high", "low", "close", "volume"]]

    # -------------------------------------------------------------------------
    # Append to SQLite DB
    # -------------------------------------------------------------------------
    # Use pandas to_sql to append rows; the table will be created if missing.
    conn = sqlite3.connect(DB_PATH)
    try:
        df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
    finally:
        conn.close()

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    # Print a brief summary of what was appended.
    print(f"Appended {len(df)} rows into '{TABLE_NAME}'")