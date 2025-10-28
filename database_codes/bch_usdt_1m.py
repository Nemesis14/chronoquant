# =============================================================================
# BCHUSDT 1m sync utilities
# =============================================================================
import os
import sqlite3
import json
import pandas as pd
from binance.client import Client
from datetime import datetime, timezone
import utils as utils

# =============================================================================
# Create / reset database and table
# =============================================================================
def create_bchusdt_1m(config_path=None):
    # ---------------------------------------------------------------------
    # Purpose:
    #   Drop (if exists) and recreate the sqlite database file and the base table.
    #
    # Behavior:
    #   - Reads db_path and table_name from config via utils._load_config(config_path)
    #   - If the DB file exists it is removed (full reset)
    #   - Creates the table with columns:
    #       open_time_ms INTEGER, open_time TEXT, open/high/low/close/volume REAL
    #     Composite PRIMARY KEY (open_time_ms, open_time)
    # ---------------------------------------------------------------------
    cfg         = utils._load_config(config_path)
    db_path     = cfg["database"]["db_path"]
    table_name  = cfg.get("database", {}).get("dev_data", {}).get("table_name", "bchusdt_1m")

    # remove existing DB file if present (full reset)
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"removed existing database file: {db_path}")
    else:
        print(f"no existing database file to remove: {db_path}")

    # create database and table
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                open_time_ms  INTEGER,
                open_time     TEXT,
                open          REAL,
                high          REAL,
                low           REAL,
                close         REAL,
                volume        REAL,
                PRIMARY KEY (open_time_ms, open_time),
                UNIQUE (open_time_ms),
                UNIQUE (open_time)
            )
        """)
        conn.commit()

    # final status
    print(f"created table '{table_name}' in database: {db_path}")

# =============================================================================
# Sync function: fetch klines from Binance and upsert into SQLite
# -----------------------------------------------------------------------------
# NOTE:
# - This function no longer performs timezone shifting on the klines.
#   The open_time column will be directly derived (formatted) from open_time_ms.
# - The caller (main) provides start_str and end_str as local formatted strings.
# - The Binance API keys path and config path are fixed inside this module.
# -----------------------------------------------------------------------------
def sync_bchusdt_1m(start_str, end_str):
    # ---------------------------------------------------------------------
    # Purpose:
    #   Sync 1-minute BCHUSDT klines from Binance between start_str and end_str.
    #
    # Inputs:
    #   - start_str: formatted local time string "YYYY-MM-DD HH:MM:SS" or ISO-like.
    #   - end_str:   formatted local time string "YYYY-MM-DD HH:MM:SS" or ISO-like.
    #
    # Behavior:
    #   - Reads DB path and table name from config via utils._load_config()
    #   - Loads Binance API keys from a fixed path inside the function
    #   - Converts provided start/end strings to epoch milliseconds (no manual tz shifts)
    #   - Fetches klines, sets open_time by formatting open_time_ms directly
    #   - Inserts rows into sqlite using INSERT OR IGNORE to avoid duplicates
    # ---------------------------------------------------------------------
    cfg        = utils._load_config()
    db_path    = cfg["database"]["db_path"]
    table_name = cfg.get("database", {}).get("dev_data", {}).get("table_name", "bchusdt_1m")
    symbol     = cfg.get("database", {}).get("dev_data", {}).get("symbol", "BCHUSDT")
    interval   = Client.KLINE_INTERVAL_1MINUTE

    # Fixed keys path (not a function parameter)
    keys_path = "C:/connection/binance_keys.json"

    # ---------------------------------------------------------------------
    # Load Binance keys and instantiate client
    # ---------------------------------------------------------------------
    if not os.path.exists(keys_path):
        raise FileNotFoundError(f"Binance keys file not found: {keys_path}")
    with open(keys_path, "r", encoding="utf-8") as f:
        keys = json.load(f)

    api_key    = keys.get("api_key") or keys.get("key")
    api_secret = keys.get("api_secret") or keys.get("secret")
    if not api_key or not api_secret:
        raise RuntimeError("Binance API key/secret missing in keys file")

    client = Client(api_key, api_secret)

    # ---------------------------------------------------------------------
    # Parse provided start/end strings into epoch milliseconds.
    # Note: naive datetimes are interpreted as system-local time here.
    # ---------------------------------------------------------------------
    def _str_to_epoch_ms(s):
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        # If naive, datetime.timestamp() treats it as system local time.
        return int(dt.timestamp() * 1000)

    try:
        start_ms = _str_to_epoch_ms(start_str)
        end_ms   = _str_to_epoch_ms(end_str)
    except Exception as e:
        print(f"Error parsing start/end times: {e}")
        return


    # ---------------------------------------------------------------------
    # Fetch historical klines from Binance
    # ---------------------------------------------------------------------
    try:
        raw_data = client.get_historical_klines(symbol, interval, start_ms, end_ms)
    except Exception as e:
        print(f"Binance API error while fetching klines: {e}")
        return

    if not raw_data:
        print("No data returned by Binance.")
        return

    # ---------------------------------------------------------------------
    # Build DataFrame from raw_data
    # ---------------------------------------------------------------------
    df = pd.DataFrame(
        raw_data,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ]
    )

    # original timestamp in ms (as provided by Binance)
    df["open_time_ms"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")

    # Derive open_time directly from open_time_ms (no manual tz shift).
    # Use UTC-based conversion to human readable string; this keeps the
    # timestamp consistent with open_time_ms (same instant expressed).
    df["open_time"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")

    # coerce numeric columns to float (allow NaN)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # keep only relevant columns in correct order
    df = df[["open_time_ms", "open_time", "open", "high", "low", "close", "volume"]]

    # prepare records for bulk insert
    records = list(df.itertuples(index=False, name=None))

    # ---------------------------------------------------------------------
    # Insert into sqlite with INSERT OR IGNORE to avoid duplicates
    # ---------------------------------------------------------------------
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        before_changes = conn.total_changes
        cursor.executemany(
            f"""
            INSERT OR IGNORE INTO {table_name}
            (open_time_ms, open_time, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            records
        )
        conn.commit()
        after_changes = conn.total_changes
        inserted = after_changes - before_changes

    # print summary
    print(f"Attempted to insert rows: {len(records)}")
    print(f"Inserted rows (this run): {inserted}")
    if not df.empty:
        print(f"Range inserted (source frame): {df['open_time'].min()} -> {df['open_time'].max()}")