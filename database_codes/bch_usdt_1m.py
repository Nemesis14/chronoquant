# =============================================================================
# drop and recreate the sqlite database and bchusdt_1m table (composite pk)
# =============================================================================

import os
import sqlite3
import json
import pandas as pd
from binance.client import Client
from datetime       import datetime, timezone
import utils as utils

# =============================================================================
# Create / reset database and table
# =============================================================================
# Logic:
#   - read database path and dev table name from config via utils._load_config()
#   - if database file exists: remove it to fully reset (drop)
#   - create a new sqlite file and a table with:
#       * columns: open_time_ms INTEGER, open_time TEXT, open/high/low/close/volume REAL
#       * composite PRIMARY KEY (open_time_ms, open_time)
#       * UNIQUE constraints on open_time_ms and open_time individually
#   - print status messages about removal/creation
# =============================================================================

def create_bchusdt_1m(config_path=None):
    # load configuration
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
# =============================================================================
# Logic:
#   - read db path, table name and optional symbol from config via utils._load_config()
#   - load Binance API keys from keys_path and create Client
#   - determine start_ms:
#       * if no rows in table -> fallback to 2017-01-01 UTC in ms
#       * else -> last open_time_ms + 60_000 (next minute)
#   - determine end_ms using Binance server time, floored to full minute
#   - if start_ms >= end_ms -> no new data
#   - fetch historical klines between start_ms and end_ms
#   - transform into DataFrame:
#       * add open_time_ms as integer
#       * convert to local time (UTC+2), floor to minute, format "YYYY-MM-DD HH:MM"
#       * coerce numeric columns
#       * keep relevant columns in desired order
#   - insert using INSERT OR IGNORE to avoid duplicates
#   - print inserted counts and range (if any)
# =============================================================================

def sync_bchusdt_1m(config_path=None, keys_path="C:/connection/binance_keys.json"):
    # =============================================================================
    #  load configuration
    # =============================================================================
    cfg         = utils._load_config(config_path)
    db_path     = cfg["database"]["db_path"]
    table_name  = cfg.get("database", {}).get("dev_data", {}).get("table_name", "bchusdt_1m")
    symbol      = cfg.get("database", {}).get("dev_data", {}).get("symbol", "BCHUSDT")
    interval    = Client.KLINE_INTERVAL_1MINUTE

    # load Binance API keys and instantiate client
    with open(keys_path, "r", encoding="utf-8") as f:
        keys       = json.load(f)
    api_key     = keys["api_key"]
    api_secret  = keys["api_secret"]
    client      = Client(api_key, api_secret)

    # fetch last stored timestamp from DB
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(open_time_ms) FROM {table_name}")
        row = cursor.fetchone()

    # compute start_ms
    if row is None or row[0] is None:
        # fallback start date (UTC -> ms)
        start_ms = int(datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    else:
        # next minute after last stored candle
        start_ms = int(row[0]) + 60_000

    # determine end_ms from Binance server time (floor to full minute)
    server_ms   = client.get_server_time()["serverTime"]
    end_ms      = server_ms - (server_ms % 60_000)

    # nothing to fetch?
    if start_ms >= end_ms:
        print("No new data to sync.")
        return

    # =============================================================================
    #  fetch historical klines from Binance
    # =============================================================================
    raw_data    = client.get_historical_klines(symbol, interval, start_ms, end_ms)
    if not raw_data:
        print("No data returned by Binance.")
        return

    # build DataFrame from raw_data
    df = pd.DataFrame(
        raw_data,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ]
    )

    # original timestamp in ms
    df["open_time_ms"] = df["open_time"].astype("int64")

    # convert times: UTC -> local (UTC+2), floor to minute, format
    dt_utc          = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    dt_local        = (dt_utc + pd.Timedelta(hours=2)).dt.floor("min")
    df["open_time"] = dt_local.dt.strftime("%Y-%m-%d %H:%M")

    # coerce numeric columns to float (allow NaN)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col]    = pd.to_numeric(df[col], errors="coerce")

    # keep only relevant columns in correct order
    df = df[["open_time_ms", "open_time", "open", "high", "low", "close", "volume"]]

    # prepare records for bulk insert
    records     = list(df.itertuples(index=False, name=None))

    # =============================================================================
    #  insert into sqlite with INSERT OR IGNORE to avoid duplicates
    # =============================================================================
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            f"""
            INSERT OR IGNORE INTO {table_name}
            (open_time_ms, open_time, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            records
        )
        conn.commit()
        inserted = conn.total_changes

    # print summary
    print(f"Attempted to insert rows: {len(records)}")
    print(f"SQLite reported total changes in this connection: {inserted}")
    if not df.empty:
        print(f"Range inserted (source frame): {df['open_time'].min()} -> {df['open_time'].max()}")

