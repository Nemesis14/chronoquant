# =============================================================================
# data_sync.py - Synchronizes local SQLite database with Binance data
# =============================================================================

import os
import json
import sqlite3
import pandas as pd
from binance.client import Client
from datetime import datetime, timezone, timedelta

# =============================================================================
# Load API keys from JSON file
# =============================================================================
keys_path = "C:/connection/binance_keys.json"
with open(keys_path, "r") as f:
    keys       = json.load(f)
API_KEY       = keys["api_key"]
API_SECRET    = keys["api_secret"]

# =============================================================================
# Initialize Binance client
# =============================================================================
client        = Client(API_KEY, API_SECRET)

# =============================================================================
# Database and table parameters
# =============================================================================
db_path       = "data/bchusdt_data.db"
table         = "bchusdt_1m"
symbol        = "BCHUSDT"
interval      = Client.KLINE_INTERVAL_1MINUTE

# =============================================================================
# Function to sync data from Binance to SQLite
# =============================================================================
def sync_data():
    """
    Synchronizes the local SQLite database with Binance data.
    """

    # -------------------------------------------------------------------------
    # Determine start_ms from the database
    # -------------------------------------------------------------------------
    conn       = sqlite3.connect(db_path)
    cursor     = conn.cursor()
    cursor.execute(f"SELECT MAX(open_time_ms) FROM {table}")
    row        = cursor.fetchone()
    conn.close()

    if row is None or row[0] is None:
        # Fallback to 2017-01-01 00:00:00 UTC in milliseconds
        start_ms = int(datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    else:
        # Next minute after the last stored candle
        start_ms = int(row[0]) + 60_000

    # -------------------------------------------------------------------------
    # Get Binance server time to determine end_ms
    # -------------------------------------------------------------------------
    server_ms  = client.get_server_time()["serverTime"]
    end_ms     = server_ms - (server_ms % 60_000)  # Floor to full minute

    if start_ms >= end_ms:
        print("No new data to sync.")
        return

    # -------------------------------------------------------------------------
    # Fetch historical klines from Binance
    # -------------------------------------------------------------------------
    raw_data   = client.get_historical_klines(symbol, interval, start_ms, end_ms)

    if not raw_data:
        print("No data returned by Binance.")
        return

    # -------------------------------------------------------------------------
    # Process raw data into a DataFrame
    # -------------------------------------------------------------------------
    df = pd.DataFrame(
        raw_data,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ]
    )

    # Add open_time_ms as original Binance timestamp for PK
    df["open_time_ms"] = df["open_time"].astype("int64")

    # Convert open_time to local time (UTC+2) and round to minute precision
    dt_utc             = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    dt_local           = (dt_utc + pd.Timedelta(hours=2)).dt.floor("min")
    df["open_time"]    = dt_local.dt.strftime("%Y-%m-%d %H:%M")

    # Convert numeric columns to float (allow NaN if any errors)
    for column in ["open", "high", "low", "close", "volume"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # Retain relevant columns only
    df = df[["open_time_ms", "open_time", "open", "high", "low", "close", "volume"]]

    # -------------------------------------------------------------------------
    # Insert processed data into SQLite with upsert behavior
    # -------------------------------------------------------------------------
    conn       = sqlite3.connect(db_path)
    cursor     = conn.cursor()
    records    = list(df.itertuples(index=False, name=None))

    cursor.executemany(
        f"""
        INSERT OR IGNORE INTO {table}
        (open_time_ms, open_time, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        records
    )
    conn.commit()
    conn.close()

    # -------------------------------------------------------------------------
    # Print summary of the sync process
    # -------------------------------------------------------------------------
    print(f"Inserted rows: {len(records)}")
    print(f"Range inserted: {df['open_time'].min()} -> {df['open_time'].max()}")