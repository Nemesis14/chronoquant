#!/usr/bin/env python3
# =============================================================================
# Fetch and sync OHLCV data from Binance
# =============================================================================
# Purpose:
#  - Query Binance klines API
#  - Insert raw OHLCV data into database table
#  - Config-driven, minimal approach
# =============================================================================

import json
import os
import pandas as pd
from binance.client import Client

import utils
from db.table_ops import drop_existing_open_times, sqlite_connect

# =============================================================================
# sync_ohlcv(open_time_ms_from: int, asset_id: str | None = None) -> None
# =============================================================================
# Purpose:
#  - Fetch klines from Binance starting at open_time_ms_from (epoch ms)
#  - Build DataFrame with normalized columns
#  - Insert rows into database OHLCV table
# Parameters:
#  - open_time_ms_from: epoch milliseconds (UTC)
#  - asset_id: optional asset id from config/assets.json
# =============================================================================
def sync_ohlcv(open_time_ms_from: int, asset_id: str | None = None) -> None:
    # -------------------------------------------------------------------------
    # Load configuration
    # -------------------------------------------------------------------------
    db_cfg       = utils.load_asset_config(asset_id)
    env_cfg      = utils.load_env_config()
    db_path      = db_cfg["database"]["db_path"]
    symbol       = db_cfg["database"]["symbol"]
    table_name   = db_cfg["database"]["tables"]["ohlcv"]
    binance_keys = env_cfg["api"]["binance_keys_path"]

    # -------------------------------------------------------------------------
    # Load Binance API keys
    # -------------------------------------------------------------------------
    with open(binance_keys, "r", encoding="utf-8") as f:
        keys = json.load(f)
    api_key    = keys.get("api_key") or keys.get("key")
    api_secret = keys.get("api_secret") or keys.get("secret")

    # -------------------------------------------------------------------------
    # Fetch klines from Binance (paginated)
    # -------------------------------------------------------------------------
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    client   = Client(api_key, api_secret)
    limit    = 1000
    start_ms = int(open_time_ms_from)
    inserted_total = 0
    batch_count = 0

    while True:
        rows = client.get_klines(
            symbol    = symbol,
            interval  = Client.KLINE_INTERVAL_1MINUTE,
            startTime = start_ms,
            limit     = limit,
        )

        if not rows:
            break

        # ---------------------------------------------------------------------
        # Build DataFrame and normalize columns
        # ---------------------------------------------------------------------
        df = pd.DataFrame(
            rows,
            columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades",
                "taker_buy_base", "taker_buy_quote", "ignore",
            ],
        )

        df["open_time_ms"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
        df["open_time"] = df["open_time_ms"].apply(lambda ms: utils.ms_to_utc_str(int(ms)))

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df[["open_time", "open", "high", "low", "close", "volume"]]
        df = drop_existing_open_times(df, db_path, table_name)
        if df.empty:
            last_open_ms = int(rows[-1][0])
            start_ms = last_open_ms + 60000
            if len(rows) < limit:
                break
            continue

        with sqlite_connect(db_path) as conn:
            df.to_sql(table_name, conn, index=False, if_exists="append")

        inserted_total += len(df)
        batch_count += 1

        last_open_ms = int(rows[-1][0])
        if batch_count == 1 or batch_count % 10 == 0 or len(rows) < limit:
            print(
                "OHLCV progress: "
                f"batches={batch_count}, inserted={inserted_total}, "
                f"latest={utils.ms_to_utc_str(last_open_ms)}",
                flush=True,
            )

        next_start = last_open_ms + 60000
        if next_start <= start_ms:
            break

        start_ms = next_start
        if len(rows) < limit:
            break

    if inserted_total == 0:
        print("No new klines to insert")
        return

    print(f"Synced {inserted_total} klines into '{table_name}'")



