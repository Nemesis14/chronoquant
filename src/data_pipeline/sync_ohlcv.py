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
from db.table_ops import ensure_table_columns, sqlite_connect, table_exists


def _create_ohlcv_table_if_not_exists(db_path: str, table_name: str, df: pd.DataFrame) -> None:
    if table_exists(db_path, table_name):
        return
    col_defs = []
    for col in df.columns:
        if df[col].dtype.kind == "f":
            sql_type = "REAL"
        elif df[col].dtype.kind in ("i", "u"):
            sql_type = "INTEGER"
        else:
            sql_type = "TEXT"
        col_defs.append(f'"{col}" {sql_type}')
    ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)})'
    with sqlite_connect(db_path) as conn:
        conn.execute(ddl)
        conn.commit()


def _upsert_ohlcv(conn, table_name: str, df: "pd.DataFrame") -> int:
    cols          = ", ".join(f'"{c}"' for c in df.columns)
    placeholders  = ", ".join("?" for _ in df.columns)
    update_cols   = [c for c in df.columns if c != "open_time"]
    update_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in update_cols)
    stmt          = (
        f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders}) '
        f'ON CONFLICT("open_time") DO UPDATE SET {update_clause}'
    )
    cursor       = conn.executemany(stmt, df.itertuples(index=False, name=None))
    conn.commit()
    return cursor.rowcount


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
    market       = db_cfg["database"].get("market", "spot")
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

    client       = Client(api_key, api_secret)
    server_ms    = int(client.get_server_time()["serverTime"])
    limit        = 1000
    start_ms     = int(open_time_ms_from)
    inserted_total = 0
    batch_count = 0

    while True:
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

        df["open_time_ms"]  = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
        df["close_time_ms"] = pd.to_numeric(df["close_time"], errors="coerce").astype("int64")
        df = df[df["close_time_ms"] < server_ms].copy()
        if df.empty:
            break

        df["open_time"] = df["open_time_ms"].apply(lambda ms: utils.ms_to_utc_str(int(ms)))

        for col in ["open", "high", "low", "close", "volume",
                    "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df[["open_time", "open", "high", "low", "close", "volume",
                 "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]]
        _create_ohlcv_table_if_not_exists(db_path, table_name, df)
        ensure_table_columns(db_path, table_name, df)

        with sqlite_connect(db_path) as conn:
            _upsert_ohlcv(conn, table_name, df)

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


