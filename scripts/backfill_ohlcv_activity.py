#!/usr/bin/env python3
# =============================================================================
# Backfill Binance kline activity fields into existing OHLCV table rows
# =============================================================================
# Purpose:
#  - Re-fetch Binance klines for rows where quote_volume IS NULL
#  - UPDATE solusdt_1m (or any asset) with quote_volume, trades,
#    taker_buy_base, taker_buy_quote
#  - Idempotent: only updates rows where the columns are NULL
# =============================================================================

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd
from binance.client import Client

import utils
from db.table_ops import ensure_table_columns, sqlite_connect


# =============================================================================
# _fetch_and_update_batch(...) -> int
# =============================================================================
# Purpose:
#  - Fetch one batch of klines from Binance for start_ms..end_ms
#  - UPDATE matching rows in the OHLCV table with activity field values
# Parameters:
#  - client: Binance Client instance
#  - db_path: SQLite database path
#  - table_name: OHLCV table name
#  - start_ms: epoch ms start
#  - limit: number of klines to fetch
# Returns: last open_time epoch ms fetched, 0 if no rows returned
# =============================================================================
def _fetch_and_update_batch(
    client:     Client,
    db_path:    str,
    table_name: str,
    symbol:     str,
    start_ms:   int,
    limit:      int = 1000,
) -> tuple[int, int]:
    rows = client.get_klines(
        symbol    = symbol,
        interval  = Client.KLINE_INTERVAL_1MINUTE,
        startTime = start_ms,
        limit     = limit,
    )
    if not rows:
        return 0, 0

    df = pd.DataFrame(
        rows,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades",
            "taker_buy_base", "taker_buy_quote", "ignore",
        ],
    )
    df["open_time_ms"] = pd.to_numeric(df["open_time"], errors="coerce").astype("int64")
    df["open_time"]    = df["open_time_ms"].apply(lambda ms: utils.ms_to_utc_str(int(ms)))

    for col in ["quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    updated = 0
    with sqlite_connect(db_path) as conn:
        for _, row in df.iterrows():
            result = conn.execute(
                f"""
                UPDATE {table_name}
                SET quote_volume     = ?,
                    trades           = ?,
                    taker_buy_base   = ?,
                    taker_buy_quote  = ?
                WHERE open_time = ?
                  AND (quote_volume IS NULL OR trades IS NULL)
                """,
                (
                    row["quote_volume"],
                    row["trades"],
                    row["taker_buy_base"],
                    row["taker_buy_quote"],
                    row["open_time"],
                ),
            )
            updated += result.rowcount
        conn.commit()

    last_ms = int(rows[-1][0])
    return last_ms, updated


# =============================================================================
# backfill_ohlcv_activity(asset_id: str | None) -> None
# =============================================================================
# Purpose:
#  - Find earliest NULL activity row and iterate forward until all rows updated
#  - Print progress every 50 batches
# =============================================================================
def backfill_ohlcv_activity(asset_id: str | None = None) -> None:
    # -------------------------------------------------------------------------
    # Load configuration
    # -------------------------------------------------------------------------
    db_cfg     = utils.load_asset_config(asset_id)
    env_cfg    = utils.load_env_config()
    db_path    = db_cfg["database"]["db_path"]
    symbol     = db_cfg["database"]["symbol"]
    table_name = db_cfg["database"]["tables"]["ohlcv"]

    api_key, api_secret = "", ""
    try:
        binance_keys = env_cfg.get("api", {}).get("binance_keys_path")
        if binance_keys and os.path.exists(binance_keys):
            with open(binance_keys, "r", encoding="utf-8") as f:
                keys = json.load(f)
            api_key    = keys.get("api_key") or keys.get("key") or ""
            api_secret = keys.get("api_secret") or keys.get("secret") or ""
    except Exception:
        pass  # public klines endpoint works without keys

    # -------------------------------------------------------------------------
    # Ensure activity columns exist in the table
    # -------------------------------------------------------------------------
    with sqlite_connect(db_path) as conn:
        existing_cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table_name})")]

    for col in ["quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]:
        if col not in existing_cols:
            with sqlite_connect(db_path) as conn:
                conn.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" REAL')
                conn.commit()
            print(f"INFO: Added column {col} to {table_name}")

    # -------------------------------------------------------------------------
    # Find null count and earliest NULL row
    # -------------------------------------------------------------------------
    with sqlite_connect(db_path) as conn:
        null_count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE quote_volume IS NULL"
        ).fetchone()[0]

    if null_count == 0:
        print("INFO: All rows already have activity fields. Nothing to backfill.")
        return

    print(f"INFO: {null_count:,} rows need backfill for {table_name}")

    with sqlite_connect(db_path) as conn:
        earliest_null = conn.execute(
            f"SELECT MIN(open_time) FROM {table_name} WHERE quote_volume IS NULL"
        ).fetchone()[0]

    start_ms = utils.utc_str_to_ms(earliest_null)
    print(f"INFO: Starting backfill from {earliest_null}")

    # -------------------------------------------------------------------------
    # Fetch and update batches
    # -------------------------------------------------------------------------
    client        = Client(api_key, api_secret)
    limit         = 1000
    total_updated = 0
    batch_count   = 0

    while True:
        last_ms, updated = _fetch_and_update_batch(
            client     = client,
            db_path    = db_path,
            table_name = table_name,
            symbol     = symbol,
            start_ms   = start_ms,
            limit      = limit,
        )

        if last_ms == 0:
            break

        total_updated += updated
        batch_count   += 1
        next_start     = last_ms + 60000

        if batch_count % 50 == 0 or batch_count == 1:
            latest_str = utils.ms_to_utc_str(last_ms)
            print(
                f"INFO: batch={batch_count}, updated={total_updated:,}, latest={latest_str}",
                flush=True,
            )

        if next_start <= start_ms:
            break
        start_ms = next_start

        # Stop when Binance returns less than a full batch (caught up to live)
        # but continue until we've covered all NULL rows
        with sqlite_connect(db_path) as conn:
            remaining = conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE quote_volume IS NULL"
            ).fetchone()[0]
        if remaining == 0:
            break

    print(f"OK: Backfill complete. Updated {total_updated:,} rows in '{table_name}'")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill kline activity fields (quote_volume, trades, taker_buy_*) into the OHLCV table.",
    )
    parser.add_argument(
        "--asset-id",
        default = None,
        help    = "Asset ID from config/assets.json (e.g. solusdt_fw60); omit for default",
    )
    args = parser.parse_args()
    backfill_ohlcv_activity(asset_id=args.asset_id)


if __name__ == "__main__":
    main()
