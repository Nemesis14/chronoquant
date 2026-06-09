# =============================================================================
# Database maintenance workflows
# =============================================================================
# Purpose:
#  - Provide reusable database maintenance operations for scripts and tests
#  - Rebuild derived tables from existing OHLCV data
#  - Update live prediction signal columns
# =============================================================================

from datetime import datetime, timedelta

import utils
from db.table_ops import sqlite_connect, table_columns, table_exists
from data_pipeline.sync_features import sync_features
from data_pipeline.sync_predictions import sync_predictions


# =============================================================================
# drop_table(db_path: str, table_name: str) -> None
# =============================================================================
# Purpose:
#  - Drop a table before a clean rebuild
# Parameters:
#  - db_path: SQLite database path
#  - table_name: table to drop
# =============================================================================
def drop_table(db_path: str, table_name: str) -> None:
    with sqlite_connect(db_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
    print(f"Dropped table '{table_name}'")


# =============================================================================
# update_prediction_signals(db_path: str, table_name: str) -> None
# =============================================================================
# Purpose:
#  - Add signal column if missing
#  - Recompute LONG/SHORT/NEUTRAL signal values from the generic live prediction
# Parameters:
#  - db_path: SQLite database path
#  - table_name: predictions table
# =============================================================================
def update_prediction_signals(
    db_path: str,
    table_name: str,
    asset_id: str | None = None,
) -> None:
    model_cfg       = utils.load_models_config()
    predictions_cfg = utils.load_predictions_config()
    _, model_meta   = utils.live_model_meta(model_cfg, asset_id=asset_id)
    target_name     = model_meta["target_name"]
    direction = utils.target_direction_from_name(target_name)
    threshold = utils.signal_probability_threshold(predictions_cfg)
    signal_value = "LONG" if direction == "long" else "SHORT"
    live_cols = utils.live_prediction_columns()
    prediction_col = live_cols["prediction"]
    columns = table_columns(db_path, table_name)

    with sqlite_connect(db_path) as conn:
        if "signal" not in columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN signal TEXT")

        conn.execute(
            f"""
            UPDATE {table_name}
            SET signal = CASE
                WHEN {prediction_col} >= ? THEN ?
                ELSE 'NEUTRAL'
            END
            """,
            (threshold, signal_value),
        )
        conn.commit()

    print(f"Updated signal column in '{table_name}' using {prediction_col} >= {threshold}")


# =============================================================================
# print_table_check(db_path: str, table_name: str) -> None
# =============================================================================
# Purpose:
#  - Print row count and open_time duplicate check for a table
# Parameters:
#  - db_path: SQLite database path
#  - table_name: table to validate
# =============================================================================
def print_table_check(db_path: str, table_name: str) -> None:
    with sqlite_connect(db_path) as conn:
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        unique_count = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT open_time FROM {table_name} GROUP BY open_time)"
        ).fetchone()[0]
        min_time, max_time = conn.execute(
            f"SELECT MIN(open_time), MAX(open_time) FROM {table_name}"
        ).fetchone()

    print(f"{table_name}")
    print(f"  rows:               {row_count}")
    print(f"  unique open_time:   {unique_count}")
    print(f"  duplicate rows:     {row_count - unique_count}")
    print(f"  range:              {min_time} -> {max_time}")


# =============================================================================
# rebuild_derived_tables(...) -> None
# =============================================================================
# Purpose:
#  - Rebuild FEATURES and PREDICTIONS from existing OHLCV data
# Parameters:
#  - start: start time, "YYYY-MM-DD HH:MM:SS"
#  - end: optional end time, "YYYY-MM-DD HH:MM:SS"
#  - drop: whether to drop derived tables first
#  - features_only: rebuild only FEATURES
#  - predictions_only: rebuild only PREDICTIONS
#  - asset_id: optional asset id from config/assets.json
# =============================================================================
# _chunk_date_ranges(start, end, chunk_months) -> list[tuple[str, str]]
# =============================================================================
# Purpose:
#  - Split a date range into sequential monthly chunks to limit peak memory
# =============================================================================
def _chunk_date_ranges(
    start: str,
    end: str | None,
    chunk_months: int,
) -> list[tuple[str, str | None]]:
    end_dt    = datetime.fromisoformat(end) if end else datetime.utcnow()
    start_dt  = datetime.fromisoformat(start)
    chunks    = []
    chunk_s   = start_dt

    while chunk_s < end_dt:
        # Advance by chunk_months months
        month  = chunk_s.month - 1 + chunk_months
        year   = chunk_s.year + month // 12
        month  = month % 12 + 1
        chunk_e = chunk_s.replace(year=year, month=month) - timedelta(seconds=1)
        if chunk_e >= end_dt:
            chunks.append((chunk_s.strftime("%Y-%m-%d %H:%M:%S"), end))
            break
        chunks.append((chunk_s.strftime("%Y-%m-%d %H:%M:%S"), chunk_e.strftime("%Y-%m-%d %H:%M:%S")))
        chunk_s = chunk_e + timedelta(seconds=1)

    return chunks


# =============================================================================
def rebuild_derived_tables(
    start: str,
    end: str | None = None,
    drop: bool = False,
    features_only: bool = False,
    predictions_only: bool = False,
    asset_id: str | None = None,
    chunk_months: int = 6,
) -> None:
    db_cfg     = utils.load_asset_config(asset_id)["database"]
    db_path    = db_cfg["db_path"]
    table_feat = db_cfg["tables"]["features"]
    table_pred = db_cfg["tables"]["predictions"]

    print("=" * 80)
    print("REBUILD DERIVED TABLES")
    print("=" * 80)
    print(f"DB:    {db_path}")
    print(f"Start: {start}")
    print(f"End:   {end or '(latest)'}")
    print(f"Drop:  {drop}")
    print("=" * 80)

    if drop:
        if not predictions_only:
            drop_table(db_path, table_feat)
        if not features_only:
            drop_table(db_path, table_pred)

    if not predictions_only:
        chunks = _chunk_date_ranges(start, end, chunk_months)
        print(f"INFO: Rebuilding features in {len(chunks)} chunk(s) of {chunk_months} months each")
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            print(f"  Chunk {i}/{len(chunks)}: {chunk_start} -> {chunk_end or '(latest)'}", flush=True)
            sync_features(chunk_start, end_time=chunk_end, asset_id=asset_id)
        print_table_check(db_path, table_feat)

    if not features_only:
        sync_predictions(start, end_time=end, asset_id=asset_id)
        update_prediction_signals(db_path, table_pred, asset_id=asset_id)
        print_table_check(db_path, table_pred)

    print("=" * 80)
    print("Rebuild complete")
    print("=" * 80)


# =============================================================================
# _upgrade_to_unique_index(db_path: str, table_name: str) -> None
# =============================================================================
# Purpose:
#  - Replace the existing non-unique open_time index with a UNIQUE one
#  - No-op if the UNIQUE index is already in place
# =============================================================================
def _upgrade_to_unique_index(db_path: str, table_name: str) -> None:
    index_name = f"idx_{table_name}_open_time"
    with sqlite_connect(db_path) as conn:
        for _seq, name, unique, _origin, _partial in conn.execute(
            f'PRAGMA index_list("{table_name}")'
        ).fetchall():
            if name == index_name:
                if unique:
                    print(f"  '{table_name}': UNIQUE index already in place")
                    return
                break
        conn.execute(f'DROP INDEX IF EXISTS "{index_name}"')
        conn.execute(
            f'CREATE UNIQUE INDEX "{index_name}" ON "{table_name}" (open_time)'
        )
        conn.commit()
    print(f"  '{table_name}': upgraded to UNIQUE index on open_time")


# =============================================================================
# fix_ohlcv_duplicates(asset_id: str | None = None) -> None
# =============================================================================
# Purpose:
#  - Remove duplicate open_time rows from the OHLCV table (keep newest rowid)
#  - Delete features/predictions rows from the earliest affected time onwards
#  - Upgrade all three tables to UNIQUE open_time index
#  - Rebuild derived tables for the affected time range
# =============================================================================
def fix_ohlcv_duplicates(asset_id: str | None = None) -> None:
    db_cfg      = utils.load_asset_config(asset_id)["database"]
    db_path     = db_cfg["db_path"]
    table_ohlcv = db_cfg["tables"]["ohlcv"]
    table_feat  = db_cfg["tables"]["features"]
    table_pred  = db_cfg["tables"]["predictions"]

    print("=" * 60)
    print("FIX OHLCV DUPLICATES")
    print("=" * 60)
    print(f"DB:    {db_path}")
    print(f"Table: {table_ohlcv}")
    print("=" * 60)

    with sqlite_connect(db_path) as conn:
        dup_rows = conn.execute(
            f"""
            SELECT open_time, COUNT(*) AS cnt
            FROM {table_ohlcv}
            GROUP BY open_time
            HAVING cnt > 1
            ORDER BY open_time
            """
        ).fetchall()

    min_dup_time = None
    if dup_rows:
        min_dup_time = dup_rows[0][0]
        print(f"Found {len(dup_rows)} duplicate open_time(s)")
        print(f"  Earliest: {min_dup_time}")
        print(f"  Latest:   {dup_rows[-1][0]}")

        with sqlite_connect(db_path) as conn:
            deleted = conn.execute(
                f"""
                DELETE FROM {table_ohlcv}
                WHERE rowid NOT IN (
                    SELECT MAX(rowid) FROM {table_ohlcv} GROUP BY open_time
                )
                """
            ).rowcount
            conn.commit()
        print(f"Deleted {deleted} duplicate row(s) from '{table_ohlcv}' (kept newest)")

        for tbl in [table_feat, table_pred]:
            if not table_exists(db_path, tbl):
                print(f"  '{tbl}': table does not exist, skipping")
                continue
            with sqlite_connect(db_path) as conn:
                deleted = conn.execute(
                    f"DELETE FROM {tbl} WHERE open_time >= ?",
                    (min_dup_time,),
                ).rowcount
                conn.commit()
            print(f"Deleted {deleted} row(s) from '{tbl}' (>= {min_dup_time})")
    else:
        print(f"No duplicates found in '{table_ohlcv}'")

    print("\nUpgrading indexes to UNIQUE...")
    for tbl in [table_ohlcv, table_feat, table_pred]:
        if table_exists(db_path, tbl):
            _upgrade_to_unique_index(db_path, tbl)

    if min_dup_time:
        print(f"\nRebuilding derived tables from {min_dup_time}...")
        rebuild_derived_tables(start=min_dup_time, asset_id=asset_id)

    print("=" * 60)
    print("Done")
    print("=" * 60)

