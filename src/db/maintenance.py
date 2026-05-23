# =============================================================================
# Database maintenance workflows
# =============================================================================
# Purpose:
#  - Provide reusable database maintenance operations for scripts and tests
#  - Rebuild derived tables from existing OHLCV data
#  - Update spread-based prediction signal columns
# =============================================================================

import sqlite3

import utils
from db.table_ops import table_columns
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
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
    print(f"Dropped table '{table_name}'")


# =============================================================================
# update_prediction_signals(db_path: str, table_name: str) -> None
# =============================================================================
# Purpose:
#  - Add spread and signal columns if missing
#  - Recompute spread-based LONG/SHORT/NEUTRAL signal values
# Parameters:
#  - db_path: SQLite database path
#  - table_name: predictions table
# =============================================================================
def update_prediction_signals(db_path: str, table_name: str) -> None:
    model_cfg = utils.load_models_config()
    long_col, short_col = utils.long_short_prediction_columns(model_cfg)
    long_cutoff, short_cutoff = utils.signal_cutoffs_from_config(model_cfg)
    columns = table_columns(db_path, table_name)

    with sqlite3.connect(db_path) as conn:
        if "spread" not in columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN spread REAL")

        if "signal" not in columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN signal TEXT")

        conn.execute(
            f"""
            UPDATE {table_name}
            SET spread = {long_col} - {short_col}
            """,
        )
        conn.execute(
            f"""
            UPDATE {table_name}
            SET signal = CASE
                WHEN spread >= ? THEN 'LONG'
                WHEN spread <= ? THEN 'SHORT'
                ELSE 'NEUTRAL'
            END
            """,
            (long_cutoff, short_cutoff),
        )
        conn.commit()

    print(f"Updated spread and signal columns in '{table_name}'")


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
    with sqlite3.connect(db_path) as conn:
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
# =============================================================================
def rebuild_derived_tables(
    start: str,
    end: str | None = None,
    drop: bool = False,
    features_only: bool = False,
    predictions_only: bool = False,
) -> None:
    db_cfg     = utils.load_db_config()["database"]
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
        sync_features(start, end_time=end)
        print_table_check(db_path, table_feat)

    if not features_only:
        sync_predictions(start, end_time=end)
        update_prediction_signals(db_path, table_pred)
        print_table_check(db_path, table_pred)

    print("=" * 80)
    print("Rebuild complete")
    print("=" * 80)

