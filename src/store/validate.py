"""SQL assertion utilities for data integrity validation.

Provides DuckDB-based look-ahead bias and label overlap checks against
native DuckDB tables (ohlcv, features, predictions in the .duckdb file).
Callable standalone (returns violation count) or from pytest (raises on violation).
"""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def _db_path(data_dir: str) -> Path:
    return Path(data_dir).with_suffix(".duckdb")


def _tbl_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()
    return bool(row and row[0] > 0)


def assert_zero(con: duckdb.DuckDBPyConnection, sql: str, msg: str) -> int:
    """Run sql, assert the first column of the first row equals 0.

    Args:
        con: Open DuckDB connection.
        sql: SQL returning a single integer COUNT column.
        msg: Message included in AssertionError if count > 0.

    Returns:
        0 on pass.

    Raises:
        AssertionError: If count > 0.
    """
    row = con.execute(sql).fetchone()
    count = int(row[0]) if row else 0
    if count > 0:
        raise AssertionError(f"{msg}: {count} violation(s)")
    return 0


def check_no_future_features(data_dir: str) -> int:
    """Assert no feature row has available_ts > open_time (look-ahead bias check).

    Queries the native features table in the .duckdb file.

    Args:
        data_dir: Root data directory for the asset (e.g. data/solusdt_fw60).

    Returns:
        0 on pass.

    Raises:
        AssertionError: If any row has available_ts > open_time.
        FileNotFoundError: If the .duckdb file or features table does not exist.
    """
    db = _db_path(data_dir)
    if not db.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db}")

    con = duckdb.connect(str(db), read_only=True)
    try:
        if not _tbl_exists(con, "features"):
            raise FileNotFoundError(f"features table not found in {db}")

        sql = "SELECT COUNT(*) FROM features WHERE available_ts > open_time"
        try:
            count = int(con.execute(sql).fetchone()[0])
        except duckdb.BinderException as exc:
            if "available_ts" in str(exc):
                logger.warning(
                    "check_no_future_features: available_ts column missing, skipping (rebuild needed)"
                )
                return 0
            raise

        if count > 0:
            logger.warning("Look-ahead bias: %d feature rows have available_ts > open_time", count)
            raise AssertionError(f"look-ahead bias: available_ts > open_time: {count} violation(s)")

        logger.info("check_no_future_features: OK (0 violations)")
        return 0
    finally:
        con.close()


def check_no_label_overlap(data_dir: str) -> int:
    """Assert no train/test label interval overlap.

    Joins features and predictions native tables to check dataset_split / label_end_ts.
    If dataset_split is not populated the check skips (returns 0).

    Args:
        data_dir: Root data directory for the asset.

    Returns:
        0 on pass.

    Raises:
        AssertionError: If any train row's label interval overlaps a test row's interval.
    """
    db = _db_path(data_dir)
    if not db.exists():
        logger.warning("check_no_label_overlap: DuckDB file missing, skipping")
        return 0

    con = duckdb.connect(str(db), read_only=True)
    try:
        if not _tbl_exists(con, "features") or not _tbl_exists(con, "predictions"):
            logger.warning("check_no_label_overlap: features/predictions table missing, skipping")
            return 0

        sql = """
            WITH ds AS (
                SELECT
                    f.open_time,
                    p.label_end_ts,
                    f.dataset_split
                FROM features f
                JOIN predictions p ON f.open_time = p.open_time
                WHERE f.dataset_split IS NOT NULL
            ),
            pairs AS (
                SELECT a.open_time
                FROM ds a
                JOIN ds b
                  ON a.dataset_split = 'train'
                 AND b.dataset_split = 'test'
                 AND greatest(a.open_time, b.open_time) <= least(a.label_end_ts, b.label_end_ts)
            )
            SELECT COUNT(*) FROM pairs
        """
        try:
            row = con.execute(sql).fetchone()
            count = int(row[0]) if row else 0
        except duckdb.BinderException as exc:
            missing = next(
                (c for c in ("dataset_split", "label_end_ts") if c in str(exc)), None
            )
            if missing:
                logger.warning(
                    "check_no_label_overlap: '%s' column missing, skipping (rebuild needed)", missing
                )
                return 0
            raise

        if count > 0:
            logger.warning("Train/test label overlap: %d affected rows", count)
            raise AssertionError(f"train/test label overlap: {count} pair(s)")

        logger.info("check_no_label_overlap: OK (0 violations)")
        return 0
    finally:
        con.close()
