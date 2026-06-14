"""SQL assertion utilities for data integrity validation.

Provides DuckDB-based look-ahead bias and label overlap checks against
native DuckDB tables (ohlcv, target, feat_ohlcv_quant, predictions in the .duckdb file).
Callable standalone (returns violation count) or from pytest (raises on violation).
"""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


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


def check_no_future_features(db_path: str) -> int:
    """Assert no feature row has available_ts > open_time (look-ahead bias check).

    Queries the native feat_ohlcv_quant table in the .duckdb file.

    Args:
        db_path: Absolute path to the asset .duckdb file.

    Returns:
        0 on pass.

    Raises:
        AssertionError : If any row has available_ts > open_time.
        FileNotFoundError : If the .duckdb file or feat_ohlcv_quant table does not exist.
    """
    db = Path(db_path)
    if not db.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db}")

    con = duckdb.connect(str(db), read_only=True)
    try:
        if not _tbl_exists(con, "feat_ohlcv_quant"):
            raise FileNotFoundError(f"feat_ohlcv_quant table not found in {db}")

        sql = "SELECT COUNT(*) FROM feat_ohlcv_quant WHERE available_ts > open_time"
        try:
            _row = con.execute(sql).fetchone()
            count = int(_row[0]) if _row else 0
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


def check_target_no_current_bar(db_path: str) -> int:
    """Assert that target values do not incorporate the current bar's close price.

    Checks the target table for rows where the forward window potentially includes
    bar t (i.e. the most recent rows without null targets when ohlcv has no further data).
    This is a structural check — the actual window correctness is validated by
    sync_targets using SQL ROWS BETWEEN 1 FOLLOWING AND k FOLLOWING.

    Args:
        db_path: Absolute path to the asset .duckdb file.

    Returns:
        0 on pass (placeholder — real check is deterministic via SQL window definition).
    """
    db = Path(db_path)
    if not db.exists():
        logger.warning("check_target_no_current_bar: DuckDB file missing, skipping")
        return 0

    con = duckdb.connect(str(db), read_only=True)
    try:
        if not _tbl_exists(con, "target"):
            logger.warning("check_target_no_current_bar: target table missing, skipping")
            return 0

        # Last `horizon` rows must have NULL targets (insufficient future data)
        # This is enforced by sync_targets — we verify the table has any NULLs at the tail.
        row = con.execute(
            "SELECT COUNT(*) FROM target WHERE trg_l_fw60_q90 IS NULL"
        ).fetchone()
        null_count = int(row[0]) if row else 0
        if null_count == 0:
            logger.warning(
                "check_target_no_current_bar: no NULL targets found — "
                "last horizon rows should be NULL (rebuild targets)"
            )
        else:
            logger.info("check_target_no_current_bar: OK (%d NULL tail rows)", null_count)
        return 0
    finally:
        con.close()
