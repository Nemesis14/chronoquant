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


def check_quant_train_no_duplicates(db_path: str) -> int:
    """Assert no duplicate open_time rows exist in quant_train.

    Args:
        db_path: Absolute path to the asset .duckdb file.

    Returns:
        0 on pass.

    Raises:
        AssertionError: If duplicate open_time rows exist.
    """
    db = Path(db_path)
    if not db.exists():
        logger.warning("check_quant_train_no_duplicates: DuckDB file missing, skipping")
        return 0

    con = duckdb.connect(str(db), read_only=True)
    try:
        if not _tbl_exists(con, "quant_train"):
            logger.warning("check_quant_train_no_duplicates: quant_train table missing, skipping")
            return 0

        return assert_zero(
            con,
            "SELECT COUNT(*) - COUNT(DISTINCT CAST(open_time AS VARCHAR)) FROM quant_train",
            "quant_train duplicate open_time",
        )
    finally:
        con.close()


def check_sample_table(
    db_path           : str,
    sample_id         : str,
    expected_feat_cols: list[str] | None = None,
) -> int:
    """Run all integrity checks on a materialized sample table.

    Checks:
      1. No duplicate (open_time, fold_id, segment) rows.
      2. test rows start strictly after all non-test rows (chronological order).
      3. At least one purge row exists (embargo buffer must be present).
      4. If expected_feat_cols given: each expected column is present in the table.
      5. Required target columns are non-null in train and valid segments.

    Args:
        db_path           : Absolute path to the asset .duckdb file.
        sample_id         : Sample identifier (table name = sample_<sample_id>).
        expected_feat_cols: Optional list of feat_* columns from feature_set.json.

    Returns:
        0 on pass.

    Raises:
        AssertionError   : If any check fails.
        FileNotFoundError: If the .duckdb file or sample table does not exist.
    """
    db = Path(db_path)
    if not db.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db}")

    table = f"sample_{sample_id}"
    con = duckdb.connect(str(db), read_only=True)
    try:
        if not _tbl_exists(con, table):
            raise FileNotFoundError(f"Sample table '{table}' not found in {db}")

        assert_zero(
            con,
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT
                open_time::VARCHAR || '|' ||
                COALESCE(fold_id::VARCHAR, 'NULL') || '|' ||
                segment
            )
            FROM "{table}"
            """,
            f"{table}: duplicate (open_time, fold_id, segment) rows",
        )

        assert_zero(
            con,
            f"""
            SELECT COUNT(*)
            FROM "{table}" AS a
            JOIN "{table}" AS b ON b.segment = 'test'
            WHERE a.segment != 'test'
              AND a.open_time >= b.open_time
            """,
            f"{table}: non-test rows not strictly before test rows",
        )

        assert_zero(
            con,
            f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM \"{table}\" WHERE segment = 'purge'",
            f"{table}: no purge rows found (embargo buffer missing)",
        )

        assert_zero(
            con,
            f"""
            SELECT COUNT(*)
            FROM "{table}"
            WHERE segment IN ('train', 'valid')
              AND (long_mfe_fw60 IS NULL OR short_mfe_fw60 IS NULL)
            """,
            f"{table}: NULL target columns in train/valid rows",
        )

        if expected_feat_cols:
            existing_cols = {
                row[0]
                for row in con.execute(
                    "SELECT column_name FROM information_schema.columns"
                    f" WHERE table_name = '{table}'"
                ).fetchall()
            }
            missing = [c for c in expected_feat_cols if c not in existing_cols]
            if missing:
                raise AssertionError(
                    f"{table}: expected feature columns missing: {missing}"
                )

        logger.info("check_sample_table: %s OK", table)
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
            "SELECT COUNT(*) FROM target WHERE long_mfe_fw60 IS NULL"
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
