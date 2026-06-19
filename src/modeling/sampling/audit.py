"""Feature table auditor — determines safe data boundaries and data quality metrics.

All queries use DuckDB native SQL.  No Polars, no pandas.
"""

import logging

import duckdb

from data_handling.store.duckdb_query import dataset_columns

logger = logging.getLogger(__name__)


# %% Public API


def audit_feature_table(db_path: str, target_col: str) -> dict:
    """Audit feat_ohlcv_quant and target tables to determine safe sampling boundaries.

    Args:
        db_path    : Absolute path to the asset .duckdb file.
        target_col : Target column to check for non-null end boundary
                     (e.g. 'long_mfe_fw60').

    Returns:
        Dict with keys:
            data_start_safe      : First open_time where all feat_* columns are NOT NULL.
            data_end_safe        : Last open_time where target_col IS NOT NULL.
            row_count            : Total row count in feat_ohlcv_quant.
            unique_timestamps    : Count of distinct open_time values.
            duplicate_count      : row_count - unique_timestamps.
            target_null_count    : Rows in target where target_col IS NULL.
            feature_null_summary : {col: null_rate} for every feat_* column.
            gap_count            : Number of 1-minute gaps larger than 1 minute.
            gap_minutes_total    : Total missing minutes across all gaps.

    Raises:
        ValueError: If the feature table is missing or has no feat_* columns.
    """
    # --- resolve feat_* columns via shared helper ---
    all_cols  = dataset_columns(db_path, "feat_ohlcv_quant")
    feat_cols = [c for c in all_cols if c.startswith("feat_")]

    if not feat_cols:
        raise ValueError(
            f"No feat_* columns found in feat_ohlcv_quant: {db_path}"
        )

    logger.debug("audit_feature_table: %d feat_ columns", len(feat_cols))

    conn = duckdb.connect(db_path, read_only=True)
    try:
        return _run_audit(conn, feat_cols, target_col)
    finally:
        conn.close()


# %% Internal


def _run_audit(
    conn      : duckdb.DuckDBPyConnection,
    feat_cols : list[str],
    target_col: str,
) -> dict:
    """Execute all audit queries on an open connection."""

    # --- row / uniqueness stats ---
    row = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT open_time) FROM feat_ohlcv_quant"
    ).fetchone()
    row_count         = int(row[0]) if row else 0
    unique_timestamps = int(row[1]) if row else 0
    duplicate_count   = row_count - unique_timestamps

    # --- data_start_safe: first row where ALL feat_* are NOT NULL ---
    null_where = " AND ".join(f'"{c}" IS NOT NULL' for c in feat_cols)
    row = conn.execute(
        f"SELECT MIN(open_time) FROM feat_ohlcv_quant WHERE {null_where}"
    ).fetchone()
    data_start_safe = str(row[0]) if row and row[0] is not None else None

    # --- data_end_safe: last row in target where target_col IS NOT NULL ---
    row = conn.execute(
        f'SELECT MAX(open_time) FROM target WHERE "{target_col}" IS NOT NULL'
    ).fetchone()
    data_end_safe = str(row[0]) if row and row[0] is not None else None

    # --- target null count ---
    row = conn.execute(
        f'SELECT COUNT(*) FROM target WHERE "{target_col}" IS NULL'
    ).fetchone()
    target_null_count = int(row[0]) if row else 0

    # --- feature null summary (single aggregation) ---
    null_rate_exprs = ", ".join(
        f'AVG(CASE WHEN "{c}" IS NULL THEN 1.0 ELSE 0.0 END) AS "{c}"'
        for c in feat_cols
    )
    rel = conn.execute(f"SELECT {null_rate_exprs} FROM feat_ohlcv_quant")
    null_row = rel.fetchone()
    if null_row:
        col_names            = [d[0] for d in rel.description]
        feature_null_summary = {
            c: float(v) for c, v in zip(col_names, null_row, strict=False) if v is not None
        }
    else:
        feature_null_summary = {}

    # --- gap detection via LAG window function ---
    gap_sql = """
        WITH lagged AS (
            SELECT
                open_time,
                LAG(open_time) OVER (ORDER BY open_time) AS prev_time
            FROM feat_ohlcv_quant
        )
        SELECT
            COUNT(*)                                                 AS gap_count,
            COALESCE(SUM(DATEDIFF('minute', prev_time, open_time) - 1), 0) AS gap_minutes_total
        FROM lagged
        WHERE prev_time IS NOT NULL
          AND DATEDIFF('minute', prev_time, open_time) > 1
    """
    row           = conn.execute(gap_sql).fetchone()
    gap_count         = int(row[0]) if row else 0
    gap_minutes_total = int(row[1]) if row else 0

    return {
        "data_start_safe"     : data_start_safe,
        "data_end_safe"       : data_end_safe,
        "row_count"           : row_count,
        "unique_timestamps"   : unique_timestamps,
        "duplicate_count"     : duplicate_count,
        "target_null_count"   : target_null_count,
        "feature_null_summary": feature_null_summary,
        "gap_count"           : gap_count,
        "gap_minutes_total"   : gap_minutes_total,
    }
