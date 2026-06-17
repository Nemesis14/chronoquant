"""Smoke tests for sampling.audit — audit_feature_table with synthetic in-memory DuckDB."""

import duckdb
import pytest

from modeling.quantitative.sampling.audit import _run_audit

pytestmark = pytest.mark.smoke


def _make_conn(n_rows: int = 200) -> duckdb.DuckDBPyConnection:
    """Create in-memory DuckDB with synthetic feat_ohlcv_quant and target tables."""
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE feat_ohlcv_quant AS
        SELECT
            TIMESTAMP '2022-01-01 00:00:00' + (i * INTERVAL '1' MINUTE) AS open_time,
            CAST(i AS DOUBLE) AS feat_close_ret,
            CAST(i * 2 AS DOUBLE) AS feat_vol
        FROM range(0, ?) t(i)
    """, [n_rows])
    conn.execute("""
        CREATE TABLE target AS
        SELECT
            TIMESTAMP '2022-01-01 00:00:00' + (i * INTERVAL '1' MINUTE) AS open_time,
            CASE WHEN i < ? - 10 THEN CAST(i AS DOUBLE) * 0.001 ELSE NULL END AS long_mfe_fw60
        FROM range(0, ?) t(i)
    """, [n_rows, n_rows])
    return conn


def test_audit_returns_dict() -> None:
    conn = _make_conn()
    try:
        result = _run_audit(conn, ["feat_close_ret", "feat_vol"], "long_mfe_fw60")
    finally:
        conn.close()
    assert isinstance(result, dict)


def test_audit_required_keys() -> None:
    conn = _make_conn()
    try:
        result = _run_audit(conn, ["feat_close_ret", "feat_vol"], "long_mfe_fw60")
    finally:
        conn.close()
    required = {
        "data_start_safe", "data_end_safe", "row_count", "unique_timestamps",
        "duplicate_count", "target_null_count", "feature_null_summary",
        "gap_count", "gap_minutes_total",
    }
    assert required <= set(result.keys())


def test_audit_row_count() -> None:
    conn = _make_conn(n_rows=50)
    try:
        result = _run_audit(conn, ["feat_close_ret", "feat_vol"], "long_mfe_fw60")
    finally:
        conn.close()
    assert result["row_count"] == 50
    assert result["unique_timestamps"] == 50
    assert result["duplicate_count"] == 0


def test_audit_no_gaps_in_contiguous_data() -> None:
    conn = _make_conn(n_rows=100)
    try:
        result = _run_audit(conn, ["feat_close_ret", "feat_vol"], "long_mfe_fw60")
    finally:
        conn.close()
    assert result["gap_count"] == 0


def test_audit_feature_null_summary_keys() -> None:
    conn = _make_conn()
    try:
        result = _run_audit(conn, ["feat_close_ret", "feat_vol"], "long_mfe_fw60")
    finally:
        conn.close()
    assert "feat_close_ret" in result["feature_null_summary"]
    assert "feat_vol" in result["feature_null_summary"]


def test_audit_target_null_count_positive() -> None:
    conn = _make_conn(n_rows=50)
    try:
        result = _run_audit(conn, ["feat_close_ret", "feat_vol"], "long_mfe_fw60")
    finally:
        conn.close()
    assert result["target_null_count"] == 10
