"""Informational DuckDB statistics smoke report tests.

Verifies that the validation report handles missing databases and collects
basic count, range, and timing metrics for populated native DuckDB tables.
"""

from pathlib import Path

import pandas as pd
import pytest

from store.duckdb_stats import collect_duckdb_stats_report, format_duckdb_stats_report
from store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

pytestmark = pytest.mark.duckdb_stats


def test_stats_report_skips_missing_database(tmp_path: Path) -> None:
    """Verify missing databases produce SKIP rows instead of failures."""
    data_dir = str(tmp_path / "missing_asset")

    report = collect_duckdb_stats_report(data_dir)

    assert all(table.status == "SKIP_DB_MISSING" for table in report.tables)
    assert format_duckdb_stats_report(report)


def test_stats_report_collects_counts_ranges_and_timings(tmp_path: Path) -> None:
    """Verify populated OHLCV data yields counts and timing smoke metrics."""
    data_dir = str(tmp_path / "asset_data")
    conn     = get_connection(data_dir)
    try:
        ensure_tables(conn)
        rows = pd.DataFrame(
            {
                "open_time"       : pd.date_range("2024-01-01", periods=48, freq="h"),
                "open"            : [100.0] * 48,
                "high"            : [101.0] * 48,
                "low"             : [99.0] * 48,
                "close"           : [100.5] * 48,
                "volume"          : [10.0] * 48,
                "quote_volume"    : [1000.0] * 48,
                "trades"          : [1] * 48,
                "taker_buy_base"  : [5.0] * 48,
                "taker_buy_quote" : [500.0] * 48,
            }
        )
        insert_ohlcv(conn, rows)
    finally:
        conn.close()

    report = collect_duckdb_stats_report(data_dir)
    rendered = format_duckdb_stats_report(report)
    ohlcv = next(table for table in report.tables if table.table == "ohlcv")

    assert ohlcv.status == "OK"
    assert ohlcv.row_count == 48
    assert any(metric.label == "range_ohlcv_1d" for metric in report.timings)
    assert any(metric.label == "groupby_ohlcv_year" for metric in report.timings)
    assert "informational" in rendered
