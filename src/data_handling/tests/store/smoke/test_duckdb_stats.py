"""DuckDB statistics smoke tests.

Verifies that the stats report is callable, handles missing databases gracefully,
and collects basic metrics on populated data.
"""

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from data_handling.store.duckdb_stats import collect_duckdb_stats_report, format_duckdb_stats_report
from data_handling.store.duckdb_store import (
    ensure_tables,
    get_connection,
    insert_feat_ohlcv_quant,
    insert_ohlcv,
    insert_target,
    rebuild_quant_train,
)

pytestmark = pytest.mark.smoke


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
        rows = pl.DataFrame(
            {
                "open_time"       : [datetime(2024, 1, 1) + timedelta(hours=i) for i in range(48)],
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

    report   = collect_duckdb_stats_report(data_dir)
    rendered = format_duckdb_stats_report(report)
    ohlcv    = next(table for table in report.tables if table.table == "ohlcv")

    assert ohlcv.status == "OK"
    assert ohlcv.row_count == 48
    assert any(metric.label == "range_ohlcv_1d" for metric in report.timings)
    assert any(metric.label == "groupby_ohlcv_year" for metric in report.timings)
    assert "informational" in rendered


def test_stats_report_quant_train_null_ratios_include_targets(tmp_path: Path) -> None:
    """quant_train stats must report null_ratios for target columns."""
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        insert_feat_ohlcv_quant(
            conn,
            pl.DataFrame({
                "open_time":   [datetime(2024, 1, 1, 0, i) for i in range(3)],
                "close":       [100.0, 101.0, 102.0],
                "feat_rsi_14": [40.0, 45.0, 50.0],
            }),
        )
        insert_target(
            conn,
            pl.DataFrame({
                "open_time":         [datetime(2024, 1, 1, 0, i) for i in range(3)],
                "close":             [100.0, 101.0, 102.0],
                "fw60_close":        [101.0, 102.0, 103.0],
                "fw60_max":          [102.0, 103.0, 104.0],
                "fw60_min":          [99.0, 100.0, 101.0],
                "fw60_close_ret":    [0.01, 0.01, 0.01],
                "fw60_close_logret": [0.01, 0.01, 0.01],
                "fw60_max_ratio":    [0.02, 0.02, 0.02],
                "fw60_min_ratio":    [-0.01, -0.01, -0.01],
                "long_mfe_fw60":     [0.02, 0.02, 0.02],
                "short_mfe_fw60":    [-0.01, -0.01, -0.01],
            }),
        )
        rebuild_quant_train(conn)
    finally:
        conn.close()

    report = collect_duckdb_stats_report(db_path)
    qt = next((t for t in report.tables if t.table == "quant_train"), None)

    assert qt is not None
    assert qt.status == "OK"
    assert qt.row_count == 3
    assert qt.dup_count == 0
    assert "long_mfe_fw60" in qt.null_ratios
    assert "short_mfe_fw60" in qt.null_ratios
    assert qt.null_ratios["long_mfe_fw60"] == 0.0
    assert qt.null_ratios["short_mfe_fw60"] == 0.0
    rendered = format_duckdb_stats_report(report)
    assert "dups=0" in rendered
    assert "quant_train" in rendered
