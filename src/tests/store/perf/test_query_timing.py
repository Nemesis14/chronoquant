"""Performance benchmarks for store queries on the production database.

All tests measure wall-clock time and assert it stays within acceptable bounds.
Skips automatically when the database is absent.
"""

import time

import duckdb
import pandas as pd
import pytest

pytestmark = pytest.mark.perf


# conn and db_path fixtures come from _tests/store/conftest.py


def _time_ms(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    params: list | None = None,
) -> tuple[float, int]:
    t0   = time.perf_counter()
    rows = conn.execute(sql, params or []).fetchall()
    return (time.perf_counter() - t0) * 1000.0, len(rows)



# %% feat_ohlcv_quant


def test_timing_feat_count(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, _ = _time_ms(conn, "SELECT COUNT(*) FROM feat_ohlcv_quant")
    print(f"\n[perf] feat COUNT(*): {elapsed:.1f} ms")
    assert elapsed < 2000, f"COUNT(*) too slow: {elapsed:.1f} ms"


def test_timing_feat_range_query_7d(conn: duckdb.DuckDBPyConnection) -> None:
    row    = conn.execute("SELECT MAX(open_time) FROM feat_ohlcv_quant").fetchone()
    max_ts = str(row[0]) if row and row[0] else "2026-01-01 00:00:00"
    end    = pd.Timestamp(max_ts)
    start  = (end - pd.Timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    end_s  = end.strftime("%Y-%m-%d %H:%M:%S")

    elapsed, rows = _time_ms(
        conn,
        "SELECT open_time, feat_rsi_14, feat_roc_14 FROM feat_ohlcv_quant"
        " WHERE open_time BETWEEN ? AND ?",
        [start, end_s],
    )
    print(f"[perf] feat range 7d ({rows:,} rows): {elapsed:.1f} ms")
    assert elapsed < 3000, f"Range query 7d too slow: {elapsed:.1f} ms"


def test_timing_feat_range_query_30d(conn: duckdb.DuckDBPyConnection) -> None:
    row    = conn.execute("SELECT MAX(open_time) FROM feat_ohlcv_quant").fetchone()
    max_ts = str(row[0]) if row and row[0] else "2026-01-01 00:00:00"
    end    = pd.Timestamp(max_ts)
    start  = (end - pd.Timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    end_s  = end.strftime("%Y-%m-%d %H:%M:%S")

    elapsed, rows = _time_ms(
        conn,
        "SELECT open_time, feat_rsi_14, feat_roc_14 FROM feat_ohlcv_quant"
        " WHERE open_time BETWEEN ? AND ?",
        [start, end_s],
    )
    print(f"[perf] feat range 30d ({rows:,} rows): {elapsed:.1f} ms")
    assert elapsed < 5000, f"Range query 30d too slow: {elapsed:.1f} ms"


def test_timing_feat_groupby_year(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, rows = _time_ms(
        conn,
        "SELECT DATE_TRUNC('year', open_time) AS yr, COUNT(*) FROM feat_ohlcv_quant"
        " GROUP BY 1 ORDER BY 1",
    )
    print(f"[perf] feat GROUP BY year ({rows} groups): {elapsed:.1f} ms")
    assert elapsed < 3000, f"GROUP BY year too slow: {elapsed:.1f} ms"


def test_timing_feat_groupby_month(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, rows = _time_ms(
        conn,
        "SELECT DATE_TRUNC('month', open_time) AS mo, COUNT(*) FROM feat_ohlcv_quant"
        " GROUP BY 1 ORDER BY 1",
    )
    print(f"[perf] feat GROUP BY month ({rows} groups): {elapsed:.1f} ms")
    assert elapsed < 3000, f"GROUP BY month too slow: {elapsed:.1f} ms"



# %% target


def test_timing_target_count(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, _ = _time_ms(conn, "SELECT COUNT(*) FROM target")
    print(f"[perf] target COUNT(*): {elapsed:.1f} ms")
    assert elapsed < 2000, f"COUNT(*) too slow: {elapsed:.1f} ms"


def test_timing_target_label_groupby(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, _ = _time_ms(
        conn,
        "SELECT trg_l_fw60_q90, trg_s_fw60_q10, COUNT(*) FROM target"
        " GROUP BY 1, 2 ORDER BY 1, 2",
    )
    print(f"[perf] target label GROUP BY: {elapsed:.1f} ms")
    assert elapsed < 3000, f"Label GROUP BY too slow: {elapsed:.1f} ms"


def test_timing_target_range_query_30d(conn: duckdb.DuckDBPyConnection) -> None:
    row    = conn.execute("SELECT MAX(open_time) FROM target").fetchone()
    max_ts = str(row[0]) if row and row[0] else "2026-01-01 00:00:00"
    end    = pd.Timestamp(max_ts)
    start  = (end - pd.Timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    end_s  = end.strftime("%Y-%m-%d %H:%M:%S")

    elapsed, rows = _time_ms(
        conn,
        "SELECT open_time, trg_l_fw60_q90, trg_s_fw60_q10 FROM target"
        " WHERE open_time BETWEEN ? AND ?",
        [start, end_s],
    )
    print(f"[perf] target range 30d ({rows:,} rows): {elapsed:.1f} ms")
    assert elapsed < 3000, f"Target range 30d too slow: {elapsed:.1f} ms"



# %% predictions


def test_timing_predictions_count(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, _ = _time_ms(conn, "SELECT COUNT(*) FROM predictions")
    print(f"\n[perf] predictions COUNT(*): {elapsed:.1f} ms")
    assert elapsed < 2000, f"COUNT(*) too slow: {elapsed:.1f} ms"


def test_timing_predictions_range_query_7d(conn: duckdb.DuckDBPyConnection) -> None:
    row    = conn.execute("SELECT MAX(open_time) FROM predictions").fetchone()
    max_ts = str(row[0]) if row and row[0] else "2026-01-01 00:00:00"
    end    = pd.Timestamp(max_ts)
    start  = (end - pd.Timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    end_s  = end.strftime("%Y-%m-%d %H:%M:%S")

    elapsed, rows = _time_ms(
        conn,
        "SELECT open_time, long_pred, short_pred FROM predictions"
        " WHERE open_time BETWEEN ? AND ?",
        [start, end_s],
    )
    print(f"[perf] predictions range 7d ({rows:,} rows): {elapsed:.1f} ms")
    assert elapsed < 3000, f"Range query 7d too slow: {elapsed:.1f} ms"


def test_timing_predictions_range_query_30d(conn: duckdb.DuckDBPyConnection) -> None:
    row    = conn.execute("SELECT MAX(open_time) FROM predictions").fetchone()
    max_ts = str(row[0]) if row and row[0] else "2026-01-01 00:00:00"
    end    = pd.Timestamp(max_ts)
    start  = (end - pd.Timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    end_s  = end.strftime("%Y-%m-%d %H:%M:%S")

    elapsed, rows = _time_ms(
        conn,
        "SELECT open_time, long_pred, short_pred FROM predictions"
        " WHERE open_time BETWEEN ? AND ?",
        [start, end_s],
    )
    print(f"[perf] predictions range 30d ({rows:,} rows): {elapsed:.1f} ms")
    assert elapsed < 5000, f"Range query 30d too slow: {elapsed:.1f} ms"


def test_timing_predictions_groupby_year(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, rows = _time_ms(
        conn,
        "SELECT DATE_TRUNC('year', open_time) AS yr, COUNT(*) FROM predictions"
        " GROUP BY 1 ORDER BY 1",
    )
    print(f"[perf] predictions GROUP BY year ({rows} groups): {elapsed:.1f} ms")
    assert elapsed < 3000, f"GROUP BY year too slow: {elapsed:.1f} ms"


def test_timing_predictions_groupby_month(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, rows = _time_ms(
        conn,
        "SELECT DATE_TRUNC('month', open_time) AS mo, COUNT(*) FROM predictions"
        " GROUP BY 1 ORDER BY 1",
    )
    print(f"[perf] predictions GROUP BY month ({rows} groups): {elapsed:.1f} ms")
    assert elapsed < 3000, f"GROUP BY month too slow: {elapsed:.1f} ms"
