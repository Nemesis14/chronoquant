"""Performance benchmarks for store queries on the production database.

All tests measure wall-clock time and assert it stays within acceptable bounds.
Skips automatically when the database is absent or a table is missing.
"""

import shutil
import tempfile
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from database.store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

pytestmark = pytest.mark.perf


# conn and db_path fixtures come from src/database/tests/store/conftest.py


def _time_ms(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    params: list | None = None,
) -> tuple[float, int]:
    t0   = time.perf_counter()
    rows = conn.execute(sql, params or []).fetchall()
    return (time.perf_counter() - t0) * 1000.0, len(rows)


# %% ohlcv


def test_timing_ohlcv_count(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, _ = _time_ms(conn, "SELECT COUNT(*) FROM ohlcv")
    print(f"\n[perf] ohlcv COUNT(*): {elapsed:.1f} ms")
    assert elapsed < 2000, f"COUNT(*) too slow: {elapsed:.1f} ms"


def test_timing_ohlcv_range_query_7d(conn: duckdb.DuckDBPyConnection) -> None:
    row    = conn.execute("SELECT MAX(open_time) FROM ohlcv").fetchone()
    max_ts = str(row[0]) if row and row[0] else "2026-01-01 00:00:00"
    end    = pd.Timestamp(max_ts)
    start  = (end - pd.Timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    end_s  = end.strftime("%Y-%m-%d %H:%M:%S")

    elapsed, rows = _time_ms(
        conn,
        "SELECT open_time, open, high, low, close, volume FROM ohlcv"
        " WHERE open_time BETWEEN ? AND ?",
        [start, end_s],
    )
    print(f"[perf] ohlcv range 7d ({rows:,} rows): {elapsed:.1f} ms")
    assert elapsed < 3000, f"Range query 7d too slow: {elapsed:.1f} ms"


def test_timing_ohlcv_daily_aggregation(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, rows = _time_ms(
        conn,
        """SELECT
            DATE_TRUNC('day', open_time) AS day,
            MIN(low)   AS day_low,
            MAX(high)  AS day_high,
            LAST(close ORDER BY open_time) AS day_close,
            SUM(volume) AS day_volume
        FROM ohlcv
        GROUP BY 1
        ORDER BY 1""",
    )
    print(f"[perf] ohlcv daily agg ({rows} days): {elapsed:.1f} ms")
    assert elapsed < 5000, f"Daily aggregation too slow: {elapsed:.1f} ms"


def test_timing_ohlcv_rolling_sma60(conn: duckdb.DuckDBPyConnection) -> None:
    elapsed, rows = _time_ms(
        conn,
        """SELECT
            open_time,
            AVG(close)    OVER (ORDER BY open_time ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS sma60,
            STDDEV(close) OVER (ORDER BY open_time ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS std60
        FROM ohlcv
        ORDER BY open_time""",
    )
    print(f"[perf] ohlcv rolling SMA60+STD60 ({rows:,} rows): {elapsed:.1f} ms")
    assert elapsed < 15000, f"Rolling SMA60 too slow: {elapsed:.1f} ms"


# %% INSERT (synthetic, temp db)


def test_timing_insert_100k_rows() -> None:
    n_rows = 100_000
    rng    = np.random.default_rng(42)
    ts     = pd.date_range("2020-01-01", periods=n_rows, freq="min")
    close  = 100.0 + np.cumsum(rng.normal(0, 0.1, n_rows))
    df_syn = pd.DataFrame({
        "open_time":       ts,
        "open":            close * (1 + rng.uniform(-0.001, 0.001, n_rows)),
        "high":            close * (1 + rng.uniform(0, 0.002, n_rows)),
        "low":             close * (1 - rng.uniform(0, 0.002, n_rows)),
        "close":           close,
        "volume":          rng.uniform(1000, 5000, n_rows),
        "quote_volume":    rng.uniform(1e5, 5e5, n_rows),
        "trades":          rng.integers(100, 1000, n_rows).astype("int64"),
        "taker_buy_base":  rng.uniform(400, 2500, n_rows),
        "taker_buy_quote": rng.uniform(4e4, 2.5e5, n_rows),
    })

    tmp = tempfile.mkdtemp()
    try:
        bench_db = str(Path(tmp) / "bench.duckdb")
        c = get_connection(bench_db)
        ensure_tables(c)

        times = []
        for _ in range(3):
            c.execute("DELETE FROM ohlcv WHERE 1=1")
            t0 = time.perf_counter()
            insert_ohlcv(c, df_syn)
            times.append((time.perf_counter() - t0) * 1000.0)

        elapsed = min(times)
        c.close()
        print(f"\n[perf] insert {n_rows:,} rows (best of 3): {elapsed:.1f} ms")
        assert elapsed < 10000, f"INSERT {n_rows:,} rows too slow: {elapsed:.1f} ms"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


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


# %% ASOF JOIN


def test_timing_asof_join(conn: duckdb.DuckDBPyConnection) -> None:
    for tbl in ("predictions", "feat_ohlcv_quant"):
        row = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?", [tbl]
        ).fetchone()
        if not row or row[0] == 0:
            pytest.skip(f"{tbl} table missing")

    elapsed, rows = _time_ms(
        conn,
        """SELECT p.open_time, p.long_pred, p.short_pred, f.feat_rsi_14
        FROM predictions p
        ASOF JOIN feat_ohlcv_quant f ON p.open_time >= f.open_time
        ORDER BY p.open_time""",
    )
    print(f"\n[perf] ASOF JOIN predictions⋈features ({rows:,} rows): {elapsed:.1f} ms")
    assert elapsed < 10000, f"ASOF JOIN too slow: {elapsed:.1f} ms"
