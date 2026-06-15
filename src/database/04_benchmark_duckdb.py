"""DuckDB storage layer benchmarks.

Measures insert throughput, range query speed, aggregation, and ASOF join
on the native ohlcv / features / predictions tables.

Usage:
    uv run python scripts/benchmark_duckdb.py
    uv run python scripts/benchmark_duckdb.py --asset-id solusdt_fw60 --rows 50000
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import duckdb
import numpy as np
import pandas as pd

import utils
from database.store.duckdb_query import (
    asof_join_predictions_features,
    ohlcv_row_count,
    ohlcv_time_stats,
    query_range,
)
from database.store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.1f} ms"
    return f"{seconds:.3f} s"


def _section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _result(label: str, value: str) -> None:
    print(f"  {label:<40} {value}")


def _bench(fn, label: str, reps: int = 1) -> float:
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    elapsed = min(times)
    _result(label, _fmt(elapsed))
    return elapsed


# ---------------------------------------------------------------------------
# Benchmark sections
# ---------------------------------------------------------------------------

def bench_insert(data_dir: str, n_rows: int) -> None:
    _section(f"INSERT — {n_rows:,} synthetic OHLCV rows")
    import shutil
    import tempfile
    tmp = tempfile.mkdtemp()
    try:
        rng = np.random.default_rng(42)
        ts  = pd.date_range("2020-01-01", periods=n_rows, freq="min")
        close = 100.0 + np.cumsum(rng.normal(0, 0.1, n_rows))
        df_syn = pd.DataFrame({
            "open_time":          ts,
            "open":               close * (1 + rng.uniform(-0.001, 0.001, n_rows)),
            "high":               close * (1 + rng.uniform(0, 0.002, n_rows)),
            "low":                close * (1 - rng.uniform(0, 0.002, n_rows)),
            "close":              close,
            "volume":             rng.uniform(1000, 5000, n_rows),
            "quote_volume":       rng.uniform(1e5, 5e5, n_rows),
            "trades":             rng.integers(100, 1000, n_rows).astype("int64"),
            "taker_buy_base":     rng.uniform(400, 2500, n_rows),
            "taker_buy_quote":    rng.uniform(4e4, 2.5e5, n_rows),
        })

        bench_db = str(Path(tmp) / "bench.duckdb")
        conn = get_connection(bench_db)
        ensure_tables(conn)

        def _do_insert():
            conn.execute("DELETE FROM ohlcv WHERE 1=1")
            insert_ohlcv(conn, df_syn)

        _bench(_do_insert, f"insert {n_rows:,} rows (cold)", reps=3)
        conn.close()
        _result("row count after insert", f"{ohlcv_row_count(bench_db):,}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def bench_query(data_dir: str) -> None:
    _section("RANGE QUERY — ohlcv table")
    count, min_ts, max_ts = ohlcv_time_stats(data_dir)
    if count == 0:
        print("  SKIP — ohlcv table is empty")
        return

    _result("total rows", f"{count:,}")
    _result("date range", f"{min_ts} -> {max_ts}")

    # Recent 7 days
    end_dt   = pd.Timestamp(max_ts or "1970-01-01")
    start_7d = (end_dt - pd.Timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    end_str  = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    def _query_7d():
        return query_range(data_dir, "ohlcv", start=start_7d, end=end_str)

    _bench(_query_7d, "range query last 7 days (warm)", reps=5)

    # Full scan
    def _full_scan():
        return query_range(data_dir, "ohlcv", columns=["open_time", "close"])

    _bench(_full_scan, "full scan (open_time, close)", reps=3)


def bench_aggregation(data_dir: str) -> None:
    _section("AGGREGATION — ohlcv table")
    count = ohlcv_row_count(data_dir)
    if count == 0:
        print("  SKIP — ohlcv table is empty")
        return

    db = Path(data_dir)
    conn = duckdb.connect(str(db), read_only=True)
    try:
        def _daily_agg():
            conn.execute("""
                SELECT
                    DATE_TRUNC('day', open_time) AS day,
                    MIN(low)   AS day_low,
                    MAX(high)  AS day_high,
                    LAST(close ORDER BY open_time) AS day_close,
                    SUM(volume) AS day_volume
                FROM ohlcv
                GROUP BY 1
                ORDER BY 1
            """).df()

        _bench(_daily_agg, "daily OHLCV aggregation", reps=5)

        def _rolling_stats():
            conn.execute("""
                SELECT
                    open_time,
                    AVG(close) OVER (ORDER BY open_time ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS sma60,
                    STDDEV(close) OVER (ORDER BY open_time ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS std60
                FROM ohlcv
                ORDER BY open_time
            """).df()

        _bench(_rolling_stats, "rolling SMA60 + STD60 (DuckDB window)", reps=3)
    finally:
        conn.close()


def bench_asof_join(data_dir: str) -> None:
    _section("ASOF JOIN — predictions ⋈ features")
    db = Path(data_dir)
    if not db.exists():
        print("  SKIP — database not found")
        return
    conn = duckdb.connect(str(db), read_only=True)
    try:
        for tbl in ("predictions", "feat_ohlcv_quant"):
            row = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{tbl}'").fetchone()
            if not row or row[0] == 0:
                print(f"  SKIP — {tbl} table missing")
                conn.close()
                return
        pred_row = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
        feat_row = conn.execute("SELECT COUNT(*) FROM feat_ohlcv_quant").fetchone()
        pred_count = pred_row[0] if pred_row else 0
        feat_count = feat_row[0] if feat_row else 0
        _result("predictions rows", f"{pred_count:,}")
        _result("features rows",    f"{feat_count:,}")
    finally:
        conn.close()

    def _asof():
        asof_join_predictions_features(data_dir, feature_cols=None, start=None, end=None)

    _bench(_asof, "ASOF JOIN predictions ⋈ features (full)", reps=3)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DuckDB storage layer benchmarks")
    p.add_argument("--asset-id", default=None, help="Asset ID (default: config default)")
    p.add_argument("--rows", type=int, default=100_000, help="Synthetic rows for insert benchmark")
    return p.parse_args()


def main() -> None:
    args   = _parse_args()
    cfg      = utils.load_asset_config(args.asset_id)
    db_path = cfg["database"]["db_path"]
    asset_id = cfg["database"]["asset_id"]

    print(f"\nDuckDB Benchmark — asset_id={asset_id}")
    print(f"  db_path  : {db_path}")

    bench_insert(db_path, args.rows)
    bench_query(db_path)
    bench_aggregation(db_path)
    bench_asof_join(db_path)

    print(f"\n{'=' * 60}")
    print("  Done.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
