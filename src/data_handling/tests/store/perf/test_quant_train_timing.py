"""Performance benchmarks for quant_train queries on the production database."""

import time

import duckdb
import pytest

pytestmark = pytest.mark.perf


def _time_ms(conn: duckdb.DuckDBPyConnection, sql: str) -> float:
    t0 = time.perf_counter()
    conn.execute(sql).fetchall()
    return (time.perf_counter() - t0) * 1000.0


# conn fixture comes from src/database/tests/store/conftest.py


def test_timing_quant_train_count(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'quant_train'"
    ).fetchone()
    if not row or row[0] == 0:
        pytest.skip("quant_train table not found")
    elapsed = _time_ms(conn, "SELECT COUNT(*) FROM quant_train")
    print(f"\n[perf] quant_train COUNT(*): {elapsed:.1f} ms")
    assert elapsed < 2000, f"COUNT(*) too slow: {elapsed:.1f} ms"


def test_timing_quant_train_range_7d(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'quant_train'"
    ).fetchone()
    if not row or row[0] == 0:
        pytest.skip("quant_train table not found")

    import pandas as pd

    max_row = conn.execute("SELECT MAX(open_time) FROM quant_train").fetchone()
    max_ts  = str(max_row[0]) if max_row and max_row[0] else "2026-01-01 00:00:00"
    end     = pd.Timestamp(max_ts)
    start_s = (end - pd.Timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    end_s   = end.strftime("%Y-%m-%d %H:%M:%S")

    elapsed = _time_ms(
        conn,
        f"SELECT open_time, long_mfe_fw60, short_mfe_fw60 FROM quant_train"
        f" WHERE open_time BETWEEN '{start_s}' AND '{end_s}'",
    )
    print(f"[perf] quant_train range 7d: {elapsed:.1f} ms")
    assert elapsed < 3000, f"Range query 7d too slow: {elapsed:.1f} ms"
