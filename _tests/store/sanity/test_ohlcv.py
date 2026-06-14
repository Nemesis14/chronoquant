"""Sanity tests for the ohlcv table on the production database.

Checks presence, schema, null constraints, date range, OHLC invariants,
volume positivity, and 1-minute cadence. Skips when the database is absent.
"""

import duckdb
import pytest

pytestmark = pytest.mark.sanity


# conn and db_path fixtures come from _tests/store/conftest.py


def _tbl_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0] > 0)


def test_ohlcv_table_exists(conn: duckdb.DuckDBPyConnection) -> None:
    assert _tbl_exists(conn, "ohlcv"), "ohlcv table is missing"


def test_ohlcv_row_count(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()
    count = row[0] if row else 0
    print(f"\nohlcv: {count:,} rows")
    assert count > 0, "ohlcv table is empty"


def test_ohlcv_required_columns(conn: duckdb.DuckDBPyConnection) -> None:
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'ohlcv'"
        ).fetchall()
    }
    required = {"open_time", "open", "high", "low", "close", "volume",
                "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"}
    missing = required - cols
    assert not missing, f"Missing ohlcv columns: {missing}"


def test_ohlcv_date_range(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT MIN(open_time), MAX(open_time) FROM ohlcv").fetchone()
    assert row and row[0] is not None and row[1] is not None
    print(f"\nohlcv range: {row[0]} -> {row[1]}")


def test_ohlcv_no_null_open_time(conn: duckdb.DuckDBPyConnection) -> None:
    row   = conn.execute("SELECT COUNT(*) FROM ohlcv WHERE open_time IS NULL").fetchone()
    count = row[0] if row else 0
    assert count == 0, f"ohlcv has {count} NULL open_time rows"


def test_ohlcv_no_null_ohlc(conn: duckdb.DuckDBPyConnection) -> None:
    row   = conn.execute(
        "SELECT COUNT(*) FROM ohlcv WHERE open IS NULL OR high IS NULL"
        " OR low IS NULL OR close IS NULL"
    ).fetchone()
    count = row[0] if row else 0
    assert count == 0, f"ohlcv has {count} rows with NULL OHLC values"


def test_ohlcv_high_gte_low(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT COUNT(*) FROM ohlcv WHERE high < low").fetchone()
    violations = row[0] if row else 0
    assert violations == 0, f"{violations} ohlcv rows where high < low"


def test_ohlcv_high_gte_open_and_close(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute(
        "SELECT COUNT(*) FROM ohlcv WHERE high < open OR high < close"
    ).fetchone()
    violations = row[0] if row else 0
    assert violations == 0, f"{violations} ohlcv rows where high < open or high < close"


def test_ohlcv_low_lte_open_and_close(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute(
        "SELECT COUNT(*) FROM ohlcv WHERE low > open OR low > close"
    ).fetchone()
    violations = row[0] if row else 0
    assert violations == 0, f"{violations} ohlcv rows where low > open or low > close"


def test_ohlcv_volume_non_negative(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT COUNT(*) FROM ohlcv WHERE volume < 0").fetchone()
    violations = row[0] if row else 0
    assert violations == 0, f"{violations} ohlcv rows with negative volume"


def test_ohlcv_prices_positive(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute(
        "SELECT COUNT(*) FROM ohlcv WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0"
    ).fetchone()
    violations = row[0] if row else 0
    assert violations == 0, f"{violations} ohlcv rows with non-positive price"


def test_ohlcv_no_time_gap(conn: duckdb.DuckDBPyConnection) -> None:
    """No missing 1-minute bars within the covered range."""
    row = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT open_time,
                   LAG(open_time) OVER (ORDER BY open_time) AS prev_time
            FROM ohlcv
        ) sub
        WHERE prev_time IS NOT NULL
          AND DATEDIFF('minute', prev_time, open_time) <> 1
    """).fetchone()
    gaps = row[0] if row else 0
    assert gaps == 0, f"ohlcv has {gaps} time gaps (expected 1-minute cadence)"


def test_ohlcv_no_duplicate_open_time(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute(
        "SELECT COUNT(*) FROM (SELECT open_time FROM ohlcv GROUP BY open_time HAVING COUNT(*) > 1)"
    ).fetchone()
    dupes = row[0] if row else 0
    assert dupes == 0, f"ohlcv has {dupes} duplicate open_time values"
