"""Sanity tests: target table forward window correctness.

Verifies:
- Forward window starts at t+1 (not t+0): bar t's close is not in the window.
- Last horizon rows have NULL fw60 outcome values (insufficient future data).
- NULL rows are at the tail and symmetric across outcome columns.
"""

import duckdb
import pytest

pytestmark = pytest.mark.sanity

_HORIZON = 60


# %% Helpers


def _tbl_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0] > 0)


# %% Forward window starts at t+1


def test_target_sql_uses_1_following_boundary(conn: duckdb.DuckDBPyConnection) -> None:
    """Structural check: _TARGET_SQL must use ROWS BETWEEN 1 FOLLOWING."""
    from database.sync_tables.sync_targets import _TARGET_SQL
    assert "1 FOLLOWING" in _TARGET_SQL, (
        "_TARGET_SQL must start forward window at ROWS BETWEEN 1 FOLLOWING — "
        "CURRENT ROW would include bar t's close (data leakage)"
    )


# %% NULL count == horizon


def test_target_null_count_equals_horizon(conn: duckdb.DuckDBPyConnection) -> None:
    """Exactly horizon rows must have NULL fw60 outcomes (incomplete forward window at tail)."""
    if not _tbl_exists(conn, "target"):
        pytest.skip("target table missing")

    row = conn.execute(
        "SELECT COUNT(*) FROM target WHERE long_mfe_fw60 IS NULL"
    ).fetchone()
    null_count = row[0] if row else 0

    assert null_count == _HORIZON, (
        f"Expected {_HORIZON} NULL rows in target (rows with < horizon future bars), "
        f"got {null_count}. Run sync_targets to rebuild."
    )


def test_target_null_rows_are_at_tail(conn: duckdb.DuckDBPyConnection) -> None:
    """All NULL fw60 rows must be at the end of the time series."""
    if not _tbl_exists(conn, "target"):
        pytest.skip("target table missing")

    row = conn.execute("SELECT MAX(open_time) FROM target").fetchone()
    if not row or row[0] is None:
        pytest.skip("target table is empty")

    max_ts = row[0]
    cutoff_sql = f"(TIMESTAMP '{max_ts}') - INTERVAL '{_HORIZON}' MINUTE"
    row2 = conn.execute(f"""
        SELECT COUNT(*) FROM target
        WHERE long_mfe_fw60 IS NULL
          AND open_time < {cutoff_sql}
    """).fetchone()
    early_nulls = row2[0] if row2 else 0
    assert early_nulls == 0, (
        f"{early_nulls} NULL rows found before the expected tail window — "
        f"NULLs should only appear in the last {_HORIZON} rows (max_ts={max_ts})"
    )


def test_target_null_symmetry(conn: duckdb.DuckDBPyConnection) -> None:
    """long_mfe_fw60 IS NULL and short_mfe_fw60 IS NULL must coincide on the same rows."""
    if not _tbl_exists(conn, "target"):
        pytest.skip("target table missing")

    row = conn.execute("""
        SELECT COUNT(*) FROM target
        WHERE (long_mfe_fw60 IS NULL) != (short_mfe_fw60 IS NULL)
    """).fetchone()
    asymmetric = row[0] if row else 0
    assert asymmetric == 0, (
        f"{asymmetric} rows where only one outcome column is NULL — "
        "all fw60 columns must be NULL on the same rows"
    )


# %% Synthetic unit test: small DB with known horizon


def test_synthetic_target_null_count(tmp_path: "pytest.TempPathFactory") -> None:
    """Synthetic: 10-bar DB with horizon=3 → exactly 3 NULL rows at the tail."""
    from pathlib import Path

    import pandas as pd

    from database.store.duckdb_store import ensure_tables, get_connection
    from database.sync_tables.sync_targets import _TARGET_SQL

    db_file = str(Path(str(tmp_path)) / "test.duckdb")
    test_conn = get_connection(db_file)
    ensure_tables(test_conn)

    ohlcv_rows = pd.DataFrame({
        "open_time" : pd.date_range("2024-01-01", periods=10, freq="1min"),
        "close"     : [100.0 + i for i in range(10)],
        "open"      : [100.0 + i for i in range(10)],
        "high"      : [101.0 + i for i in range(10)],
        "low"       : [99.0 + i for i in range(10)],
        "volume"    : [1000.0] * 10,
        "quote_volume"    : [0.0] * 10,
        "trades"          : [0] * 10,
        "taker_buy_base"  : [0.0] * 10,
        "taker_buy_quote" : [0.0] * 10,
    })
    test_conn.register("ohlcv_rows", ohlcv_rows)
    test_conn.execute(
        "INSERT INTO ohlcv SELECT open_time, open, high, low, close, volume, "
        "quote_volume, trades, taker_buy_base, taker_buy_quote FROM ohlcv_rows"
    )
    test_conn.unregister("ohlcv_rows")

    horizon = 3
    sql = _TARGET_SQL.format(horizon=horizon)
    df = test_conn.execute(sql).pl()

    assert len(df) == 10
    null_count = df["long_mfe_fw60"].is_null().sum()
    assert null_count == horizon, (
        f"Expected {horizon} NULLs with n=10, horizon={horizon}, got {null_count}"
    )

    tail = df.tail(horizon)
    assert tail["long_mfe_fw60"].is_null().all(), "Last horizon rows must all be NULL"

    head = df.head(10 - horizon)
    assert not head["long_mfe_fw60"].is_null().any(), "Rows before tail must not be NULL"

    test_conn.close()
