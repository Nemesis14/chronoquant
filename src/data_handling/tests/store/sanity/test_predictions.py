"""Sanity tests for the predictions table on the production database.

Checks presence, schema, null constraints, score range, label consistency,
and alignment with the ohlcv table. Skips when absent.
"""

import duckdb
import pytest

from data_handling.store.duckdb_query import _tbl_exists

pytestmark = pytest.mark.sanity


# conn and db_path fixtures come from _tests/store/conftest.py


def test_predictions_table_exists(conn: duckdb.DuckDBPyConnection) -> None:
    assert _tbl_exists(conn, "predictions"), "predictions table is missing"


def test_predictions_row_count(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
    count = row[0] if row else 0
    if count == 0:
        pytest.skip("predictions table is empty — run sync_predictions to populate")
    print(f"\npredictions: {count:,} rows")
    assert count > 0, "predictions table is empty"


def test_predictions_required_columns(conn: duckdb.DuckDBPyConnection) -> None:
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'predictions'"
        ).fetchall()
    }
    required = {"open_time", "close", "label_end_ts",
                "long_mfe_fw60", "short_mfe_fw60", "long_pred", "short_pred"}
    missing = required - cols
    assert not missing, f"Missing predictions columns: {missing}"


def test_predictions_date_range(conn: duckdb.DuckDBPyConnection) -> None:
    count_row = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
    if not count_row or count_row[0] == 0:
        pytest.skip("predictions table is empty — run sync_predictions to populate")
    row = conn.execute("SELECT MIN(open_time), MAX(open_time) FROM predictions").fetchone()
    assert row and row[0] is not None and row[1] is not None
    print(f"\npredictions range: {row[0]} -> {row[1]}")


def test_predictions_no_null_open_time(conn: duckdb.DuckDBPyConnection) -> None:
    row   = conn.execute("SELECT COUNT(*) FROM predictions WHERE open_time IS NULL").fetchone()
    count = row[0] if row else 0
    assert count == 0, f"predictions has {count} NULL open_time rows"


def test_predictions_no_null_scores(conn: duckdb.DuckDBPyConnection) -> None:
    row   = conn.execute(
        "SELECT COUNT(*) FROM predictions WHERE long_pred IS NULL OR short_pred IS NULL"
    ).fetchone()
    count = row[0] if row else 0
    assert count == 0, f"predictions has {count} rows with NULL score columns"


def test_predictions_score_range(conn: duckdb.DuckDBPyConnection) -> None:
    """long_pred / short_pred are finite and within a plausible magnitude.

    The champion models are LightGBM *regressors* (``objective: regression``) of
    the fw60 MFE log-returns, so short_pred is negative and neither is a
    probability — the former ``[0, 1]`` expectation dates from classifier models
    (epic_076 / t949).  The bound [-2, 2] matches the target MFE sanity bounds
    and also admits a ``predict_proba`` model (values in [0, 1]).
    """
    row = conn.execute("""
        SELECT
            COUNT(*) FILTER (WHERE NOT isfinite(long_pred) OR NOT isfinite(short_pred)
                               OR ABS(long_pred) > 2 OR ABS(short_pred) > 2),
            MIN(long_pred), MAX(long_pred), MIN(short_pred), MAX(short_pred)
        FROM predictions
    """).fetchone()
    assert row is not None
    violations, l_min, l_max, s_min, s_max = row
    print(f"\nlong_pred: [{l_min}, {l_max}]  short_pred: [{s_min}, {s_max}]")
    assert violations == 0, f"{violations} predictions rows with non-finite or |score| > 2"


def test_predictions_no_duplicate_open_time(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute(
        "SELECT COUNT(*) FROM (SELECT open_time FROM predictions GROUP BY open_time HAVING COUNT(*) > 1)"
    ).fetchone()
    dupes = row[0] if row else 0
    assert dupes == 0, f"predictions has {dupes} duplicate open_time values"


def test_predictions_label_end_ts_after_open_time(conn: duckdb.DuckDBPyConnection) -> None:
    """label_end_ts must always be after open_time (forward-looking window)."""
    row = conn.execute(
        "SELECT COUNT(*) FROM predictions WHERE label_end_ts <= open_time"
    ).fetchone()
    violations = row[0] if row else 0
    assert violations == 0, f"{violations} predictions rows where label_end_ts <= open_time"


def test_predictions_open_time_in_ohlcv(conn: duckdb.DuckDBPyConnection) -> None:
    """Every predictions row must have a corresponding ohlcv row."""
    if not _tbl_exists(conn, "ohlcv"):
        pytest.skip("ohlcv table missing — cannot check alignment")
    row = conn.execute("""
        SELECT COUNT(*) FROM predictions p
        WHERE NOT EXISTS (SELECT 1 FROM ohlcv o WHERE o.open_time = p.open_time)
    """).fetchone()
    orphans = row[0] if row else 0
    assert orphans == 0, f"{orphans} predictions rows have no matching ohlcv row"


def test_predictions_close_matches_ohlcv(conn: duckdb.DuckDBPyConnection) -> None:
    """predictions.close must match ohlcv.close for every shared open_time."""
    if not _tbl_exists(conn, "ohlcv"):
        pytest.skip("ohlcv table missing — cannot check close alignment")
    row = conn.execute("""
        SELECT COUNT(*) FROM predictions p
        JOIN ohlcv o ON o.open_time = p.open_time
        WHERE ABS(p.close - o.close) > 1e-8
    """).fetchone()
    mismatches = row[0] if row else 0
    assert mismatches == 0, f"{mismatches} predictions rows where close != ohlcv.close"
