"""Sanity tests for feat_ohlcv_quant and target tables on the production database.

Checks presence, schema, null constraints, date range, label distribution, and
cross-table row-count alignment. Skips automatically when the database is absent.
"""

import duckdb
import pytest

from data_handling.store.duckdb_query import _tbl_exists

pytestmark = pytest.mark.sanity


# conn and db_path fixtures come from _tests/store/conftest.py



# %% feat_ohlcv_quant


def test_feat_ohlcv_quant_table_exists(conn: duckdb.DuckDBPyConnection) -> None:
    assert _tbl_exists(conn, "feat_ohlcv_quant"), "feat_ohlcv_quant table is missing"


def test_feat_ohlcv_quant_row_count(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT COUNT(*) FROM feat_ohlcv_quant").fetchone()
    count = row[0] if row else 0
    print(f"\nfeat_ohlcv_quant: {count:,} rows")
    assert count > 0, "feat_ohlcv_quant is empty"


def test_feat_ohlcv_quant_column_count(conn: duckdb.DuckDBPyConnection) -> None:
    cols = conn.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_name = 'feat_ohlcv_quant' ORDER BY ordinal_position"
    ).fetchall()
    feat_cols = [c[0] for c in cols if c[0].startswith("feat_")]
    print(f"\nfeat_ohlcv_quant: {len(cols)} total cols, {len(feat_cols)} feat_ cols")
    assert len(feat_cols) >= 100, f"Expected ≥100 feat_ columns, got {len(feat_cols)}"


def test_feat_ohlcv_quant_required_metadata_columns(conn: duckdb.DuckDBPyConnection) -> None:
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'feat_ohlcv_quant'"
        ).fetchall()
    }
    required = {"open_time", "close", "available_ts", "lookback_end_ts"}
    missing  = required - cols
    assert not missing, f"Missing metadata columns: {missing}"


def test_feat_ohlcv_quant_date_range(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute(
        "SELECT MIN(open_time), MAX(open_time) FROM feat_ohlcv_quant"
    ).fetchone()
    assert row and row[0] is not None and row[1] is not None
    print(f"\nfeat_ohlcv_quant range: {row[0]} -> {row[1]}")


def test_feat_ohlcv_quant_no_null_open_time(conn: duckdb.DuckDBPyConnection) -> None:
    row   = conn.execute("SELECT COUNT(*) FROM feat_ohlcv_quant WHERE open_time IS NULL").fetchone()
    count = row[0] if row else 0
    assert count == 0, f"feat_ohlcv_quant has {count} NULL open_time rows"


def test_feat_ohlcv_quant_available_ts_no_lookahead(conn: duckdb.DuckDBPyConnection) -> None:
    """available_ts must not be after open_time — that would be a look-ahead leak."""
    row = conn.execute(
        "SELECT COUNT(*) FROM feat_ohlcv_quant WHERE available_ts > open_time"
    ).fetchone()
    violations = row[0] if row else 0
    assert violations == 0, f"{violations} rows where available_ts > open_time (look-ahead leak)"


def test_feat_ohlcv_quant_no_dataset_split_column(conn: duckdb.DuckDBPyConnection) -> None:
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'feat_ohlcv_quant'"
        ).fetchall()
    }
    assert "dataset_split" not in cols, "dataset_split column must not exist in feat_ohlcv_quant"
    assert "fold_id" not in cols, "fold_id column must not exist in feat_ohlcv_quant"


def test_feat_ohlcv_quant_no_time_gap(conn: duckdb.DuckDBPyConnection) -> None:
    """No missing 1-minute bars within the covered range (using epoch diff check)."""
    row = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT open_time,
                   LAG(open_time) OVER (ORDER BY open_time) AS prev_time
            FROM feat_ohlcv_quant
        ) sub
        WHERE prev_time IS NOT NULL
          AND DATEDIFF('minute', prev_time, open_time) <> 1
    """).fetchone()
    gaps = row[0] if row else 0
    assert gaps == 0, f"feat_ohlcv_quant has {gaps} time gaps (expected 1-minute cadence)"



# %% target


def test_target_table_exists(conn: duckdb.DuckDBPyConnection) -> None:
    assert _tbl_exists(conn, "target"), "target table is missing"


def test_target_row_count(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT COUNT(*) FROM target").fetchone()
    count = row[0] if row else 0
    print(f"\ntarget: {count:,} rows")
    assert count > 0, "target table is empty"


def test_target_required_columns(conn: duckdb.DuckDBPyConnection) -> None:
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'target'"
        ).fetchall()
    }
    required = {"open_time", "close", "long_mfe_fw60", "short_mfe_fw60"}
    missing  = required - cols
    assert not missing, f"Missing target columns: {missing}"


def test_target_date_range(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT MIN(open_time), MAX(open_time) FROM target").fetchone()
    assert row and row[0] is not None and row[1] is not None
    print(f"\ntarget range: {row[0]} -> {row[1]}")


def test_target_long_mfe_range(conn: duckdb.DuckDBPyConnection) -> None:
    """long_mfe_fw60 values should be log returns — mostly in a reasonable range."""
    row = conn.execute(
        "SELECT MIN(long_mfe_fw60), MAX(long_mfe_fw60), AVG(long_mfe_fw60) FROM target"
        " WHERE long_mfe_fw60 IS NOT NULL"
    ).fetchone()
    if not row or row[0] is None:
        pytest.skip("No non-null long_mfe_fw60 values")
    min_val, max_val, avg_val = row
    print(f"\nlong_mfe_fw60: min={min_val:.4f} max={max_val:.4f} avg={avg_val:.4f}")
    assert min_val > -2.0, f"long_mfe_fw60 min={min_val:.4f} seems unrealistically low"
    assert max_val < 2.0, f"long_mfe_fw60 max={max_val:.4f} seems unrealistically high"
    assert avg_val > 0, "long_mfe_fw60 average should be positive (prices generally move up)"


def test_target_short_mfe_range(conn: duckdb.DuckDBPyConnection) -> None:
    """short_mfe_fw60 values should be negative log returns (downward moves)."""
    row = conn.execute(
        "SELECT MIN(short_mfe_fw60), MAX(short_mfe_fw60), AVG(short_mfe_fw60) FROM target"
        " WHERE short_mfe_fw60 IS NOT NULL"
    ).fetchone()
    if not row or row[0] is None:
        pytest.skip("No non-null short_mfe_fw60 values")
    min_val, max_val, avg_val = row
    print(f"\nshort_mfe_fw60: min={min_val:.4f} max={max_val:.4f} avg={avg_val:.4f}")
    assert min_val > -2.0, f"short_mfe_fw60 min={min_val:.4f} seems unrealistically low"
    assert max_val < 0.1, f"short_mfe_fw60 max={max_val:.4f} should be near zero or negative"
    assert avg_val < 0, "short_mfe_fw60 average should be negative (minimum move is down)"


def test_target_row_count_matches_features(conn: duckdb.DuckDBPyConnection) -> None:
    """target and feat_ohlcv_quant align row-for-row over target's covered range.

    The live worker (sync_full: ohlcv -> funding -> scout -> features ->
    predictions) does NOT refresh ``target``; it is rebuilt at retrain time via
    ``02_sync_pipeline.py --tables targets``.  So target legitimately ends before
    feat_ohlcv_quant (epic_076 / t949) and a total-count equality is the wrong
    expectation.  The invariant that must hold: same start, target not past the
    feature tail, and an identical row count up to target's MAX(open_time) with
    no target row lacking a feature row.
    """
    row = conn.execute("""
        SELECT
            (SELECT MIN(open_time) FROM feat_ohlcv_quant),
            (SELECT MAX(open_time) FROM feat_ohlcv_quant),
            (SELECT MIN(open_time) FROM target),
            (SELECT MAX(open_time) FROM target),
            (SELECT COUNT(*) FROM target),
            (SELECT COUNT(*) FROM feat_ohlcv_quant
              WHERE open_time <= (SELECT MAX(open_time) FROM target)),
            (SELECT COUNT(*) FROM target t ANTI JOIN feat_ohlcv_quant f USING (open_time))
    """).fetchone()
    assert row is not None
    f_min, f_max, t_min, t_max, tgt_count, feat_upto, orphans = row
    print(
        f"\nfeat_ohlcv_quant: {f_min} -> {f_max}  |  target: {t_min} -> {t_max}"
        f"\nrows up to target max: feat={feat_upto:,} target={tgt_count:,}"
    )
    assert t_min == f_min, f"Start mismatch: feat={f_min}, target={t_min}"
    assert t_max <= f_max, f"target ({t_max}) extends past feat_ohlcv_quant ({f_max})"
    assert orphans == 0, f"{orphans} target rows have no feat_ohlcv_quant row"
    assert feat_upto == tgt_count, (
        f"Row count mismatch up to target max: feat_ohlcv_quant={feat_upto:,}, target={tgt_count:,}"
    )



# %% validate helpers


def test_no_future_features(conn: duckdb.DuckDBPyConnection, db_path: str) -> None:
    """No feature computed from data points after open_time (look-ahead)."""
    from data_handling.store.validate import check_no_future_features
    check_no_future_features(db_path)


