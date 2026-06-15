"""Sanity tests for feat_ohlcv_quant and target tables on the production database.

Checks presence, schema, null constraints, date range, label distribution, and
cross-table row-count alignment. Skips automatically when the database is absent.
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
    required = {"open_time", "close", "trg_l_fw60_q90", "trg_s_fw60_q10"}
    missing  = required - cols
    assert not missing, f"Missing target columns: {missing}"


def test_target_date_range(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT MIN(open_time), MAX(open_time) FROM target").fetchone()
    assert row and row[0] is not None and row[1] is not None
    print(f"\ntarget range: {row[0]} -> {row[1]}")


def test_target_label_distribution_long(conn: duckdb.DuckDBPyConnection) -> None:
    """trg_l_fw60_q90 positive rate should be ~10% (q90 decile threshold)."""
    rows = conn.execute(
        "SELECT trg_l_fw60_q90, COUNT(*) AS n FROM target GROUP BY 1 ORDER BY 1"
    ).fetchall()
    dist       = {r[0]: r[1] for r in rows}
    total      = sum(dist.values())
    true_ratio = dist.get(True, 0) / total if total else 0
    print(f"\ntrg_l_fw60_q90 distribution: {dist}, positive rate: {true_ratio:.3%}")
    assert 0.05 <= true_ratio <= 0.20, \
        f"Unexpected positive rate for trg_l_fw60_q90: {true_ratio:.3%}"


def test_target_label_distribution_short(conn: duckdb.DuckDBPyConnection) -> None:
    """trg_s_fw60_q10 positive rate should be ~10% (q10 decile threshold)."""
    rows = conn.execute(
        "SELECT trg_s_fw60_q10, COUNT(*) AS n FROM target GROUP BY 1 ORDER BY 1"
    ).fetchall()
    dist       = {r[0]: r[1] for r in rows}
    total      = sum(dist.values())
    true_ratio = dist.get(True, 0) / total if total else 0
    print(f"\ntrg_s_fw60_q10 distribution: {dist}, positive rate: {true_ratio:.3%}")
    assert 0.05 <= true_ratio <= 0.20, \
        f"Unexpected positive rate for trg_s_fw60_q10: {true_ratio:.3%}"


def test_target_row_count_matches_features(conn: duckdb.DuckDBPyConnection) -> None:
    feat_row   = conn.execute("SELECT COUNT(*) FROM feat_ohlcv_quant").fetchone()
    tgt_row    = conn.execute("SELECT COUNT(*) FROM target").fetchone()
    feat_count = feat_row[0] if feat_row else 0
    tgt_count  = tgt_row[0] if tgt_row else 0
    print(f"\nfeat_ohlcv_quant rows: {feat_count:,}  |  target rows: {tgt_count:,}")
    assert feat_count == tgt_count, (
        f"Row count mismatch: feat_ohlcv_quant={feat_count:,}, target={tgt_count:,}"
    )



# %% validate helpers


def test_no_future_features(conn: duckdb.DuckDBPyConnection, db_path: str) -> None:
    """No feature computed from data points after open_time (look-ahead)."""
    from database.store.validate import check_no_future_features
    check_no_future_features(db_path)


