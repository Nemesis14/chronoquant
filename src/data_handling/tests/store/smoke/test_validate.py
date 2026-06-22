"""Smoke and exception tests for store.validate assertion helpers."""

from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest

from data_handling.store.duckdb_store import (
    ensure_tables,
    get_connection,
    insert_feat_ohlcv_quant,
    insert_target,
    rebuild_quant_train,
)
from data_handling.store.validate import (
    assert_zero,
    check_quant_train_no_duplicates,
    check_sample_table,
)

pytestmark = pytest.mark.smoke


def test_assert_zero_passes() -> None:
    """Verify assert_zero accepts zero-count SQL."""
    con = duckdb.connect()
    try:
        assert assert_zero(con, "SELECT 0 AS n", "should not fire") == 0
    finally:
        con.close()


def test_assert_zero_raises() -> None:
    """Verify assert_zero raises AssertionError when SQL reports violations."""
    con = duckdb.connect()
    try:
        with pytest.raises(AssertionError, match="1 violation"):
            assert_zero(con, "SELECT 1 AS n", "expected violation")
    finally:
        con.close()


# --- check_quant_train_no_duplicates ---

def _seed_quant_train(db_path: str) -> None:
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        insert_feat_ohlcv_quant(
            conn,
            pl.DataFrame({
                "open_time":   [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 1)],
                "close":       [100.0, 101.0],
                "feat_rsi_14": [40.0, 45.0],
            }),
        )
        insert_target(
            conn,
            pl.DataFrame({
                "open_time":         [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 1)],
                "close":             [100.0, 101.0],
                "fw60_close":        [101.0, 102.0],
                "fw60_max":          [102.0, 103.0],
                "fw60_min":          [99.0, 100.0],
                "fw60_close_ret":    [0.01, 0.01],
                "fw60_close_logret": [0.01, 0.01],
                "fw60_max_ratio":    [0.02, 0.02],
                "fw60_min_ratio":    [-0.01, -0.01],
                "long_mfe_fw60":     [0.02, 0.02],
                "short_mfe_fw60":    [-0.01, -0.01],
            }),
        )
        rebuild_quant_train(conn)
    finally:
        conn.close()


def test_check_quant_train_no_duplicates_passes(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    _seed_quant_train(db_path)
    assert check_quant_train_no_duplicates(db_path) == 0


def test_check_quant_train_no_duplicates_raises_on_dup(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    _seed_quant_train(db_path)
    conn = duckdb.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO quant_train SELECT * FROM quant_train LIMIT 1"
        )
    finally:
        conn.close()
    with pytest.raises(AssertionError, match="duplicate open_time"):
        check_quant_train_no_duplicates(db_path)


def test_check_quant_train_no_duplicates_skips_missing_db(tmp_path: Path) -> None:
    assert check_quant_train_no_duplicates(str(tmp_path / "nonexistent.duckdb")) == 0


def test_check_quant_train_no_duplicates_skips_missing_table(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
    finally:
        conn.close()
    assert check_quant_train_no_duplicates(db_path) == 0


# --- check_sample_table ---

def _make_valid_segment_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "open_time":      [
                datetime(2024, 1, 1,  0, 0),
                datetime(2024, 1, 1,  1, 0),
                datetime(2024, 1, 1,  2, 0),
                datetime(2024, 1, 1,  3, 0),
                datetime(2024, 1, 1,  4, 0),
                datetime(2024, 1, 1,  5, 0),
            ],
            "fold_id":        [None, 0, 0, None, None, None],
            "segment":        ["train", "valid", "purge", "train", "purge", "test"],
            "feat_rsi_14":    [40.0, 45.0, 47.0, 50.0, 52.0, 55.0],
            "long_mfe_fw60":  [0.02, 0.01, 0.03, 0.02, 0.01, 0.03],
            "short_mfe_fw60": [-0.01, -0.02, -0.01, -0.02, -0.01, -0.02],
        }
    )


def _seed_sample(db_path: str, sample_id: str, df: pl.DataFrame | None = None) -> None:
    """Seed a sample_<sample_id> table directly via DuckDB register."""
    src = df if df is not None else _make_valid_segment_df()
    feat_cols = sorted(c for c in src.columns if c.startswith("feat_"))
    target_cols = [c for c in ("long_mfe_fw60", "short_mfe_fw60") if c in src.columns]
    df_out = src.select(["open_time", "fold_id", "segment"] + feat_cols + target_cols)
    table_name = f"sample_{sample_id}"
    conn = get_connection(db_path)
    try:
        conn.register("_sample_seed_tmp", df_out)
        conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _sample_seed_tmp')
        conn.unregister("_sample_seed_tmp")
    finally:
        conn.close()


def test_check_sample_table_happy_path(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    _seed_sample(db_path, "solusdt_fw60_yearly_2024")
    assert check_sample_table(db_path, "solusdt_fw60_yearly_2024") == 0


def test_check_sample_table_missing_db_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="DuckDB file not found"):
        check_sample_table(str(tmp_path / "nonexistent.duckdb"), "any_2024")


def test_check_sample_table_missing_table_raises(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    conn.close()
    with pytest.raises(FileNotFoundError, match="Sample table"):
        check_sample_table(db_path, "missing_2024")


def test_check_sample_table_duplicate_rows_raises(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    _seed_sample(db_path, "dup_2024")
    conn = duckdb.connect(db_path)
    try:
        conn.execute('INSERT INTO sample_dup_2024 SELECT * FROM sample_dup_2024 LIMIT 1')
    finally:
        conn.close()
    with pytest.raises(AssertionError, match="duplicate"):
        check_sample_table(db_path, "dup_2024")


def test_check_sample_table_no_purge_rows_raises(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "open_time":      [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 1, 0), datetime(2024, 1, 1, 2, 0)],
            "fold_id":        [None, 0, None],
            "segment":        ["train", "valid", "test"],
            "feat_rsi_14":    [40.0, 45.0, 50.0],
            "long_mfe_fw60":  [0.02, 0.01, 0.03],
            "short_mfe_fw60": [-0.01, -0.02, -0.01],
        }
    )
    db_path = str(tmp_path / "test.duckdb")
    _seed_sample(db_path, "nopurge_2024", df)
    with pytest.raises(AssertionError, match="purge rows"):
        check_sample_table(db_path, "nopurge_2024")


def test_check_sample_table_null_targets_in_train_raises(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "open_time":      [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 1, 0), datetime(2024, 1, 1, 2, 0), datetime(2024, 1, 1, 3, 0)],
            "fold_id":        [None, None, 0, None],
            "segment":        ["train", "purge", "valid", "test"],
            "feat_rsi_14":    [40.0, 43.0, 45.0, 50.0],
            "long_mfe_fw60":  [None, None, 0.01, 0.03],
            "short_mfe_fw60": [None, None, -0.02, -0.01],
        }
    )
    db_path = str(tmp_path / "test.duckdb")
    _seed_sample(db_path, "nulltgt_2024", df)
    with pytest.raises(AssertionError, match="NULL target"):
        check_sample_table(db_path, "nulltgt_2024")


def test_check_sample_table_missing_feature_col_raises(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    _seed_sample(db_path, "featcheck_2024")
    with pytest.raises(AssertionError, match="expected feature columns missing"):
        check_sample_table(db_path, "featcheck_2024", expected_feat_cols=["feat_rsi_14", "feat_nonexistent"])
