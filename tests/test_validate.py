"""Pytest tests for data integrity validation assertions.

Tests require a populated DuckDB file (data/solusdt_fw60.duckdb).
Skipped automatically when the database or table is absent.
"""

import pytest

import utils
from store.validate import assert_zero, check_no_future_features, check_no_label_overlap

import duckdb


def _data_dir() -> str:
    cfg = utils.load_asset_config()
    return cfg["database"]["data_dir"]


def _table_exists(data_dir: str, table: str) -> bool:
    from pathlib import Path
    db = Path(data_dir).with_suffix(".duckdb")
    if not db.exists():
        return False
    con = duckdb.connect(str(db), read_only=True)
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
        ).fetchone()
        return bool(row and row[0] > 0)
    finally:
        con.close()


def _features_exist(data_dir: str) -> bool:
    return _table_exists(data_dir, "features")


def _predictions_exist(data_dir: str) -> bool:
    return _table_exists(data_dir, "predictions")


# --- assert_zero ---

def test_assert_zero_passes():
    con = duckdb.connect()
    assert assert_zero(con, "SELECT 0 AS n", "should not fire") == 0
    con.close()


def test_assert_zero_raises():
    con = duckdb.connect()
    with pytest.raises(AssertionError, match="1 violation"):
        assert_zero(con, "SELECT 1 AS n", "expected violation")
    con.close()


# --- check_no_future_features ---

def test_no_future_features():
    data_dir = _data_dir()
    if not _features_exist(data_dir):
        pytest.skip("features dataset not present")
    check_no_future_features(data_dir)


# --- check_no_label_overlap ---

def test_no_label_overlap_between_train_and_test():
    data_dir = _data_dir()
    if not _features_exist(data_dir) or not _predictions_exist(data_dir):
        pytest.skip("features or predictions dataset not present")
    check_no_label_overlap(data_dir)
