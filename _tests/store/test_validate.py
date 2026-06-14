"""Data integrity validation assertion tests.

Tests require a populated DuckDB file (data/solusdt_fw60.duckdb).
Skipped automatically when the database or table is absent.
"""

from pathlib import Path

import duckdb
import pytest

import utils
from store.validate import assert_zero, check_no_future_features, check_no_label_overlap


def _data_dir() -> str:
    """Return configured asset data_dir."""
    cfg = utils.load_asset_config()
    return cfg["database"]["data_dir"]


def _table_exists(data_dir: str, table: str) -> bool:
    """Return True when a native DuckDB table exists."""
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
    """Return True when the features table exists."""
    return _table_exists(data_dir, "features")


def _predictions_exist(data_dir: str) -> bool:
    """Return True when the predictions table exists."""
    return _table_exists(data_dir, "predictions")


# %% SQL assertion helper tests


def test_assert_zero_passes() -> None:
    """Verify assert_zero accepts zero-count SQL."""
    con = duckdb.connect()
    try:
        assert assert_zero(con, "SELECT 0 AS n", "should not fire") == 0
    finally:
        con.close()


def test_assert_zero_raises() -> None:
    """Verify assert_zero raises when SQL reports violations."""
    con = duckdb.connect()
    try:
        with pytest.raises(AssertionError, match="1 violation"):
            assert_zero(con, "SELECT 1 AS n", "expected violation")
    finally:
        con.close()


# %% Configured database validation tests


def test_no_future_features() -> None:
    """Run the future-feature validation when local feature data exists."""
    data_dir = _data_dir()
    if not _features_exist(data_dir):
        pytest.skip("features dataset not present")
    check_no_future_features(data_dir)


def test_no_label_overlap_between_train_and_test() -> None:
    """Run the label-overlap validation when local prediction data exists."""
    data_dir = _data_dir()
    if not _features_exist(data_dir) or not _predictions_exist(data_dir):
        pytest.skip("features or predictions dataset not present")
    check_no_label_overlap(data_dir)
