"""Smoke tests for raw_manifest_audit and log_dataset_check (duckdb_stats).

Verifies happy-path behaviour with synthetic data and graceful handling of
missing databases and tables. No real production DB required.
"""

from pathlib import Path

import pandas as pd
import pytest

from database.store.duckdb_stats import log_dataset_check, raw_manifest_audit
from database.store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

pytestmark = pytest.mark.smoke


# %% Helpers


def _make_ohlcv_db(tmp_path: Path, n: int = 10) -> str:
    """Create a temp DuckDB with n OHLCV rows and return the db_path string."""
    db_path = str(tmp_path / "asset.duckdb")
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        rows = pd.DataFrame(
            {
                "open_time"       : pd.date_range("2024-01-01", periods=n, freq="min"),
                "open"            : [100.0] * n,
                "high"            : [101.0] * n,
                "low"             : [99.0] * n,
                "close"           : [100.5] * n,
                "volume"          : [10.0] * n,
                "quote_volume"    : [1000.0] * n,
                "trades"          : [1] * n,
                "taker_buy_base"  : [5.0] * n,
                "taker_buy_quote" : [500.0] * n,
            }
        )
        insert_ohlcv(conn, rows)
    finally:
        conn.close()
    return db_path


# %% raw_manifest_audit


def test_raw_manifest_audit_happy_path(tmp_path: Path) -> None:
    """Verify raw_manifest_audit does not raise when data is present."""
    db_path = _make_ohlcv_db(tmp_path)
    raw_manifest_audit(db_path, "ohlcv")  # must not raise


def test_raw_manifest_audit_missing_db(tmp_path: Path) -> None:
    """Verify raw_manifest_audit does not raise when the DB file is absent."""
    missing = str(tmp_path / "nonexistent.duckdb")
    raw_manifest_audit(missing, "ohlcv")  # must not raise


def test_raw_manifest_audit_missing_table(tmp_path: Path) -> None:
    """Verify raw_manifest_audit does not raise for an unknown table name."""
    db_path = _make_ohlcv_db(tmp_path)
    raw_manifest_audit(db_path, "nonexistent_table")  # must not raise


# %% log_dataset_check


def test_log_dataset_check_ohlcv_happy_path(tmp_path: Path) -> None:
    """Verify log_dataset_check for ohlcv dataset does not raise."""
    db_path = _make_ohlcv_db(tmp_path)
    log_dataset_check(db_path, "ohlcv")  # must not raise


def test_log_dataset_check_missing_db(tmp_path: Path) -> None:
    """Verify log_dataset_check does not raise when the DB file is absent."""
    missing = str(tmp_path / "nonexistent.duckdb")
    log_dataset_check(missing, "ohlcv")  # must not raise


def test_log_dataset_check_missing_dataset(tmp_path: Path) -> None:
    """Verify log_dataset_check handles a dataset with no rows gracefully."""
    db_path = _make_ohlcv_db(tmp_path)
    # 'target' table exists in schema but has no rows
    log_dataset_check(db_path, "target")  # must not raise
