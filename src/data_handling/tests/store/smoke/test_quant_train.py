"""Smoke tests for rebuild_quant_train in duckdb_store."""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from data_handling.store.duckdb_store import (
    ensure_tables,
    get_connection,
    insert_feat_ohlcv_quant,
    insert_target,
    rebuild_quant_train,
)

pytestmark = pytest.mark.smoke


def _feat_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "open_time":    [
                datetime(2024, 1, 1, 0, 0, 0),
                datetime(2024, 1, 1, 0, 1, 0),
                datetime(2024, 1, 1, 0, 2, 0),
            ],
            "close":        [100.0, 101.0, 102.0],
            "feat_rsi_14":  [40.0, 45.0, 50.0],
            "feat_roc_14":  [0.1, 0.2, 0.3],
        }
    )


def _target_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "open_time":       [
                datetime(2024, 1, 1, 0, 0, 0),
                datetime(2024, 1, 1, 0, 1, 0),
                datetime(2024, 1, 1, 0, 2, 0),
            ],
            "close":           [100.0, 101.0, 102.0],
            "fw60_close":      [101.0, 102.0, 103.0],
            "fw60_max":        [102.0, 103.0, 104.0],
            "fw60_min":        [99.0, 100.0, 101.0],
            "fw60_close_ret":  [0.01, 0.01, 0.01],
            "fw60_close_logret": [0.01, 0.01, 0.01],
            "fw60_max_ratio":  [0.02, 0.02, 0.02],
            "fw60_min_ratio":  [-0.01, -0.01, -0.01],
            "long_mfe_fw60":   [0.02, 0.02, None],
            "short_mfe_fw60":  [-0.01, -0.01, None],
        }
    )


def _seed(db_path: str) -> None:
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        insert_feat_ohlcv_quant(conn, _feat_frame())
        insert_target(conn, _target_frame())
    finally:
        conn.close()


def test_rebuild_quant_train_full_returns_int(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    _seed(db_path)
    conn = get_connection(db_path)
    try:
        n = rebuild_quant_train(conn)
    finally:
        conn.close()
    assert isinstance(n, int)
    assert n >= 0


def test_rebuild_quant_train_excludes_null_targets(tmp_path: Path) -> None:
    """Rows with NULL long_mfe_fw60 or short_mfe_fw60 must be excluded."""
    db_path = str(tmp_path / "test.duckdb")
    _seed(db_path)
    conn = get_connection(db_path)
    try:
        n = rebuild_quant_train(conn)
        null_count = conn.execute(
            "SELECT COUNT(*) FROM quant_train"
            " WHERE long_mfe_fw60 IS NULL OR short_mfe_fw60 IS NULL"
        ).fetchone()
    finally:
        conn.close()
    assert n == 2
    assert null_count is not None and null_count[0] == 0


def test_rebuild_quant_train_missing_feat_table_returns_zero(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        n = rebuild_quant_train(conn)
    finally:
        conn.close()
    assert n == 0


def test_rebuild_quant_train_range_rebuild_is_idempotent(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    _seed(db_path)
    conn = get_connection(db_path)
    try:
        rebuild_quant_train(conn)
        n1 = rebuild_quant_train(conn, start_time="2024-01-01 00:00:00", end_time="2024-01-01 00:01:00")
        n2 = rebuild_quant_train(conn, start_time="2024-01-01 00:00:00", end_time="2024-01-01 00:01:00")
    finally:
        conn.close()
    assert n1 == n2


def test_rebuild_quant_train_full_rebuild_is_idempotent(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    _seed(db_path)
    conn = get_connection(db_path)
    try:
        n1 = rebuild_quant_train(conn)
        n2 = rebuild_quant_train(conn)
    finally:
        conn.close()
    assert n1 == n2
