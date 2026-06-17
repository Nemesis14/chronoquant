"""Smoke tests for materialize_sample_table in duckdb_store."""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from database.store.duckdb_store import get_connection, materialize_sample_table

pytestmark = pytest.mark.smoke


def _segment_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "open_time":      [
                datetime(2024, 1, 1, 0, 0, 0),
                datetime(2024, 1, 1, 1, 0, 0),
                datetime(2024, 1, 1, 2, 0, 0),
            ],
            "fold_id":        [None, 0, None],
            "segment":        ["train", "valid", "test"],
            "feat_rsi_14":    [40.0, 45.0, 50.0],
            "feat_roc_14":    [0.1, 0.2, 0.3],
            "long_mfe_fw60":  [0.02, 0.01, 0.03],
            "short_mfe_fw60": [-0.01, -0.02, -0.01],
        }
    )


def test_materialize_sample_table_returns_row_count(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    try:
        n = materialize_sample_table(conn, "test_sample_2024", _segment_frame())
    finally:
        conn.close()
    assert isinstance(n, int)
    assert n == 3


def test_materialize_sample_table_creates_named_table(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    try:
        materialize_sample_table(conn, "myasset_fw60_yearly_2024", _segment_frame())
        exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables"
            " WHERE table_name = 'sample_myasset_fw60_yearly_2024'"
        ).fetchone()
    finally:
        conn.close()
    assert exists is not None and exists[0] == 1


def test_materialize_sample_table_column_order(tmp_path: Path) -> None:
    """open_time, fold_id, segment, feat_* (sorted), target cols."""
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    try:
        materialize_sample_table(conn, "col_order_2024", _segment_frame())
        cols = [
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'sample_col_order_2024'"
                " ORDER BY ordinal_position"
            ).fetchall()
        ]
    finally:
        conn.close()
    assert cols[0] == "open_time"
    assert cols[1] == "fold_id"
    assert cols[2] == "segment"
    feat_cols = [c for c in cols if c.startswith("feat_")]
    assert feat_cols == sorted(feat_cols)
    target_cols = [c for c in cols if c in ("long_mfe_fw60", "short_mfe_fw60")]
    assert set(target_cols) == {"long_mfe_fw60", "short_mfe_fw60"}


def test_materialize_sample_table_is_idempotent(tmp_path: Path) -> None:
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    try:
        n1 = materialize_sample_table(conn, "idempotent_2024", _segment_frame())
        n2 = materialize_sample_table(conn, "idempotent_2024", _segment_frame())
    finally:
        conn.close()
    assert n1 == n2 == 3


def test_materialize_sample_table_multiple_folds(tmp_path: Path) -> None:
    """Multiple fold_id values can coexist in one table."""
    df = pl.DataFrame(
        {
            "open_time":      [
                datetime(2024, 1, 1, 0, 0, 0),
                datetime(2024, 1, 1, 1, 0, 0),
                datetime(2024, 1, 1, 2, 0, 0),
                datetime(2024, 1, 1, 3, 0, 0),
            ],
            "fold_id":        [None, 0, 1, None],
            "segment":        ["train", "valid", "valid", "test"],
            "feat_rsi_14":    [40.0, 45.0, 50.0, 55.0],
            "long_mfe_fw60":  [0.02, 0.01, 0.03, 0.02],
            "short_mfe_fw60": [-0.01, -0.02, -0.01, -0.02],
        }
    )
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    try:
        n = materialize_sample_table(conn, "multi_fold_2024", df)
        folds = conn.execute(
            "SELECT DISTINCT fold_id FROM sample_multi_fold_2024 ORDER BY fold_id"
        ).fetchall()
    finally:
        conn.close()
    assert n == 4
    fold_values = [row[0] for row in folds]
    assert 0 in fold_values
    assert 1 in fold_values
