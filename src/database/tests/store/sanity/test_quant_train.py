"""Sanity tests for quant_train: invariants on column set, open_time uniqueness, and ordering."""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from database.store.duckdb_store import (
    ensure_tables,
    get_connection,
    insert_feat_ohlcv_quant,
    insert_target,
    rebuild_quant_train,
)

pytestmark = pytest.mark.sanity


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


def _seed_and_build(tmp_path: Path) -> tuple[str, int]:
    db_path = str(tmp_path / "test.duckdb")
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        insert_feat_ohlcv_quant(conn, _feat_frame())
        insert_target(conn, _target_frame())
        n = rebuild_quant_train(conn)
    finally:
        conn.close()
    return db_path, n


def test_quant_train_has_required_columns(tmp_path: Path) -> None:
    db_path, _ = _seed_and_build(tmp_path)
    import duckdb
    conn = duckdb.connect(db_path, read_only=True)
    try:
        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'quant_train'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert "open_time" in cols
    assert "long_mfe_fw60" in cols
    assert "short_mfe_fw60" in cols
    assert any(c.startswith("feat_") for c in cols), "quant_train must have feat_* columns"


def test_quant_train_open_time_is_unique(tmp_path: Path) -> None:
    db_path, _ = _seed_and_build(tmp_path)
    import duckdb
    conn = duckdb.connect(db_path, read_only=True)
    try:
        total = conn.execute("SELECT COUNT(*) FROM quant_train").fetchone()[0]
        distinct = conn.execute("SELECT COUNT(DISTINCT open_time) FROM quant_train").fetchone()[0]
    finally:
        conn.close()
    assert total == distinct, "open_time must be unique in quant_train"


def test_quant_train_open_time_not_null(tmp_path: Path) -> None:
    db_path, _ = _seed_and_build(tmp_path)
    import duckdb
    conn = duckdb.connect(db_path, read_only=True)
    try:
        null_count = conn.execute(
            "SELECT COUNT(*) FROM quant_train WHERE open_time IS NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert null_count == 0


def test_quant_train_is_ordered_by_open_time(tmp_path: Path) -> None:
    db_path, _ = _seed_and_build(tmp_path)
    import duckdb
    conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = [r[0] for r in conn.execute("SELECT open_time FROM quant_train").fetchall()]
    finally:
        conn.close()
    assert rows == sorted(rows), "quant_train must be ordered by open_time"


def test_quant_train_no_null_targets(tmp_path: Path) -> None:
    db_path, _ = _seed_and_build(tmp_path)
    import duckdb
    conn = duckdb.connect(db_path, read_only=True)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM quant_train"
            " WHERE long_mfe_fw60 IS NULL OR short_mfe_fw60 IS NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert n == 0, "quant_train must not contain NULL target rows"


def test_quant_train_excludes_prediction_columns(tmp_path: Path) -> None:
    db_path, _ = _seed_and_build(tmp_path)
    import duckdb
    conn = duckdb.connect(db_path, read_only=True)
    try:
        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'quant_train'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert "long_pred" not in cols
    assert "short_pred" not in cols
    assert "label_end_ts" not in cols
