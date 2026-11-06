"""Smoke tests for the snapshot-native sampler (snap source -> model.__sample).

Covers the IO-free SQL builders and an end-to-end CTAS over a synthetic snapshot
table in an in-memory DuckDB connection, validating the deterministic hourly
select + train/valid split semantics and the reg.feature_sets contract.
"""

from __future__ import annotations

import duckdb
import pytest

from data_handling.store import registry
from modeling.sampling.create_sample import create_snapshot_sample_train_valid_split
from modeling.sampling.snapshot_sampler import (
    build_feature_set_id,
    sample_table_fqn,
    snapshot_table_fqn,
)

pytestmark = pytest.mark.smoke

SNAPSHOT_ID = "solusdt_fw60_2024__deadbeef"
MODEL_ID    = "lgbm_solusdt_l_fw60_2024_v1"
TARGET      = "long_mfe_fw60"


# ---------------------------------------------------------------------------
# Naming / SQL builders
# ---------------------------------------------------------------------------


def test_sample_table_fqn() -> None:
    assert sample_table_fqn(MODEL_ID) == f'model."{MODEL_ID}__sample"'


def test_snapshot_table_fqn() -> None:
    assert snapshot_table_fqn(SNAPSHOT_ID) == f'snap."{SNAPSHOT_ID}"'


def test_feature_set_id_is_order_insensitive() -> None:
    a = build_feature_set_id("solusdt", 60, "l", ["feat_b", "feat_a"])
    b = build_feature_set_id("solusdt", 60, "l", ["feat_a", "feat_b"])
    assert a == b
    assert a.startswith("fs_solusdt_fw60_l__")
    assert len(a.rsplit("__", 1)[1]) == 8


def test_feature_set_id_changes_with_selection() -> None:
    a = build_feature_set_id("solusdt", 60, "l", ["feat_a", "feat_b"])
    b = build_feature_set_id("solusdt", 60, "l", ["feat_a"])
    assert a != b


# ---------------------------------------------------------------------------
# End-to-end: synthetic snapshot -> model.__sample + reg.feature_sets
# ---------------------------------------------------------------------------

_SPLIT: dict[str, str] = {
    "train_start": "2024-01-01",
    "train_end":   "2024-01-31",
    "valid_start": "2024-02-01",
    "valid_end":   "2024-02-28",
}


def _build_lab_with_snapshot(tmp_path) -> duckdb.DuckDBPyConnection:
    """In-memory lab connection with snap/model schemas, reg attached, and a
    synthetic minute-level snapshot table covering January-February 2024."""
    reg_path = str(tmp_path / "registry.duckdb")
    registry.ensure_registry(reg_path)

    conn = duckdb.connect(":memory:")
    registry.attach_registry(conn, reg_path, read_only=False)
    conn.execute("CREATE SCHEMA snap")
    conn.execute("CREATE SCHEMA model")

    conn.execute(
        f'''CREATE TABLE snap."{SNAPSHOT_ID}" AS
        SELECT
            ts AS open_time,
            1.5 + (i % 7) * 0.1 AS feat_a,
            0.4 + (i % 3) * 0.05 AS long_mfe_fw60
        FROM (
            SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i) MINUTE AS ts, i
            FROM range(60 * 24 * 60) t(i)
        )'''
    )
    return conn


def _create(conn) -> dict:
    return create_snapshot_sample_train_valid_split(
        conn=conn, model_id=MODEL_ID, snapshot_id=SNAPSHOT_ID, asset_id="solusdt",
        target_cols=[TARGET], seed=42, selected_cols=["feat_a"],
        train_start=_SPLIT["train_start"], train_end=_SPLIT["train_end"],
        valid_start=_SPLIT["valid_start"], valid_end=_SPLIT["valid_end"],
    )


def test_create_sample_end_to_end(tmp_path) -> None:
    conn = _build_lab_with_snapshot(tmp_path)
    try:
        summary = _create(conn)

        # Sample table exists with exactly the small projection.
        cols = [d[0] for d in conn.execute(f'SELECT * FROM model."{MODEL_ID}__sample" LIMIT 0').description]
        assert cols == ["open_time", TARGET, "split"]

        # Hourly select: at most one row per hour.
        dup = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT date_trunc('hour', open_time) h, COUNT(*) c "
            f'FROM model."{MODEL_ID}__sample" GROUP BY h HAVING c > 1)'
        ).fetchone()
        assert dup is not None and dup[0] == 0

        # split is TINYINT with both train (0) and valid (1) rows present.
        stype = conn.execute(
            f"SELECT data_type FROM information_schema.columns "
            f"WHERE table_schema='model' AND table_name='{MODEL_ID}__sample' AND column_name='split'"
        ).fetchone()
        assert stype is not None and "TINYINT" in stype[0].upper()
        assert set(summary["split_row_counts"]) == {"0", "1"}

        assert summary["n_rows"] > 0
        assert summary["n_input"] == 1
        assert summary["n_selected"] == 1

        # reg.feature_sets row written with the contract columns.
        fs = registry.get(conn, "feature_sets", summary["feature_set_id"])
        assert fs is not None
        assert fs["snapshot_id"] == SNAPSHOT_ID
        assert fs["n_input"] == 1
        assert fs["n_selected"] == 1

        # reg.models link written.
        model_row = registry.get(conn, "models", MODEL_ID)
        assert model_row is not None
        assert model_row["snapshot_id"] == SNAPSHOT_ID
        assert model_row["feature_set_id"] == summary["feature_set_id"]
    finally:
        conn.close()


def test_create_sample_deterministic(tmp_path) -> None:
    """Same (snapshot, seed) -> bit-identical sample (immutable source + fixed seed)."""
    conn = _build_lab_with_snapshot(tmp_path)
    try:
        query = f'SELECT open_time, split FROM model."{MODEL_ID}__sample" ORDER BY open_time'
        _create(conn)
        first = conn.execute(query).fetchall()
        _create(conn)
        second = conn.execute(query).fetchall()
        assert first == second
    finally:
        conn.close()


def test_missing_snapshot_raises(tmp_path) -> None:
    reg_path = str(tmp_path / "registry.duckdb")
    registry.ensure_registry(reg_path)
    conn = duckdb.connect(":memory:")
    registry.attach_registry(conn, reg_path, read_only=False)
    conn.execute("CREATE SCHEMA snap")
    try:
        with pytest.raises(ValueError, match="not found"):
            create_snapshot_sample_train_valid_split(
                conn=conn, model_id=MODEL_ID, snapshot_id="nope", asset_id="solusdt",
                target_cols=[TARGET],
                train_start=_SPLIT["train_start"], train_end=_SPLIT["train_end"],
                valid_start=_SPLIT["valid_start"], valid_end=_SPLIT["valid_end"],
            )
    finally:
        conn.close()
