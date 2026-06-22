"""Smoke tests for the snapshot-native sampler (snap source -> model.__sample).

Covers the IO-free SQL builders and an end-to-end CTAS over a synthetic snapshot
table in an in-memory DuckDB connection, validating the deterministic hourly
select + walk-forward fold_id semantics and the reg.feature_sets contract.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import duckdb
import pytest

from data_handling.store import registry
from modeling.sampling.create_sample import create_snapshot_sample
from modeling.sampling.snapshot_sampler import (
    build_feature_set_id,
    build_fold_case_sql,
    build_sample_ctas_sql,
    sample_table_fqn,
    snapshot_table_fqn,
)
from modeling.sampling.yearly_sampler import generate_walk_forward_folds

pytestmark = pytest.mark.smoke

SNAPSHOT_ID = "solusdt_fw60_2023__deadbeef"
MODEL_ID    = "lgbm_solusdt_l_fw60_2023_v1"
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


def test_empty_fold_windows_yield_zero_case() -> None:
    assert build_fold_case_sql([]) == "CAST(0 AS TINYINT)"


def test_fold_case_contains_each_fold() -> None:
    folds = generate_walk_forward_folds(year=2023, n_folds=4)
    case_sql = build_fold_case_sql(folds)
    for fw in folds:
        assert f"THEN {fw['fold_id']}" in case_sql
    assert "ELSE 0" in case_sql


def test_ctas_sql_wraps_create_or_replace() -> None:
    folds = generate_walk_forward_folds(year=2023, n_folds=4)
    sql = build_sample_ctas_sql(MODEL_ID, SNAPSHOT_ID, seed=42, target_cols=[TARGET], fold_time_windows=folds)
    assert sql.startswith(f'CREATE OR REPLACE TABLE model."{MODEL_ID}__sample" AS')
    assert f'FROM snap."{SNAPSHOT_ID}"' in sql


# ---------------------------------------------------------------------------
# End-to-end: synthetic snapshot -> model.__sample + reg.feature_sets
# ---------------------------------------------------------------------------


def _build_lab_with_snapshot(tmp_path) -> duckdb.DuckDBPyConnection:
    """In-memory lab connection with snap/model schemas, reg attached, and a
    synthetic minute-level snapshot table spanning the 2023 walk-forward range."""
    reg_path = str(tmp_path / "registry.duckdb")
    registry.ensure_registry(reg_path)

    conn = duckdb.connect(":memory:")
    registry.attach_registry(conn, reg_path, read_only=False)
    conn.execute("CREATE SCHEMA snap")
    conn.execute("CREATE SCHEMA model")

    # 200 days of minute bars from 2023-08-01 (covers folds 1..4 validation windows).
    start = datetime(2023, 8, 1)
    rows = []
    for i in range(200 * 24 * 60):
        t = start + timedelta(minutes=i)
        rows.append((t, 1.5 + (i % 7) * 0.1, 0.4 + (i % 3) * 0.05))
    conn.execute(
        f'CREATE TABLE snap."{SNAPSHOT_ID}" '
        "(open_time TIMESTAMP, feat_a DOUBLE, long_mfe_fw60 DOUBLE)"
    )
    conn.executemany(
        f'INSERT INTO snap."{SNAPSHOT_ID}" VALUES (?, ?, ?)', rows
    )
    return conn


def test_create_snapshot_sample_end_to_end(tmp_path) -> None:
    conn = _build_lab_with_snapshot(tmp_path)
    try:
        summary = create_snapshot_sample(
            conn        = conn,
            model_id    = MODEL_ID,
            snapshot_id = SNAPSHOT_ID,
            asset_id    = "solusdt",
            target_cols = [TARGET],
            year        = 2023,
            seed        = 2065,
            selected_cols = ["feat_a"],
        )

        # Sample table exists with exactly the small projection.
        cols = [d[0] for d in conn.execute(f'SELECT * FROM model."{MODEL_ID}__sample" LIMIT 0').description]
        assert cols == ["open_time", TARGET, "fold_id"]

        # Hourly select: at most one row per hour.
        dup = conn.execute(
            f'SELECT COUNT(*) FROM (SELECT date_trunc(\'hour\', open_time) h, COUNT(*) c '
            f'FROM model."{MODEL_ID}__sample" GROUP BY h HAVING c > 1)'
        ).fetchone()
        assert dup is not None and dup[0] == 0

        # fold_id is Int8/TINYINT and within 0..4.
        ftype = conn.execute(
            f"SELECT data_type FROM information_schema.columns "
            f"WHERE table_schema='model' AND table_name='{MODEL_ID}__sample' AND column_name='fold_id'"
        ).fetchone()
        assert ftype is not None and "TINYINT" in ftype[0].upper()
        fold_vals = {int(r[0]) for r in conn.execute(f'SELECT DISTINCT fold_id FROM model."{MODEL_ID}__sample"').fetchall()}
        assert fold_vals <= {0, 1, 2, 3, 4}
        assert 0 in fold_vals  # train-only rows present

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


def test_create_snapshot_sample_deterministic(tmp_path) -> None:
    """Same (snapshot, seed) -> bit-identical sample (immutable source + fixed seed)."""
    conn = _build_lab_with_snapshot(tmp_path)
    try:
        create_snapshot_sample(
            conn=conn, model_id=MODEL_ID, snapshot_id=SNAPSHOT_ID, asset_id="solusdt",
            target_cols=[TARGET], year=2023, seed=2065, selected_cols=["feat_a"],
        )
        first = conn.execute(f'SELECT open_time, fold_id FROM model."{MODEL_ID}__sample" ORDER BY open_time').fetchall()
        create_snapshot_sample(
            conn=conn, model_id=MODEL_ID, snapshot_id=SNAPSHOT_ID, asset_id="solusdt",
            target_cols=[TARGET], year=2023, seed=2065, selected_cols=["feat_a"],
        )
        second = conn.execute(f'SELECT open_time, fold_id FROM model."{MODEL_ID}__sample" ORDER BY open_time').fetchall()
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
            create_snapshot_sample(
                conn=conn, model_id=MODEL_ID, snapshot_id="nope", asset_id="solusdt",
                target_cols=[TARGET], year=2023, seed=42,
            )
    finally:
        conn.close()
