"""Smoke tests for modeling.predict — offline predict into model."<id>__pred".

Covers t315: scoring the full snapshot range into a separate prediction table,
the snapshot staying immutable (content hash unchanged), the snap ⋈ pred join on
open_time, and the reg.models -> predicted transition.  A synthetic in-memory
snapshot + a tiny LightGBM model stand in for a real lab database; the
utils gateway is monkeypatched onto one non-closing connection so the writes can
be inspected afterwards.
"""

from __future__ import annotations

import json
import pickle

import duckdb
import lightgbm as lgb
import numpy as np
import pytest

from data_handling.store import registry
from modeling import predict, provenance

pytestmark = pytest.mark.smoke

MODEL_ID    = "lgbm_solusdt_l_fw60_2023_v1"
SNAPSHOT_ID = "solusdt_fw60_2023__deadbeef"
TARGET      = "long_mfe_fw60"
FEATURES    = ["feat_a", "feat_b", "feat_c"]


class _NoCloseConn:
    """Proxy over a DuckDB connection whose ``close`` is a no-op (survives per-call)."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def close(self) -> None:  # no-op
        pass

    def __getattr__(self, name: str):
        return getattr(self._conn, name)


def _snapshot_hash(conn, snapshot_id: str) -> str:
    row = conn.execute(
        f"""
        WITH ordered AS (
            SELECT to_json(t) AS row_json FROM snap."{snapshot_id}" t ORDER BY open_time
        )
        SELECT COALESCE(sha256(string_agg(row_json, '\n')), sha256('')) FROM ordered
        """
    ).fetchone()
    return str(row[0])


@pytest.fixture
def lab(tmp_path, monkeypatch):
    """In-memory lab connection with snap.\"<id>\" + reg attached, a trained model on
    disk, and the utils/provenance gateways patched onto the same proxy connection.
    """
    reg_path = str(tmp_path / "registry.duckdb")
    registry.ensure_registry(reg_path)
    conn = duckdb.connect(":memory:")
    registry.attach_registry(conn, reg_path, read_only=False)

    # Synthetic immutable snapshot: open_time + features + target.
    conn.execute("CREATE SCHEMA IF NOT EXISTS snap")
    conn.execute("CREATE SCHEMA IF NOT EXISTS model")
    rng = np.random.default_rng(0)
    n = 200
    df = {  # noqa: F841 — registered below
        "open_time": np.arange(n).astype("datetime64[m]"),
        "feat_a": rng.random(n),
        "feat_b": rng.random(n),
        "feat_c": rng.random(n),
        TARGET: rng.random(n),
    }
    import pandas as pd

    snap_df = pd.DataFrame(df)
    conn.register("_snap_df", snap_df)
    conn.execute(f'CREATE TABLE snap."{SNAPSHOT_ID}" AS SELECT * FROM _snap_df ORDER BY open_time')
    conn.unregister("_snap_df")

    content_hash = _snapshot_hash(conn, SNAPSHOT_ID)
    registry.upsert(
        conn, "snapshots", {"snapshot_id": SNAPSHOT_ID, "content_sha256": content_hash}
    )
    registry.upsert(
        conn, "models",
        {"model_id": MODEL_ID, "snapshot_id": SNAPSHOT_ID, "status": "trained", "direction": "l"},
    )

    # Trained model + artifacts on disk.
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    model = lgb.LGBMRegressor(n_estimators=5, verbosity=-1)
    model.fit(snap_df[FEATURES].to_numpy(), snap_df[TARGET].to_numpy())
    with open(artifact_dir / "model.pkl", "wb") as f:
        pickle.dump(model, f)
    (artifact_dir / "features.json").write_text(json.dumps({"features": FEATURES}), encoding="utf-8")

    meta = {
        "asset_id":     "solusdt",
        "target_name":  TARGET,
        "family":       "lgbm",
        "trainer":      "lightgbm_regression",
        "sampling":     {"snapshot_id": SNAPSHOT_ID},
        "artifact_dir": str(artifact_dir),
    }

    proxy = _NoCloseConn(conn)
    monkeypatch.setattr(predict.utils, "open_lab_connection", lambda asset_id=None: proxy)
    monkeypatch.setattr(predict.utils, "load_models_config", lambda: {"models": {MODEL_ID: meta}})
    monkeypatch.setattr(predict.utils, "_resolve_path", lambda p: p)
    monkeypatch.setattr(provenance.utils, "open_lab_connection", lambda asset_id=None: proxy)
    monkeypatch.setattr(provenance.utils, "load_models_config", lambda: {"models": {MODEL_ID: meta}})

    return conn, content_hash


def test_predict_writes_pred_table_full_range(lab) -> None:
    conn, _ = lab
    result = predict.predict_offline(MODEL_ID)

    assert result["pred_table"] == f'model."{MODEL_ID}__pred"'
    assert result["n_features"] == 3

    cols = [d[0] for d in conn.execute(f'SELECT * FROM model."{MODEL_ID}__pred" LIMIT 0').description]
    assert cols == ["open_time", "pred"]

    n_pred = conn.execute(f'SELECT COUNT(*) FROM model."{MODEL_ID}__pred"').fetchone()[0]
    n_snap = conn.execute(f'SELECT COUNT(*) FROM snap."{SNAPSHOT_ID}"').fetchone()[0]
    assert n_pred == n_snap == result["n_rows"]


def test_predict_keeps_snapshot_immutable(lab) -> None:
    conn, hash_before = lab
    result = predict.predict_offline(MODEL_ID)
    assert result["snapshot_immutable"] is True
    assert _snapshot_hash(conn, SNAPSHOT_ID) == hash_before


def test_snap_join_pred_on_open_time(lab) -> None:
    conn, _ = lab
    predict.predict_offline(MODEL_ID)
    joined = conn.execute(
        f'SELECT COUNT(*) FROM snap."{SNAPSHOT_ID}" s '
        f'JOIN model."{MODEL_ID}__pred" p ON s.open_time = p.open_time'
    ).fetchone()[0]
    n_snap = conn.execute(f'SELECT COUNT(*) FROM snap."{SNAPSHOT_ID}"').fetchone()[0]
    assert joined == n_snap


def test_predict_sets_reg_models_predicted(lab) -> None:
    conn, _ = lab
    predict.predict_offline(MODEL_ID)
    row = registry.get(conn, "models", MODEL_ID)
    assert row is not None and row["status"] == "predicted"


def test_predict_registers_pred_artifact(lab) -> None:
    conn, _ = lab
    predict.predict_offline(MODEL_ID)
    art = registry.get(conn, "artifacts", f"{MODEL_ID}__{predict.PRED_ARTIFACT_KIND}")
    assert art is not None
    assert art["owner_id"] == MODEL_ID
    assert art["path"] == f'model."{MODEL_ID}__pred"'


def test_predict_is_deterministic(lab) -> None:
    conn, _ = lab
    predict.predict_offline(MODEL_ID)
    first = conn.execute(f'SELECT pred FROM model."{MODEL_ID}__pred" ORDER BY open_time').fetchall()
    predict.predict_offline(MODEL_ID)
    second = conn.execute(f'SELECT pred FROM model."{MODEL_ID}__pred" ORDER BY open_time').fetchall()
    assert first == second


def test_predict_raises_for_unknown_model(lab) -> None:
    with pytest.raises(ValueError, match="not found in config/models.json"):
        predict.predict_offline("nonexistent_model_xyz")
