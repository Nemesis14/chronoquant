"""Smoke tests for modeling.provenance — manifest provenance + registry writes.

Covers the data-provenance contract of t314: manifest.json provenance fields,
reg.models status chain (draft -> sampled -> trained -> predicted), the
reg.search_runs row (best params + objective), reg.feature_sets linking, and the
reg.artifacts rows.  The registry-writing helpers open the lab connection through
utils; here that gateway is monkeypatched onto a single in-memory connection with
the registry attached, so the contract is exercised without a real lab database.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from data_handling.store import registry
from modeling import provenance

pytestmark = pytest.mark.smoke

MODEL_ID    = "lgbm_solusdt_l_fw60_2023_v1"
SNAPSHOT_ID = "solusdt_fw60_2023__deadbeef"
TARGET      = "long_mfe_fw60"


class _NoCloseConn:
    """Delegating proxy over a DuckDB connection whose ``close`` is a no-op.

    The provenance helpers open and close a lab connection per call; in tests we
    hand them this proxy so a single in-memory registry connection survives across
    calls and can be inspected afterwards.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def close(self) -> None:  # no-op — keep the underlying connection alive
        pass

    def __getattr__(self, name: str):
        return getattr(self._conn, name)


@pytest.fixture
def reg_conn(tmp_path, monkeypatch):
    """In-memory connection with the registry attached as ``reg``; utils gateway
    patched to hand modeling.provenance a non-closing proxy over it (no real lab DB).
    """
    reg_path = str(tmp_path / "registry.duckdb")
    registry.ensure_registry(reg_path)
    conn = duckdb.connect(":memory:")
    registry.attach_registry(conn, reg_path, read_only=False)

    proxy = _NoCloseConn(conn)
    monkeypatch.setattr(provenance.utils, "open_lab_connection", lambda asset_id=None: proxy)
    return conn


def _row(conn, table: str, pk: str) -> dict:
    """Fetch a registry row, asserting it exists (keeps the type non-optional)."""
    row = registry.get(conn, table, pk)
    assert row is not None, f"{table}.{pk} not found"
    return row


def _model_meta(artifact_dir: Path) -> dict:
    return {
        "asset_id":    "solusdt",
        "target_name": TARGET,
        "family":      "lgbm",
        "trainer":     "lightgbm_regression",
        "sampling":    {"snapshot_id": SNAPSHOT_ID},
        "artifact_dir": str(artifact_dir),
    }


# ---------------------------------------------------------------------------
# manifest provenance (IO on a file, no registry)
# ---------------------------------------------------------------------------


def test_update_manifest_provenance_merges_fields(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(json.dumps({"model_id": MODEL_ID}), encoding="utf-8")

    provenance.update_manifest_provenance(tmp_path, snapshot_id=SNAPSHOT_ID)
    provenance.update_manifest_provenance(tmp_path, feature_set_id="fs_x", content_sha256="abc123")

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["model_id"] == MODEL_ID  # untouched
    prov = manifest["provenance"]
    assert prov["snapshot_id"] == SNAPSHOT_ID
    assert prov["feature_set_id"] == "fs_x"
    assert prov["content_sha256"] == "abc123"


def test_update_manifest_provenance_ignores_none(tmp_path: Path) -> None:
    provenance.update_manifest_provenance(tmp_path, snapshot_id=SNAPSHOT_ID)
    provenance.update_manifest_provenance(tmp_path, snapshot_id=None, feature_set_id="fs_y")
    prov = json.loads((tmp_path / "manifest.json").read_text())["provenance"]
    assert prov["snapshot_id"] == SNAPSHOT_ID  # not clobbered by None
    assert prov["feature_set_id"] == "fs_y"


# ---------------------------------------------------------------------------
# reg.models status chain
# ---------------------------------------------------------------------------


def test_register_model_draft_writes_row_and_manifest(tmp_path, reg_conn) -> None:
    # snapshot row so content_sha256 resolves into the manifest.
    registry.upsert(reg_conn, "snapshots", {"snapshot_id": SNAPSHOT_ID, "content_sha256": "deadbeefcafe"})

    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    provenance.register_model_draft(MODEL_ID, _model_meta(artifact_dir), artifact_dir)

    row = registry.get(reg_conn, "models", MODEL_ID)
    assert row is not None
    assert row["status"] == "draft"
    assert row["snapshot_id"] == SNAPSHOT_ID
    assert row["direction"] == "l"

    prov = json.loads((artifact_dir / "manifest.json").read_text())["provenance"]
    assert prov["snapshot_id"] == SNAPSHOT_ID
    assert prov["content_sha256"] == "deadbeefcafe"


def test_model_status_chain_progresses(tmp_path, reg_conn) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    provenance.register_model_draft(MODEL_ID, _model_meta(artifact_dir), artifact_dir)

    for status in ("sampled", "trained", "predicted"):
        provenance.set_model_status(MODEL_ID, status, asset_id="solusdt")
        assert _row(reg_conn, "models", MODEL_ID)["status"] == status


def test_mark_model_trained_records_metric_and_link(tmp_path, reg_conn) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    provenance.register_model_draft(MODEL_ID, _model_meta(artifact_dir), artifact_dir)

    provenance.mark_model_trained(
        MODEL_ID, oos_metric=-0.0123, search_run_id=f"{MODEL_ID}__search_refine", asset_id="solusdt"
    )
    row = _row(reg_conn, "models", MODEL_ID)
    assert row["status"] == "trained"
    assert row["oos_metric"] == pytest.approx(-0.0123)
    assert row["search_run_id"] == f"{MODEL_ID}__search_refine"


# ---------------------------------------------------------------------------
# reg.search_runs
# ---------------------------------------------------------------------------


def test_register_search_run_writes_best_params_and_objective(tmp_path, reg_conn) -> None:
    best = {"params": {"num_leaves": 31, "learning_rate": 0.02}, "objective_score": -0.045}
    run_id = provenance.register_search_run(MODEL_ID, "refine", best, asset_id="solusdt")

    assert run_id == f"{MODEL_ID}__search_refine"
    row = _row(reg_conn, "search_runs", run_id)
    assert row["model_id"] == MODEL_ID
    assert row["stage"] == "refine"
    assert row["objective"] == pytest.approx(-0.045)
    assert json.loads(row["best_params"])["num_leaves"] == 31

    assert provenance.latest_search_run_id(MODEL_ID, asset_id="solusdt") == run_id


# ---------------------------------------------------------------------------
# reg.feature_sets linking
# ---------------------------------------------------------------------------


def test_link_feature_set_writes_and_links(tmp_path, reg_conn, monkeypatch) -> None:
    artifact_dir = tmp_path / "artifact"
    fe_dir = artifact_dir / "feature_engineering"
    fe_dir.mkdir(parents=True)
    (fe_dir / "feature_set.json").write_text(
        json.dumps({"selected": ["feat_b", "feat_a", "not_a_feat"]}), encoding="utf-8"
    )
    (artifact_dir / "manifest.json").write_text(json.dumps({"model_id": MODEL_ID}), encoding="utf-8")

    monkeypatch.setattr(
        provenance.utils, "load_models_config",
        lambda: {"models": {MODEL_ID: _model_meta(artifact_dir)}},
    )
    monkeypatch.setattr(provenance.utils, "_resolve_path", lambda p: p)

    fs_id = provenance.link_feature_set(MODEL_ID, asset_id="solusdt")
    assert fs_id is not None and fs_id.startswith("fs_solusdt_fw60_l__")

    fs = _row(reg_conn, "feature_sets", fs_id)
    assert fs["n_selected"] == 2  # not_a_feat dropped
    assert sorted(json.loads(fs["selected_cols"])) == ["feat_a", "feat_b"]

    # linked onto reg.models + manifest provenance
    assert _row(reg_conn, "models", MODEL_ID)["feature_set_id"] == fs_id
    prov = json.loads((artifact_dir / "manifest.json").read_text())["provenance"]
    assert prov["feature_set_id"] == fs_id


def test_link_feature_set_skips_when_no_selection(tmp_path, reg_conn, monkeypatch) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    monkeypatch.setattr(
        provenance.utils, "load_models_config",
        lambda: {"models": {MODEL_ID: _model_meta(artifact_dir)}},
    )
    monkeypatch.setattr(provenance.utils, "_resolve_path", lambda p: p)
    assert provenance.link_feature_set(MODEL_ID, asset_id="solusdt") is None


# ---------------------------------------------------------------------------
# reg.artifacts
# ---------------------------------------------------------------------------


def test_register_artifacts_records_existing_only(tmp_path, reg_conn) -> None:
    real = tmp_path / "model.pkl"
    real.write_text("x", encoding="utf-8")
    missing = tmp_path / "absent.json"

    provenance.register_artifacts(
        MODEL_ID, [("model_pkl", real), ("metrics", missing)], asset_id="solusdt"
    )

    pkl = _row(reg_conn, "artifacts", f"{MODEL_ID}__model_pkl")
    assert pkl["owner_id"] == MODEL_ID
    assert pkl["kind"] == "model_pkl"
    assert pkl["path"] == str(real)

    # missing file not registered
    assert registry.get(reg_conn, "artifacts", f"{MODEL_ID}__metrics") is None
