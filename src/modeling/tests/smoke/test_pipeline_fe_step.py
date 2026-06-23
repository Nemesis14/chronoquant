"""Smoke tests for pipeline.step_feature_engineering — snapshot_id propagation.

Covers t42: the snapshot_id parameter is resolved from (1) explicit arg,
(2) meta["sampling"]["snapshot_id"] fallback, and passed as SNAPSHOT_ID into
the papermill parameters dict.  The actual notebook execution is monkeypatched
out; only the parameter-building logic is exercised.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.smoke

MODEL_ID    = "lgbm_solusdt_l_fw60_2101_2605"
SNAPSHOT_ID = "solusdt_fw60_2101_2605__abc123"


def _base_meta(snapshot_id: str = SNAPSHOT_ID) -> dict[str, Any]:
    return {
        "asset_id":     "solusdt",
        "target_name":  "long_mfe_fw60",
        "family":       "lgbm",
        "trainer":      "lightgbm_regression",
        "sampling":     {"snapshot_id": snapshot_id},
        "artifact_dir": "/tmp/artifact",
    }


def _run_fe_step(
    tmp_path: Path,
    meta: dict[str, Any],
    snapshot_id_arg: str | None,
) -> dict[str, Any]:
    """Invoke step_feature_engineering with papermill + provenance monkeypatched.

    Returns the ``parameters`` dict passed to ``pm.execute_notebook``.
    """
    captured: dict[str, Any] = {}

    pm_mock = MagicMock()

    def fake_execute_notebook(
        template: str,
        output: str,
        parameters: dict[str, Any],
        kernel_name: str,
    ) -> None:
        captured.update(parameters)

    pm_mock.execute_notebook.side_effect = fake_execute_notebook

    prov_mock = MagicMock()
    prov_mock.link_feature_set.return_value = "fs_solusdt__test"
    prov_mock.register_artifacts.return_value = None

    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()

    with (
        patch.dict("sys.modules", {"papermill": pm_mock}),
        patch("modeling.provenance.link_feature_set", prov_mock.link_feature_set),
        patch("modeling.provenance.register_artifacts", prov_mock.register_artifacts),
        patch("modeling.pipeline._update_manifest_status"),
        patch("subprocess.run", return_value=MagicMock(returncode=1, stderr="")),
    ):
        import modeling.pipeline as pipeline  # noqa: PLC0415
        pipeline.step_feature_engineering(
            MODEL_ID,
            meta,
            artifact_dir,
            snapshot_id=snapshot_id_arg,
        )

    return captured


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_explicit_snapshot_id_propagates_to_papermill(tmp_path: Path) -> None:
    """Explicit snapshot_id arg overrides meta and lands in papermill params."""
    meta = _base_meta(snapshot_id="meta_fallback_id")
    params = _run_fe_step(tmp_path, meta, snapshot_id_arg="explicit_snapshot_id")

    assert params.get("SNAPSHOT_ID") == "explicit_snapshot_id"
    assert params.get("MODEL_ID") == MODEL_ID


def test_meta_fallback_snapshot_id_used_when_no_arg(tmp_path: Path) -> None:
    """When snapshot_id arg is None, meta['sampling']['snapshot_id'] is used."""
    meta = _base_meta(snapshot_id=SNAPSHOT_ID)
    params = _run_fe_step(tmp_path, meta, snapshot_id_arg=None)

    assert params.get("SNAPSHOT_ID") == SNAPSHOT_ID


def test_empty_string_when_no_snapshot_anywhere(tmp_path: Path) -> None:
    """When neither arg nor meta has snapshot_id, SNAPSHOT_ID is '' (not None)."""
    meta = _base_meta(snapshot_id="")
    meta["sampling"] = {}  # no snapshot_id key at all
    params = _run_fe_step(tmp_path, meta, snapshot_id_arg=None)

    assert params.get("SNAPSHOT_ID") == ""


def test_artifact_dir_and_model_id_in_papermill_params(tmp_path: Path) -> None:
    """ARTIFACT_DIR, SAMPLE_DIR, MODEL_ID are always present in papermill params."""
    meta = _base_meta()
    params = _run_fe_step(tmp_path, meta, snapshot_id_arg=None)

    assert "ARTIFACT_DIR" in params
    assert "SAMPLE_DIR" in params
    assert params.get("MODEL_ID") == MODEL_ID
