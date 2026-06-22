# =============================================================================
# Shared model artifact persistence
# =============================================================================
# Purpose:
#  - Save model, feature, metric, parameter, and CV artifacts consistently
#  - Keep artifact layout stable across model families
# =============================================================================

import json
import logging
import pickle
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# (kind, filename) pairs for the standard training artifacts, in registry order.
# Paths are resolved relative to the model's output dir at registration time.
TRAINING_ARTIFACT_FILES: tuple[tuple[str, str], ...] = (
    ("model_pkl",   "model.pkl"),
    ("features",    "features.json"),
    ("params",      "params.json"),
    ("metrics",     "metrics.json"),
    ("cv_results",  "cv_results.csv"),
)


# =============================================================================
# save_training_artifacts(...) -> None
# =============================================================================
# Purpose:
#  - Persist standard model training artifacts under models/<model_id>/
# =============================================================================
def save_training_artifacts(
    output_dir: str | Path,
    model: Any,
    feature_cols: list[str],
    cv_df: pd.DataFrame,
    artifacts: dict,
    selected_features: list[str] | None = None,
    model_params: dict | None = None,
    validation_predictions_df: pd.DataFrame | None = None,
) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_dir / "model.pkl", "wb") as f:
        pickle.dump(model, f)

    features_payload = {
        "features": selected_features or feature_cols,
        "input_features": feature_cols,
    }
    if selected_features is not None:
        features_payload["selected_features"] = selected_features

    _write_json(output_dir / "features.json", features_payload)
    _write_json(output_dir / "metrics.json", artifacts)
    cv_df.to_csv(output_dir / "cv_results.csv", index=False)
    if validation_predictions_df is not None:
        validation_predictions_df.to_csv(output_dir / "validation_predictions.csv", index=False)

    if model_params is not None:
        _write_json(output_dir / "params.json", model_params)


# =============================================================================
# register_training_artifacts(model_id, output_dir, ...) -> None
# =============================================================================
# Purpose:
#  - Record the produced training artifact file paths in reg.artifacts (P2).
#  - Mark the model 'trained' on reg.models with oos_metric + search_run link.
# =============================================================================
def register_training_artifacts(
    model_id:      str,
    output_dir:    str | Path,
    oos_metric:    float | None = None,
    search_run_id: str | None   = None,
    asset_id:      str | None    = None,
) -> None:
    """Register training artifact paths + mark the model trained in the registry.

    Best-effort provenance: a registry failure must not discard a fitted model,
    so the writes are wrapped — the artifact files on disk stay authoritative.

    Args:
        model_id      : Owning model id.
        output_dir    : Directory holding model.pkl / features.json / ... .
        oos_metric    : Out-of-sample objective to record on reg.models.
        search_run_id : Search run to link the model to (resolved if None).
        asset_id      : Asset key for the lab connection; resolved if None.
    """
    output_dir = Path(output_dir)
    try:
        from modeling import provenance

        run_id = search_run_id or provenance.latest_search_run_id(model_id, asset_id)
        provenance.mark_model_trained(
            model_id, oos_metric=oos_metric, search_run_id=run_id, asset_id=asset_id,
        )
        provenance.register_artifacts(
            model_id,
            [(kind, output_dir / fname) for kind, fname in TRAINING_ARTIFACT_FILES],
            asset_id=asset_id,
        )
    except Exception as exc:  # noqa: BLE001 — provenance must never abort training
        logger.warning("register_training_artifacts: registry write failed: %s", exc)


# =============================================================================
# _write_json(path: Path, payload: dict) -> None
# =============================================================================
# Purpose:
#  - Write stable, readable JSON artifacts
# =============================================================================
def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=4), encoding="utf-8")
