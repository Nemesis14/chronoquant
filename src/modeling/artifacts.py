# =============================================================================
# Shared model artifact persistence
# =============================================================================
# Purpose:
#  - Save model, feature, metric, parameter, and CV artifacts consistently
#  - Keep artifact layout stable across model families
# =============================================================================

import json
import pickle
from pathlib import Path
from typing import Any

import pandas as pd


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

    if model_params is not None:
        _write_json(output_dir / "params.json", model_params)


# =============================================================================
# _write_json(path: Path, payload: dict) -> None
# =============================================================================
# Purpose:
#  - Write stable, readable JSON artifacts
# =============================================================================
def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=4), encoding="utf-8")
