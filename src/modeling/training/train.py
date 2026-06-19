"""Dispatcher for model training — routes model_id to the correct trainer.

Reads trainer type from config/models.json and delegates to the appropriate
training function.  Only lightgbm_binary is supported in this version.
"""

from pathlib import Path

import utils
from modeling.training.lightgbm_model import train_lightgbm_binary

# %% Public API


def train_model(model_id: str) -> dict:
    """Train a configured model and return the artifact metadata dict.

    Args:
        model_id : Model key from config/models.json.

    Returns:
        Artifact dict with at minimum: model_id, trainer, tuning_param,
        best_tuning_value, n_features_input, n_features_selected, output_dir.

    Raises:
        ValueError: If model_id is not found or the trainer is unsupported.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta       = models_cfg["models"][model_id]
    trainer    = meta["trainer"]
    target_col = meta["target_name"]
    sample_dir = Path(meta["training"]["sample_dir"])
    output_dir = Path(utils._resolve_path(meta["paths"]["model_dir"]))
    row_stride = meta["training"].get("row_stride", 1)
    asset_id   = meta.get("asset_id")

    if trainer == "lightgbm_binary":
        return train_lightgbm_binary(
            model_id   = model_id,
            target_col = target_col,
            sample_dir = sample_dir,
            output_dir = output_dir,
            row_stride = row_stride,
            asset_id   = asset_id,
        )

    raise ValueError(f"Unsupported trainer: {trainer!r}")
