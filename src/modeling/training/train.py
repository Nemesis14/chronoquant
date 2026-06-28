"""Dispatcher for model training — routes model_id to the correct trainer.

Reads trainer type from config/models.json and delegates to the appropriate
training function.  Only lightgbm_regression is supported in this version.
"""

import utils
from modeling.training.fit_lgbm import fit_lightgbm_from_search

# %% Public API


def train_model(
    model_id:    str,
    search_tag:  str | None = None,
    feature_key: str        = "selected",
) -> dict:
    """Train a configured model and return the artifact metadata dict.

    Args:
        model_id:    Model key from config/models.json.
        search_tag:  Optional search directory tag (e.g. ``"joint"``,
                     ``"joint_reg_gp20"``).  Reads from ``search_{tag}/``.
        feature_key: Feature set key in feature_set.json (default ``"selected"``).

    Returns:
        Artifact dict with at minimum: model_id, n_estimators, n_features,
        selected_features, artifact_dir.

    Raises:
        ValueError: If model_id is not found or the trainer is unsupported.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    trainer = models_cfg["models"][model_id]["trainer"]

    if trainer == "lightgbm_regression":
        return fit_lightgbm_from_search(model_id, search_tag=search_tag, feature_key=feature_key)

    raise ValueError(f"Unsupported trainer: {trainer!r}")
