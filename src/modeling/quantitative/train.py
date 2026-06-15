# =============================================================================
# Generic model training orchestrator
# =============================================================================
# Purpose:
#  - Load model training config and dispatch to the configured trainer
# =============================================================================

from pathlib import Path

import utils
from modeling.quantitative.lasso_logreg import train_lasso_logreg
from modeling.quantitative.lightgbm_model import train_lightgbm_binary
from modeling.quantitative.statsmodels_logreg import train_statsmodels_pvalue_logreg


# =============================================================================
# train_model(model_id: str) -> dict
# =============================================================================
# Purpose:
#  - Train one configured model and return artifact metadata
# =============================================================================
def train_model(model_id: str) -> dict:
    model_cfg = utils.load_models_config()
    models = model_cfg.get("models", {})
    if model_id not in models:
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta = models[model_id]
    trainer = meta.get("trainer")
    paths = meta["paths"]
    training_cfg = meta.get("training", {})
    asset_id = meta.get("asset_id")

    output_dir = Path(utils._resolve_path(paths["model_dir"]))
    sample_dir = Path(training_cfg.get("sample_dir", f"samples/{training_cfg['sample_id']}"))

    if trainer == "sklearn_lasso_logreg":
        return train_lasso_logreg(
            model_id     = model_id,
            target_col   = meta["target_name"],
            sample_dir   = sample_dir,
            output_dir   = output_dir,
            alphas       = training_cfg.get("alphas"),
            class_weight = training_cfg.get("class_weight"),
            solver       = training_cfg.get("solver", "liblinear"),
            row_stride   = training_cfg.get("row_stride", 1),
            max_iter     = training_cfg.get("max_iter", 300),
            random_state = training_cfg.get("random_state", 42),
            verbose      = training_cfg.get("verbose", True),
            asset_id     = asset_id,
        )

    if trainer == "lightgbm_binary":
        return train_lightgbm_binary(
            model_id      = model_id,
            target_col    = meta["target_name"],
            sample_dir    = sample_dir,
            output_dir    = output_dir,
            param_profile = training_cfg.get("param_profile", "lightgbm_binary_stable_v1"),
            tuning_param  = training_cfg.get("tuning_param"),
            tuning_values = training_cfg.get("tuning_values"),
            row_stride    = training_cfg.get("row_stride", 1),
            random_state  = training_cfg.get("random_state", 42),
            verbose       = training_cfg.get("verbose", True),
            asset_id      = asset_id,
        )

    if trainer == "statsmodels_pvalue_logreg":
        return train_statsmodels_pvalue_logreg(
            model_id       = model_id,
            target_col     = meta["target_name"],
            sample_dir     = sample_dir,
            output_dir     = output_dir,
            p_threshold    = training_cfg.get("p_threshold", 0.01),
            pvalue_rounds  = training_cfg.get("pvalue_rounds"),
            row_stride     = training_cfg.get("row_stride", 1),
            max_fit_iter   = training_cfg.get("max_fit_iter", 100),
            min_features   = training_cfg.get("min_features", 1),
            verbose        = training_cfg.get("verbose", True),
            asset_id       = asset_id,
        )

    raise ValueError(f"Unsupported trainer for train_model: {trainer}")
