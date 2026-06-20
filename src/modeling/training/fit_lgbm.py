"""LightGBM fit from search artifacts — single fit, no CV sweep.

Reads best_params.json + search_best.json (n_estimators), feature_set.json.
Loads training data from the model's own sample_train_valid.parquet (artifact dir)
which already contains all feat_* columns — no DuckDB join needed for training.

Outputs:
  model.pkl                   — serialised LGBMRegressor
  features.json               — selected feature list
  params.json                 — final training params + n_estimators
  sample_train_valid.parquet  — updated in-place with pred_{dir} column added
"""

from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path

import lightgbm as lgb
import pandas as pd

import utils

logger = logging.getLogger(__name__)

_FIXED_PARAMS: dict = {
    "objective":      "regression",
    "boosting_type":  "gbdt",
    "metric":         "rmse",
    "subsample_freq": 1,
    "force_col_wise": True,
    "verbosity":      -1,
    "n_jobs":         4,
}


def fit_lightgbm_from_search(model_id: str) -> dict:
    """Fit a LightGBM model from search artifacts.

    Training data is loaded entirely from the model's sample_train_valid.parquet
    (artifact directory), which must already contain all selected feat_* columns.
    After fitting, pred_{dir} is appended to sample_train_valid.parquet.

    Args:
        model_id: Key from config/models.json.

    Returns:
        Dict with model_id, n_estimators, n_features, selected_features,
        artifact_dir.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta         = models_cfg["models"][model_id]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))
    target_name  = meta["target_name"]

    best_params: dict = json.loads(
        (artifact_dir / "search" / "best_params.json").read_text(encoding="utf-8")
    )
    search_best: dict = json.loads(
        (artifact_dir / "search" / "search_best.json").read_text(encoding="utf-8")
    )
    feature_set: dict = json.loads(
        (artifact_dir / "feature_engineering" / "feature_set.json").read_text(encoding="utf-8")
    )
    selected_features: list[str] = feature_set["selected"]

    best_iterations = [int(fold["best_iteration"]) for fold in search_best["fold_summary"]]
    n_estimators    = round(sum(best_iterations) / len(best_iterations) * 1.1)

    final_params: dict = {**_FIXED_PARAMS, **best_params, "n_estimators": n_estimators, "random_state": 42}

    sample_df, X_train, y_train = _load_train_data(artifact_dir, selected_features, target_name)

    logger.info(
        "[fit_lgbm] Fitting %s: n_samples=%d, n_features=%d, n_estimators=%d",
        model_id, len(y_train), len(selected_features), n_estimators,
    )

    model = lgb.LGBMRegressor(**final_params)
    model.fit(X_train, y_train)

    _save_artifacts(artifact_dir, model, selected_features, final_params, n_estimators)

    _add_predictions_to_sample(artifact_dir, model, sample_df, selected_features, target_name)

    return {
        "model_id":          model_id,
        "n_estimators":      n_estimators,
        "n_features":        len(selected_features),
        "selected_features": selected_features,
        "artifact_dir":      str(artifact_dir),
    }


def _load_train_data(
    artifact_dir     : Path,
    selected_features: list[str],
    target_name      : str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """Load training matrix from the model's sample parquet.

    Returns:
        (full sample_df, X_train, y_train) — full df kept for later prediction step.
    """
    sample_path = artifact_dir / "sample_train_valid.parquet"
    sample_df   = pd.read_parquet(sample_path)
    sample_df["open_time"] = pd.to_datetime(sample_df["open_time"])

    train_valid = sample_df.copy()

    X_train = train_valid[selected_features]
    y_train = train_valid[target_name].astype(float)

    return sample_df, pd.DataFrame(X_train), pd.Series(y_train)


def _add_predictions_to_sample(
    artifact_dir     : Path,
    model            : lgb.LGBMRegressor,
    sample_df        : pd.DataFrame,
    selected_features: list[str],
    target_name      : str,
) -> None:
    """Predict on all rows and write updated sample_train_valid.parquet.

    Column order: open_time, {target_name}, pred_{dir}, fold_id, feat_*
    """
    pred_col = "pred_long" if "long" in target_name else "pred_short"

    rows         = sample_df
    X_pred       = pd.DataFrame(rows[selected_features])
    preds_values = model.predict(X_pred)

    pred_series = pd.Series(index=sample_df.index, dtype="float64", name=pred_col)
    pred_series.loc[rows.index] = preds_values

    feat_cols = sorted(c for c in sample_df.columns if c.startswith("feat_"))
    col_order = ["open_time", target_name, pred_col, "fold_id"] + feat_cols

    out = sample_df.copy()
    out[pred_col] = pred_series

    out = out[col_order]

    out.to_parquet(artifact_dir / "sample_train_valid.parquet", compression="zstd", index=False)


def _save_artifacts(
    artifact_dir     : Path,
    model            : lgb.LGBMRegressor,
    selected_features: list[str],
    params           : dict,
    n_estimators     : int,
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)

    with open(artifact_dir / "model.pkl", "wb") as f:
        pickle.dump(model, f)

    (artifact_dir / "features.json").write_text(
        json.dumps({"features": selected_features}, indent=4), encoding="utf-8"
    )
    (artifact_dir / "params.json").write_text(
        json.dumps({"params": params, "n_estimators": n_estimators}, indent=4), encoding="utf-8"
    )
