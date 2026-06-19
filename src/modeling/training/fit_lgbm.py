"""LightGBM fit from search artifacts — single fit, no CV sweep.

Reads best_params.json + search_best.json (n_estimators), feature_set.json,
loads training data from sample parquet + quant_train DuckDB join, and
optionally runs OOS scoring to produce sample_oos.parquet.
"""

from __future__ import annotations

import json
import logging
import pickle
from pathlib import Path

import lightgbm as lgb
import pandas as pd

import utils
from data_handling.store.duckdb_query import query_range

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
    """Fit a LightGBM model from search artifacts and optionally run OOS scoring.

    Args:
        model_id: Key from config/models.json.

    Returns:
        Dict with model_id, n_estimators, n_features, selected_features,
        artifact_dir, oos_year.
    """
    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta = models_cfg["models"][model_id]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))
    sample_dir   = Path(utils._resolve_path(meta["sampling"]["sample_dir"]))
    asset_id: str      = meta["asset_id"]
    target_name: str   = meta["target_name"]
    row_stride: int    = meta["sampling"].get("row_stride", 1)
    oos_year: int | None = meta.get("oos_year")

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
    n_estimators = round(sum(best_iterations) / len(best_iterations) * 1.1)

    final_params: dict = {**_FIXED_PARAMS, **best_params, "n_estimators": n_estimators, "random_state": 42}

    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]

    X_train, y_train = _load_train_data(sample_dir, db_path, selected_features, target_name, row_stride)
    logger.info(
        "[fit_lgbm] Fitting %s: n_samples=%d, n_features=%d, n_estimators=%d",
        model_id, len(y_train), len(selected_features), n_estimators,
    )

    model = lgb.LGBMRegressor(**final_params)
    model.fit(X_train, y_train)

    _save_artifacts(artifact_dir, model, selected_features, final_params, n_estimators)

    if oos_year is not None:
        _score_oos(model, db_path, selected_features, target_name, oos_year, artifact_dir)
        logger.info("[fit_lgbm] OOS scoring done for year %d -> sample_oos.parquet", oos_year)

    return {
        "model_id":          model_id,
        "n_estimators":      n_estimators,
        "n_features":        len(selected_features),
        "selected_features": selected_features,
        "artifact_dir":      str(artifact_dir),
        "oos_year":          oos_year,
    }


def _load_train_data(
    sample_dir: Path,
    db_path: str,
    selected_features: list[str],
    target_name: str,
    row_stride: int,
) -> tuple[pd.DataFrame, pd.Series]:
    """Load aligned training matrix from sample parquet + quant_train DuckDB join."""
    sample_df: pd.DataFrame = pd.read_parquet(sample_dir / "sample_train_valid.parquet")
    train_valid: pd.DataFrame = pd.DataFrame(sample_df[sample_df["segment"].isin(["train", "valid"])])
    train_valid["open_time"] = pd.to_datetime(train_valid["open_time"])

    # Year-bounded query — sample is always one calendar year
    ot_min = str(train_valid["open_time"].min())
    ot_max = str(train_valid["open_time"].max())
    feat_df: pd.DataFrame = query_range(
        db_path, "quant_train",
        start=ot_min, end=ot_max,
        columns=["open_time"] + selected_features,
    )
    feat_df["open_time"] = pd.to_datetime(feat_df["open_time"])

    subset: pd.DataFrame = pd.DataFrame(train_valid[["open_time", target_name]])
    merged: pd.DataFrame = subset.merge(feat_df, on="open_time", how="inner")

    if row_stride > 1:
        merged = merged.iloc[::row_stride].reset_index(drop=True)

    return pd.DataFrame(merged[selected_features]), pd.Series(merged[target_name])


def _score_oos(
    model: lgb.LGBMRegressor,
    db_path: str,
    selected_features: list[str],
    target_name: str,
    oos_year: int,
    artifact_dir: Path,
) -> None:
    """Score model on the OOS year and save sample_oos.parquet to artifact_dir."""
    oos_start = f"{oos_year}-01-01 00:00:00"
    oos_end   = f"{oos_year}-12-31 23:59:59"

    feat_oos = query_range(
        db_path, "quant_train",
        start=oos_start, end=oos_end,
        columns=["open_time"] + selected_features,
    )
    target_oos = query_range(
        db_path, "target",
        start=oos_start, end=oos_end,
        columns=["open_time", "long_mfe_fw60", "short_mfe_fw60"],
    )

    feat_oos["open_time"]   = pd.to_datetime(feat_oos["open_time"])
    target_oos["open_time"] = pd.to_datetime(target_oos["open_time"])

    # left join: OOS rows may have null targets at horizon boundary
    oos_df = feat_oos.merge(target_oos, on="open_time", how="left")

    preds: pd.Series = pd.Series(
        model.predict(oos_df[selected_features]),
        name="pred",
    )

    if target_name == "long_mfe_fw60":
        out = pd.DataFrame({
            "open_time":      oos_df["open_time"],
            "pred_long":      preds.values,
            "long_mfe_fw60":  oos_df["long_mfe_fw60"],
            "short_mfe_fw60": oos_df["short_mfe_fw60"],
        })
    else:
        out = pd.DataFrame({
            "open_time":      oos_df["open_time"],
            "pred_short":     preds.values,
            "long_mfe_fw60":  oos_df["long_mfe_fw60"],
            "short_mfe_fw60": oos_df["short_mfe_fw60"],
        })

    out.to_parquet(artifact_dir / "sample_oos.parquet", index=False)


def _save_artifacts(
    artifact_dir: Path,
    model: lgb.LGBMRegressor,
    selected_features: list[str],
    params: dict,
    n_estimators: int,
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
