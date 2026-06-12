# =============================================================================
# Load models and generate predictions from feature data
# =============================================================================
# Purpose:
#  - Load all active models for the asset and generate prediction columns
#  - Each model writes to its own {model_id}_p column in the predictions dataset
# =============================================================================

import json
import logging
import os
import pickle
from store.duckdb_query import query_range
from store.parquet_store import upsert_partition

import pandas as pd

import utils

logger = logging.getLogger(__name__)


def sync_predictions(
    start_time: str,
    end_time: str | None = None,
    asset_id: str | None = None,
) -> None:
    db_cfg     = utils.load_asset_config(asset_id)
    model_cfg  = utils.load_models_config()
    data_dir   = db_cfg["database"]["data_dir"]

    active_models = _active_models_for_asset(model_cfg, asset_id)
    if not active_models:
        logger.warning("Nincs aktiv model: asset_id=%r", asset_id)
        return

    for model_id, model_meta in active_models.items():
        _sync_single_model(
            model_id   = model_id,
            model_meta = model_meta,
            data_dir   = data_dir,
            start_time = start_time,
            end_time   = end_time,
        )


def _active_models_for_asset(model_cfg: dict, asset_id: str | None) -> dict:
    models = model_cfg.get("models", {})
    result = {}
    for mid, meta in models.items():
        if not meta.get("active", False):
            continue
        if meta.get("asset_id") != asset_id:
            continue
        result[mid] = meta
    return result


def _sync_single_model(
    model_id   : str,
    model_meta : dict,
    data_dir   : str,
    start_time : str,
    end_time   : str | None,
) -> None:
    target_col = model_meta["target_name"]
    paths      = model_meta["paths"]
    model_dir  = paths["model_dir"]
    feat_file  = paths["features_file"]
    model_file = paths["model_file"]

    resolved_model_dir = utils._resolve_path(model_dir)
    features_path      = os.path.join(resolved_model_dir, feat_file)
    model_path         = os.path.join(resolved_model_dir, model_file)

    if not os.path.exists(features_path) or not os.path.exists(model_path):
        logger.warning("Model artifact nem talalhato: model_id=%s, kihagyva", model_id)
        return

    with open(features_path, encoding="utf-8") as f:
        features_data = json.load(f)
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    feature_list = _feature_list_for_prediction(
        features_data = features_data,
        model         = model,
        trainer       = model_meta.get("trainer", ""),
    )

    select_cols = ["open_time", "close", target_col] + feature_list
    select_cols = list(dict.fromkeys(select_cols))

    df = query_range(data_dir, "features", start=start_time, end=end_time, columns=select_cols)

    if df.empty:
        logger.warning("Nincs feature sor: model_id=%s start_time=%s", model_id, start_time)
        return

    df = df.drop_duplicates(subset=["open_time"], keep="last").copy()

    trainer        = model_meta.get("trainer", "")
    predict_method = model_meta.get("predict", {}).get("method", "predict")

    X = df[feature_list].apply(pd.to_numeric, errors="coerce").fillna(0)
    if trainer.startswith("statsmodels") and "const" not in X.columns:
        X.insert(0, "const", 1.0)

    if predict_method == "predict_proba":
        proba = model.predict_proba(X)
        if hasattr(proba, "shape") and len(proba.shape) == 2:
            proba = proba[:, 1]
    else:
        proba = model.predict(X)

    pred_col  = utils.prediction_col_name(model_id)
    live_cols = utils.live_prediction_columns()

    df_out: pd.DataFrame       = pd.DataFrame(df[["open_time", "close"]])
    df_out[live_cols["target"]] = df[target_col]
    df_out[target_col]          = df[target_col]
    df_out[pred_col]            = pd.to_numeric(pd.Series(proba), errors="coerce")

    written = upsert_partition(data_dir, "predictions", df_out)
    logger.info("OK: %d predikció irva (model=%s -> %s), %d sor a particiokban", len(df_out), model_id, pred_col, written)


def _feature_list_for_prediction(features_data: dict | list, model: object, trainer: str = "") -> list[str]:
    payload_features = _features_from_payload(features_data)
    if trainer.startswith("statsmodels"):
        return payload_features

    input_features = (
        features_data.get("input_features")
        if isinstance(features_data, dict)
        else None
    )
    if input_features:
        return list(input_features)

    feature_names_in = getattr(model, "feature_names_in_", None)
    if feature_names_in is not None and len(feature_names_in) > 0:
        return list(feature_names_in)

    feature_name = getattr(model, "feature_name_", None)
    if feature_name is not None and len(feature_name) > 0:
        return list(feature_name)

    return payload_features


def _features_from_payload(features_data: dict | list) -> list[str]:
    if isinstance(features_data, dict):
        return list(features_data["features"])
    return list(features_data)
