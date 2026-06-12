"""Load champion models and generate unified long/short predictions from feature data.

Resolves the asset-specific champion long and short model, loads feature rows
once with both target columns, runs inference for each model, and writes a
single unified row per open_time with stable long_pred/short_pred columns.
Idempotent by open_time — safe to re-run.
"""

import json
import logging
import os
import pickle

import pandas as pd

import utils
from store.duckdb_query import query_range
from store.parquet_store import upsert_partition

logger = logging.getLogger(__name__)

_LONG_PRED_COL  = "long_pred"
_SHORT_PRED_COL = "short_pred"


# %% Public entry point


def sync_predictions(
    start_time : str,
    end_time   : str | None = None,
    asset_id   : str | None = None,
) -> None:
    """Sync predictions for the champion long and short models.

    Resolves the champion long and short model for the asset, loads feature
    rows once with both target columns, runs inference, and writes a single
    unified df per open_time with stable long_pred/short_pred columns.

    Args:
        start_time : Start of the prediction window (UTC string).
        end_time   : End of the prediction window (UTC string, or None for latest).
        asset_id   : Asset ID from config/assets.json; uses default if None.
    """
    # --- config ---
    resolved_asset = utils.resolve_asset_id(asset_id)
    db_cfg         = utils.load_asset_config(resolved_asset)
    model_cfg      = utils.load_models_config()
    data_dir       = db_cfg["database"]["data_dir"]

    # --- champion model resolution ---
    try:
        long_id, long_meta, short_id, short_meta = utils.champion_models_for_asset(
            model_cfg, resolved_asset
        )
    except ValueError as exc:
        logger.warning("Champion model feloldas sikertelen: %s", exc)
        return

    long_target  = long_meta["target_name"]
    short_target = short_meta["target_name"]

    # --- load model artifacts ---
    long_artifacts = _load_model_artifacts(long_id, long_meta)
    if long_artifacts is None:
        return
    long_model, long_feat_list = long_artifacts

    short_artifacts = _load_model_artifacts(short_id, short_meta)
    if short_artifacts is None:
        return
    short_model, short_feat_list = short_artifacts

    # --- feature query (single pass, both targets) ---
    all_features = list(dict.fromkeys(long_feat_list + short_feat_list))
    select_cols  = list(dict.fromkeys(
        ["open_time", "close", long_target, short_target] + all_features
    ))

    df = query_range(data_dir, "features", start=start_time, end=end_time, columns=select_cols)
    if df.empty:
        logger.warning(
            "Nincs feature sor: asset_id=%s start_time=%s", resolved_asset, start_time
        )
        return

    df = df.drop_duplicates(subset=["open_time"], keep="last").reset_index(drop=True)

    # --- inference ---
    long_proba  = _run_inference(df, long_feat_list,  long_model,  long_meta)
    short_proba = _run_inference(df, short_feat_list, short_model, short_meta)

    # --- unified output (index-aligned, single upsert) ---
    df_out = pd.DataFrame({
        "open_time"    : df["open_time"],
        "close"        : df["close"],
        long_target    : df[long_target],
        short_target   : df[short_target],
        _LONG_PRED_COL : pd.to_numeric(pd.Series(long_proba,  dtype=object), errors="coerce"),
        _SHORT_PRED_COL: pd.to_numeric(pd.Series(short_proba, dtype=object), errors="coerce"),
    })

    written = upsert_partition(data_dir, "predictions", df_out)
    logger.info(
        "OK: %d predikció irva (long=%s, short=%s), %d sor a particiokban",
        len(df_out), long_id, short_id, written,
    )


# %% Private helpers


def _load_model_artifacts(
    model_id   : str,
    model_meta : dict,
) -> tuple[object, list[str]] | None:
    """Load the pickled model and feature list for a model config entry.

    Args:
        model_id   : Model ID string (used only for log messages).
        model_meta : Model metadata dict from models.json.

    Returns:
        (model, feature_list) on success, (None, None) if artifacts are missing.
    """
    paths     = model_meta["paths"]
    model_dir = utils._resolve_path(paths["model_dir"])
    feat_path = os.path.join(model_dir, paths["features_file"])
    mdl_path  = os.path.join(model_dir, paths["model_file"])

    if not os.path.exists(feat_path) or not os.path.exists(mdl_path):
        logger.warning("Model artifact nem talalhato: model_id=%s, kihagyva", model_id)
        return None

    with open(feat_path, encoding="utf-8") as f:
        features_data = json.load(f)
    with open(mdl_path, "rb") as f:
        model = pickle.load(f)

    feature_list = _feature_list_for_prediction(
        features_data = features_data,
        model         = model,
        trainer       = model_meta.get("trainer", ""),
    )
    return model, feature_list


def _run_inference(
    df           : pd.DataFrame,
    feature_list : list[str],
    model        : object,
    model_meta   : dict,
) -> object:
    """Run model inference and return a probability array.

    Args:
        df           : Feature DataFrame (all rows, all columns available).
        feature_list : Columns to use as model input.
        model        : Loaded model object.
        model_meta   : Model metadata dict from models.json.

    Returns:
        Array-like of probability values aligned to df rows.
    """
    trainer        = model_meta.get("trainer", "")
    predict_method = model_meta.get("predict", {}).get("method", "predict")

    X = df[feature_list].apply(pd.to_numeric, errors="coerce").fillna(0)
    if trainer.startswith("statsmodels") and "const" not in X.columns:
        X.insert(0, "const", 1.0)

    if predict_method == "predict_proba":
        proba = model.predict_proba(X)  # type: ignore[union-attr]
        if hasattr(proba, "shape") and len(proba.shape) == 2:
            proba = proba[:, 1]
    else:
        proba = model.predict(X)  # type: ignore[union-attr]

    return proba


def _feature_list_for_prediction(
    features_data : dict | list,
    model         : object,
    trainer       : str = "",
) -> list[str]:
    """Resolve the ordered feature list for prediction from artifact metadata.

    Args:
        features_data : Loaded features.json payload (dict or list).
        model         : Loaded model object (may expose feature_names_in_).
        trainer       : Trainer string from model metadata.

    Returns:
        Ordered list of feature column names.
    """
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
    """Extract the raw feature name list from a features.json payload.

    Args:
        features_data : Loaded features.json (dict with 'features' key, or list).

    Returns:
        List of feature column names.
    """
    if isinstance(features_data, dict):
        return list(features_data["features"])
    return list(features_data)
