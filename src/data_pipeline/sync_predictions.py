# =============================================================================
# Load model and generate predictions from feature data
# =============================================================================
# Purpose:
#  - Load the single runtime model and feature list
#  - Fetch feature rows from database
#  - Compute prediction probabilities
#  - Insert live prediction rows into database
# =============================================================================

import os
import json
import sqlite3
import pickle
import pandas as pd

import utils
from db.table_ops import drop_existing_open_times, ensure_table_columns

# =============================================================================
# sync_predictions(start_time: str, end_time: str | None = None) -> None
# =============================================================================
# Purpose:
#  - Load the configured runtime model and feature list from disk
#  - Fetch feature data from start_time onwards
#  - Compute probabilities using configured method
#  - Insert generic [open_time, close, target, prediction] rows into the live
#    predictions table
# Parameters:
#  - start_time: "YYYY-MM-DD HH:MM:SS" (UTC)
#  - end_time: optional "YYYY-MM-DD HH:MM:SS" upper bound for controlled rebuilds
# =============================================================================

def sync_predictions(start_time: str, end_time: str | None = None) -> None:
    # -------------------------------------------------------------------------
    # Load configuration
    # -------------------------------------------------------------------------
    db_cfg     = utils.load_db_config()
    model_cfg  = utils.load_models_config()
    db_path    = db_cfg["database"]["db_path"]
    table_feat = db_cfg["database"]["tables"]["features"]
    table_pred = db_cfg["database"]["tables"]["predictions"]

    models = model_cfg.get("models", {})
    if not models:
        raise ValueError("No models defined in config/models.json")

    model_id, model_meta = utils.live_model_meta(model_cfg)
    target_col = model_meta["target_name"]
    paths      = model_meta["paths"]
    model_dir  = paths["model_dir"]
    feat_file  = paths["features_file"]
    model_file = paths["model_file"]
    resolved_model_dir = utils._resolve_path(model_dir)
    features_path      = os.path.join(resolved_model_dir, feat_file)
    model_path         = os.path.join(resolved_model_dir, model_file)

    with open(features_path, "r", encoding="utf-8") as f:
        features_data = json.load(f)
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    feature_list = _feature_list_for_prediction(
        features_data = features_data,
        model         = model,
        trainer       = model_meta.get("trainer", ""),
    )

    # -------------------------------------------------------------------------
    # Fetch feature data
    # -------------------------------------------------------------------------
    select_cols = ["open_time", "close", target_col] + feature_list
    select_cols = list(dict.fromkeys(select_cols))
    cols_str = ", ".join([f'"{c}"' for c in select_cols])

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            f"""
            SELECT {cols_str} FROM {table_feat}
            WHERE open_time >= ?
                AND (? IS NULL OR open_time <= ?)
            ORDER BY open_time ASC
            """,
            conn,
            params=(start_time, end_time, end_time),
        )

    if df.empty:
        print(f"No feature rows found since {start_time}")
        return

    df = df.drop_duplicates(subset=["open_time"], keep="last").copy()

    # -------------------------------------------------------------------------
    # Insert live prediction rows (no feature columns)
    # -------------------------------------------------------------------------
    trainer        = model_meta.get("trainer", "")
    predict_cfg    = model_meta.get("predict", {})
    predict_method = predict_cfg.get("method", "predict")

    X = df[feature_list].fillna(0)
    if trainer.startswith("statsmodels") and "const" not in X.columns:
        X.insert(0, "const", 1.0)

    if predict_method == "predict_proba":
        proba = model.predict_proba(X)
        if hasattr(proba, "shape") and len(proba.shape) == 2:
            proba = proba[:, 1]
    else:
        proba = model.predict(X)

    live_cols = utils.live_prediction_columns()
    df_out = df[["open_time", "close"]].copy()
    df_out[live_cols["target"]] = df[target_col]
    df_out[live_cols["prediction"]] = pd.to_numeric(proba, errors="coerce").astype(float)

    ensure_table_columns(db_path, table_pred, df_out)
    df_out = drop_existing_open_times(df_out, db_path, table_pred)
    if df_out.empty:
        print(f"No new prediction rows to insert into '{table_pred}'")
        return

    with sqlite3.connect(db_path) as conn:
        df_out.to_sql(table_pred, conn, index=False, if_exists="append")

    print(f"Inserted {len(df_out)} predictions into '{table_pred}' ({model_id} -> prediction)")


# =============================================================================
# _feature_list_for_prediction(features_data: dict | list, model, trainer: str = "") -> list[str]
# =============================================================================
# Purpose:
#  - Resolve the exact feature columns required by a saved model artifact
# =============================================================================
def _feature_list_for_prediction(features_data, model, trainer: str = "") -> list[str]:
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


# =============================================================================
# _features_from_payload(features_data: dict | list) -> list[str]
# =============================================================================
# Purpose:
#  - Read legacy list-style and newer dict-style features.json artifacts
# =============================================================================
def _features_from_payload(features_data) -> list[str]:
    if isinstance(features_data, dict):
        return list(features_data["features"])
    return list(features_data)



