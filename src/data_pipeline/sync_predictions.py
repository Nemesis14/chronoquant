# =============================================================================
# Load models and generate predictions from feature data
# =============================================================================
# Purpose:
#  - Load all active models for the asset and generate prediction columns
#  - Each model writes to its own {model_id}_p column in the predictions table
#  - The live/primary model also writes to the generic "prediction" column
# =============================================================================

import os
import json
import pickle
import pandas as pd

import utils
from db.table_ops import ensure_table_columns, sqlite_connect, table_exists


def sync_predictions(
    start_time: str,
    end_time: str | None = None,
    asset_id: str | None = None,
) -> None:
    db_cfg     = utils.load_asset_config(asset_id)
    model_cfg  = utils.load_models_config()
    db_path    = db_cfg["database"]["db_path"]
    table_feat = db_cfg["database"]["tables"]["features"]
    table_pred = db_cfg["database"]["tables"]["predictions"]

    active_models = _active_models_for_asset(model_cfg, asset_id)
    if not active_models:
        print(f"No active models found for asset_id={asset_id!r}")
        return

    live_model_id = _resolve_live_model_id(model_cfg, asset_id)

    for model_id, model_meta in active_models.items():
        _sync_single_model(
            model_id=model_id,
            model_meta=model_meta,
            db_path=db_path,
            table_feat=table_feat,
            table_pred=table_pred,
            start_time=start_time,
            end_time=end_time,
            is_live=(model_id == live_model_id),
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


def _resolve_live_model_id(model_cfg: dict, asset_id: str | None) -> str | None:
    try:
        return utils.live_model_id(model_cfg=model_cfg, asset_id=asset_id)
    except ValueError:
        return None


def _sync_single_model(
    model_id: str,
    model_meta: dict,
    db_path: str,
    table_feat: str,
    table_pred: str,
    start_time: str,
    end_time: str | None,
    is_live: bool,
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
        print(f"Model artifacts not found for {model_id}, skipping")
        return

    with open(features_path, "r", encoding="utf-8") as f:
        features_data = json.load(f)
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    feature_list = _feature_list_for_prediction(
        features_data=features_data,
        model=model,
        trainer=model_meta.get("trainer", ""),
    )

    select_cols = ["open_time", "close", target_col] + feature_list
    select_cols = list(dict.fromkeys(select_cols))
    cols_str = ", ".join([f'"{c}"' for c in select_cols])

    with sqlite_connect(db_path) as conn:
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
        print(f"No feature rows found for {model_id} since {start_time}")
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

    pred_col = utils.prediction_col_name(model_id)
    live_cols = utils.live_prediction_columns()

    df_out = df[["open_time", "close"]].copy()
    df_out[live_cols["target"]] = df[target_col]
    df_out[pred_col] = pd.to_numeric(proba, errors="coerce").astype(float)
    if is_live:
        df_out[live_cols["prediction"]] = df_out[pred_col]

    ensure_table_columns(db_path, table_pred, df_out)

    update_cols = [pred_col] + ([live_cols["prediction"]] if is_live else [])
    new_count, updated_count = _write_predictions(db_path, table_pred, df_out, update_cols)

    if new_count:
        print(f"Inserted {new_count} predictions into '{table_pred}' ({model_id} -> {pred_col})")
    if updated_count:
        print(f"Updated {updated_count} existing predictions in '{table_pred}' ({model_id} -> {pred_col})")
    if not new_count and not updated_count:
        print(f"No new prediction rows to write for {model_id} into '{table_pred}'")


def _write_predictions(
    db_path: str,
    table_name: str,
    df: pd.DataFrame,
    update_cols: list[str],
) -> tuple[int, int]:
    min_time = df["open_time"].min()
    max_time = df["open_time"].max()

    if table_exists(db_path, table_name):
        with sqlite_connect(db_path) as conn:
            existing = pd.read_sql_query(
                f'SELECT open_time FROM "{table_name}" WHERE open_time BETWEEN ? AND ?',
                conn,
                params=(min_time, max_time),
            )
        existing_times = set(existing["open_time"].astype(str))
    else:
        existing_times = set()

    mask = df["open_time"].astype(str).isin(existing_times)
    df_new = df[~mask]
    df_update = df[mask]

    new_count = 0
    updated_count = 0

    if not df_new.empty:
        with sqlite_connect(db_path) as conn:
            df_new.to_sql(table_name, conn, index=False, if_exists="append")
        new_count = len(df_new)

    if not df_update.empty:
        set_clause = ", ".join([f'"{col}" = ?' for col in update_cols])
        rows = [
            tuple(row[col] for col in update_cols) + (row["open_time"],)
            for _, row in df_update.iterrows()
        ]
        with sqlite_connect(db_path) as conn:
            conn.executemany(
                f'UPDATE "{table_name}" SET {set_clause} WHERE open_time = ?',
                rows,
            )
            conn.commit()
        updated_count = len(df_update)

    return new_count, updated_count


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


def _features_from_payload(features_data) -> list[str]:
    if isinstance(features_data, dict):
        return list(features_data["features"])
    return list(features_data)
