# =============================================================================
# Load model and generate predictions from feature data
# =============================================================================
# Purpose:
#  - Load active model and feature list
#  - Fetch feature rows from database
#  - Compute prediction probabilities
#  - Insert predictions into database
# =============================================================================

import os
import json
import sqlite3
import pickle
import pandas as pd

import utils

# =============================================================================
# _drop_existing_open_times(df: pd.DataFrame, db_path: str, table_name: str) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Remove rows whose open_time already exists in the target table
#  - Keep sync_predictions idempotent when source features contain overlap
# Parameters:
#  - df: DataFrame prepared for database insert
#  - db_path: SQLite database path
#  - table_name: target predictions table
# =============================================================================
def _drop_existing_open_times(df: pd.DataFrame, db_path: str, table_name: str) -> pd.DataFrame:
	if df.empty:
		return df

	min_time = df["open_time"].min()
	max_time = df["open_time"].max()

	with sqlite3.connect(db_path) as conn:
		table_exists = conn.execute(
			"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
			(table_name,)
		).fetchone()
		if table_exists is None:
			return df

		existing = pd.read_sql_query(
			f"""
			SELECT open_time FROM {table_name}
			WHERE open_time BETWEEN ? AND ?
			""",
			conn,
			params=(min_time, max_time)
		)

	if existing.empty:
		return df

	existing_times = set(existing["open_time"].astype(str))
	return df[~df["open_time"].astype(str).isin(existing_times)].copy()

# =============================================================================
# sync_predictions(start_time: str, end_time: str | None = None) -> None
# =============================================================================
# Purpose:
#  - Load active model and feature list from disk
#  - Fetch feature data from start_time onwards
#  - Compute probabilities using configured method
#  - Insert [open_time, close, target columns, <model_id>_p] into predictions table
# Parameters:
#  - start_time: "YYYY-MM-DD HH:MM:SS" (UTC)
#  - end_time: optional "YYYY-MM-DD HH:MM:SS" upper bound for controlled rebuilds
# =============================================================================

def sync_predictions(start_time: str, end_time: str | None = None) -> None:
	# -------------------------------------------------------------------------
	# Load configuration
	# -------------------------------------------------------------------------
	db_cfg     = utils.load_db_config()
	feat_cfg   = utils.load_features_config()
	model_cfg  = utils.load_models_config()
	db_path    = db_cfg["database"]["db_path"]
	table_feat = db_cfg["database"]["tables"]["features"]
	table_pred = db_cfg["database"]["tables"]["predictions"]

	models = model_cfg.get("models", {})
	if not models:
		raise ValueError("No models defined in config/models.json")

	active_models = [mid for mid, meta in models.items() if meta.get("active")]
	if len(active_models) == 0:
		raise ValueError("No active models found in config/models.json")

	active_meta = {}
	all_features = []
	for model_id in active_models:
		model_meta = models[model_id]
		paths      = model_meta["paths"]
		model_dir  = paths["model_dir"]
		feat_file  = paths["features_file"]
		resolved_model_dir = utils._resolve_path(model_dir)
		features_path      = os.path.join(resolved_model_dir, feat_file)
		with open(features_path, "r", encoding="utf-8") as f:
			features_data = json.load(f)
		feature_list = features_data["features"] if isinstance(features_data, dict) else features_data
		active_meta[model_id] = {
			"model_meta": model_meta,
			"model_dir": model_dir,
			"features": feature_list
		}
		all_features.extend(feature_list)
	all_features = list(dict.fromkeys(all_features))

	# -------------------------------------------------------------------------
	# Fetch feature data
	# -------------------------------------------------------------------------
	target_cols = utils.target_columns_from_config(feat_cfg)
	cols_str = ", ".join([f'"{c}"' for c in (["open_time", "close"] + target_cols + all_features)])

	with sqlite3.connect(db_path) as conn:
		df = pd.read_sql_query(
			f"""
			SELECT {cols_str} FROM {table_feat}
			WHERE open_time >= ?
				AND (? IS NULL OR open_time <= ?)
			ORDER BY open_time ASC
			""",
			conn,
			params=(start_time, end_time, end_time)
		)

	if df.empty:
		print(f"No feature rows found since {start_time}")
		return

	df = df.drop_duplicates(subset=["open_time"], keep="last").copy()

	# -------------------------------------------------------------------------
	# Insert predictions (no feature columns)
	# -------------------------------------------------------------------------
	out_cols = ["open_time", "close"] + target_cols
	df_out = df[out_cols].copy()

	for model_id in active_models:
		meta         = active_meta[model_id]["model_meta"]
		model_dir    = meta["paths"]["model_dir"]
		model_file   = meta["paths"]["model_file"]
		feature_list = active_meta[model_id]["features"]
		trainer      = meta.get("trainer", "")
		predict_cfg  = meta.get("predict", {})
		predict_method = predict_cfg.get("method", "predict")

		resolved_model_dir = utils._resolve_path(model_dir)
		model_path         = os.path.join(resolved_model_dir, model_file)
		with open(model_path, "rb") as f:
			model = pickle.load(f)

		X = df[feature_list].fillna(0)
		if trainer == "statsmodels" and "const" not in X.columns:
			X.insert(0, "const", 1.0)

		if predict_method == "predict_proba":
			proba = model.predict_proba(X)
			if hasattr(proba, "shape") and len(proba.shape) == 2:
				proba = proba[:, 1]
		else:
			proba = model.predict(X)

		pred_col = f"{model_id}_p"
		df_out[pred_col] = pd.to_numeric(proba, errors="coerce").astype(float)

	df_out = _drop_existing_open_times(df_out, db_path, table_pred)
	if df_out.empty:
		print(f"No new prediction rows to insert into '{table_pred}'")
		return

	with sqlite3.connect(db_path) as conn:
		df_out.to_sql(table_pred, conn, index=False, if_exists="append")

	print(f"Inserted {len(df_out)} predictions into '{table_pred}' ({', '.join([f'{mid}_p' for mid in active_models])})")



