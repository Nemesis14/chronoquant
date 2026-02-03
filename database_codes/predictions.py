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
# sync_predictions(start_time: str) -> None
# =============================================================================
# Purpose:
#  - Load active model and feature list from disk
#  - Fetch feature data from start_time onwards
#  - Compute probabilities using configured method
#  - Insert [open_time, close, target columns, <model_id>_p] into predictions table
# Parameters:
#  - start_time: "YYYY-MM-DD HH:MM:SS" (UTC)
# =============================================================================

def sync_predictions(start_time: str) -> None:
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
			f"SELECT {cols_str} FROM {table_feat} WHERE open_time >= ? ORDER BY open_time ASC",
			conn,
			params=(start_time,)
		)

	if df.empty:
		print(f"No feature rows found since {start_time}")
		return

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

	with sqlite3.connect(db_path) as conn:
		df_out.to_sql(table_pred, conn, index=False, if_exists="append")

	print(f"Inserted {len(df_out)} predictions into '{table_pred}' ({', '.join([f'{mid}_p' for mid in active_models])})")
