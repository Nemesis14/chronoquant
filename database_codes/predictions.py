# =============================================================================
# Load model and generate predictions from feature data
# =============================================================================
# Purpose:
#  - Load trained logistic regression model and feature list
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
#  - Load model and feature list from disk
#  - Fetch feature data from start_time onwards
#  - Compute probabilities using model.predict()
#  - Insert [open_time, pred_prob, target] into predictions table
# Parameters:
#  - start_time: "YYYY-MM-DD HH:MM:SS" (UTC)
# =============================================================================
def sync_predictions(start_time: str) -> None:
	# -------------------------------------------------------------------------
	# Load configuration
	# -------------------------------------------------------------------------
	db_cfg        = utils.load_db_config()
	model_cfg     = utils.load_models_config()
	db_path       = db_cfg["database"]["db_path"]
	table_feat    = db_cfg["database"]["tables"]["features"]
	table_pred    = db_cfg["database"]["tables"]["predictions"]
	model_dir     = model_cfg["model"]["model_dir"]
	model_file    = model_cfg["model"]["model_file"]
	features_file = model_cfg["model"]["features_file"]

	# -------------------------------------------------------------------------
	# Load model and features
	# -------------------------------------------------------------------------
	resolved_model_dir = utils._resolve_path(model_dir)
	features_path = os.path.join(resolved_model_dir, features_file)
	model_path    = os.path.join(resolved_model_dir, model_file)

	with open(features_path, "r", encoding="utf-8") as f:
		features_data = json.load(f)
	# Handle both dict {"features": [...]} and bare list [...]
	feature_list = features_data["features"] if isinstance(features_data, dict) else features_data

	with open(model_path, "rb") as f:
		model = pickle.load(f)

	# -------------------------------------------------------------------------
	# Fetch feature data
	# -------------------------------------------------------------------------
	cols_str = ", ".join([f'"{c}"' for c in (["open_time", "close", "target"] + feature_list)])

	with sqlite3.connect(db_path) as conn:
		df = pd.read_sql_query(
			f"""
			SELECT {cols_str} FROM {table_feat}
			WHERE open_time >= ? ORDER BY open_time ASC
			""",
			conn,
			params=(start_time,)
		)

	# -------------------------------------------------------------------------
	# Compute predictions
	# -------------------------------------------------------------------------
	X = df[feature_list].fillna(0)
	if "const" not in X.columns:
		X.insert(0, "const", 1.0)

	df["pred_prob"] = model.predict(X).astype(float)

	# -------------------------------------------------------------------------
	# Insert predictions
	# -------------------------------------------------------------------------

	with sqlite3.connect(db_path) as conn:
		df.to_sql(table_pred, conn, index=False, if_exists = "append")

	print(f"✅ Inserted {len(df)} predictions into '{table_pred}'")
