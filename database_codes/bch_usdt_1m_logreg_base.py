# =============================================================================
# Logistic regression base table builder for BCH 1m (logreg_base)
# =============================================================================
import os
import json
import sqlite3
import pickle
import pandas as pd
import numpy as np
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
import utils as utils


def sync_bchusdt_1m_logreg_base(open_time_from):
	# =============================================================================
	# Config and paths
	# =============================================================================
	config = utils._load_config()

	db_cfg  = config.get("database", {})
	dev_cfg = db_cfg.get("dev_data", {})

	DB_PATH           = db_cfg.get("db_path")
	TABLE_NAME_DEV    = dev_cfg.get("table_name_dev")
	TABLE_NAME_LOGREG = config.get("model", {}).get("logreg_base_table")
	TARGET            = dev_cfg.get("target")

	this_dir           = os.path.dirname(os.path.abspath(__file__))
	repo_root          = os.path.abspath(os.path.join(this_dir, "..", ".."))
	model_features_dir = os.path.join(repo_root, "chronoquant", "model_dev", "logreg_base")

	features_json_path = os.path.join(model_features_dir, "features.json")
	model_path         = os.path.join(model_features_dir, "model.pkl")

	# =============================================================================
	# Load features and model
	# =============================================================================
	with open(features_json_path, "r", encoding="utf-8") as f:
		features_data = json.load(f)
	feature_list = features_data["features"] if isinstance(features_data, dict) else features_data

	with open(model_path, "rb") as mf:
		model = pickle.load(mf)

	select_cols = ["open_time"] + feature_list + [TARGET]

	# =============================================================================
	# Fetch data from dev table after open_time_from
	# =============================================================================
	conn = sqlite3.connect(DB_PATH)
	query = f"""
		SELECT {', '.join([f'"{c}"' for c in select_cols])}
		FROM {TABLE_NAME_DEV}
		WHERE open_time >= ?
		ORDER BY open_time ASC
	"""
	df = pd.read_sql_query(query, conn, params=(open_time_from,))
	conn.close()

	# =============================================================================
	# Compute probabilities and append results
	# =============================================================================
	X = df[feature_list].fillna(0)
	if "const" not in X.columns:
		X.insert(0, "const", 1.0)

	df["target_prob"] = model.predict(X).astype(float)

    # =============================================================================
    # Inset data into logreg_base table
    # =============================================================================
	conn = sqlite3.connect(DB_PATH)
	df.to_sql(TABLE_NAME_LOGREG, conn, index=False, if_exists="append")
	conn.close()

	print(f"Appended {len(df)} rows to '{TABLE_NAME_LOGREG}' (from {open_time_from}).")
