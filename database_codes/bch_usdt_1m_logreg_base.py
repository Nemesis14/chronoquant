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
import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
import utils as utils

# -------------------------------------------------------------------------
# NOTE:
# - This module provides a function to build/append a "final" feature table
#   for a logistic-regression base dataset.
# - The function queries the dev table for rows in a provided open_time range,
#   selects the feature columns listed in a features.json located in:
#       <repo_root>/chronoquant/model_dev/logreg_base/features.json
#   and the target column (name is read from config).
# - The module also loads a trained model from model.pkl in the same folder
#   and computes a 'target_prob' for every row which is appended to the final
#   table together with the other columns.
# -------------------------------------------------------------------------

def create_logreg_base(from_open_time, to_open_time):
    # ---------------------------------------------------------------------
    # Purpose:
    #   Query the dev table between from_open_time and to_open_time,
    #   select feature columns (from features.json) and the target column,
    #   compute predicted probability using a persisted model (model.pkl),
    #   then append the resulting rows to the final table in the sqlite DB.
    #
    # Inputs:
    #   - from_open_time: string, formatted timestamp "YYYY-MM-DD HH:MM:SS"
    #   - to_open_time:   string, formatted timestamp "YYYY-MM-DD HH:MM:SS"
    #
    # Behavior:
    #   - Reads DB path and dev table name from utils._load_config()
    #   - Reads target column name from config under database.dev_data.target
    #   - Reads features.json from chronoquant/model_dev/logreg_base
    #   - Loads model.pkl from the same folder and computes target_prob for
    #     each row (simple predict logic)
    #   - Appends results into final table using pandas.DataFrame.to_sql(if_exists='append')
    # ---------------------------------------------------------------------

    # ---------------------------------------------------------------------
    # Load configuration values
    # ---------------------------------------------------------------------
    cfg         = utils._load_config()
    db_path     = cfg.get("database", {}).get("db_path")
    dev_table   = cfg.get("database", {}).get("dev_data", {}).get("table_name_dev")
    target_col  = cfg.get("database", {}).get("dev_data", {}).get("target")
    final_table = "bch_usdt_1m_logreg_base"

    if not db_path:
        raise RuntimeError("Database path not found in configuration (database.db_path).")
    if not dev_table:
        raise RuntimeError("Dev table name not found in configuration (database.dev_data.table_name_dev).")
    if not target_col:
        raise RuntimeError("Target column name not found in configuration (database.dev_data.target).")

    # ---------------------------------------------------------------------
    # Resolve repository root and model/features directory
    # ---------------------------------------------------------------------
    this_dir           = os.path.dirname(os.path.abspath(__file__))
    repo_root          = os.path.abspath(os.path.join(this_dir, "..", ".."))
    model_features_dir = os.path.join(repo_root, "chronoquant", "model_dev", "logreg_base")

    if not os.path.isdir(model_features_dir):
        raise FileNotFoundError(f"Model features directory not found: {model_features_dir}")

    features_json_path = os.path.join(model_features_dir, "features.json")
    if not os.path.isfile(features_json_path):
        raise FileNotFoundError(f"features.json not found at: {features_json_path}")

    model_path = os.path.join(model_features_dir, "model.pkl")
    if not os.path.isfile(model_path):
        raise FileNotFoundError(f"model.pkl not found at: {model_path}")

    # ---------------------------------------------------------------------
    # Load feature list from features.json
    # ---------------------------------------------------------------------
    with open(features_json_path, "r", encoding="utf-8") as f:
        features_data = json.load(f)

    if isinstance(features_data, dict) and "features" in features_data:
        feature_list = features_data["features"]
    elif isinstance(features_data, list):
        feature_list = features_data
    else:
        raise RuntimeError("features.json must be either a list of feature names or an object with 'features' list.")

    if not feature_list:
        raise RuntimeError("No features found in features.json.")

    # Ensure target is included in the selected columns (target is expected in dev table)
    select_cols = ["open_time"] + feature_list + [target_col]

    # ---------------------------------------------------------------------
    # Build SQL query to fetch required columns from dev table
    # ---------------------------------------------------------------------
    cols_sql = ", ".join([f'"{c}"' for c in select_cols])
    query = f'SELECT {cols_sql} FROM "{dev_table}" WHERE "open_time" BETWEEN ? AND ? ORDER BY "open_time" ASC'

    # ---------------------------------------------------------------------
    # Execute query and load into pandas DataFrame
    # ---------------------------------------------------------------------
    with sqlite3.connect(db_path) as conn:
        try:
            df = pd.read_sql_query(query, conn, params=(from_open_time, to_open_time))
        except Exception as e:
            raise RuntimeError(f"Error querying dev table: {e}")

    # ---------------------------------------------------------------------
    # Sanity checks: ensure target and features exist in returned frame
    # ---------------------------------------------------------------------
    missing = [c for c in select_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"The following expected columns are missing from the query result: {missing}")

    # ---------------------------------------------------------------------
    # Load persisted model (simple load)
    # ---------------------------------------------------------------------
    try:
        with open(model_path, "rb") as mf:
            model = pickle.load(mf)
    except Exception as e:
        raise RuntimeError(f"Failed to load model from {model_path}: {e}")

    # ---------------------------------------------------------------------
    # Prepare features and compute probabilities (simplified)
    # - Keep it minimal: select feature columns, fillna(0), call model.
    # - If model has predict_proba use it, otherwise use predict.
    # - If predict returns class labels, convert to 0/1 probs.
    # ---------------------------------------------------------------------
    # prepare features and add statsmodels constant
    X = df[feature_list].fillna(0)

    # Ensure a statsmodels-style constant column is present
    if "const" not in X.columns:
        X.insert(0, "const", 1.0)

    # Predict probabilities with the statsmodels Logit model (model.predict returns probs for Logit)
    probs = model.predict(X)

    # Attach predicted probability column
    df["target_prob"] = pd.Series(probs, index=df.index).astype(float)
    
    # ---------------------------------------------------------------------
    # Append results to the final table in the sqlite database
    # ---------------------------------------------------------------------
    with sqlite3.connect(db_path) as conn:
        try:
            df.to_sql(final_table, conn, index=False, if_exists="append")
        except Exception as e:
            raise RuntimeError(f"Error appending to final table '{final_table}': {e}")

    # ---------------------------------------------------------------------
    # Report summary
    # ---------------------------------------------------------------------
    row_count = len(df)
    print(f"Appended {row_count} rows to '{final_table}' (from {from_open_time} to {to_open_time}).")