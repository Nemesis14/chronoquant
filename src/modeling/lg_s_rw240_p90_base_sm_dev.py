# =============================================================================
# Converted from legacy SHORT model development notebook
# =============================================================================

# --- Markdown cell 1 ---
# # import and data

# --- Code cell 2 ---
# =============================================================================
# imports
# =============================================================================
import sqlite3
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import utils

# =============================================================================
# Load configuration
# =============================================================================
config = utils.load_db_config()
config["database"]["features"] = utils.load_features_config()["database"]["features"]
models_cfg = utils.load_models_config().get("models", {})
MODEL_ID = "lg_s_rw240_p90_base_sm"
if MODEL_ID not in models_cfg:
    raise KeyError(f"Model not found in config/models.json: {MODEL_ID}")
config["model"] = models_cfg[MODEL_ID]
config["api"] = utils.load_env_config().get("api", {})

# -------------------------------------------------------------------------
# Extract database config
# -------------------------------------------------------------------------
DB_PATH       = config["database"]["db_path"]
tables_cfg    = config["database"]["tables"]
features_cfg  = config["database"]["features"]

# -------------------------------------------------------------------------
# Extract table names
# -------------------------------------------------------------------------
table_features    = tables_cfg["features"]
table_predictions = tables_cfg["predictions"]

# -------------------------------------------------------------------------
# Dev params
# -------------------------------------------------------------------------
open_time_from  = "2017-01-01 00:00:00"
open_time_to    = "2026-01-31 00:00:00"
ROLLING_WINDOW  = 240

# =============================================================================
# Database connection and query
# =============================================================================
conn = sqlite3.connect(DB_PATH)

query = f"""
    SELECT *
    FROM {table_features}
    WHERE open_time BETWEEN '{open_time_from}' AND '{open_time_to}'
    ORDER BY open_time ASC
"""
df_dev = pd.read_sql_query(query, conn)
conn.close()

# -------------------------------------------------------------------------
# Process dataframe
# -------------------------------------------------------------------------
df_dev["open_time"] = pd.to_datetime(df_dev["open_time"], utc=True)
df_dev.set_index("open_time", inplace=True)

# -------------------------------------------------------------------------
# Display
# -------------------------------------------------------------------------
print(f"\n{'='*70}")
print(f"LOADED DATA FROM '{table_features}'")
print(f"{'='*70}")
print(f"Shape: {df_dev.shape}")
print(f"Time range: {df_dev.index.min()} to {df_dev.index.max()}")
print("\nLast 5 rows:")
print(f"{'-'*70}")
print(df_dev.tail().to_string())
print(f"{'='*70}\n")

# --- Code cell 3 ---
# Target column name (from model config)
target_name = config["model"].get("target_name")
if not target_name:
    raise KeyError("target_name missing from model config")

# --- Markdown cell 4 ---
# # model

# --- Code cell 5 ---
# =============================================================================
# parameters for logistic elimination
# =============================================================================
import statsmodels.api as sm

p_threshold = 0.01    # p-value cutoff for keeping a variable
max_iter    = 100     # maximum elimination iterations

# =============================================================================
# prepare features list
# =============================================================================
# Expect df_dev to be available in the environment (DataFrame with feature cols and target)
features = [col for col in df_dev.columns if col.startswith("feat_")]

# =============================================================================
# CREATE df_sample BY FILTERING df_dev
# =============================================================================
# Drop rows with NA in any explanatory variable or the target
df_sample = df_dev.dropna(subset=features + [target_name])

# Exclude the most recent ROLLING_WINDOW minutes of observations
cutoff_time = df_sample.index.max() - pd.Timedelta(minutes=ROLLING_WINDOW)
df_sample   = df_sample.loc[df_sample.index < cutoff_time]

# =============================================================================
# LOGISTIC REGRESSION MODEL (iterative backward elimination)
# =============================================================================
remaining = features.copy()
removed   = []   # list of tuples (feature_name, p_value)
result    = None

for iteration in range(1, max_iter + 1):
    if not remaining:
        print("No features remaining to fit. Stopping.")
        break

    X = df_sample[remaining]
    X = sm.add_constant(X, has_constant='add')  # keep intercept
    y = df_sample[target_name]

    try:
        model = sm.Logit(y, X).fit(disp=False)
    except Exception as fit_err:
        print(f"Model fitting failed at iteration {iteration}: {fit_err}")
        # fallback: try a regularized fit to handle separation/numerical issues
        try:
            model = sm.Logit(y, X).fit_regularized(disp=False)
            print("Regularized fit succeeded as fallback.")
        except Exception as reg_err:
            print(f"Regularized fit also failed: {reg_err}. Stopping iteration.")
            break

    # p-values excluding the constant
    pvalues = model.pvalues.drop(labels=['const'], errors='ignore')

    if pvalues.empty:
        result = model
        print("No explanatory variables left after dropping constants.")
        break

    worst_var = pvalues.idxmax()
    worst_p = float(pvalues.max())

    print(f"Iteration {iteration}: {len(remaining)} features. Worst p-value = {worst_p:.6f} ({worst_var})")

    if worst_p <= p_threshold:
        result = model
        print(f"All remaining p-values <= {p_threshold}. Stopping elimination.")
        break

    # remove the worst variable and continue
    removed.append((worst_var, worst_p))
    remaining.remove(worst_var)

else:
    # reached max_iter without meeting the threshold
    print("Reached maximum iterations without satisfying p-value threshold.")
    try:
        X_final = sm.add_constant(df_sample[remaining], has_constant='add')
        result = sm.Logit(df_sample[target_name], X_final).fit(disp=False)
    except Exception as final_err:
        print("Final fit after max iterations failed:", final_err)
        result = None

# =============================================================================
# reporting results
# =============================================================================
print("\nRemoved features (in order):")
for name, pv in removed:
    print(f" - {name}: p = {pv:.6g}")

print("\nRemaining features:")
print(remaining)

if result is not None:
    try:
        print(result.summary().tables[1])
    except Exception:
        print(result.summary())
else:
    print("No final model available to display.")

# --- Markdown cell 6 ---
# # exports

# --- Code cell 7 ---
import json

# =============================================================================
# export only remaining feature names to JSON
# =============================================================================
output_path = Path("features.json")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(remaining, f, ensure_ascii=False, indent=4)

print(f"\nOK: Remaining feature names saved to: {output_path.resolve()}")


# =============================================================================
# Save compact statsmodels artifact
# =============================================================================

import pickle

# Model output path
model_path = "model.pkl"

# Warm up summary statistics before removing data
try:
    _ = result.summary()
except Exception:
    pass

if result is None:
    raise RuntimeError("No fitted model available to save")

# Primary save path: statsmodels native save without training data
try:
    result.save(model_path, remove_data=True)
    print(f"OK: Slim model saved to: {model_path} (via save(remove_data=True))")

# Fallback when save(remove_data=True) is unavailable
except Exception:
    print("WARN: save(remove_data=True) unavailable, using pickle fallback...")
    try:
        result.remove_data()
    except Exception:
        pass
    with open(model_path, "wb") as f:
        pickle.dump(result, f)
    print(f"OK: Slim model saved to: {model_path} (via pickle fallback)")




