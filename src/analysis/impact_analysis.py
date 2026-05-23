# =============================================================================
# Converted from legacy impact analysis notebook
# =============================================================================

# --- Markdown cell 1 ---
# # Base Data

# --- Code cell 2 ---
# =============================================================================
# Query all tables from database: row count, min/max open_time
# =============================================================================

import sys
sys.path.insert(0, "..")
import utils
import sqlite3
import pandas as pd

# =============================================================================
# Load config
# =============================================================================
config = utils.load_db_config()
config["database"]["features"] = utils.load_features_config()["database"]["features"]
config["model"] = utils.load_models_config()["model"]
config["api"] = utils.load_env_config().get("api", {})
DB_PATH           = config["database"]["db_path"]
table_predictions = "bchusdt_1m_predictions"

# --- Code cell 3 ---
# Target column name (from config)
feat_cfg = utils.load_features_config()
targets = feat_cfg["database"]["features"]["targets"]
long_cfg = next((t for t in targets if t.get("direction") == "long"), None)
if not long_cfg:
    raise ValueError("No long target config found")
target_name = long_cfg.get("name") or utils.target_col_name("long", long_cfg["rolling_window"], long_cfg["percentile"])

# --- Code cell 4 ---
# =============================================================================
# parameters
# =============================================================================
open_time_from  = "2017-01-01 00:00:00"
open_time_to    = "2025-10-31 23:59:00"
ROLLING_WINDOW  = 240

# =============================================================================
# Database connection and query
# =============================================================================
conn = sqlite3.connect(DB_PATH)

query = f"""
	SELECT *
	FROM {table_predictions}
"""
df_imp_a = pd.read_sql_query(query, conn)
conn.close()

df_imp_a.tail()

# --- Markdown cell 5 ---
# # Calculation of Actions
# 
# ## Profit/Loss Rules
# 
# * **If price reaches +3% first** â†’ Realize 3% profit
# * **If price reaches -1% first** â†’ Realize 1% loss
# * **If price reaches +1% first** â†’ Wait and observe:
#     * If price then reaches +3% (overall from entry) â†’ Realize 3% profit
#     * If price then drops to -1% (overall from entry) â†’ Realize 0% (breakeven)

# --- Markdown cell 7 ---
# # Summary

# --- Code cell 8 ---
# =============================================================================
# summary
# =============================================================================
import pandas as pd

summary = pd.DataFrame({
    target_name: [0, 1, 'Total'],
    'db': [
        (df_imp_a[target_name] == 0).sum(),
        (df_imp_a[target_name] == 1).sum(),
        len(df_imp_a)
    ]
})

# Add ratio column (4 decimals)
summary['ratio'] = [
    round((df_imp_a[target_name] == 0).sum() / len(df_imp_a), 4),
    round((df_imp_a[target_name] == 1).sum() / len(df_imp_a), 4),
    1.0
]

# Add average pred_prob column
summary['avg_pred_prob'] = [
    round(df_imp_a[df_imp_a[target_name] == 0]['pred_prob'].mean(), 4),
    round(df_imp_a[df_imp_a[target_name] == 1]['pred_prob'].mean(), 4),
    round(df_imp_a['pred_prob'].mean(), 4)
]

# Format: add thousand separator to db column
summary['db'] = summary['db'].apply(lambda x: f"{x:,}")

display(summary)



