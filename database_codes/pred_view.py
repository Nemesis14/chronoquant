# =============================================================================
# Simple prediction probability viewer
# =============================================================================
# Purpose:
#  - Query the final logistic-regression table for the last 2 hours
#    and display:
#     1) a small tail of the dataframe
#     2) a simple line chart of predicted probability over time
#  - Minimal, no parameters (lookback fixed to 2 hours = 120 minutes)
#  - Plot megjelenik a printek UTÁN (display használatával)
# =============================================================================

import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display

import utils as utils  # project utils that exposes _load_config()

# Lookback (minutes)
LOOKBACK_MINUTES = 120

def run_pred_view():
    """
    Single-shot viewer (no loop, no clear_output).
    Displays logs first, then the plot below.
    """
    cfg         = utils._load_config()
    db_path     = cfg.get("database", {}).get("db_path")
    final_table = "bch_usdt_1m_logreg_base"

    if not db_path:
        print("⚠️ Database path not configured (database.db_path). Exiting.")
        return

    now = datetime.now().replace(second=0, microsecond=0)
    start_dt  = now - timedelta(minutes=LOOKBACK_MINUTES)
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"📊 Querying last {LOOKBACK_MINUTES} minutes from table '{final_table}' since {start_str}")

    try:
        with sqlite3.connect(db_path) as conn:
            sql = f"""
                SELECT "open_time", "target_prob"
                FROM "{final_table}"
                WHERE "open_time" >= ?
                ORDER BY "open_time" ASC
            """
            df = pd.read_sql_query(sql, conn, params=(start_str,))
    except Exception as e:
        print(f"❌ DB query error: {e}")
        return

    if df.empty:
        print("⚠️ No rows found for the requested interval.")
        return

    # Print statistics FIRST
    print(f"✅ Found {len(df)} data points")
    print(f"⏰ Time range: {df['open_time'].min()} to {df['open_time'].max()}")
    
    # Create the plot
    fig = plt.figure(figsize=(10, 4))
    
    try:
        x = pd.to_datetime(df["open_time"])
    except Exception:
        x = df["open_time"]
    y = pd.to_numeric(df["target_prob"], errors="coerce")

    plt.plot(x, y, label="Predicted Probability", color="dodgerblue", marker="o", markersize=3)
    plt.axhline(y=0.01, color="red", linestyle="--", linewidth=1, label="Short Threshold (0.01)")
    plt.axhline(y=0.1, color="green", linestyle="--", linewidth=1, label="Long Threshold (0.1)")
    plt.title("Predicted Probability Over Time")
    plt.xlabel("open_time")
    plt.ylabel("target_prob")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    
    # Display the plot AFTER all prints
    display(fig)
    plt.close(fig)  # Close to avoid duplicate display


