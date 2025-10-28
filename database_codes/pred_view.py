# =============================================================================
# Simple prediction probability viewer
# =============================================================================
# Purpose:
#  - Query the final logistic-regression table for the last 2 hours
#    and display:
#     1) a small tail of the dataframe
#     2) a simple line chart of predicted probability over time
#  - Minimal, no parameters (lookback fixed to 2 hours = 120 minutes)
#  - Intended to be run interactively (Jupyter / simple script)
# =============================================================================

import time
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import clear_output, display

import utils as utils  # project utils that exposes _load_config()

# Lookback (minutes)
LOOKBACK_MINUTES = 120
# Refresh interval in seconds
REFRESH_SECONDS = 5

def run_pred_view():
    # =============================================================================
    # Minimal interactive viewer loop:
    # Loads DB path and final table name from utils._load_config()
    # Every REFRESH_SECONDS seconds queries last LOOKBACK_MINUTES of rows
    # (open_time and target_prob) from the final table and displays them.
    # Draws a simple line plot of target_prob vs open_time.
    # =============================================================================
    cfg         = utils._load_config()
    db_path     = cfg.get("database", {}).get("db_path")
    final_table = "bch_usdt_1m_logreg_base"

    if not db_path:
        print("Database path not configured (database.db_path). Exiting.")
        return

    try:
        while True:
            clear_output(wait=True)
            now = datetime.now().replace(second=0, microsecond=0)
            start_dt  = now - timedelta(minutes=LOOKBACK_MINUTES)
            start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")

            print("🔁 Pred view refresh at", now.strftime("%Y-%m-%d %H:%M:%S"))
            print(f"Querying last {LOOKBACK_MINUTES} minutes from table '{final_table}' since {start_str}")

            if not db_path or not db_path or not isinstance(db_path, str):
                print("Invalid database path.")
                time.sleep(REFRESH_SECONDS)
                continue

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
                print(f"DB query error: {e}")
                time.sleep(REFRESH_SECONDS)
                continue

            if df.empty:
                print("No rows found for the requested interval.")
            else:
                # show latest 5 rows (sorted descending by time)
                display(df.sort_values(by="open_time", ascending=False).head(5))

                # simple line plot
                plt.figure(figsize=(10, 4))
                # ensure open_time is plotted sensibly
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
                plt.show()

            time.sleep(REFRESH_SECONDS)

    except KeyboardInterrupt:
        print("Pred view stopped by user.")

