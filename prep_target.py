import sqlite3
import pandas as pd
from datetime import timedelta
import json

# =============================================================================
# Load configuration
# =============================================================================
with open("config.json", "r") as config_file:
    config = json.load(config_file)
DB_PATH = config["db_path"]

# =============================================================================
# Main function for calculating ratios and percentiles
# =============================================================================
def calculate_target_percentiles(table, open_time_from, open_time_to, window=240):
    """
    Calculates the ratio of 'close' to its rolling maximum for a given time window
    and computes p50, p75, and p90 percentiles of the ratio.

    Parameters:
    - table (str): Name of the database table to query.
    - open_time_from (str): Start of the time range in "YYYY-MM-DD HH:MM" format.
    - open_time_to (str): End of the time range in "YYYY-MM-DD HH:MM" format.
    - window (int): Rolling window interval in minutes (e.g., 240 for 4 hours). Default is 240.

    Returns:
    - DataFrame: A DataFrame with percentiles for the ratio.
    """

    # -------------------------------------------------------------------------
    # Connect to the database and fetch relevant data
    # -------------------------------------------------------------------------
    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT open_time, close
        FROM {table}
        WHERE open_time BETWEEN ? AND ?
    """
    df = pd.read_sql_query(query, conn, params=(open_time_from, open_time_to))
    conn.close()

    if df.empty:
        raise ValueError("No data found for the given time range.")

    # -------------------------------------------------------------------------
    # Calculate rolling maximum and ratio
    # -------------------------------------------------------------------------
    df["rolling_max"] = df["close"].rolling(window=window, min_periods = 1).max()
    df["ratio"]       = df["rolling_max"] / df["close"] 

    # -------------------------------------------------------------------------
    # Calculate percentiles for the ratio
    # -------------------------------------------------------------------------
    ratio_percentiles = df["ratio"].describe(percentiles=[0.5, 0.75, 0.9]).rename({
        "50%": "p50",
        "75%": "p75",
        "90%": "p90"
    })
    result = pd.DataFrame(ratio_percentiles[["p50", "p75", "p90"]]).transpose()

    return result