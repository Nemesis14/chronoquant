# =============================================================================
# Display latest prediction probabilities and chart
# =============================================================================
# Purpose:
#  - Query predictions table for last N minutes
#  - Print statistics
#  - Plot probability over time with decision thresholds
# =============================================================================

import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display

import utils

# =============================================================================
# CONSTANTS
# =============================================================================
LOOKBACK_MINUTES = 120

# =============================================================================
# display_predictions() -> None
# =============================================================================
# Purpose:
#  - Query last LOOKBACK_MINUTES from predictions table
#  - Print stats (count, time range)
#  - Plot pred_prob over time with thresholds
#  - Display using IPython.display
# =============================================================================
def display_predictions() -> None:
	# -------------------------------------------------------------------------
	# Load configuration
	# -------------------------------------------------------------------------
	config       = utils._load_config()
	db_path      = config["database"]["db_path"]
	table_pred   = config["database"]["tables"]["predictions"]

	# -------------------------------------------------------------------------
	# Compute time range
	# -------------------------------------------------------------------------
	now      = datetime.now().replace(second=0, microsecond=0)
	start_dt = now - timedelta(minutes=LOOKBACK_MINUTES)
	start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")

	print(f"📊 Querying {LOOKBACK_MINUTES}m from '{table_pred}' since {start_str}")

	# -------------------------------------------------------------------------
	# Fetch data
	# -------------------------------------------------------------------------
	with sqlite3.connect(db_path) as conn:
		df = pd.read_sql_query(
			f"""
			SELECT open_time, pred_prob FROM {table_pred}
			WHERE open_time >= ? ORDER BY open_time ASC
			""",
			conn,
			params=(start_str,)
		)

	if df.empty:
		print("⚠️ No data found")
		return

	# -------------------------------------------------------------------------
	# Print statistics
	# -------------------------------------------------------------------------
	print(f"✅ Found {len(df)} data points")
	print(f"⏰ Range: {df['open_time'].min()} to {df['open_time'].max()}")

	# -------------------------------------------------------------------------
	# Plot
	# -------------------------------------------------------------------------
	fig, ax = plt.subplots(figsize=(10, 4))

	x = pd.to_datetime(df["open_time"])
	y = pd.to_numeric(df["pred_prob"], errors="coerce")

	ax.plot(x, y, label="Pred Prob", color="dodgerblue", marker="o", markersize=3)
	ax.axhline(y=0.01, color="red", linestyle="--", lw=1, label="Short (0.01)")
	ax.axhline(y=0.10, color="green", linestyle="--", lw=1, label="Long (0.1)")

	ax.set_title("Prediction Probability")
	ax.set_xlabel("Time")
	ax.set_ylabel("Probability")
	ax.legend()
	ax.grid(True)
	plt.tight_layout()

	display(fig)
	plt.close(fig)