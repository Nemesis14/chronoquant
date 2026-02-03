# =============================================================================
# Display latest prediction probabilities and chart
# =============================================================================
# Purpose:
#  - Query predictions table for last N minutes
#  - Print statistics
#  - Plot probability over time with decision thresholds
# =============================================================================

import sqlite3
from datetime import datetime, timedelta, timezone
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import display

import utils

# =============================================================================
# CONSTANTS
# =============================================================================
LOOKBACK_MINUTES = 120

# =============================================================================
# fetch_predictions_df(lookback_minutes: int, print_status: bool) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Query last LOOKBACK_MINUTES from predictions table
#  - Optionally print stats (count, time range)
# =============================================================================
def fetch_predictions_df(lookback_minutes: int = LOOKBACK_MINUTES, print_status: bool = True) -> pd.DataFrame:
	# -------------------------------------------------------------------------
	# Load configuration
	# -------------------------------------------------------------------------
	db_cfg       = utils.load_db_config()
	model_cfg    = utils.load_models_config()
	db_path      = db_cfg["database"]["db_path"]
	table_pred   = db_cfg["database"]["tables"]["predictions"]
	models       = model_cfg.get("models", {})
	active_models = [mid for mid, meta in models.items() if meta.get("active")]
	if len(active_models) == 0:
		raise ValueError("No active models found in config/models.json")
	pred_cols = [f"{mid}_p" for mid in active_models]

	# -------------------------------------------------------------------------
	# Compute time range
	# -------------------------------------------------------------------------
	now      = datetime.now(timezone.utc).replace(second=0, microsecond=0)
	start_dt = now - timedelta(minutes=lookback_minutes)
	start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")

	if print_status:
		print(f"Querying {lookback_minutes}m from '{table_pred}' since {start_str}")

	# -------------------------------------------------------------------------
	# Fetch data
	# -------------------------------------------------------------------------
	with sqlite3.connect(db_path) as conn:
		select_cols = ", ".join(["open_time"] + pred_cols)
		df = pd.read_sql_query(
			f"SELECT {select_cols} FROM {table_pred} WHERE open_time >= ? ORDER BY open_time ASC",
			conn,
			params=(start_str,)
		)

	if df.empty:
		if print_status:
			print("No data found")
		return df

	# -------------------------------------------------------------------------
	# Print statistics
	# -------------------------------------------------------------------------
	if print_status:
		print(f"Found {len(df)} data points")
		print(f"Range: {df['open_time'].min()} to {df['open_time'].max()}")

	return df

# =============================================================================
# plot_predictions_df(df: pd.DataFrame, ax: matplotlib axis | None) -> Figure
# =============================================================================
def plot_predictions_df(df: pd.DataFrame, ax=None):
	# -------------------------------------------------------------------------
	# Plot
	# -------------------------------------------------------------------------
	if ax is None:
		fig, ax = plt.subplots(figsize=(10, 4))
	else:
		fig = ax.figure
		ax.clear()

	x = pd.to_datetime(df["open_time"])
	pred_cols = [c for c in df.columns if c != "open_time"]
	color_map = {}
	for col in pred_cols:
		if "_s_" in col:
			color_map[col] = "crimson"
		elif "_l_" in col:
			color_map[col] = "dodgerblue"
		else:
			color_map[col] = "gray"

	for col in pred_cols:
		y = pd.to_numeric(df[col], errors="coerce")
		ax.plot(x, y, label=col, color=color_map.get(col, "gray"), marker="o", markersize=3)
	ax.axhline(y=0.01, color="red", linestyle="--", lw=1, label="Short (0.01)")
	ax.axhline(y=0.10, color="green", linestyle="--", lw=1, label="Long (0.1)")

	ax.set_title("Prediction Probability")
	ax.set_xlabel("Time")
	ax.set_ylabel("Probability")
	ax.legend()
	ax.grid(True)
	fig.tight_layout()

	return fig

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
	df = fetch_predictions_df()
	if df.empty:
		return

	fig = plot_predictions_df(df)
	display(fig)
	plt.close(fig)
