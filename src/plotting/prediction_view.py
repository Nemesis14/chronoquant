# =============================================================================
# Display latest prediction probabilities and chart
# =============================================================================
# Purpose:
#  - Query predictions table for last N minutes
#  - Print statistics
#  - Plot the single live model probability over time with its decision threshold
# =============================================================================

import sqlite3
from datetime import datetime, timedelta, timezone
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import utils
from db.table_ops import table_columns

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
    db_cfg        = utils.load_db_config()
    db_path       = db_cfg["database"]["db_path"]
    table_pred    = db_cfg["database"]["tables"]["predictions"]
    columns   = table_columns(db_path, table_pred)
    live_cols = utils.live_prediction_columns()
    base_cols = [
        "open_time",
        live_cols["prediction"],
    ]
    if live_cols["signal"] in columns:
        base_cols.append(live_cols["signal"])
    select_cols = [col for col in base_cols if col in columns]

    # -------------------------------------------------------------------------
    # Compute time range
    # -------------------------------------------------------------------------
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = now - timedelta(minutes=lookback_minutes)
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")

    if print_status:
        print(f"Querying {lookback_minutes}m from '{table_pred}' since {start_str}")

    # -------------------------------------------------------------------------
    # Fetch data for the live probability and signal visualization
    # -------------------------------------------------------------------------
    with sqlite3.connect(db_path) as conn:
        select_clause = ", ".join(select_cols)
        df = pd.read_sql_query(
            f"SELECT {select_clause} FROM {table_pred} WHERE open_time >= ? ORDER BY open_time ASC",
            conn,
            params=(start_str,),
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
# Live trading view:
# - TOP PANEL: selected runtime model probability
# - BOTTOM PANEL: derived LONG/SHORT/NEUTRAL signal
# =============================================================================
def plot_predictions_df(df: pd.DataFrame, ax=None):
    # -------------------------------------------------------------------------
    # Setup figure with 2 subplots
    # -------------------------------------------------------------------------
    if ax is None:
        fig = plt.figure(figsize=(14, 8))
        ax1 = fig.add_subplot(211)
        ax2 = fig.add_subplot(212)
    else:
        fig = ax.figure
        # Clear all axes and rebuild
        for old_ax in fig.axes:
            fig.delaxes(old_ax)
        ax1 = fig.add_subplot(211)
        ax2 = fig.add_subplot(212)

    x = pd.to_datetime(df["open_time"])

    live_cols = utils.live_prediction_columns()
    predictions_cfg = utils.load_predictions_config()
    threshold = utils.signal_probability_threshold(predictions_cfg)
    prediction_col = live_cols["prediction"]

    prediction = pd.to_numeric(df[prediction_col], errors="coerce")
    label = "Live prediction"

    ax1.plot(
        x,
        prediction,
        color="dodgerblue",
        linewidth=2.5,
        label=label,
        marker="o",
        markersize=5,
    )
    ax1.axhline(y=threshold, color="green", linestyle="--", lw=2, alpha=0.7, label="Signal threshold")
    ax1.axhline(y=0.0, color="black", linestyle="-", lw=0.5, alpha=0.3)

    ax1.set_title("Live Model Prediction Over Time", fontsize=13, fontweight="bold")
    ax1.set_ylabel("Probability", fontsize=11)
    ax1.legend(loc="upper left", fontsize=10, framealpha=0.95)
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(-0.05, 1.05)

    # =========================================================================
    # PANEL 2 (BOTTOM): SIGNAL STATE
    # =========================================================================
    signal_col = live_cols["signal"]
    signal_values = df[signal_col] if signal_col in df.columns else pd.Series(["NEUTRAL"] * len(df))
    signal_map = {"SHORT": -1, "NEUTRAL": 0, "LONG": 1}
    signal_y = signal_values.map(signal_map).fillna(0)
    ax2.step(x, signal_y, where="post", color="navy", linewidth=2.0, label="Signal")
    ax2.fill_between(x, 0, signal_y, where=signal_y > 0, step="post", color="green", alpha=0.2)
    ax2.fill_between(x, 0, signal_y, where=signal_y < 0, step="post", color="red", alpha=0.2)
    ax2.axhline(y=0, color="black", linestyle="-", lw=0.5, alpha=0.4)
    ax2.set_yticks([-1, 0, 1])
    ax2.set_yticklabels(["SHORT", "NEUTRAL", "LONG"])
    ax2.set_ylim(-1.2, 1.2)
    ax2.set_title("Live Trading Signal", fontsize=13, fontweight="bold")
    ax2.set_xlabel("Time", fontsize=11)
    ax2.legend(loc="upper left", fontsize=9, framealpha=0.95)
    ax2.grid(True, alpha=0.3)

    # Format X-axis labels for both panels to be readable
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate(rotation=45, ha="right")

    # Align x-axis between plots
    ax2.set_xlim(ax1.get_xlim())
    
    fig.tight_layout()
    return fig

