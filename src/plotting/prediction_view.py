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
    model_cfg     = utils.load_models_config()
    db_path       = db_cfg["database"]["db_path"]
    table_pred    = db_cfg["database"]["tables"]["predictions"]
    active_models = utils.active_model_ids(model_cfg)
    if len(active_models) == 0:
        raise ValueError("No active models found in config/models.json")
    pred_cols = [utils.prediction_col_name(mid) for mid in active_models]
    columns   = table_columns(db_path, table_pred)
    base_cols = ["open_time"] + (["signal"] if "signal" in columns else [])

    # -------------------------------------------------------------------------
    # Compute time range
    # -------------------------------------------------------------------------
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = now - timedelta(minutes=lookback_minutes)
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")

    if print_status:
        print(f"Querying {lookback_minutes}m from '{table_pred}' since {start_str}")

    # -------------------------------------------------------------------------
    # Fetch data (including spread and signal for new visualization)
    # -------------------------------------------------------------------------
    with sqlite3.connect(db_path) as conn:
        select_cols = ", ".join(base_cols + pred_cols)
        df = pd.read_sql_query(
            f"SELECT {select_cols} FROM {table_pred} WHERE open_time >= ? ORDER BY open_time ASC",
            conn,
            params=(start_str,),
        )

    if df.empty:
        if print_status:
            print("No data found")
        return df

    # Calculate spread if model columns exist
    long_col, short_col = utils.long_short_prediction_columns(model_cfg)
    if long_col and short_col:
        df["spread"] = (
            pd.to_numeric(df[long_col], errors="coerce")
            - pd.to_numeric(df[short_col], errors="coerce")
        )

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
# NEW DUAL-PANEL LIVE TRADING VIEW:
# - TOP PANEL: Model predictions (LONG blue, SHORT red lines)
# - BOTTOM PANEL: Trading signal zones (LONG/SHORT/NEUTRAL colored backgrounds)
# This helps traders see predictions over time and when to trade
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

    # =========================================================================
    # PANEL 1 (TOP): MODEL PREDICTIONS
    # =========================================================================
    # Extract LONG and SHORT probabilities
    model_cfg = utils.load_models_config()
    long_col, short_col = utils.long_short_prediction_columns(model_cfg)

    y_min, y_max = None, None
    if long_col and long_col in df.columns:
        long_p = pd.to_numeric(df[long_col], errors="coerce")
        ax1.plot(
            x,
            long_p,
            color="dodgerblue",
            linewidth=2.5,
            label=f"LONG ({long_col})",
            marker="o",
            markersize=5,
        )
        y_min = long_p.min() if y_min is None else min(y_min, long_p.min())
        y_max = long_p.max() if y_max is None else max(y_max, long_p.max())

    if short_col and short_col in df.columns:
        short_p = pd.to_numeric(df[short_col], errors="coerce")
        ax1.plot(
            x,
            short_p,
            color="crimson",
            linewidth=2.5,
            label=f"SHORT ({short_col})",
            marker="s",
            markersize=5,
        )
        y_min = short_p.min() if y_min is None else min(y_min, short_p.min())
        y_max = short_p.max() if y_max is None else max(y_max, short_p.max())

    # Add reference lines
    ax1.axhline(y=0.5, color="gray", linestyle=":", lw=1, alpha=0.5)
    ax1.axhline(y=0.0, color="black", linestyle="-", lw=0.5, alpha=0.3)

    ax1.set_title("Model Predictions Over Time (Probabilities)", fontsize=13, fontweight="bold")
    ax1.set_ylabel("Probability", fontsize=11)
    ax1.legend(loc="upper left", fontsize=10, framealpha=0.95)
    ax1.grid(True, alpha=0.3)

    # Dynamically set Y limits with some padding
    if y_min is not None and y_max is not None:
        padding = (y_max - y_min) * 0.1
        ax1.set_ylim(max(0, y_min - padding), min(1, y_max + padding))
    else:
        ax1.set_ylim(-0.05, 1.05)

    # =========================================================================
    # PANEL 2 (BOTTOM): SPREAD VALUE WITH TRADING ZONES
    # =========================================================================
    # Plot spread as line chart with zone backgrounds
    if "spread" in df.columns:
        spread_vals = pd.to_numeric(df["spread"], errors="coerce")
        long_cutoff_val, short_cutoff_val = utils.signal_cutoffs_from_config(model_cfg)

        # Add zone background colors
        spread_min = spread_vals.min()
        spread_max = spread_vals.max()

        ax2.axhspan(
            spread_min - 0.01,
            short_cutoff_val,
            alpha=0.2,
            color="red",
            label=f"SHORT Zone (<= {short_cutoff_val:.6f})",
        )
        ax2.axhspan(
            short_cutoff_val,
            long_cutoff_val,
            alpha=0.1,
            color="gold",
            label="NEUTRAL Zone",
        )
        ax2.axhspan(
            long_cutoff_val,
            spread_max + 0.01,
            alpha=0.2,
            color="green",
            label=f"LONG Zone (>= {long_cutoff_val:.6f})",
        )

        # Plot spread as line with markers
        ax2.plot(
            x,
            spread_vals,
            color="navy",
            linewidth=2.5,
            label="Spread (long_p - short_p)",
            marker="D",
            markersize=6,
            alpha=0.8,
        )

        # Cut-off threshold lines
        ax2.axhline(y=long_cutoff_val, color="green", linestyle="--", lw=2, alpha=0.7, label="LONG Cut-off")
        ax2.axhline(y=short_cutoff_val, color="red", linestyle="--", lw=2, alpha=0.7, label="SHORT Cut-off")
        ax2.axhline(y=0, color="black", linestyle="-", lw=0.5, alpha=0.3)

        ax2.set_title(
            "Trading Zones: Spread Over Time (Red=SHORT, Yellow=NEUTRAL, Green=LONG)",
            fontsize=13,
            fontweight="bold",
        )
        ax2.set_xlabel("Time", fontsize=11)
        ax2.set_ylabel("Spread (Long Prob - Short Prob)", fontsize=11)
        ax2.legend(loc="upper left", fontsize=9, framealpha=0.95)
        ax2.grid(True, alpha=0.3)

        # Set FIXED Y limits to show all zones clearly (SHORT, NEUTRAL, LONG)
        # This ensures zone boundaries are always visible even when data is concentrated
        ax2.set_ylim(-0.03, +0.03)

    # Format X-axis labels for both panels to be readable
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate(rotation=45, ha="right")

    # Align x-axis between plots
    ax2.set_xlim(ax1.get_xlim())
    
    fig.tight_layout()
    return fig

