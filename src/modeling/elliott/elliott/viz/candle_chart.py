# =============================================================================
# CandleChart — reusable dark-theme candlestick chart
# =============================================================================
# Purpose:
#  - Dark-theme OHLCV candlestick chart using matplotlib
#  - add_candles(df): add candlestick layer
#  - save(path) / show(): output methods
# =============================================================================

from __future__ import annotations

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure

# -------------------------------------------------------------------------
# Dark theme constants
# -------------------------------------------------------------------------
BG_COLOR  = "#0d1117"
UP_COLOR  = "#26a69a"
DN_COLOR  = "#ef5350"
GRID_COLOR = "#1e2530"
TEXT_COLOR = "#c9d1d9"


class CandleChart:
    # ==========================================================================
    # CandleChart(title, figsize) -> None
    # ==========================================================================
    # Purpose:
    #  - Initialize a dark-theme figure with one primary axis
    # ==========================================================================
    def __init__(
        self,
        title:   str              = "",
        figsize: tuple[int, int]  = (16, 8),
    ) -> None:
        self.fig: Figure
        self.ax:  Axes
        self.fig, self.ax = plt.subplots(figsize=figsize, facecolor=BG_COLOR)
        self.ax.set_facecolor(BG_COLOR)
        self.ax.tick_params(colors=TEXT_COLOR)
        self.ax.yaxis.label.set_color(TEXT_COLOR)
        self.ax.xaxis.label.set_color(TEXT_COLOR)
        for spine in self.ax.spines.values():
            spine.set_edgecolor(GRID_COLOR)
        self.ax.grid(color=GRID_COLOR, linewidth=0.5, alpha=0.6)
        if title:
            self.ax.set_title(title, color=TEXT_COLOR, fontsize=11)
        self._n_candles = 0

    def add_candles(self, df: pd.DataFrame) -> CandleChart:
        """Draw candlestick bars from OHLCV DataFrame."""
        df = df.reset_index(drop=True)
        n  = len(df)
        self._n_candles = n

        opens  = df["open"].to_numpy(dtype=float)
        highs  = df["high"].to_numpy(dtype=float)
        lows   = df["low"].to_numpy(dtype=float)
        closes = df["close"].to_numpy(dtype=float)
        xs     = np.arange(n)

        width     = 0.6
        half      = width / 2.0
        up_mask   = closes >= opens
        dn_mask   = ~up_mask

        # Wicks
        self.ax.vlines(xs[up_mask], lows[up_mask], highs[up_mask], color=UP_COLOR, linewidth=0.8)
        self.ax.vlines(xs[dn_mask], lows[dn_mask], highs[dn_mask], color=DN_COLOR, linewidth=0.8)

        # Bodies
        for i in range(n):
            body_lo = min(opens[i], closes[i])
            body_hi = max(opens[i], closes[i])
            color   = UP_COLOR if closes[i] >= opens[i] else DN_COLOR
            rect    = mpatches.Rectangle(
                (i - half, body_lo),
                width,
                max(body_hi - body_lo, (highs[i] - lows[i]) * 0.01),
                color  = color,
                zorder = 2,
            )
            self.ax.add_patch(rect)

        # X-axis labels (sparse)
        if "open_time" in df.columns:
            step  = max(1, n // 10)
            ticks = list(range(0, n, step))
            labels = [str(df["open_time"].iloc[t])[:16] for t in ticks]
            self.ax.set_xticks(ticks)
            self.ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=7, color=TEXT_COLOR)

        self.ax.set_xlim(-1, n)
        return self

    def save(self, path: str) -> CandleChart:
        """Save figure to file."""
        self.fig.tight_layout()
        self.fig.savefig(path, dpi=150, facecolor=BG_COLOR)
        return self

    def show(self) -> None:
        """Display figure interactively."""
        self.fig.tight_layout()
        plt.show()

    def close(self) -> None:
        plt.close(self.fig)
