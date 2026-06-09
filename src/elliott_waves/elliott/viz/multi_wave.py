# =============================================================================
# MultiWavePlot — plot multiple pattern candidates simultaneously
# =============================================================================
# Purpose:
#  - Show top-K candidates on one chart, color-coded by score
#  - Green = high score (>= emit_score), yellow = medium, red = low
#  - Each candidate's pivots drawn with its own color
# =============================================================================

from __future__ import annotations

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import pandas as pd

from elliott_waves.elliott.data import PatternCandidate
from elliott_waves.elliott.viz.candle_chart import CandleChart
from elliott_waves.elliott.viz.wave_plot import WavePlot


def _score_to_color(score: float) -> str:
    """Map score 0–100 to green-yellow-red colormap."""
    norm = min(1.0, max(0.0, score / 100.0))
    r    = 1.0 - norm
    g    = norm
    return f"#{int(r*255):02x}{int(g*255):02x}33"


class MultiWavePlot:
    # ==========================================================================
    # MultiWavePlot(chart) -> None
    # ==========================================================================
    def __init__(self, chart: CandleChart) -> None:
        self.chart = chart
        self.ax    = chart.ax

    def add_candidates(
        self,
        candidates: list[PatternCandidate],
        df:         pd.DataFrame,
        top_n:      int = 5,
    ) -> "MultiWavePlot":
        """
        Draw top_n candidates (by score) with score-based color coding.
        Strongest candidate (highest score) drawn last to appear on top.
        """
        top = sorted(candidates, key=lambda c: c.score)[:top_n]

        for i, candidate in enumerate(top):
            color = _score_to_color(candidate.score)
            wp    = WavePlot(self.chart)

            wp.add_wave_lines(candidate.pivots, color=color, linewidth=0.8 + i * 0.3)

            for p in candidate.pivots:
                x = p.idx
                if x >= len(df):
                    continue
                self.ax.scatter(x, p.price, color=color, s=30 + i * 15, zorder=4 + i, alpha=0.8)

            # Label the pattern type + score in top-left area
            self.ax.annotate(
                f"{candidate.pattern_type} {candidate.score:.0f}",
                xy       = (0.01, 0.99 - i * 0.05),
                xycoords = "axes fraction",
                fontsize = 7,
                color    = color,
                va       = "top",
            )

        return self
