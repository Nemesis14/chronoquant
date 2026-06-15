# =============================================================================
# WavePlot — Elliott Wave pivot annotations and zone overlays
# =============================================================================
# Purpose:
#  - add_pivots: draw pivot points with labels on a CandleChart
#  - add_wave_lines: connect pivots with colored lines
#  - add_confirmation_zone: highlight the confirmation bar range
#  - add_trigger_line: horizontal line at P1 + buffer for Wave 3 trigger
# =============================================================================

from __future__ import annotations

import pandas as pd

from modeling.elliott.elliott.data import PatternCandidate, Pivot, PivotKind
from modeling.elliott.elliott.viz.candle_chart import DN_COLOR, CandleChart

PIVOT_HIGH_COLOR = "#ffd700"
PIVOT_LOW_COLOR  = "#00bfff"
WAVE_LINE_COLOR  = "#ffffff"
TRIGGER_COLOR    = "#ff9800"
ZONE_COLOR       = "#26a69a"

_WAVE_LABELS = ["0", "1", "2", "3", "4", "5", "A", "B", "C"]


class WavePlot:
    # ==========================================================================
    # WavePlot(chart) -> None
    # ==========================================================================
    def __init__(self, chart: CandleChart) -> None:
        self.chart = chart
        self.ax    = chart.ax

    def add_pivots(
        self,
        pivots:     list[Pivot],
        df:         pd.DataFrame,
        labels:     list[str] | None = None,
    ) -> WavePlot:
        """Plot pivot markers and optional labels on the candlestick chart."""
        df = df.reset_index(drop=True)

        for k, p in enumerate(pivots):
            # Map pivot bar index to chart x position
            x = p.idx
            if x >= len(df):
                continue

            color  = PIVOT_HIGH_COLOR if p.kind == PivotKind.HIGH else PIVOT_LOW_COLOR
            offset = 1.003 if p.kind == PivotKind.HIGH else 0.997
            label  = labels[k] if (labels and k < len(labels)) else str(k)

            self.ax.scatter(x, p.price * offset, color=color, s=60, zorder=5)
            self.ax.annotate(
                label,
                (x, p.price * offset),
                color     = color,
                fontsize  = 8,
                ha        = "center",
                va        = "bottom" if p.kind == PivotKind.HIGH else "top",
                zorder    = 6,
            )

        return self

    def add_wave_lines(
        self,
        pivots:    list[Pivot],
        color:     str = WAVE_LINE_COLOR,
        linewidth: float = 1.2,
    ) -> WavePlot:
        """Draw lines connecting consecutive pivots."""
        for i in range(1, len(pivots)):
            p_prev = pivots[i - 1]
            p_curr = pivots[i]
            self.ax.plot(
                [p_prev.idx, p_curr.idx],
                [p_prev.price, p_curr.price],
                color     = color,
                linewidth = linewidth,
                linestyle = "--",
                alpha     = 0.7,
                zorder    = 4,
            )
        return self

    def add_confirmation_zone(
        self,
        pivot: Pivot,
        color: str = ZONE_COLOR,
    ) -> WavePlot:
        """Shade the area between pivot bar and confirmation bar."""
        self.ax.axvspan(
            pivot.idx,
            pivot.confirmed_idx,
            alpha = 0.15,
            color = color,
            zorder = 1,
        )
        return self

    def add_trigger_line(
        self,
        price: float,
        label: str = "trigger",
        color: str = TRIGGER_COLOR,
    ) -> WavePlot:
        """Draw horizontal trigger line at specified price."""
        self.ax.axhline(
            y         = price,
            color     = color,
            linewidth = 1.0,
            linestyle = "-.",
            alpha     = 0.8,
            zorder    = 3,
        )
        self.ax.annotate(
            label,
            xy        = (0.01, price),
            xycoords  = ("axes fraction", "data"),
            color     = color,
            fontsize  = 7,
            va        = "bottom",
        )
        return self

    def add_target_zones(
        self,
        target_zones: dict[str, float],
        color: str = "#80cbc4",
    ) -> WavePlot:
        """Draw horizontal lines for each target zone."""
        for label, price in target_zones.items():
            self.ax.axhline(
                y         = price,
                color     = color,
                linewidth = 0.8,
                linestyle = ":",
                alpha     = 0.6,
                zorder    = 3,
            )
            self.ax.annotate(
                label,
                xy       = (0.99, price),
                xycoords = ("axes fraction", "data"),
                color    = color,
                fontsize = 6,
                ha       = "right",
                va       = "bottom",
            )
        return self

    def add_candidate(
        self,
        candidate: PatternCandidate,
        df:        pd.DataFrame,
    ) -> WavePlot:
        """Plot a full PatternCandidate with pivots, lines, and target zones."""
        labels = _WAVE_LABELS[:len(candidate.pivots)]
        self.add_pivots(candidate.pivots, df, labels=labels)
        self.add_wave_lines(candidate.pivots)
        if candidate.target_zones:
            self.add_target_zones(candidate.target_zones)
        if candidate.invalidation_level is not None:
            self.add_trigger_line(
                candidate.invalidation_level,
                label = "stop",
                color = DN_COLOR,
            )
        return self
