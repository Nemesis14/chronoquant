# =============================================================================
# Momentum and volume context scoring
# =============================================================================
# Purpose:
#  - volume_score: above-average volume during wave = bullish signal
#  - momentum_score: EMA slope and range expansion within a segment
#  - shallow_pullback_score: corrections within a wave stay shallow
# =============================================================================

from __future__ import annotations

import numpy as np
import pandas as pd

from elliott_waves.elliott.data import Pivot
from elliott_waves.elliott.indicators.momentum import ema_slope, range_expansion, volume_ratio


# =============================================================================
# volume_score(df, start_idx, end_idx) -> float [0, 1]
# =============================================================================
def volume_score(
    df:        pd.DataFrame,
    start_idx: int,
    end_idx:   int,
) -> float:
    """Score based on mean volume ratio during the segment."""
    if "volume" not in df.columns:
        return 0.5
    vol = df["volume"].to_numpy(dtype=float)
    if len(vol) < 5:
        return 0.5
    ratio = volume_ratio(vol, window=20)
    seg_start = max(0, start_idx)
    seg_end   = min(len(ratio), end_idx + 1)
    if seg_end <= seg_start:
        return 0.5
    mean_ratio = float(np.mean(ratio[seg_start:seg_end]))
    return min(1.0, max(0.0, (mean_ratio - 0.5) / 1.5))


# =============================================================================
# momentum_score(df, start_idx, end_idx, cfg) -> float [0, 1]
# =============================================================================
def momentum_score(
    df:        pd.DataFrame,
    start_idx: int,
    end_idx:   int,
    cfg        = None,
) -> float:
    """Score based on EMA slope and range expansion during the segment."""
    closes  = df["close"].to_numpy(dtype=float)
    if len(closes) < 5:
        return 0.5

    slope   = ema_slope(closes, period=20)
    re      = range_expansion(df, window=20)

    seg_start = max(0, start_idx)
    seg_end   = min(len(closes), end_idx + 1)
    if seg_end <= seg_start:
        return 0.5

    mean_slope = float(np.mean(slope[seg_start:seg_end]))
    mean_re    = float(np.mean(re[seg_start:seg_end]))

    # Normalise: positive slope and range expansion > 1 are good
    slope_score = min(1.0, max(0.0, mean_slope / (closes[seg_start] * 0.001 + 1e-10)))
    re_score    = min(1.0, max(0.0, (mean_re - 0.5) / 1.5))

    return (slope_score + re_score) / 2.0


# =============================================================================
# shallow_pullback_score(df, pivots_in_wave, direction) -> float [0, 1]
# =============================================================================
def shallow_pullback_score(
    df:              pd.DataFrame,
    pivots_in_wave:  list[Pivot],
    direction:       int,
) -> float:
    """
    Score based on how shallow the pullbacks within a wave are.
    Strong Wave 3 has few and shallow pullbacks.
    """
    if len(pivots_in_wave) < 3:
        return 0.5

    closes = df["close"].to_numpy(dtype=float)
    total_move = abs(
        pivots_in_wave[-1].y(direction) - pivots_in_wave[0].y(direction)
    )
    if total_move <= 0:
        return 0.5

    # Measure contra-direction legs within the wave
    contra_depths = []
    for i in range(1, len(pivots_in_wave) - 1, 2):
        # Alternating pivot: this is a pullback
        prev = pivots_in_wave[i - 1]
        curr = pivots_in_wave[i]
        pullback = abs(curr.y(direction) - prev.y(direction))
        contra_depths.append(pullback)

    if not contra_depths:
        return 0.5

    max_pullback = max(contra_depths)
    depth_ratio  = max_pullback / total_move
    return max(0.0, 1.0 - depth_ratio * 2.0)
