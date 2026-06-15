# =============================================================================
# Momentum indicators for Elliott Wave context scoring
# =============================================================================
# Purpose:
#  - EMA, EMA slope, range expansion, volume ratio
#  - Used as soft-score inputs for Wave 3 strength and momentum context
# =============================================================================

from __future__ import annotations

import numpy as np
import pandas as pd


# =============================================================================
# ema(series, period) -> np.ndarray
# =============================================================================
def ema(series: np.ndarray, period: int) -> np.ndarray:
    """Standard exponential moving average."""
    alpha  = 2.0 / (period + 1)
    result = np.empty(len(series))
    result[0] = series[0]
    for i in range(1, len(series)):
        result[i] = alpha * series[i] + (1.0 - alpha) * result[i - 1]
    return result


# =============================================================================
# ema_slope(closes, period) -> np.ndarray
# =============================================================================
def ema_slope(closes: np.ndarray, period: int = 20) -> np.ndarray:
    """First difference of EMA — positive = upsloping."""
    e      = ema(closes, period)
    slope  = np.empty(len(closes))
    slope[0] = 0.0
    slope[1:] = np.diff(e)
    return slope


# =============================================================================
# range_expansion(df, window) -> np.ndarray
# =============================================================================
def range_expansion(df: pd.DataFrame, window: int = 20) -> np.ndarray:
    """True range / rolling-median true range. Values > 1.0 = expansion."""
    high  = df["high"].to_numpy(dtype=float)
    low   = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)
    n     = len(close)

    tr    = np.empty(n)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i]  - close[i - 1]),
        )

    result = np.empty(n)
    for i in range(n):
        start   = max(0, i - window + 1)
        med     = float(np.median(tr[start : i + 1]))
        result[i] = tr[i] / med if med > 0 else 1.0

    return result


# =============================================================================
# volume_ratio(volume, window) -> np.ndarray
# =============================================================================
def volume_ratio(volume: np.ndarray, window: int = 20) -> np.ndarray:
    """Current volume / rolling-mean volume. Values > 1.0 = above average."""
    n      = len(volume)
    result = np.empty(n)
    for i in range(n):
        start   = max(0, i - window + 1)
        mean    = float(np.mean(volume[start : i + 1]))
        result[i] = float(volume[i]) / mean if mean > 0 else 1.0
    return result
