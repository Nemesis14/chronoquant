# =============================================================================
# ATR-14 indicator (Wilder smoothing)
# =============================================================================
# Purpose:
#  - Compute ATR-14 aligned to DataFrame index
#  - Used for pivot confirmation thresholds and eps calculations
# Parameters:
#  - df: OHLCV DataFrame with high, low, close columns
# =============================================================================

from __future__ import annotations

import numpy as np
import pandas as pd


def atr14(df: pd.DataFrame) -> np.ndarray:
    """Wilder ATR-14, array aligned to df index."""
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

    atr    = np.empty(n)
    atr[0] = tr[0]
    alpha  = 1.0 / 14.0
    for i in range(1, n):
        atr[i] = atr[i - 1] * (1.0 - alpha) + tr[i] * alpha

    return atr
