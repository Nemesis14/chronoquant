# =============================================================================
# ZigZag threshold pivot motor
# =============================================================================
# Purpose:
#  - Confirmed, alternating pivot list from OHLCV (no lookahead)
#  - Swing low confirmed when close rises >= threshold from the low
#  - Swing high confirmed when close falls >= threshold from the high
#  - conf_bar = the bar at which confirmation was detected
# Parameters:
#  - df: OHLCV DataFrame (open_time, high, low, close)
#  - cfg: ElliottConfig (zigzag_threshold, min_reversal_atr)
#  - degree: pivot degree tag (0=micro, 1=trade, 2=macro)
# =============================================================================

from __future__ import annotations

import pandas as pd

from modeling.elliott.elliott.config import ElliottConfig
from modeling.elliott.elliott.data import Pivot, PivotKind
from modeling.elliott.elliott.indicators.atr import atr14


def detect_zigzag(
    df:     pd.DataFrame,
    cfg:    ElliottConfig,
    degree: int = 0,
) -> list[Pivot]:
    """Return confirmed, alternating ZigZag pivots."""
    highs  = df["high"].to_numpy(dtype=float)
    lows   = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    times  = df["open_time"].astype(str).to_numpy()
    atr    = atr14(df)
    n      = len(closes)

    if n < 4:
        return []

    threshold = cfg.zigzag_threshold
    pivots: list[Pivot] = []
    trend    = 0        # 0 = undecided, +1 = up, -1 = down
    hi_price = float(closes[0])
    lo_price = float(closes[0])
    hi_bar   = 0
    lo_bar   = 0

    for i in range(1, n):
        c  = float(closes[i])
        h  = float(highs[i])
        lo = float(lows[i])

        if trend >= 0:
            if h > hi_price:
                hi_price = h
                hi_bar   = i
            if c <= hi_price * (1.0 - threshold):
                atr_val = float(atr[hi_bar])
                pivots.append(Pivot(
                    idx           = hi_bar,
                    ts            = str(times[hi_bar]),
                    price         = hi_price,
                    kind          = PivotKind.HIGH,
                    degree        = degree,
                    confirmed_idx = i,
                    confirmed_ts  = str(times[i]),
                    atr           = atr_val,
                ))
                trend    = -1
                lo_price = lo
                lo_bar   = i

        if trend <= 0:
            if lo < lo_price:
                lo_price = lo
                lo_bar   = i
            if c >= lo_price * (1.0 + threshold):
                atr_val = float(atr[lo_bar])
                pivots.append(Pivot(
                    idx           = lo_bar,
                    ts            = str(times[lo_bar]),
                    price         = lo_price,
                    kind          = PivotKind.LOW,
                    degree        = degree,
                    confirmed_idx = i,
                    confirmed_ts  = str(times[i]),
                    atr           = atr_val,
                ))
                trend    = 1
                hi_price = h
                hi_bar   = i

    return pivots
