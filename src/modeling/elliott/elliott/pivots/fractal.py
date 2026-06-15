# =============================================================================
# Williams Fractal pivot motor
# =============================================================================
# Purpose:
#  - Fractal pivot: high[i] is max over [i-L .. i+R], low[i] is min
#  - Confirmation bar: i + R (the pivot is only "known" then)
#  - ATR-based minimum reversal filter: reversal from extremum to conf bar
#  - Output is compressed to strictly alternating sequence
# Parameters:
#  - df: OHLCV DataFrame
#  - cfg: ElliottConfig (fractal_left, fractal_right, min_reversal_atr)
#  - degree: pivot degree tag
# =============================================================================

from __future__ import annotations

import pandas as pd

from modeling.elliott.elliott.config import ElliottConfig
from modeling.elliott.elliott.data import Pivot, PivotKind
from modeling.elliott.elliott.indicators.atr import atr14
from modeling.elliott.elliott.pivots._utils import compress_alternating


def detect_fractal(
    df:     pd.DataFrame,
    cfg:    ElliottConfig,
    degree: int = 0,
) -> list[Pivot]:
    """Return confirmed Williams Fractal pivots, alternating."""
    highs  = df["high"].to_numpy(dtype=float)
    lows   = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    times  = df["open_time"].astype(str).to_numpy()
    atr    = atr14(df)
    n      = len(closes)

    L = cfg.fractal_left
    R = cfg.fractal_right

    raw: list[Pivot] = []

    for i in range(L, n - R):
        atr_val = float(atr[i])
        min_rev = cfg.min_reversal_atr * atr_val
        conf_i  = i + R

        window_high = highs[i - L : i + R + 1]
        window_low  = lows[i  - L : i + R + 1]

        if highs[i] >= window_high.max():
            reversal = highs[i] - closes[conf_i]
            if reversal >= min_rev:
                raw.append(Pivot(
                    idx           = i,
                    ts            = str(times[i]),
                    price         = float(highs[i]),
                    kind          = PivotKind.HIGH,
                    degree        = degree,
                    confirmed_idx = conf_i,
                    confirmed_ts  = str(times[conf_i]),
                    atr           = atr_val,
                ))

        if lows[i] <= window_low.min():
            reversal = closes[conf_i] - lows[i]
            if reversal >= min_rev:
                raw.append(Pivot(
                    idx           = i,
                    ts            = str(times[i]),
                    price         = float(lows[i]),
                    kind          = PivotKind.LOW,
                    degree        = degree,
                    confirmed_idx = conf_i,
                    confirmed_ts  = str(times[conf_i]),
                    atr           = atr_val,
                ))

    raw.sort(key=lambda p: p.idx)
    return compress_alternating(raw)
