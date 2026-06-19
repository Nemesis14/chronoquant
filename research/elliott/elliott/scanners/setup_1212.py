# =============================================================================
# 1-2-1-2 Bullish Setup Scanner
# =============================================================================
# Purpose:
#  - Refactored version of elliott_1212.detect_1212 using the new Pivot objects
#  - Detects nested 1-2-1-2 bullish setups on confirmed pivots
#  - Output format is backward-compatible with the original detect_1212 output
# Parameters:
#  - df: OHLCV DataFrame
#  - cfg: ElliottConfig (uses zigzag_threshold, min_reversal_atr)
# =============================================================================

from __future__ import annotations

import pandas as pd

from elliott.elliott.config import ElliottConfig
from elliott.elliott.indicators.atr import atr14
from elliott.elliott.pivots.zigzag import detect_zigzag

# -------------------------------------------------------------------------
# Constants — same as original elliott_1212
# -------------------------------------------------------------------------
FIB_IDEAL    = [0.500, 0.618]
MIN_RETRACE  = 0.382
MAX_RETRACE  = 0.854

D1_MIN, D1_MAX = 5, 50
D2_MIN, D2_MAX = 3, 50
D3_MIN, D3_MAX = 3, 30
D4_MIN, D4_MAX = 2, 30
TOTAL_MIN      = 10
TOTAL_MAX      = 120

AMP_BIG_MIN_ATR = 1.0
AMP_SUB_MIN_ATR = 0.5


def _fib_score(retrace: float) -> float:
    best = min(abs(retrace - t) for t in FIB_IDEAL)
    return max(0.0, 1.0 - best / 0.15)


def _duration_score(d1: int, d2: int, d3: int, d4: int) -> float:
    import numpy as np
    scores = []
    for d, lo, hi in [(d1, D1_MIN, 30), (d2, D2_MIN, 30), (d3, D3_MIN, 20), (d4, D4_MIN, 15)]:
        if d < lo:
            scores.append(0.0)
        elif d <= hi:
            scores.append(1.0)
        else:
            scores.append(max(0.0, 1.0 - (d - hi) / float(hi)))
    return float(np.mean(scores))


def _amplitude_score(w1_big: float, w1_sub: float) -> float:
    if w1_big <= 0:
        return 0.0
    ratio = w1_sub / w1_big
    return max(0.0, 1.0 - abs(ratio - 0.5) / 0.4)


def _score_setup(r_big, r_sub, d1, d2, d3, d4, w1_big, w1_sub) -> float:
    fib = (_fib_score(r_big) + _fib_score(r_sub)) / 2.0
    dur = _duration_score(d1, d2, d3, d4)
    amp = _amplitude_score(w1_big, w1_sub)
    return round(0.50 * fib + 0.25 * dur + 0.25 * amp, 4)


# =============================================================================
# detect_1212(df, cfg, min_retrace, max_retrace) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Scan confirmed pivots for nested 1-2-1-2 bullish setups
#  - Returns one row per setup at P4 confirmation bar
#  - Output columns match original elliott_1212.detect_1212 for backward compat
# =============================================================================
def detect_1212(
    df:          pd.DataFrame,
    cfg:         ElliottConfig | None = None,
    min_retrace: float = MIN_RETRACE,
    max_retrace: float = MAX_RETRACE,
) -> pd.DataFrame:
    if cfg is None:
        cfg = ElliottConfig()

    df   = df.reset_index(drop=True)
    atr  = atr14(df)
    pivs = detect_zigzag(df, cfg, degree=0)

    times = df["open_time"].astype(str).to_numpy()
    rows  = []

    for i in range(4, len(pivs)):
        p0, p1, p2, p3, p4 = pivs[i - 4], pivs[i - 3], pivs[i - 2], pivs[i - 1], pivs[i]

        # -------------------------------------------------------------------------
        # Alternating direction: low-high-low-high-low
        # -------------------------------------------------------------------------
        from elliott.elliott.data import PivotKind
        if [p.kind for p in (p0, p1, p2, p3, p4)] != [
            PivotKind.LOW, PivotKind.HIGH, PivotKind.LOW, PivotKind.HIGH, PivotKind.LOW
        ]:
            continue

        # -------------------------------------------------------------------------
        # Hard structural rules
        # -------------------------------------------------------------------------
        if not (p1.price > p0.price): continue
        if not (p2.price > p0.price): continue
        if not (p3.price > p2.price): continue
        if not (p4.price > p2.price): continue
        if not (p4.price < p3.price): continue

        w1_big = p1.price - p0.price
        w1_sub = p3.price - p2.price

        if w1_big <= 0 or w1_sub <= 0:
            continue

        r_big = (p1.price - p2.price) / w1_big
        r_sub = (p3.price - p4.price) / w1_sub

        # -------------------------------------------------------------------------
        # Retrace zone filter
        # -------------------------------------------------------------------------
        if not (min_retrace <= r_big <= max_retrace): continue
        if not (min_retrace <= r_sub <= max_retrace): continue

        # -------------------------------------------------------------------------
        # Nested structure: sub-wave must be smaller than big wave
        # -------------------------------------------------------------------------
        if w1_sub >= w1_big: continue

        # -------------------------------------------------------------------------
        # Amplitude filter
        # -------------------------------------------------------------------------
        atr_p0 = float(atr[p0.idx])
        atr_p2 = float(atr[p2.idx])
        if w1_big < AMP_BIG_MIN_ATR * atr_p0: continue
        if w1_sub < AMP_SUB_MIN_ATR * atr_p2: continue

        # -------------------------------------------------------------------------
        # Duration filter
        # -------------------------------------------------------------------------
        d1 = p1.idx - p0.idx
        d2 = p2.idx - p1.idx
        d3 = p3.idx - p2.idx
        d4 = p4.idx - p3.idx
        total = p4.idx - p0.idx

        if not (D1_MIN <= d1 <= D1_MAX): continue
        if not (D2_MIN <= d2 <= D2_MAX): continue
        if not (D3_MIN <= d3 <= D3_MAX): continue
        if not (D4_MIN <= d4 <= D4_MAX): continue
        if not (TOTAL_MIN <= total <= TOTAL_MAX): continue

        score = _score_setup(r_big, r_sub, d1, d2, d3, d4, w1_big, w1_sub)

        rows.append({
            "conf_time":  str(times[p4.confirmed_idx]),
            "p0_time":    str(times[p0.idx]),
            "p4_time":    str(times[p4.idx]),
            "p0":         round(p0.price, 4),
            "p1":         round(p1.price, 4),
            "p2":         round(p2.price, 4),
            "p3":         round(p3.price, 4),
            "p4":         round(p4.price, 4),
            "r_big":      round(r_big, 4),
            "r_sub":      round(r_sub, 4),
            "w1_big":     round(w1_big, 4),
            "w1_sub":     round(w1_sub, 4),
            "d1":         d1,
            "d2":         d2,
            "d3":         d3,
            "d4":         d4,
            "total_bars": total,
            "score":      score,
            "threshold":  cfg.zigzag_threshold,
        })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
