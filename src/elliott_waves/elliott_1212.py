# =============================================================================
# Elliott Wave 1-2-1-2 Bullish Setup Detector
# =============================================================================
# Purpose:
#  - Detect nested 1-2-1-2 bullish setups on OHLCV data
#  - Pivot engine: ZigZag threshold style (no lookahead)
#  - Conditions based on docs/analysis/eliot_waves/pivot_spec.md
#  - Returns a DataFrame of detected setups with scores and pivot details
# =============================================================================

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import numpy as np
import pandas as pd


# =============================================================================
# Constants
# =============================================================================

FIB_TARGETS      = [0.382, 0.500, 0.618, 0.786]
FIB_IDEAL        = [0.500, 0.618]
MIN_RETRACE      = 0.382
MAX_RETRACE      = 0.854

# Duration limits (bars on the given timeframe)
D1_MIN, D1_MAX   = 5, 50    # P0 -> P1 (big Wave 1)
D2_MIN, D2_MAX   = 3, 50    # P1 -> P2 (big Wave 2)
D3_MIN, D3_MAX   = 3, 30    # P2 -> P3 (sub Wave 1)
D4_MIN, D4_MAX   = 2, 30    # P3 -> P4 (sub Wave 2)
TOTAL_MIN        = 10
TOTAL_MAX        = 120

# Amplitude minimums in ATR multiples
AMP_BIG_MIN_ATR  = 1.0
AMP_SUB_MIN_ATR  = 0.5


# =============================================================================
# Pivot dataclass
# =============================================================================

@dataclass(frozen=True)
class Pivot:
    bar:      int     # index in the source DataFrame
    price:    float   # extremum price (high for swing high, low for swing low)
    kind:     str     # "high" or "low"
    conf_bar: int     # bar at which this pivot was confirmed


# =============================================================================
# detect_pivots(df, threshold) -> list[Pivot]
# =============================================================================
# Purpose:
#  - ZigZag threshold pivot engine
#  - A swing low is confirmed when close rises >= threshold from the low
#  - A swing high is confirmed when close falls >= threshold from the high
#  - Uses close for confirmation, extremum price for pivot price
# Parameters:
#  - df: DataFrame with columns open_time, high, low, close (indexed 0..N-1)
#  - threshold: fractional reversal required to confirm a pivot (e.g. 0.01 = 1%)
# =============================================================================
def detect_pivots(df: pd.DataFrame, threshold: float) -> list[Pivot]:
    highs  = df["high"].to_numpy(dtype=float)
    lows   = df["low"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    n      = len(closes)
    if n < 4:
        return []

    pivots: list[Pivot] = []
    # start without a confirmed trend — let first threshold breach set direction
    trend    = 0
    hi_price = float(closes[0])
    lo_price = float(closes[0])
    hi_bar   = 0
    lo_bar   = 0

    for i in range(1, n):
        c = float(closes[i])
        h = float(highs[i])
        lo = float(lows[i])

        if trend >= 0:
            if h > hi_price:
                hi_price = h
                hi_bar   = i
            if c <= hi_price * (1.0 - threshold):
                pivots.append(Pivot(bar=hi_bar, price=hi_price, kind="high", conf_bar=i))
                trend    = -1
                lo_price = lo
                lo_bar   = i

        if trend <= 0:
            if lo < lo_price:
                lo_price = lo
                lo_bar   = i
            if c >= lo_price * (1.0 + threshold):
                pivots.append(Pivot(bar=lo_bar, price=lo_price, kind="low", conf_bar=i))
                trend    = 1
                hi_price = h
                hi_bar   = i

    return pivots


# =============================================================================
# _atr14(df) -> np.ndarray
# =============================================================================
# Purpose:
#  - Compute ATR-14 using Wilder's smoothing
#  - Returns array aligned to df index
# =============================================================================
def _atr14(df: pd.DataFrame) -> np.ndarray:
    high  = df["high"].to_numpy(dtype=float)
    low   = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)
    n     = len(close)
    tr    = np.empty(n)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
    atr = np.empty(n)
    atr[0] = tr[0]
    alpha  = 1.0 / 14.0
    for i in range(1, n):
        atr[i] = atr[i - 1] * (1.0 - alpha) + tr[i] * alpha
    return atr


# =============================================================================
# _fib_score(retrace) -> float
# =============================================================================
# Purpose:
#  - Score [0,1] based on proximity to ideal Fibonacci levels (0.5, 0.618)
#  - 1.0 = exactly on 0.5 or 0.618; 0.0 = far from both
# =============================================================================
def _fib_score(retrace: float) -> float:
    best = min(abs(retrace - t) for t in FIB_IDEAL)
    return max(0.0, 1.0 - best / 0.15)


# =============================================================================
# _duration_score(d1, d2, d3, d4) -> float
# =============================================================================
# Purpose:
#  - Penalise legs that are very short (< 4 bars) or very long (> 30 bars)
#  - Returns mean of per-leg scores in [0, 1]
# =============================================================================
def _duration_score(d1: int, d2: int, d3: int, d4: int) -> float:
    scores = []
    for d, lo, hi in [(d1, D1_MIN, 30), (d2, D2_MIN, 30), (d3, D3_MIN, 20), (d4, D4_MIN, 15)]:
        if d < lo:
            scores.append(0.0)
        elif d <= hi:
            scores.append(1.0)
        else:
            scores.append(max(0.0, 1.0 - (d - hi) / float(hi)))
    return float(np.mean(scores))


# =============================================================================
# _amplitude_score(w1_big, w1_sub) -> float
# =============================================================================
# Purpose:
#  - Score based on W1_sub/W1_big ratio: ideal around 0.3–0.7
# =============================================================================
def _amplitude_score(w1_big: float, w1_sub: float) -> float:
    if w1_big <= 0:
        return 0.0
    ratio = w1_sub / w1_big
    return max(0.0, 1.0 - abs(ratio - 0.5) / 0.4)


# =============================================================================
# score_setup(r_big, r_sub, d1, d2, d3, d4, w1_big, w1_sub) -> float
# =============================================================================
# Purpose:
#  - Composite score [0, 1] per spec Section 7
#  - 50% Fibonacci retrace, 25% duration, 25% amplitude
# =============================================================================
def score_setup(
    r_big:  float,
    r_sub:  float,
    d1:     int,
    d2:     int,
    d3:     int,
    d4:     int,
    w1_big: float,
    w1_sub: float,
) -> float:
    fib  = (_fib_score(r_big) + _fib_score(r_sub)) / 2.0
    dur  = _duration_score(d1, d2, d3, d4)
    amp  = _amplitude_score(w1_big, w1_sub)
    return round(0.50 * fib + 0.25 * dur + 0.25 * amp, 4)


# =============================================================================
# detect_1212(df, threshold, min_retrace, max_retrace) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Scan all 5-pivot windows for bullish 1-2-1-2 setups
#  - Returns one row per detected setup at the P4 confirmation bar
# Parameters:
#  - df: OHLCV DataFrame, open_time as str or Timestamp
#  - threshold: pivot confirmation reversal fraction
#  - min_retrace / max_retrace: retrace zone filter
# =============================================================================
def detect_1212(
    df:           pd.DataFrame,
    threshold:    float = 0.010,
    min_retrace:  float = MIN_RETRACE,
    max_retrace:  float = MAX_RETRACE,
) -> pd.DataFrame:
    df   = df.reset_index(drop=True)
    atr  = _atr14(df)
    pivs = detect_pivots(df, threshold)

    times = df["open_time"].astype(str).to_numpy()
    rows  = []

    for i in range(4, len(pivs)):
        p0, p1, p2, p3, p4 = pivs[i - 4], pivs[i - 3], pivs[i - 2], pivs[i - 1], pivs[i]

        # -------------------------------------------------------------------------
        # Alternating direction filter
        # -------------------------------------------------------------------------
        if [p.kind for p in (p0, p1, p2, p3, p4)] != ["low", "high", "low", "high", "low"]:
            continue

        # -------------------------------------------------------------------------
        # Hard structural rules (spec 6.3)
        # -------------------------------------------------------------------------
        if not (p1.price > p0.price):
            continue
        if not (p2.price > p0.price):
            continue
        if not (p3.price > p2.price):
            continue
        if not (p4.price > p2.price):
            continue
        if not (p4.price < p3.price):
            continue

        w1_big = p1.price - p0.price
        w1_sub = p3.price - p2.price

        if w1_big <= 0 or w1_sub <= 0:
            continue

        r_big = (p1.price - p2.price) / w1_big
        r_sub = (p3.price - p4.price) / w1_sub

        # -------------------------------------------------------------------------
        # Retrace zone filter (spec 6.3)
        # -------------------------------------------------------------------------
        if not (min_retrace <= r_big <= max_retrace):
            continue
        if not (min_retrace <= r_sub <= max_retrace):
            continue

        # -------------------------------------------------------------------------
        # Nested structure: sub-wave must be smaller than big wave
        # -------------------------------------------------------------------------
        if w1_sub >= w1_big:
            continue

        # -------------------------------------------------------------------------
        # Amplitude filter: min ATR multiples (spec 6.3)
        # -------------------------------------------------------------------------
        atr_p0 = float(atr[p0.bar])
        atr_p2 = float(atr[p2.bar])
        if w1_big < AMP_BIG_MIN_ATR * atr_p0:
            continue
        if w1_sub < AMP_SUB_MIN_ATR * atr_p2:
            continue

        # -------------------------------------------------------------------------
        # Duration filter (spec Section 4 table)
        # -------------------------------------------------------------------------
        d1 = p1.bar - p0.bar
        d2 = p2.bar - p1.bar
        d3 = p3.bar - p2.bar
        d4 = p4.bar - p3.bar
        total = p4.bar - p0.bar

        if not (D1_MIN <= d1 <= D1_MAX): continue
        if not (D2_MIN <= d2 <= D2_MAX): continue
        if not (D3_MIN <= d3 <= D3_MAX): continue
        if not (D4_MIN <= d4 <= D4_MAX): continue
        if not (TOTAL_MIN <= total <= TOTAL_MAX): continue

        score = score_setup(r_big, r_sub, d1, d2, d3, d4, w1_big, w1_sub)

        rows.append({
            "conf_time":  str(times[p4.conf_bar]),
            "p0_time":    str(times[p0.bar]),
            "p4_time":    str(times[p4.bar]),
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
            "threshold":  threshold,
        })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# =============================================================================
# load_ohlcv(db_path, table, start, end) -> pd.DataFrame
# =============================================================================
def load_ohlcv(
    db_path: str,
    table:   str,
    start:   str | None = None,
    end:     str | None = None,
) -> pd.DataFrame:
    where = ""
    params: list[str] = []
    if start and end:
        where  = "WHERE open_time BETWEEN ? AND ?"
        params = [start, end]
    elif start:
        where  = "WHERE open_time >= ?"
        params = [start]
    elif end:
        where  = "WHERE open_time <= ?"
        params = [end]

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            f"SELECT open_time, open, high, low, close, volume FROM {table} {where} ORDER BY open_time",
            conn,
            params=params,
        )
    return df


# =============================================================================
# resample_ohlcv(df_1m, freq) -> pd.DataFrame
# =============================================================================
def resample_ohlcv(df_1m: pd.DataFrame, freq: str) -> pd.DataFrame:
    df = df_1m.copy()
    df["open_time"] = pd.to_datetime(df["open_time"])
    df = (
        df.set_index("open_time")
        .resample(freq, label="left", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["close"])
        .reset_index()
    )
    df["open_time"] = df["open_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df
