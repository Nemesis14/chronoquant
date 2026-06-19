# =============================================================================
# Elliott Wave core data models
# =============================================================================
# Purpose:
#  - Immutable data structures: Candle, Pivot, WaveSegment, PatternCandidate
#  - OHLCV loading and resampling utilities
# =============================================================================

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd


class PivotKind(str, Enum):
    HIGH = "HIGH"
    LOW  = "LOW"


@dataclass(frozen=True)
class Candle:
    idx:    int
    ts:     str     # "YYYY-MM-DD HH:MM:SS"
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float = 0.0


@dataclass(frozen=True)
class Pivot:
    idx:           int         # bar index in source DataFrame
    ts:            str         # "YYYY-MM-DD HH:MM:SS"
    price:         float       # extremum price (high for HIGH, low for LOW)
    kind:          PivotKind
    degree:        int         # 0=micro, 1=trade, 2=macro
    confirmed_idx: int         # bar index at which pivot was confirmed
    confirmed_ts:  str         # timestamp of confirmation bar
    atr:           float = 0.0

    def y(self, direction: int) -> float:
        """Direction-transformed coordinate: direction * price."""
        return direction * self.price


@dataclass
class WaveSegment:
    start: Pivot
    end:   Pivot

    @property
    def direction(self) -> int:
        return 1 if self.end.price > self.start.price else -1

    @property
    def length(self) -> float:
        return abs(self.end.price - self.start.price)

    @property
    def bars(self) -> int:
        return self.end.idx - self.start.idx


@dataclass
class PatternCandidate:
    pattern_type:       str
    start_idx:          int
    end_idx:            int
    confirmed_idx:      int         # last pivot confirmed_idx — the barrier
    pivots:             list[Pivot]
    direction:          int         # +1 bullish, -1 bearish
    degree:             int
    hard_pass:          bool
    score:              float       # 0–100
    subpatterns:        list[PatternCandidate]    = field(default_factory=list)
    invalidation_level: float | None              = None
    target_zones:       dict[str, float]          = field(default_factory=dict)
    diagnostics:        dict[str, Any]            = field(default_factory=dict)


# =============================================================================
# load_ohlcv(db_path, table, start, end) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Load OHLCV rows from SQLite database
# Parameters:
#  - db_path: path to SQLite database file
#  - table: table name (e.g. "solusdt_1m")
#  - start / end: optional "YYYY-MM-DD HH:MM:SS" bounds
# =============================================================================
def load_ohlcv(
    db_path: str,
    table:   str,
    start:   str | None = None,
    end:     str | None = None,
) -> pd.DataFrame:
    where  = ""
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
            params = params,
        )
    return df


# =============================================================================
# resample_ohlcv(df_1m, freq) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Resample 1m OHLCV to any higher timeframe
# Parameters:
#  - df_1m: 1m OHLCV DataFrame
#  - freq: pandas resample frequency string (e.g. "5min", "15min", "1h")
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
