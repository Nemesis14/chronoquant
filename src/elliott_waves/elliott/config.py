# =============================================================================
# Elliott Wave configuration
# =============================================================================
# Purpose:
#  - ElliottConfig dataclass with all pivot, tolerance, and pattern settings
#  - for_timeframe() factory provides calibrated presets per timeframe
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ElliottConfig:
    timeframe:      str   = "1m"
    candle_seconds: int   = 60

    # -------------------------------------------------------------------------
    # Pivot parameters
    # -------------------------------------------------------------------------
    fractal_left:      int   = 3
    fractal_right:     int   = 3
    zigzag_threshold:  float = 0.010    # fractional reversal to confirm pivot
    min_reversal_atr:  float = 0.75     # ATR multiplier for minimum reversal
    min_reversal_pct:  float = 0.0005   # price-fraction fallback
    tick_size:         float = 0.01

    # -------------------------------------------------------------------------
    # Tolerances
    # -------------------------------------------------------------------------
    fib_tol:         float = 0.10   # Fibonacci proximity band
    overlap_atr:     float = 0.10   # ATR multiplier for Wave4/Wave1 overlap
    eps_atr:         float = 0.05   # ATR multiplier for strict comparisons
    shortest_tol:    float = 0.03   # Wave3-shortest tolerance
    barrier_eps_atr: float = 0.20   # ATR multiplier for barrier triangle side

    # -------------------------------------------------------------------------
    # Pattern permissions
    # -------------------------------------------------------------------------
    allow_truncation:         bool = True
    allow_expanding_triangle: bool = False
    allow_running_flat:       bool = True

    # -------------------------------------------------------------------------
    # Parser / candidate filter
    # -------------------------------------------------------------------------
    min_score:          float = 55.0
    emit_score:         float = 70.0
    top_k_per_interval: int   = 5
    max_window_pivots:  int   = 20

    # -------------------------------------------------------------------------
    # Wave 3 trigger
    # -------------------------------------------------------------------------
    wave3_buffer_atr: float = 0.10   # ATR multiplier for P1 + buffer trigger

    @classmethod
    def for_timeframe(cls, tf: str) -> ElliottConfig:
        """Calibrated preset per timeframe."""
        presets: dict[str, dict] = {
            "1m":  dict(timeframe="1m",  candle_seconds=60,   zigzag_threshold=0.005, fractal_left=2, fractal_right=2),
            "5m":  dict(timeframe="5m",  candle_seconds=300,  zigzag_threshold=0.010, fractal_left=3, fractal_right=3),
            "15m": dict(timeframe="15m", candle_seconds=900,  zigzag_threshold=0.015, fractal_left=3, fractal_right=3),
            "1h":  dict(timeframe="1h",  candle_seconds=3600, zigzag_threshold=0.025, fractal_left=5, fractal_right=5),
            "4h":  dict(timeframe="4h",  candle_seconds=14400, zigzag_threshold=0.035, fractal_left=5, fractal_right=5),
        }
        kw = presets.get(tf, {})
        if kw:
            return cls(**kw)
        return cls(timeframe=tf)
