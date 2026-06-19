# =============================================================================
# Fibonacci ratio scoring helpers
# =============================================================================
# Purpose:
#  - retracement, extension: compute wave ratio relationships
#  - band_score: 0-1 proximity score to ideal Fibonacci levels within hard band
#  - fib_score: simplified proximity score to any set of ideal levels
# =============================================================================

from __future__ import annotations

import math

FIB_LEVELS = [0.236, 0.382, 0.500, 0.618, 0.786, 0.854, 1.000, 1.236, 1.382, 1.618, 2.618, 4.236]


# =============================================================================
# retracement(numerator_move, base_move) -> float
# =============================================================================
def retracement(numerator_move: float, base_move: float) -> float:
    """Retrace ratio: numerator_move / base_move. Returns nan if base is 0."""
    if base_move <= 0:
        return float("nan")
    return numerator_move / base_move


# =============================================================================
# extension(extension_move, base_move) -> float
# =============================================================================
def extension(extension_move: float, base_move: float) -> float:
    """Extension ratio: extension_move / base_move. Returns nan if base is 0."""
    if base_move <= 0:
        return float("nan")
    return extension_move / base_move


# =============================================================================
# band_score(value, hard, ideal, tol) -> float
# =============================================================================
# Purpose:
#  - Returns 0 if value is outside the hard band [lo, hi]
#  - Returns 1 if value is exactly at one of the ideal levels
#  - Returns proportional score based on proximity to nearest ideal level
# Parameters:
#  - value: the ratio to score
#  - hard: (lo, hi) hard range — outside returns 0.0
#  - ideal: list of ideal Fibonacci levels within the hard band
#  - tol: tolerance for proximity scoring (default matches cfg.fib_tol)
# =============================================================================
def band_score(
    value: float,
    hard:  tuple[float, float],
    ideal: list[float],
    tol:   float = 0.10,
) -> float:
    if math.isnan(value):
        return 0.0
    lo, hi = hard
    if value < lo or value > hi:
        return 0.0
    d = min(abs(value - x) for x in ideal)
    return max(0.0, min(1.0, 1.0 - d / tol))


# =============================================================================
# fib_score(value, ideal_levels, tol) -> float
# =============================================================================
def fib_score(
    value:         float,
    ideal_levels:  list[float] | None = None,
    tol:           float = 0.10,
) -> float:
    """Proximity score to nearest ideal Fibonacci level (no hard band)."""
    if math.isnan(value):
        return 0.0
    levels = ideal_levels if ideal_levels is not None else FIB_LEVELS
    d      = min(abs(value - x) for x in levels)
    return max(0.0, 1.0 - d / tol)
