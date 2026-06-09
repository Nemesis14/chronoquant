# =============================================================================
# Simple corrective validator helper (no circular imports)
# =============================================================================
# Purpose:
#  - Used internally by double_zigzag and combination for X-wave validation
#  - Tries zigzag, flat, triangle in order; returns best passing result
#  - Does NOT include double_zigzag or combination (prevents circular deps)
# =============================================================================

from __future__ import annotations

from elliott_waves.elliott.data import Pivot
from elliott_waves.elliott.validators.base import ValidationResult
from elliott_waves.elliott.validators.zigzag_abc import ZigZagValidator
from elliott_waves.elliott.validators.flat import FlatValidator
from elliott_waves.elliott.validators.triangle import TriangleValidator

_SIMPLE_VALIDATORS = [ZigZagValidator(), FlatValidator(), TriangleValidator()]


def validate_simple_corrective(
    pivots:    list[Pivot],
    direction: int,
    cfg,
) -> ValidationResult | None:
    """Try simple corrective validators; return best passing result or None."""
    best: ValidationResult | None = None
    for v in _SIMPLE_VALIDATORS:
        r = v.validate(pivots, direction, cfg)
        if r.valid and (best is None or r.score > best.score):
            best = r
    return best
