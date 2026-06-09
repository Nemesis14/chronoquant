# =============================================================================
# Elliott Wave validators package
# =============================================================================

from elliott_waves.elliott.validators.base import ValidationResult, PatternValidator
from elliott_waves.elliott.validators.impulse import ImpulseValidator
from elliott_waves.elliott.validators.diagonal import DiagonalValidator
from elliott_waves.elliott.validators.zigzag_abc import ZigZagValidator
from elliott_waves.elliott.validators.flat import FlatValidator
from elliott_waves.elliott.validators.triangle import TriangleValidator
from elliott_waves.elliott.validators.double_zigzag import DoubleZigZagValidator
from elliott_waves.elliott.validators.combination import CombinationValidator
from elliott_waves.elliott.validators.full_cycle import FullCycleValidator
from elliott_waves.elliott.data import Pivot

_ALL_CORRECTIVE = [
    ZigZagValidator(),
    FlatValidator(),
    TriangleValidator(),
    DoubleZigZagValidator(),
    CombinationValidator(),
]


def validate_any_corrective(
    pivots:    list[Pivot],
    direction: int,
    cfg,
) -> ValidationResult | None:
    """Try all corrective validators; return best passing result or None."""
    best: ValidationResult | None = None
    for v in _ALL_CORRECTIVE:
        r = v.validate(pivots, direction, cfg)
        if r.valid and (best is None or r.score > best.score):
            best = r
    return best


__all__ = [
    "ValidationResult",
    "PatternValidator",
    "ImpulseValidator",
    "DiagonalValidator",
    "ZigZagValidator",
    "FlatValidator",
    "TriangleValidator",
    "DoubleZigZagValidator",
    "CombinationValidator",
    "FullCycleValidator",
    "validate_any_corrective",
]
