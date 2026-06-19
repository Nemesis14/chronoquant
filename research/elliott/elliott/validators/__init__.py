# =============================================================================
# Elliott Wave validators package
# =============================================================================

from elliott.elliott.data import Pivot
from elliott.elliott.validators.base import PatternValidator, ValidationResult
from elliott.elliott.validators.combination import CombinationValidator
from elliott.elliott.validators.diagonal import DiagonalValidator
from elliott.elliott.validators.double_zigzag import DoubleZigZagValidator
from elliott.elliott.validators.flat import FlatValidator
from elliott.elliott.validators.full_cycle import FullCycleValidator
from elliott.elliott.validators.impulse import ImpulseValidator
from elliott.elliott.validators.triangle import TriangleValidator
from elliott.elliott.validators.zigzag_abc import ZigZagValidator

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
