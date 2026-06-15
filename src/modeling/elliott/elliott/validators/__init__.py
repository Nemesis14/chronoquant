# =============================================================================
# Elliott Wave validators package
# =============================================================================

from modeling.elliott.elliott.data import Pivot
from modeling.elliott.elliott.validators.base import PatternValidator, ValidationResult
from modeling.elliott.elliott.validators.combination import CombinationValidator
from modeling.elliott.elliott.validators.diagonal import DiagonalValidator
from modeling.elliott.elliott.validators.double_zigzag import DoubleZigZagValidator
from modeling.elliott.elliott.validators.flat import FlatValidator
from modeling.elliott.elliott.validators.full_cycle import FullCycleValidator
from modeling.elliott.elliott.validators.impulse import ImpulseValidator
from modeling.elliott.elliott.validators.triangle import TriangleValidator
from modeling.elliott.elliott.validators.zigzag_abc import ZigZagValidator

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
