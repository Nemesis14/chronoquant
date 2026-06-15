# =============================================================================
# FullCycleValidator — complete 1-2-3-4-5-A-B-C cycle
# =============================================================================
# Purpose:
#  - Validate a full cycle: 5-wave motive + corrective ABC
#  - Tries impulse + diagonal for the motive phase
#  - Tries all corrective validators for the correction phase
#  - Hard rules per spec §9
# =============================================================================

from __future__ import annotations

from modeling.elliott.elliott.config import ElliottConfig
from modeling.elliott.elliott.data import Pivot
from modeling.elliott.elliott.validators.base import PatternValidator, ValidationResult
from modeling.elliott.elliott.validators.combination import CombinationValidator
from modeling.elliott.elliott.validators.diagonal import DiagonalValidator
from modeling.elliott.elliott.validators.double_zigzag import DoubleZigZagValidator
from modeling.elliott.elliott.validators.flat import FlatValidator
from modeling.elliott.elliott.validators.impulse import ImpulseValidator
from modeling.elliott.elliott.validators.triangle import TriangleValidator
from modeling.elliott.elliott.validators.zigzag_abc import ZigZagValidator

_IMPULSE   = ImpulseValidator()
_DIAGONAL  = DiagonalValidator()
_CORR_VALIDATORS = [
    ZigZagValidator(),
    FlatValidator(),
    TriangleValidator(),
    DoubleZigZagValidator(),
    CombinationValidator(),
]


class FullCycleValidator(PatternValidator):
    pattern_type = "FULL_CYCLE"

    def validate(
        self,
        pivots:    list[Pivot],
        direction: int,
        cfg:       ElliottConfig,
    ) -> ValidationResult:
        n = len(pivots)

        # Minimum: 6 motive pivots + 4 corrective pivots, sharing P5
        if n < 9:
            return ValidationResult.fail("Full cycle needs at least 9 pivots")

        best: ValidationResult | None = None

        # -------------------------------------------------------------------------
        # Try splitting at each possible motive/correction boundary
        # Motive is always 6 pivots (P0..P5)
        # -------------------------------------------------------------------------
        motive_pivots = pivots[:6]

        motive = _IMPULSE.validate(motive_pivots, direction, cfg)
        if not motive.valid:
            motive = _DIAGONAL.validate(motive_pivots, direction, cfg)

        if not motive.valid:
            return ValidationResult.fail("No valid motive phase found")

        # Corrective starts at P5 (last motive pivot, shared)
        corr_pivots = pivots[5:]
        if len(corr_pivots) < 4:
            return ValidationResult.fail("Not enough pivots for corrective phase")

        best_corr: ValidationResult | None = None
        for v in _CORR_VALIDATORS:
            # Try exact slice lengths the validator needs
            needed = {
                "ZigZagValidator": 4,
                "FlatValidator":   4,
                "TriangleValidator": 6,
                "DoubleZigZagValidator": None,
                "CombinationValidator": None,
            }
            v_name = type(v).__name__
            req    = needed.get(v_name)
            if req is not None:
                sub = corr_pivots[:req] if len(corr_pivots) >= req else None
                if sub is None:
                    continue
            else:
                sub = corr_pivots

            r = v.validate(sub, direction, cfg)
            if r.valid and (best_corr is None or r.score > best_corr.score):
                best_corr = r

        if best_corr is None:
            return ValidationResult.fail("No valid corrective phase found")

        score = 0.60 * motive.score + 0.40 * best_corr.score

        return ValidationResult(
            valid        = True,
            pattern_type = "FULL_CYCLE",
            score        = round(score, 2),
            subpatterns  = [motive, best_corr],
            diagnostics  = {
                "motive_type":     motive.pattern_type,
                "motive_score":    motive.score,
                "corr_type":       best_corr.pattern_type,
                "corr_score":      best_corr.score,
            },
        )
