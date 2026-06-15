# =============================================================================
# CombinationValidator — W-X-Y double/triple three correction
# =============================================================================
# Purpose:
#  - W and Y can be zigzag, flat, or triangle (triangle only as last component)
#  - X is a simple corrective wave
#  - Overall shape should be sideways (not strongly trending)
#  - Hard rules per spec §8.5
# =============================================================================

from __future__ import annotations

from modeling.elliott.elliott.config import ElliottConfig
from modeling.elliott.elliott.data import Pivot
from modeling.elliott.elliott.validators._simple_corrective import validate_simple_corrective
from modeling.elliott.elliott.validators.base import PatternValidator, ValidationResult
from modeling.elliott.elliott.validators.flat import FlatValidator
from modeling.elliott.elliott.validators.triangle import TriangleValidator
from modeling.elliott.elliott.validators.zigzag_abc import ZigZagValidator

_ZZ   = ZigZagValidator()
_FLAT = FlatValidator()
_TRI  = TriangleValidator()


def _validate_wy_component(pivots, direction, cfg, is_last: bool):
    """Validate a W or Y component: zigzag, flat, or triangle (triangle only as last)."""
    best = None
    for v in [_ZZ, _FLAT]:
        r = v.validate(pivots, direction, cfg)
        if r.valid and (best is None or r.score > best.score):
            best = r
    if is_last:
        r = _TRI.validate(pivots, direction, cfg)
        if r.valid and (best is None or r.score > best.score):
            best = r
    return best


def _sideways_score(pivots: list[Pivot], direction: int) -> float:
    """Score how sideways the overall pattern is (low net move = more sideways)."""
    if len(pivots) < 2:
        return 0.5
    total_y_range = max(abs(p.y(direction) - pivots[0].y(direction)) for p in pivots)
    net_move      = abs(pivots[-1].y(direction) - pivots[0].y(direction))
    if total_y_range <= 0:
        return 1.0
    return max(0.0, 1.0 - net_move / total_y_range)


class CombinationValidator(PatternValidator):
    pattern_type = "COMBINATION_WXY"

    def validate(
        self,
        pivots:    list[Pivot],
        direction: int,
        cfg:       ElliottConfig,
    ) -> ValidationResult:
        n = len(pivots)
        if n < 7:
            return ValidationResult.fail("Combination needs at least 7 pivots")

        best: ValidationResult | None = None

        # -------------------------------------------------------------------------
        # Try double-three splits: pivots[0:a] = W, pivots[a:b] = X, pivots[b:] = Y
        # -------------------------------------------------------------------------
        for a in range(3, n - 3):
            W_pivots = pivots[0 : a + 1]

            W = _validate_wy_component(W_pivots, direction, cfg, is_last=False)
            if W is None:
                continue

            for b in range(a + 1, n - 2):
                X_pivots = pivots[a : b + 1]
                X = validate_simple_corrective(X_pivots, direction, cfg)
                if X is None:
                    if len(X_pivots) == 2:
                        X = ValidationResult(valid=True, pattern_type="X_CORRECTIVE", score=50.0)
                    else:
                        continue

                Y_pivots = pivots[b:]
                if len(Y_pivots) < 3:
                    continue

                Y = _validate_wy_component(Y_pivots, direction, cfg, is_last=True)
                if Y is None:
                    continue

                sideways = _sideways_score(pivots, direction)
                complexity_penalty = 0.90
                score = complexity_penalty * (
                    0.35 * W.score
                    + 0.20 * X.score
                    + 0.35 * Y.score
                    + 0.10 * sideways * 100.0
                )

                candidate = ValidationResult(
                    valid        = True,
                    pattern_type = "COMBINATION_WXY",
                    score        = round(score, 2),
                    subpatterns  = [W, X, Y],
                    diagnostics  = {
                        "W_type":   W.pattern_type,
                        "Y_type":   Y.pattern_type,
                        "sideways": round(sideways, 4),
                        "split_a":  a,
                        "split_b":  b,
                    },
                )
                if best is None or candidate.score > best.score:
                    best = candidate

        if best is None:
            return ValidationResult.fail("No valid combination splits found")
        return best
