# =============================================================================
# DoubleZigZagValidator — W-X-Y correction
# =============================================================================
# Purpose:
#  - Validate a double zigzag: W=zigzag, X=corrective, Y=zigzag
#  - Y end must be further in correction direction than W end
#  - Iterates possible split points within the pivot list
#  - Hard rules per spec §8.4
# =============================================================================

from __future__ import annotations

from modeling.elliott.elliott.config import ElliottConfig
from modeling.elliott.elliott.data import Pivot
from modeling.elliott.elliott.validators._simple_corrective import validate_simple_corrective
from modeling.elliott.elliott.validators.base import PatternValidator, ValidationResult
from modeling.elliott.elliott.validators.zigzag_abc import ZigZagValidator

_ZZ = ZigZagValidator()


class DoubleZigZagValidator(PatternValidator):
    pattern_type = "DOUBLE_ZIGZAG"

    def validate(
        self,
        pivots:    list[Pivot],
        direction: int,
        cfg:       ElliottConfig,
    ) -> ValidationResult:
        # Double zigzag minimum: W(4) + shared pivot + X(2+) + shared pivot + Y(4) = at least 8 pivots
        # but they share endpoints, so minimum unique: 4 + 1 + 2 + 1 + 4 - 2 = 10 raw with overlaps
        # Practically: try splits at various points
        n = len(pivots)
        if n < 7:
            return ValidationResult.fail("Double zigzag needs at least 7 pivots")

        best: ValidationResult | None = None

        # -------------------------------------------------------------------------
        # Try all splits: pivots[0:a] = W, pivots[a:b] = X, pivots[b:] = Y
        # W and Y must be zigzags (4 pivots each)
        # -------------------------------------------------------------------------
        for a in range(3, n - 3):
            W_pivots = pivots[0 : a + 1]    # share endpoint: W ends at a
            if len(W_pivots) != 4:
                continue

            W = _ZZ.validate(W_pivots, direction, cfg)
            if not W.valid:
                continue

            W_end_y = pivots[a].y(direction)

            for b in range(a + 1, n - 3):
                X_pivots = pivots[a : b + 1]
                if len(X_pivots) < 2:
                    continue

                X = validate_simple_corrective(X_pivots, direction, cfg)
                if X is None:
                    # For X, try a minimal 2-pivot corrective (just a simple bounce)
                    if len(X_pivots) == 2:
                        X_start_y = X_pivots[0].y(direction)
                        X_end_y   = X_pivots[1].y(direction)
                        # X goes counter to correction (upward in transformed space)
                        if X_end_y <= X_start_y:
                            continue
                        X = ValidationResult(valid=True, pattern_type="X_CORRECTIVE", score=50.0)
                    else:
                        continue

                Y_pivots = pivots[b:]
                if len(Y_pivots) != 4:
                    continue

                Y = _ZZ.validate(Y_pivots, direction, cfg)
                if not Y.valid:
                    continue

                Y_end_y = pivots[-1].y(direction)

                # -------------------------------------------------------------------------
                # Hard rule: Y must continue further in correction direction than W
                # In transformed y (corrective direction shows as decreasing):
                # Y end < W end
                # -------------------------------------------------------------------------
                if Y_end_y >= W_end_y:
                    continue

                score = 0.40 * W.score + 0.20 * X.score + 0.40 * Y.score

                candidate = ValidationResult(
                    valid        = True,
                    pattern_type = "DOUBLE_ZIGZAG",
                    score        = round(score, 2),
                    subpatterns  = [W, X, Y],
                    diagnostics  = {
                        "W_score": W.score,
                        "X_score": X.score,
                        "Y_score": Y.score,
                        "split_a": a,
                        "split_b": b,
                    },
                )
                if best is None or candidate.score > best.score:
                    best = candidate

        if best is None:
            return ValidationResult.fail("No valid double zigzag splits found")
        return best
