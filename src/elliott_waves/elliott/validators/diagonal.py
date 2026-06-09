# =============================================================================
# DiagonalValidator — ending and leading diagonal (motive)
# =============================================================================
# Purpose:
#  - Validate 6-pivot diagonal: unlike impulse, Wave 4 overlaps Wave 1
#  - Ending diagonal: typical in Wave 5 or C; internal 3-3-3-3-3
#  - Leading diagonal: typical in Wave 1 or A; 3-3-3-3-3 or 5-3-5-3-5
#  - Hard rules per spec §7
# =============================================================================

from __future__ import annotations

from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import Pivot
from elliott_waves.elliott.scoring.geometry import wedge_geometry_score
from elliott_waves.elliott.scoring.ratios import band_score
from elliott_waves.elliott.validators.base import (
    PatternValidator,
    ValidationResult,
    eps_price,
    overlap_price,
)


class DiagonalValidator(PatternValidator):
    pattern_type = "DIAGONAL"

    def __init__(self, diagonal_type: str = "ENDING") -> None:
        self.diagonal_type = diagonal_type  # "ENDING" or "LEADING"

    def validate(
        self,
        pivots:    list[Pivot],
        direction: int,
        cfg:       ElliottConfig,
    ) -> ValidationResult:
        if len(pivots) != 6:
            return ValidationResult.fail("Diagonal needs exactly 6 pivots")

        y   = [p.y(direction) for p in pivots]
        P0, P1, P2, P3, P4, P5 = y

        eps    = eps_price(pivots, cfg)
        ov_eps = overlap_price(pivots, cfg)

        # -------------------------------------------------------------------------
        # Hard rule: alternating structure
        # -------------------------------------------------------------------------
        if not (P0 < P1 and P2 < P1 and P2 < P3 and P4 < P3 and P4 < P5):
            return ValidationResult.fail("Bad diagonal alternation")

        W1 = P1 - P0
        W2 = P1 - P2
        W3 = P3 - P2
        W4 = P3 - P4
        W5 = P5 - P4

        if W1 <= 0 or W3 <= 0:
            return ValidationResult.fail("Wave lengths must be positive")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 2 cannot fully retrace Wave 1
        # -------------------------------------------------------------------------
        if P2 <= P0 + eps:
            return ValidationResult.fail("Wave 2 fully retraced Wave 1 in diagonal")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 3 must exceed Wave 1 end
        # -------------------------------------------------------------------------
        if P3 <= P1 + eps:
            return ValidationResult.fail("Wave 3 did not exceed Wave 1 end in diagonal")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 4 cannot fully retrace Wave 3
        # -------------------------------------------------------------------------
        if P4 <= P2 + eps:
            return ValidationResult.fail("Wave 4 fully retraced Wave 3 in diagonal")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 3 not shortest
        # -------------------------------------------------------------------------
        tol = cfg.shortest_tol
        if W3 < W1 * (1.0 - tol) and W3 < W5 * (1.0 - tol):
            return ValidationResult.fail("Wave 3 is shortest in diagonal")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 4 overlaps Wave 1 territory (required for ending diagonal)
        # Ending diagonal requires P4 <= P1 + overlap_eps
        # -------------------------------------------------------------------------
        has_overlap = P4 <= P1 + ov_eps
        if self.diagonal_type == "ENDING" and not has_overlap:
            return ValidationResult.fail("Ending diagonal requires Wave 4 overlap with Wave 1")

        # -------------------------------------------------------------------------
        # Soft scoring
        # -------------------------------------------------------------------------
        R2 = W2 / W1
        R4 = W4 / W3
        fib   = cfg.fib_tol

        wedge  = wedge_geometry_score(pivots, direction, cfg)
        overlap_bonus = 1.0 if has_overlap else 0.2

        score = 100.0 * (
            0.30 * 1.0   # hard structure
            + 0.25 * overlap_bonus
            + 0.20 * wedge
            + 0.15 * band_score(R2, (0.236, 0.854), [0.5, 0.618], fib)
            + 0.10 * band_score(R4, (0.146, 0.786), [0.382, 0.5, 0.618], fib)
        )

        pattern = f"{self.diagonal_type}_DIAGONAL"

        return ValidationResult(
            valid        = True,
            pattern_type = pattern,
            score        = round(score, 2),
            diagnostics  = {
                "R2": round(R2, 4),
                "R4": round(R4, 4),
                "W1": round(W1, 4),
                "W3": round(W3, 4),
                "W5": round(W5, 4),
                "has_overlap": has_overlap,
                "wedge_score": round(wedge, 4),
            },
        )
