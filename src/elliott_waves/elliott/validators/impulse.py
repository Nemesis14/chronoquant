# =============================================================================
# ImpulseValidator — 1-2-3-4-5 motive wave
# =============================================================================
# Purpose:
#  - Validate 6-pivot impulse: P0 low, P1 high, P2 low, P3 high, P4 low, P5 high
#  - Hard rules per spec §3.1 and §17.1
#  - Soft scoring per spec §11 and §12.7
# =============================================================================

from __future__ import annotations

from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import Pivot
from elliott_waves.elliott.scoring.geometry import alternation_score, channel_score
from elliott_waves.elliott.scoring.ratios import band_score
from elliott_waves.elliott.validators.base import (
    PatternValidator,
    ValidationResult,
    eps_price,
    overlap_price,
)


class ImpulseValidator(PatternValidator):
    pattern_type = "IMPULSE"

    def validate(
        self,
        pivots:    list[Pivot],
        direction: int,
        cfg:       ElliottConfig,
    ) -> ValidationResult:
        if len(pivots) != 6:
            return ValidationResult.fail("Impulse needs exactly 6 pivots")

        y   = [p.y(direction) for p in pivots]
        P0, P1, P2, P3, P4, P5 = y

        eps    = eps_price(pivots, cfg)
        ov_eps = overlap_price(pivots, cfg)

        # -------------------------------------------------------------------------
        # Hard rule: alternating structure
        # -------------------------------------------------------------------------
        if not (P0 < P1 and P2 < P1 and P2 < P3 and P4 < P3 and P4 < P5):
            return ValidationResult.fail("Bad impulse alternation")

        W1 = P1 - P0
        W2 = P1 - P2
        W3 = P3 - P2
        W4 = P3 - P4
        W5 = P5 - P4

        if W1 <= 0 or W3 <= 0:
            return ValidationResult.fail("Wave lengths must be positive")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 2 cannot retrace all of Wave 1
        # -------------------------------------------------------------------------
        if P0 + eps >= P2:
            return ValidationResult.fail("Wave 2 fully retraced Wave 1")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 3 must exceed Wave 1 end
        # -------------------------------------------------------------------------
        if P1 + eps >= P3:
            return ValidationResult.fail("Wave 3 did not exceed Wave 1 end")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 4 cannot retrace all of Wave 3
        # -------------------------------------------------------------------------
        if P2 + eps >= P4:
            return ValidationResult.fail("Wave 4 fully retraced Wave 3")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 4 cannot overlap Wave 1 territory (normal impulse)
        # -------------------------------------------------------------------------
        if P1 - ov_eps >= P4:
            return ValidationResult.fail("Wave 4 overlaps Wave 1 territory")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 3 is not the shortest actionary wave
        # -------------------------------------------------------------------------
        tol = cfg.shortest_tol
        if W1 * (1.0 - tol) > W3 and W5 * (1.0 - tol) > W3:
            return ValidationResult.fail("Wave 3 is shortest actionary wave")

        # -------------------------------------------------------------------------
        # Hard rule: Wave 5 must make new high (truncation allowed if configured)
        # -------------------------------------------------------------------------
        truncation = False
        if P3 + eps >= P5:
            if not cfg.allow_truncation:
                return ValidationResult.fail("Wave 5 truncation not allowed")
            truncation = True

        # -------------------------------------------------------------------------
        # Soft scoring
        # -------------------------------------------------------------------------
        R2 = W2 / W1
        R4 = W4 / W3
        E3 = W3 / W1
        E5 = W5 / W1

        fib   = cfg.fib_tol
        score = 100.0 * (
            0.30 * 1.0   # hard structure passes
            + 0.20 * band_score(R2, (0.236, 0.854), [0.5, 0.618], fib)
            + 0.15 * band_score(R4, (0.146, 0.50),  [0.236, 0.382], fib)
            + 0.15 * band_score(E3, (0.618, 4.236), [1.382, 1.618, 2.618], fib)
            + 0.10 * band_score(E5, (0.382, 2.618), [0.618, 1.0, 1.618], fib)
            + 0.05 * channel_score(pivots, direction, cfg)
            + 0.05 * alternation_score(pivots, direction, cfg)
        )

        if truncation:
            score *= 0.75

        pattern = "IMPULSE_TRUNCATED" if truncation else "IMPULSE"

        return ValidationResult(
            valid        = True,
            pattern_type = pattern,
            score        = round(score, 2),
            diagnostics  = {
                "R2": round(R2, 4),
                "R4": round(R4, 4),
                "E3": round(E3, 4),
                "E5": round(E5, 4),
                "W1": round(W1, 4),
                "W3": round(W3, 4),
                "W5": round(W5, 4),
                "truncation": truncation,
            },
        )
