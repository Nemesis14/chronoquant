# =============================================================================
# TriangleValidator — contracting / barrier / expanding (3-3-3-3-3)
# =============================================================================
# Purpose:
#  - Validate 6-pivot triangle: Q0=start, Q1=A, Q2=B, Q3=C, Q4=D, Q5=E
#  - direction = direction of preceding impulse
#  - Hard rules per spec §8.3 and §17.7
# =============================================================================

from __future__ import annotations

from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import Pivot
from elliott_waves.elliott.validators.base import (
    PatternValidator,
    ValidationResult,
    eps_price,
)


class TriangleValidator(PatternValidator):
    pattern_type = "TRIANGLE"

    def validate(
        self,
        pivots:    list[Pivot],
        direction: int,
        cfg:       ElliottConfig,
    ) -> ValidationResult:
        if len(pivots) != 6:
            return ValidationResult.fail("Triangle needs exactly 6 pivots")

        y   = [p.y(direction) for p in pivots]
        Q0, Q1, Q2, Q3, Q4, Q5 = y
        eps = eps_price(pivots, cfg)
        barrier_eps = cfg.barrier_eps_atr * (
            sum(p.atr for p in pivots if p.atr > 0) / max(1, sum(1 for p in pivots if p.atr > 0))
        )
        if barrier_eps <= 0:
            barrier_eps = cfg.tick_size * 5

        # -------------------------------------------------------------------------
        # Hard rule: ABCDE alternating Q0 > Q1 < Q2 > Q3 < Q4 > Q5
        # -------------------------------------------------------------------------
        if not (Q1 < Q0 and Q2 > Q1 and Q3 < Q2 and Q4 > Q3 and Q5 < Q4):
            return ValidationResult.fail("Bad triangle alternation")

        # -------------------------------------------------------------------------
        # Hard rule: each leg is a corrective (at least 3 pivots needed internally
        # — proxy: legs are not too short in bars)
        # -------------------------------------------------------------------------
        min_bars = 2
        legs = list(zip(pivots, pivots[1:]))
        for pa, pb in legs:
            if abs(pb.idx - pa.idx) < min_bars:
                return ValidationResult.fail("Triangle leg too short (bars)")

        # -------------------------------------------------------------------------
        # Subtype detection
        # -------------------------------------------------------------------------
        contracting = (
            Q1 + eps < Q3
            and Q2 - eps > Q4
            and Q3 - eps <= Q5
        )
        barrier = (
            abs(Q4 - Q2) <= barrier_eps
            and Q1 + eps < Q3
        )
        expanding = (
            Q1 - eps > Q3
            and Q2 + eps < Q4
        )

        if contracting:
            pattern_type = "CONTRACTING_TRIANGLE"
            overlap_score = min(1.0, (Q3 - Q1) / max(eps, abs(Q2 - Q0)) + 0.5)
            geometry_score = min(1.0, (Q4 - Q3) / max(eps, abs(Q2 - Q1)) * 0.5 + 0.5)
            score = 100.0 * (
                0.40 * 1.0
                + 0.30 * overlap_score
                + 0.20 * geometry_score
                + 0.10 * 0.5
            )

        elif barrier:
            pattern_type = "BARRIER_TRIANGLE"
            score = 100.0 * (
                0.40 * 1.0
                + 0.30 * 0.8
                + 0.20 * 0.7
                + 0.10 * 0.5
            )

        elif expanding and cfg.allow_expanding_triangle:
            pattern_type = "EXPANDING_TRIANGLE"
            score = 100.0 * (
                0.40 * 1.0
                + 0.30 * 0.5
                + 0.20 * 0.5
                + 0.10 * 0.5
            ) * 0.80  # expanding is rarer, mild penalty

        else:
            return ValidationResult.fail("No triangle subtype matched")

        return ValidationResult(
            valid        = True,
            pattern_type = pattern_type,
            score        = round(score, 2),
            diagnostics  = {
                "Q0_y": round(Q0, 4),
                "Q1_y": round(Q1, 4),
                "Q2_y": round(Q2, 4),
                "Q3_y": round(Q3, 4),
                "Q4_y": round(Q4, 4),
                "Q5_y": round(Q5, 4),
            },
        )
