# =============================================================================
# FlatValidator — 3-3-5 ABC correction (regular / expanded / running)
# =============================================================================
# Purpose:
#  - Validate 4-pivot flat correction: Q0=top, Q1=A end, Q2=B end, Q3=C end
#  - direction = direction of the preceding impulse
#  - Hard rules + subtype auto-detection per spec §8.2 and §17.6
# =============================================================================

from __future__ import annotations

from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import Pivot
from elliott_waves.elliott.scoring.ratios import band_score
from elliott_waves.elliott.validators.base import (
    PatternValidator,
    ValidationResult,
    eps_price,
)


class FlatValidator(PatternValidator):
    pattern_type = "FLAT"

    def validate(
        self,
        pivots:    list[Pivot],
        direction: int,
        cfg:       ElliottConfig,
    ) -> ValidationResult:
        if len(pivots) != 4:
            return ValidationResult.fail("Flat needs exactly 4 pivots")

        y   = [p.y(direction) for p in pivots]
        Q0, Q1, Q2, Q3 = y
        eps = eps_price(pivots, cfg)

        # -------------------------------------------------------------------------
        # Hard rule: alternating Q0 > Q1 < Q2 > Q3
        # -------------------------------------------------------------------------
        if not (Q1 < Q0 and Q2 > Q1 and Q3 < Q2):
            return ValidationResult.fail("Bad flat alternation")

        A_len = Q0 - Q1
        if A_len <= 0:
            return ValidationResult.fail("A wave length is zero")

        B_ret = (Q2 - Q1) / A_len
        C_ext = (Q2 - Q3) / A_len

        fib = cfg.fib_tol

        # -------------------------------------------------------------------------
        # Subtype detection and hard rules
        # -------------------------------------------------------------------------
        if 0.90 <= B_ret <= 1.05 and Q1 - eps >= Q3:
            # Regular flat: B ~= A start, C slightly beyond A end
            pattern_type = "REGULAR_FLAT"
            score = 100.0 * (
                0.35 * 1.0
                + 0.35 * band_score(B_ret, (0.80, 1.10), [0.90, 1.00, 1.05], fib)
                + 0.30 * band_score(C_ext, (0.618, 1.382), [1.000, 1.236], fib)
            )

        elif B_ret > 1.05 and Q0 + eps < Q2 and Q1 - eps > Q3:
            # Expanded flat: B exceeds A start, C strongly beyond A end
            if not cfg.allow_running_flat:
                pass  # still allow expanded flat
            pattern_type = "EXPANDED_FLAT"
            score = 100.0 * (
                0.35 * 1.0
                + 0.30 * band_score(B_ret, (1.05, 1.618), [1.236, 1.382], fib)
                + 0.25 * band_score(C_ext, (1.000, 2.000), [1.236, 1.618], fib)
                + 0.10 * 0.5
            )

        elif B_ret > 1.05 and Q0 + eps < Q2 and Q1 - eps < Q3:
            # Running flat: B exceeds A start, C does NOT reach A end
            if not cfg.allow_running_flat:
                return ValidationResult.fail("Running flat not allowed by config")
            pattern_type = "RUNNING_FLAT"
            # Running flat is rare — apply a 0.70 penalty
            score = 0.70 * 100.0 * (
                0.35 * 1.0
                + 0.35 * band_score(B_ret, (1.05, 2.000), [1.382, 1.618], fib)
                + 0.30 * 0.5
            )

        else:
            return ValidationResult.fail("No flat subtype matched")

        return ValidationResult(
            valid        = True,
            pattern_type = pattern_type,
            score        = round(score, 2),
            diagnostics  = {
                "B_ret": round(B_ret, 4),
                "C_ext": round(C_ext, 4),
                "A_len": round(A_len, 4),
            },
        )
