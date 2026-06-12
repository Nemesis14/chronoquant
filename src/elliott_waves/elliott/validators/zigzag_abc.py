# =============================================================================
# ZigZagValidator — ABC 5-3-5 correction
# =============================================================================
# Purpose:
#  - Validate 4-pivot zigzag correction after an impulse
#  - Q0=top, Q1=A end, Q2=B end, Q3=C end
#  - direction = direction of the preceding impulse
#  - In direction-transformed y: Q0.y > Q1.y < Q2.y > Q3.y
#  - Hard rules per spec §8.1 and §17.5
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


class ZigZagValidator(PatternValidator):
    pattern_type = "ZIGZAG"

    def validate(
        self,
        pivots:    list[Pivot],
        direction: int,
        cfg:       ElliottConfig,
    ) -> ValidationResult:
        if len(pivots) != 4:
            return ValidationResult.fail("Zigzag needs exactly 4 pivots")

        y   = [p.y(direction) for p in pivots]
        Q0, Q1, Q2, Q3 = y
        eps = eps_price(pivots, cfg)

        # -------------------------------------------------------------------------
        # Hard rule: alternating structure Q0 > Q1 < Q2 > Q3
        # -------------------------------------------------------------------------
        if not (Q1 < Q0 and Q2 > Q1 and Q3 < Q2):
            return ValidationResult.fail("Bad zigzag alternation")

        # -------------------------------------------------------------------------
        # Hard rule: B does not exceed start of A
        # -------------------------------------------------------------------------
        if Q0 + eps <= Q2:
            return ValidationResult.fail("Zigzag B exceeded start of A")

        # -------------------------------------------------------------------------
        # Hard rule: C must extend beyond A end (in normal zigzag)
        # -------------------------------------------------------------------------
        if Q1 - eps <= Q3:
            return ValidationResult.fail("Zigzag C did not exceed A end")

        A_len = Q0 - Q1
        if A_len <= 0:
            return ValidationResult.fail("A wave length is zero")

        B_ret = (Q2 - Q1) / A_len
        C_ext = (Q2 - Q3) / A_len

        fib   = cfg.fib_tol
        score = 100.0 * (
            0.35 * 1.0   # hard structure
            + 0.30 * band_score(B_ret, (0.236, 0.786), [0.382, 0.500, 0.618], fib)
            + 0.25 * band_score(C_ext, (0.618, 2.618), [1.000, 1.618], fib)
            + 0.10 * 0.5  # internal substructure (5-3-5) — neutral default
        )

        return ValidationResult(
            valid        = True,
            pattern_type = "ZIGZAG",
            score        = round(score, 2),
            diagnostics  = {
                "B_ret": round(B_ret, 4),
                "C_ext": round(C_ext, 4),
                "A_len": round(A_len, 4),
            },
        )
