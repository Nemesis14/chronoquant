# =============================================================================
# Wave4Scanner — detect Wave 4 corrective patterns after Wave 3
# =============================================================================
# Purpose:
#  - Given a valid P0-P1-P2-P3 (1-2-3), detect Wave 4 corrective pattern
#  - Checks P4 hard rules: P4 > P2, P4 > P1 (no overlap)
#  - Validates corrective subtype: zigzag / flat / triangle / combination
# =============================================================================

from __future__ import annotations

import pandas as pd

from modeling.elliott.elliott.config import ElliottConfig
from modeling.elliott.elliott.data import PatternCandidate, Pivot, PivotKind
from modeling.elliott.elliott.validators._simple_corrective import validate_simple_corrective


class Wave4Scanner:

    def scan(
        self,
        df:        pd.DataFrame,
        cfg:       ElliottConfig,
        pivots:    list[Pivot],
        direction: int = 1,
    ) -> list[PatternCandidate]:
        d = direction

        if direction > 0:
            want_p3 = PivotKind.HIGH
            want_p4 = PivotKind.LOW
        else:
            want_p3 = PivotKind.LOW
            want_p4 = PivotKind.HIGH

        candidates: list[PatternCandidate] = []

        for i in range(4, len(pivots)):
            p0, p1, p2, p3, p4 = (
                pivots[i - 4], pivots[i - 3], pivots[i - 2], pivots[i - 1], pivots[i]
            )

            if p3.kind != want_p3 or p4.kind != want_p4:
                continue

            # -------------------------------------------------------------------------
            # Hard Wave 4 rules
            # -------------------------------------------------------------------------
            if not (d * p4.price > d * p2.price):
                continue
            if not (d * p4.price > d * p1.price):
                continue

            W3  = abs(d * p3.price - d * p2.price)
            W4  = abs(d * p3.price - d * p4.price)
            R4  = W4 / W3 if W3 > 0 else float("nan")

            # Hard: R4 must be < 1.0
            if not (0 < R4 < 1.0):
                continue

            # -------------------------------------------------------------------------
            # Try corrective pattern validation on the P3-P4 segment
            # -------------------------------------------------------------------------
            segment = [p3, p4]
            if len(segment) >= 4:
                corr = validate_simple_corrective(segment, d, cfg)
            else:
                corr = None

            score_base = max(0.0, 1.0 - abs(R4 - 0.309) / 0.309) * 100.0
            corr_score = corr.score if (corr and corr.valid) else 50.0
            score      = 0.6 * score_base + 0.4 * corr_score

            target_5_eq1 = p4.price + d * abs(d * p1.price - d * p0.price)

            candidates.append(PatternCandidate(
                pattern_type       = "WAVE4_CORRECTION",
                start_idx          = p3.idx,
                end_idx            = p4.idx,
                confirmed_idx      = p4.confirmed_idx,
                pivots             = [p0, p1, p2, p3, p4],
                direction          = direction,
                degree             = p0.degree,
                hard_pass          = True,
                score              = round(score, 2),
                invalidation_level = p2.price,
                target_zones       = {
                    "wave5_target_eq_W1": round(target_5_eq1, 4),
                },
                diagnostics = {
                    "R4":          round(R4, 4),
                    "W3":          round(W3, 4),
                    "W4":          round(W4, 4),
                    "corr_type":   corr.pattern_type if corr else "SIMPLE",
                    "corr_score":  round(corr_score, 2),
                },
            ))

        return candidates
