# =============================================================================
# Wave5Scanner — detect Wave 5 (normal / ending diagonal / truncated)
# =============================================================================
# Purpose:
#  - Given valid P0..P4 (1-2-3-4), detect Wave 5 at confirmed P5
#  - Validates P5 > P3 (normal) or truncation
#  - Checks ending diagonal pattern
#  - Returns PatternCandidate with target zones from channel projection
# =============================================================================

from __future__ import annotations

import pandas as pd

from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import PatternCandidate, Pivot, PivotKind
from elliott_waves.elliott.scoring.ratios import band_score


class Wave5Scanner:

    def scan(
        self,
        df:        pd.DataFrame,
        cfg:       ElliottConfig,
        pivots:    list[Pivot],
        direction: int = 1,
    ) -> list[PatternCandidate]:
        d = direction

        if direction > 0:
            want_p5 = PivotKind.HIGH
        else:
            want_p5 = PivotKind.LOW

        candidates: list[PatternCandidate] = []

        for i in range(5, len(pivots)):
            p0, p1, p2, p3, p4, p5 = (
                pivots[i - 5], pivots[i - 4], pivots[i - 3],
                pivots[i - 2], pivots[i - 1], pivots[i],
            )

            if p5.kind != want_p5:
                continue

            # -------------------------------------------------------------------------
            # Hard: Wave 4 hard rules must still hold
            # -------------------------------------------------------------------------
            if not (d * p4.price > d * p2.price):
                continue
            if not (d * p4.price > d * p1.price):
                continue

            W1 = abs(d * p1.price - d * p0.price)
            W3 = abs(d * p3.price - d * p2.price)
            W5 = abs(d * p5.price - d * p4.price)

            if W1 <= 0 or W3 <= 0:
                continue

            # -------------------------------------------------------------------------
            # Hard: Wave 3 not shortest
            # -------------------------------------------------------------------------
            tol = cfg.shortest_tol
            if W3 < W1 * (1.0 - tol) and W3 < W5 * (1.0 - tol):
                continue

            # -------------------------------------------------------------------------
            # Normal vs truncated
            # -------------------------------------------------------------------------
            truncation = d * p5.price <= d * p3.price
            if truncation and not cfg.allow_truncation:
                continue

            E5 = W5 / W1
            fib = cfg.fib_tol
            e5_score = band_score(E5, (0.382, 2.618), [0.618, 1.0, 1.618], fib)

            score = 100.0 * (
                0.35 * 1.0
                + 0.35 * e5_score
                + 0.20 * band_score(W3 / W1, (0.618, 4.236), [1.382, 1.618, 2.618], fib)
                + 0.10 * (0.5 if truncation else 1.0)
            )

            if truncation:
                score *= 0.75

            pattern_type = "WAVE5_TRUNCATED" if truncation else "WAVE5"

            # Channel target
            channel_target = d * p4.price + W1
            candidates.append(PatternCandidate(
                pattern_type       = pattern_type,
                start_idx          = p4.idx,
                end_idx            = p5.idx,
                confirmed_idx      = p5.confirmed_idx,
                pivots             = [p0, p1, p2, p3, p4, p5],
                direction          = direction,
                degree             = p0.degree,
                hard_pass          = True,
                score              = round(score, 2),
                invalidation_level = p4.price,
                target_zones       = {
                    "channel_target": round(channel_target / d, 4),
                },
                diagnostics = {
                    "W1": round(W1, 4),
                    "W3": round(W3, 4),
                    "W5": round(W5, 4),
                    "E5": round(E5, 4),
                    "truncation": truncation,
                },
            ))

        return candidates
