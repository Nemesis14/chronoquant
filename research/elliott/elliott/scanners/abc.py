# =============================================================================
# ABCScanner — detect ABC correction after impulse
# =============================================================================
# Purpose:
#  - Given a completed impulse (P0..P5), detect the corrective ABC phase
#  - Uses all corrective validators in order of priority
#  - Returns PatternCandidate with depth zones
# =============================================================================

from __future__ import annotations

import pandas as pd

from elliott.elliott.config import ElliottConfig
from elliott.elliott.data import PatternCandidate, Pivot
from elliott.elliott.validators._simple_corrective import validate_simple_corrective


class ABCScanner:

    def scan(
        self,
        df:        pd.DataFrame,
        cfg:       ElliottConfig,
        pivots:    list[Pivot],
        direction: int = 1,
        impulse_end_idx: int = 0,
    ) -> list[PatternCandidate]:
        """
        Scan pivots after impulse_end_idx for ABC corrections.
        direction = impulse direction (+1 bullish, -1 bearish).
        """
        d = direction

        # Pivots after the impulse end
        corr_pivots = [p for p in pivots if p.idx > impulse_end_idx]
        if len(corr_pivots) < 4:
            return []

        candidates: list[PatternCandidate] = []

        # -------------------------------------------------------------------------
        # Try each 4-pivot window as a potential ABC
        # -------------------------------------------------------------------------
        for i in range(0, len(corr_pivots) - 2):
            window = corr_pivots[i : i + 4]
            if len(window) < 4:
                break

            result = validate_simple_corrective(window, d, cfg)
            if result is None or not result.valid:
                continue

            # -------------------------------------------------------------------------
            # Compute depth (C end relative to prior impulse)
            # -------------------------------------------------------------------------
            impulse_pivots = [p for p in pivots if p.idx <= impulse_end_idx]
            if len(impulse_pivots) >= 6:
                impulse_start = impulse_pivots[-6].price
                impulse_end   = impulse_pivots[-1].price
                impulse_len   = abs(d * impulse_end - d * impulse_start)
                correction_depth = abs(d * window[-1].price - d * impulse_end) / max(1e-10, impulse_len)
            else:
                correction_depth = 0.0

            candidates.append(PatternCandidate(
                pattern_type       = f"ABC_{result.pattern_type}",
                start_idx          = window[0].idx,
                end_idx            = window[-1].idx,
                confirmed_idx      = window[-1].confirmed_idx,
                pivots             = list(window),
                direction          = direction,
                degree             = window[0].degree,
                hard_pass          = True,
                score              = round(result.score, 2),
                invalidation_level = impulse_pivots[-6].price if len(impulse_pivots) >= 6 else None,
                target_zones       = {},
                diagnostics        = {
                    "corr_type":  result.pattern_type,
                    "depth_pct":  round(correction_depth * 100, 2),
                },
            ))

        return candidates
