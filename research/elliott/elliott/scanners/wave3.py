# =============================================================================
# Wave3Scanner — detect active and completed Wave 3 setups
# =============================================================================
# Purpose:
#  - Wave3 active: close > P1 + buffer after valid P0-P1-P2 (1-2 pattern)
#  - Wave3 done: P3 confirmed high with P3 > P1
#  - Returns list of PatternCandidate with target zones
# =============================================================================

from __future__ import annotations

import pandas as pd

from elliott.elliott.config import ElliottConfig
from elliott.elliott.data import PatternCandidate, Pivot, PivotKind
from elliott.elliott.indicators.atr import atr14
from elliott.elliott.scoring.ratios import band_score


class Wave3Scanner:
    # ==========================================================================
    # Wave3Scanner(df, cfg, pivots) -> list[PatternCandidate]
    # ==========================================================================
    # Purpose:
    #  - Scan all 3-pivot (P0-P1-P2) windows for valid 1-2 setups
    #  - Checks Wave 3 trigger: close > P1 + buffer
    #  - Returns candidates where trigger has been observed
    # ==========================================================================

    def scan(
        self,
        df:         pd.DataFrame,
        cfg:        ElliottConfig,
        pivots:     list[Pivot],
        direction:  int = 1,
    ) -> list[PatternCandidate]:
        closes = df["close"].to_numpy(dtype=float)
        atr    = atr14(df)
        times  = df["open_time"].astype(str).to_numpy()

        if direction > 0:
            want_p0 = PivotKind.LOW
            want_p1 = PivotKind.HIGH
            want_p2 = PivotKind.LOW
        else:
            want_p0 = PivotKind.HIGH
            want_p1 = PivotKind.LOW
            want_p2 = PivotKind.HIGH

        candidates: list[PatternCandidate] = []
        d = direction

        for i in range(2, len(pivots)):
            p0, p1, p2 = pivots[i - 2], pivots[i - 1], pivots[i]

            if p0.kind != want_p0 or p1.kind != want_p1 or p2.kind != want_p2:
                continue

            # -------------------------------------------------------------------------
            # Hard 1-2 rules
            # -------------------------------------------------------------------------
            W1 = d * p1.price - d * p0.price
            W2 = d * p1.price - d * p2.price

            if W1 <= 0:
                continue
            if not (0 < W2 < W1):
                continue

            R2 = W2 / W1
            r2_score = band_score(R2, (0.236, 0.854), [0.5, 0.618], cfg.fib_tol)

            # -------------------------------------------------------------------------
            # Check Wave 3 trigger: any bar after P2 confirmation where
            # close > P1 + buffer (bullish) or close < P1 - buffer (bearish)
            # -------------------------------------------------------------------------
            buffer_atr = float(atr[p2.confirmed_idx]) * cfg.wave3_buffer_atr
            buffer     = max(buffer_atr, 2.0 * cfg.tick_size)

            trigger_bar = None
            trigger_close = None
            start_check   = p2.confirmed_idx + 1
            end_check     = min(len(closes), p2.confirmed_idx + 50)

            for bar in range(start_check, end_check):
                c = closes[bar]
                if direction > 0 and c > p1.price + buffer:
                    trigger_bar   = bar
                    trigger_close = c
                    break
                if direction < 0 and c < p1.price - buffer:
                    trigger_bar   = bar
                    trigger_close = c
                    break

            if trigger_bar is None:
                continue

            # -------------------------------------------------------------------------
            # Build target zones
            # -------------------------------------------------------------------------
            entry_price = trigger_close
            stop        = p2.price
            target_1    = p2.price + direction * 1.000 * W1
            target_2    = p2.price + direction * 1.618 * W1
            target_3    = p2.price + direction * 2.618 * W1

            score = 100.0 * (
                0.50 * r2_score
                + 0.30 * 1.0
                + 0.20 * min(1.0, W1 / (float(atr[p0.idx]) + 1e-10) / 2.0)
            )

            candidates.append(PatternCandidate(
                pattern_type       = "WAVE3_SETUP",
                start_idx          = p0.idx,
                end_idx            = trigger_bar,
                confirmed_idx      = trigger_bar,
                pivots             = [p0, p1, p2],
                direction          = direction,
                degree             = p0.degree,
                hard_pass          = True,
                score              = round(score, 2),
                invalidation_level = stop,
                target_zones       = {
                    "target_1000_W1": round(target_1, 4),
                    "target_1618_W1": round(target_2, 4),
                    "target_2618_W1": round(target_3, 4),
                },
                diagnostics = {
                    "R2":          round(R2, 4),
                    "W1":          round(W1, 4),
                    "trigger_bar": trigger_bar,
                    "entry":       round(entry_price, 4),
                    "stop":        round(stop, 4),
                },
            ))

        return candidates
