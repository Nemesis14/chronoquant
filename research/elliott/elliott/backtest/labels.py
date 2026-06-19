# =============================================================================
# LabelGenerator — wave setup outcome labeling for backtest
# =============================================================================
# Purpose:
#  - Given setup candidates and forward OHLCV, determine if each setup succeeded
#  - Wave3 success: price reaches target_2 before stop
#  - Wave5 success: price reaches channel_target before stop
#  - ABC success: price reaches 0.382 retrace before new high
#  - Adds binary label columns to the candidates DataFrame
# =============================================================================

from __future__ import annotations

import pandas as pd

from elliott.elliott.data import PatternCandidate


# =============================================================================
# LabelGenerator.label(candidates, df_ohlcv) -> pd.DataFrame
# =============================================================================
class LabelGenerator:

    def label(
        self,
        candidates: list[PatternCandidate],
        df_ohlcv:   pd.DataFrame,
        direction:  int = 1,
    ) -> pd.DataFrame:
        """
        Label each candidate with binary outcome columns.
        Uses only bars from confirmed_idx onward (no lookahead).
        """
        closes = df_ohlcv["close"].to_numpy(dtype=float)
        highs  = df_ohlcv["high"].to_numpy(dtype=float)
        lows   = df_ohlcv["low"].to_numpy(dtype=float)
        n      = len(closes)

        rows = []
        for cand in candidates:
            entry_bar = cand.confirmed_idx
            stop      = cand.invalidation_level

            if stop is None or entry_bar >= n:
                continue

            targets = cand.target_zones

            # -------------------------------------------------------------------------
            # Check each forward bar for target or stop hit
            # -------------------------------------------------------------------------
            hit_target = {k: False for k in targets}
            hit_stop   = False

            for bar in range(entry_bar + 1, min(n, entry_bar + 240)):
                h = highs[bar]
                lo = lows[bar]

                if direction > 0:
                    if lo <= stop:
                        hit_stop = True
                        break
                    for k, tgt in targets.items():
                        if h >= tgt and not hit_target[k]:
                            hit_target[k] = True
                else:
                    if h >= stop:
                        hit_stop = True
                        break
                    for k, tgt in targets.items():
                        if lo <= tgt and not hit_target[k]:
                            hit_target[k] = True

            row: dict = {
                "pattern_type":  cand.pattern_type,
                "start_idx":     cand.start_idx,
                "confirmed_idx": cand.confirmed_idx,
                "direction":     cand.direction,
                "degree":        cand.degree,
                "score":         cand.score,
                "stop":          stop,
                "hit_stop":      hit_stop,
            }
            for k, v in hit_target.items():
                row[f"hit_{k}"] = v

            rows.append(row)

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
