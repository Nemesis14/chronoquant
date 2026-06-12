# =============================================================================
# BacktestEvaluator — compute win rate, R-multiples, drawdown
# =============================================================================
# Purpose:
#  - Given labeled candidates DataFrame (from LabelGenerator), compute metrics
#  - evaluate(df) -> dict of metrics per pattern_type
# =============================================================================

from __future__ import annotations

import pandas as pd


class BacktestEvaluator:
    # ==========================================================================
    # evaluate(labeled_df) -> dict
    # ==========================================================================
    # Purpose:
    #  - Compute win_rate, mean_R, max_drawdown per pattern_type
    # ==========================================================================
    def evaluate(
        self,
        labeled_df: pd.DataFrame,
        target_col: str = "hit_target_1618_W1",
        r_multiple: float = 1.618,
    ) -> pd.DataFrame:
        if labeled_df.empty:
            return pd.DataFrame()

        results = []

        for pattern_type, grp in labeled_df.groupby("pattern_type"):
            n_total = len(grp)

            if target_col not in grp.columns:
                win_col = [c for c in grp.columns if c.startswith("hit_") and "target" in c]
                target_col_actual = win_col[0] if win_col else None
            else:
                target_col_actual = target_col

            if target_col_actual and target_col_actual in grp.columns:
                wins     = grp[target_col_actual].sum()
                win_rate = wins / n_total if n_total > 0 else 0.0
            else:
                wins, win_rate = 0, 0.0

            losses   = n_total - wins
            pnl      = wins * r_multiple - losses * 1.0

            results.append({
                "pattern_type": pattern_type,
                "n_total":      n_total,
                "n_wins":       wins,
                "n_losses":     losses,
                "win_rate":     round(win_rate, 4),
                "mean_R":       round(pnl / n_total, 4) if n_total > 0 else 0.0,
                "expectancy":   round(pnl, 2),
            })

        return pd.DataFrame(results)
