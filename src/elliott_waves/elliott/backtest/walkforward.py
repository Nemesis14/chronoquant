# =============================================================================
# WalkForwardEvaluator — out-of-sample walk-forward validation
# =============================================================================
# Purpose:
#  - Split data into rolling train/test windows
#  - For each window: scan, label, evaluate on test only
#  - Returns per-fold metrics — no lookahead via confirmed_idx barrier
# =============================================================================

from __future__ import annotations

import pandas as pd

from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.backtest.evaluator import BacktestEvaluator
from elliott_waves.elliott.backtest.labels import LabelGenerator
from elliott_waves.elliott.pivots.zigzag import detect_zigzag
from elliott_waves.elliott.scanners.wave3 import Wave3Scanner


class WalkForwardEvaluator:
    # ==========================================================================
    # WalkForwardEvaluator.run(df, cfg, train_bars, test_bars) -> pd.DataFrame
    # ==========================================================================
    # Purpose:
    #  - Roll windows of train_bars + test_bars across df
    #  - Scan and label candidates only in the TEST window (no lookahead)
    #  - Only use candidates whose confirmed_idx falls within the test window
    # ==========================================================================
    def run(
        self,
        df:         pd.DataFrame,
        cfg:        ElliottConfig,
        train_bars: int = 5000,
        test_bars:  int = 1000,
        step_bars:  int = 500,
        direction:  int = 1,
    ) -> pd.DataFrame:
        scanner   = Wave3Scanner()
        labeler   = LabelGenerator()
        evaluator = BacktestEvaluator()
        n         = len(df)
        all_rows  = []
        fold      = 0

        start = 0
        while start + train_bars + test_bars <= n:
            train_end = start + train_bars
            test_end  = train_end + test_bars

            # -------------------------------------------------------------------------
            # Train window: used to detect pivots only
            # -------------------------------------------------------------------------
            df_train = df.iloc[:train_end].reset_index(drop=True)
            pivots   = detect_zigzag(df_train, cfg, degree=0)

            # -------------------------------------------------------------------------
            # Candidates are only valid if confirmed in the TEST window
            # -------------------------------------------------------------------------
            candidates = scanner.scan(df_train, cfg, pivots, direction=direction)
            test_candidates = [
                c for c in candidates
                if train_end <= c.confirmed_idx < test_end
            ]

            if test_candidates:
                # Use full df up to test_end for forward price data
                df_test = df.iloc[:test_end].reset_index(drop=True)
                labeled = labeler.label(test_candidates, df_test, direction=direction)
                if not labeled.empty:
                    metrics = evaluator.evaluate(labeled)
                    if not metrics.empty:
                        for _, row in metrics.iterrows():
                            all_rows.append({
                                "fold":       fold,
                                "train_end":  train_end,
                                "test_start": train_end,
                                "test_end":   test_end,
                                "n_cands":    len(test_candidates),
                                **row.to_dict(),
                            })

            fold  += 1
            start += step_bars

        if not all_rows:
            return pd.DataFrame()
        return pd.DataFrame(all_rows).reset_index(drop=True)
