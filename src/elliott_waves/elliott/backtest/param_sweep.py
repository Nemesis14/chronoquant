# =============================================================================
# ParamSweep — grid search over zigzag_threshold × fib_tol × min_score
# =============================================================================
# Purpose:
#  - Run detect_1212 or DynamicParser over a grid of parameter combinations
#  - Evaluate each combination with BacktestEvaluator
#  - Return sorted results DataFrame
# =============================================================================

from __future__ import annotations

import itertools

import pandas as pd

from elliott_waves.elliott.backtest.evaluator import BacktestEvaluator
from elliott_waves.elliott.backtest.labels import LabelGenerator
from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.pivots.zigzag import detect_zigzag
from elliott_waves.elliott.scanners.wave3 import Wave3Scanner


class ParamSweep:
    # ==========================================================================
    # ParamSweep.run(df, thresholds, fib_tols, min_scores) -> pd.DataFrame
    # ==========================================================================
    # Purpose:
    #  - Run wave3 scan + label + evaluate for each parameter combination
    # ==========================================================================
    def run(
        self,
        df:         pd.DataFrame,
        thresholds: list[float] = [0.005, 0.010, 0.015],
        fib_tols:   list[float] = [0.08, 0.10, 0.12],
        min_scores: list[float] = [55.0, 65.0],
        direction:  int = 1,
    ) -> pd.DataFrame:
        scanner   = Wave3Scanner()
        labeler   = LabelGenerator()
        evaluator = BacktestEvaluator()
        all_rows  = []

        for threshold, fib_tol, min_score in itertools.product(thresholds, fib_tols, min_scores):
            cfg    = ElliottConfig(zigzag_threshold=threshold, fib_tol=fib_tol, min_score=min_score)
            pivots = detect_zigzag(df, cfg, degree=0)
            if len(pivots) < 5:
                continue

            candidates = scanner.scan(df, cfg, pivots, direction=direction)
            if not candidates:
                continue

            labeled = labeler.label(candidates, df, direction=direction)
            if labeled.empty:
                continue

            metrics = evaluator.evaluate(labeled)
            if metrics.empty:
                continue

            for _, row in metrics.iterrows():
                all_rows.append({
                    "threshold": threshold,
                    "fib_tol":   fib_tol,
                    "min_score": min_score,
                    **row.to_dict(),
                })

        if not all_rows:
            return pd.DataFrame()

        result = pd.DataFrame(all_rows)
        return result.sort_values("win_rate", ascending=False).reset_index(drop=True)
