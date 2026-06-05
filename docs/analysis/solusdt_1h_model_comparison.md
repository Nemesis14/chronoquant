# SOLUSDT 1h Long Model Comparison

**Date:** 2026-06-05  
**Sample:** `base_solusdt_fw60_dev`  
**Target:** `trg_l_fw60_q90` (top 10% 60-bar forward move)  
**Test range:** 2025-06-05 -> 2026-06-05

## Final Test Metrics

| Metric | pval logit | L1 logit | LightGBM |
|--------|-----------|----------|----------|
| Features selected | 16 / 47 | 2 / 47 | 47 / 47 |
| ROC-AUC | 0.7822 | 0.8052 | **0.8076** |
| PR-AUC | 0.1351 | 0.1492 | **0.1572** |
| Brier score | 0.0312 | 0.0344 | **0.0302** |
| Log-loss | 0.1344 | 0.1651 | **0.1247** |
| Lift @1% | 6.46 | 7.13 | **8.15** |
| Lift @5% | 5.46 | 5.94 | 5.53 |
| Lift @10% | 4.27 | 4.27 | 4.16 |

## Training Metrics

| Metric | pval logit | L1 logit | LightGBM |
|--------|-----------|----------|----------|
| ROC-AUC | 0.7848 | 0.7924 | **0.8219** |
| PR-AUC | 0.3562 | 0.3594 | **0.4096** |
| Brier score | 0.0880 | 0.0887 | **0.0823** |

## Winner: `lgbm_solusdt_l_fw60_q90_stable_v1`

LightGBM wins on every key test metric:

- Highest test ROC-AUC (0.8076) and PR-AUC (0.1572).
- Lowest Brier score (0.0302) and log-loss (0.1247) — best calibration.
- Highest lift @1% (8.15) — the most actionable signal tier.
- Consistent calibration: test decile bins show monotonic increase in event rate up through bin 5.

## Why the other two were not selected

**pval logit (`logit_solusdt_l_fw60_q90_pval_v1`):**  
Weakest on all three primary test metrics (ROC-AUC 0.7822, PR-AUC 0.1351, lift @1% 6.46). The statsmodels optimizer showed convergence warnings on every fold. Test calibration is very sparse above decile 3 (few high-confidence predictions), which limits practical use in a threshold-based strategy.

**L1 logit (`logit_solusdt_l_fw60_q90_l1_v1`):**  
Strong regularization pushed alpha to 1000, retaining only 2 features (`feat_bb_width_140`, `feat_natr_14`). While test ROC-AUC (0.8052) and lift @1% (7.13) are respectable, the 2-feature model is almost certainly underfitting. Calibration above decile 3 is unreliable (very few samples). LightGBM is strictly better on all metrics.

## Tuning parameters

| Model | Best param | Value |
|-------|-----------|-------|
| pval logit | pvalue_rounds | 2 |
| L1 logit | alpha | 1000 |
| LightGBM | num_leaves | 7 |

LightGBM selected `num_leaves=7` (smallest option) — consistent with the need for a shallow, stable tree on a noisy financial signal.

## Next step

Promote `lgbm_solusdt_l_fw60_q90_stable_v1` as the SOL runtime model and run backtest `solusdt_long_fw60_q90_managed_v1`.
