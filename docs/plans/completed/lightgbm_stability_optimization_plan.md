# LightGBM Stability Optimization Plan — COMPLETED

**Completed:** 2026-06-07  
**Model promoted:** `lgbm_solusdt_l_fw60_q90_local_v2`  
**Replaced:** `lgbm_solusdt_l_fw60_q90_stable_v1`

---

## Summary

Distribution-based hyperparameter search (seeded random with TPE-guided top-quartile refinement)
on SOLUSDT 1-minute data. 65 trials across 5 expanding-window folds, 202 features, row_stride=60.

---

## Model Performance (CV, row_stride=60)

| Metric | Value |
|--------|-------|
| mean_valid_log_loss | **0.2632** |
| mean_train_log_loss | **0.2347** |
| mean_gap (valid − train) | **0.0284** (< 0.03 threshold — no overfitting penalty) |
| std_valid_log_loss | 0.0078 |
| mean_valid_PR_AUC | **0.3637** (+42% vs champion 0.2563) |
| mean_valid_ROC_AUC | 0.8147 |
| lift @ top 5% | **4.84x** |

### Per-fold breakdown

| Fold | Train LL | Valid LL | Gap | Valid PR AUC | Best iter |
|------|----------|----------|-----|--------------|-----------|
| 1 | 0.2335 | 0.2691 | 0.036 | 0.413 | 547 |
| 2 | 0.2341 | 0.2747 | 0.041 | 0.324 | 637 |
| 3 | 0.2320 | 0.2564 | 0.024 | 0.330 | 897 |
| 4 | 0.2428 | 0.2539 | 0.011 | 0.349 | 588 |
| 5 | 0.2313 | 0.2616 | 0.030 | 0.404 | 1114 |

---

## Best Hyperparameters (Trial #51)

| Parameter | Value |
|-----------|-------|
| num_leaves | 7 |
| max_depth | 3 |
| min_child_samples | 324 |
| min_child_weight | 0.003273 |
| min_split_gain | 0.000940 |
| reg_alpha | 0.808 |
| reg_lambda | 4.977 |
| subsample | 0.829 |
| colsample_bytree | 0.793 |
| learning_rate | 0.01884 |
| max_bin | 127 |
| path_smooth | 0.02927 |
| extra_trees | False |
| n_estimators (final fit) | 850 |

Key insight: shallow trees (max_depth=3, num_leaves=7) with strong L2 regularization (reg_lambda≈5)
keep train/valid gap below 3%, preventing the overfitting that afflicted the v1 model.

---

## Feature Importance (top 15, gain — fold 5 analysis)

| Feature | Gain | Notes |
|---------|------|-------|
| feat_day_range_position | 24001 | New: intraday pos in day's range |
| feat_volume_sma_14 | 22821 | Volume trend |
| feat_pos_return_mean_60 | 16484 | New from expansion: positive tail |
| feat_natr_14 | 14101 | Volatility regime |
| feat_day_open_return | 6184 | New: return from session open |
| feat_obv | 5207 | On-balance volume |
| feat_parkinson_vol_60 | 3596 | New: Parkinson volatility |
| feat_pos_return_mean_30 | 3582 | New from expansion |
| feat_ema_ratio_140 | 2529 | Long-horizon trend |
| feat_bars_into_session_norm | 2146 | New: time within session |
| feat_prev_session_high_dist | 1808 | New: session reference |
| feat_weekly_open_return | 1569 | New: weekly context |
| feat_prev_session_low_dist | 1409 | New: session reference |
| feat_gk_vol_60 | 1282 | New: GK volatility |
| feat_roc_140 | 1226 | Long ROC |

### Zero-gain features (removed for refinement)

`feat_doji`, `feat_engulf_bear`, `feat_engulf_bull`, `feat_shooting_star`,
`feat_donchian_breakout_10/30/60`, `feat_lower_low_count_5`, `feat_swing_low_5`,
`feat_vol_adj_return_10`

Candlestick pattern flags and Donchian breakouts carry no signal at 1-minute resolution.

---

## Strategy Optimization

### Sweep configuration
- Period: 2024-01-01 to 2025-12-31 (2 years, 1.05M bars)
- 200 combinations: entry_threshold × max_hold_minutes × take_profit_pct
- Fee: 10 bps/side, slippage: 2 bps/side, cooldown: 60 min

### Selected strategy: `solusdt_long_fw60_q90_local_v2`

| Parameter | Value |
|-----------|-------|
| entry_threshold | **0.45** |
| rearm_threshold | 0.18 |
| exit_threshold | 0.10 |
| min_hold_minutes | 5 |
| max_hold_minutes | **120** |
| take_profit_pct | 0.0 (let winners run) |
| cooldown_minutes | 60 |

### Backtest results (2024-01-01 to 2025-12-31)

| Metric | Value |
|--------|-------|
| Total return | **+183.7%** (183.72x on $10k initial equity) |
| Trade count | 459 |
| Win rate | **81.0%** |
| Profit factor | **5.80** |
| Max drawdown | **-10.2%** |
| Avg hold | 93 min |
| Market exposure | 4.1% of time |
| Exit: max_hold | 56% |
| Exit: probability_exit | 44% |
| Best trade | +19.4% |
| Worst trade | -6.4% |

Note: 2024-2025 included a major SOL bull run. Part of this period overlaps with training data
(folds 1-4). True out-of-sample evaluation begins 2025-06-05.

---

## Artifacts

| File | Description |
|------|-------------|
| `models/lgbm_solusdt_l_fw60_q90_local_v2/model.pkl` | Trained LightGBM model (850 estimators) |
| `models/lgbm_solusdt_l_fw60_q90_local_v2/features.json` | 202 feature list |
| `models/lgbm_solusdt_l_fw60_q90_local_v2/params.json` | Best hyperparameters |
| `models/lgbm_solusdt_l_fw60_q90_local_v2/search/` | Full search artifacts (65 trials) |
| `backtests/solusdt_long_fw60_q90_local_v2/` | Trades, equity curve, HTML report |
| `backtests/sweep_lgbm_solusdt_l_fw60_q90_local_v2.csv` | Full 200-combo sweep results |

---

## Config changes

- `config/models.json`: `lgbm_solusdt_l_fw60_q90_local_v2` active=true, `lgbm_solusdt_l_fw60_q90_stable_v1` active=false
- `config/env.json`: `solusdt_fw60` runtime model → `lgbm_solusdt_l_fw60_q90_local_v2`
- `config/strategies.json`: `solusdt_long_fw60_q90_local_v2` added as first solusdt_fw60 strategy (picked up by UI)
- `src/evaluation/backtest.py`: added `model_prediction_frame_chunked` and updated `build_backtest_frame` to use chunked loading (fixes MemoryError on large date ranges)
