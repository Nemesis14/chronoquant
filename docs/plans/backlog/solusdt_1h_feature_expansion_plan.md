# SOLUSDT 1h Feature Expansion Plan

## Context

This plan is for the `solusdt_fw60` asset profile. The target horizon is one
hour:

- Long target: `trg_l_fw60_q90`
- Short target: `trg_s_fw60_q10`
- Sample: `base_solusdt_fw60_dev`
- Source DB: `database/solusdt_data_dev.db`
- Source OHLCV table: `solusdt_1m`
- Feature table: `solusdt_1m_features`

## Implementation Status

**Second-batch expansion: implemented and rebuilt. ✓**

- `sync_ohlcv.py` retains `quote_volume`, `trades`, `taker_buy_base`,
  `taker_buy_quote` for new rows.
- `scripts/backfill_ohlcv_activity.py` backfills historical rows.
- `config/features.json` — `solusdt_fw60` profile uses `indicators_extend`
  to add 20 feature groups on top of the shared base set.
- `sync_features.py` implements all 20 `_add_*` helpers.
- Full DB rebuild runs in 6-month chunks to manage peak memory.

---

## Feature Overview

Quick-reference summary of all feature groups, counts and status.
Windows 10 / 30 / 60 bars correspond to ~10 min / 30 min / 1 h lookback.

| # | Group | Count | Windows | Status |
|---|---|---|---|---|
| 1 | Momentum (RSI, ROC, Stoch, CCI, WilliamsR, ADX) | 10 | 14 (legacy) | Live |
| 2 | Trend (MACD, SMA/EMA/WMA/KAMA ratios) | 9 | 14 / 140 (legacy) | Live |
| 3 | Volatility (BB, ATR/NATR, HistVol) | 7 | 14 / 140 (legacy) | Live |
| 4 | Volume (VolSMA, VolRatio, OBV, MFI, CMF, AD) | 7 | 14 (legacy) | Live |
| 5 | Price Action (log return, stats, OHLC range) | 8 | 14 (legacy) | Live |
| 6 | Market Structure (swing H/L counts) | 6 | 5 (legacy) | Live |
| 7 | Return & Distance | 15 | 10 / 30 / 60 | Live |
| 8 | Regime Rank — OHLCV (natr/vol/bb rank, range expansion, vol accel) | 10 | 10 / 20 / 30 / 60 | Live |
| 9 | Candle Shape (body, wicks, rolling SMAs) | 10 | 10 / 30 | Live |
| 10 | Trend Slope (EMA slope, directional agreement) | 3 | 10 / 30 | Live |
| 11 | Momentum Interaction (RSI/ROC delta, vol-adj return) | 6 | 5 / 10 / 30 | Live |
| 12 | Time & Session (hour/dow sin-cos, session flags) | 8 | — | Live |
| 13 | GK Volatility (Parkinson, Garman-Klass) | 6 | 10 / 30 / 60 | Live |
| 14 | Autocorrelation (return autocorr lag 1/5, variance ratio) | 5 | 30 / 60 | Live |
| 15 | Drawdown Timing (recovery ratio, max DD, time-since H/L) | 12 | 10 / 30 / 60 | Live |
| 16 | Pattern Flags (doji, hammer, engulf, inside/outside bar, bull ratio) | 10 | 10 / 30 / 60 | Live |
| 17 | Gap (gap open, rolling gap abs mean) | 3 | 10 / 30 | Live |
| 18 | Efficiency Ratio (Kaufman price path efficiency) | 3 | 10 / 30 / 60 | Live |
| 19 | S/R Levels (ATR-norm pivot dist, prev session H/L) | 8 | 10 / 30 / 60 | Live |
| 20 | Tail Risk (pos/neg return mean, asymmetry ratio) | 9 | 10 / 30 / 60 | Live |
| 21 | Accel Extended (RSI/ROC delta, momentum delta at wider windows) | 6 | 10 / 30 | Live |
| 22 | Ichimoku (tenkan/kijun/senkou ratios, cloud thickness) | 7 | 9 / 26 / 52 | Live |
| 23 | Donchian Channel (width, position, breakout) | 9 | 10 / 30 / 60 | Live |
| 24 | Linear Regression (slope, R², residual) | 9 | 10 / 30 / 60 | Live |
| 25 | Session Relative (day open return, day range position, weekly return) | 4 | UTC day/week | Live |
| 26 | Activity Ratios (quote vol, trade count, taker buy ratios) | 11 | 10 / 30 / 60 | Pending backfill |
| 27 | Regime Rank — Activity (quote/trade vol rank, trade accel) | 5 | 10 / 30 | Pending backfill |
| 28 | Taker Flow Interaction | 2 | 10 / 30 | Pending backfill |

**Total live after rebuild: 190**
**Total after activity backfill: 208**

---

## Current Feature Database — solusdt_1m_features

After the second-batch rebuild the features table contains **190 `feat_` columns**
(up from 47 at project start). All 190 live. A further **18 columns** will be added automatically once
`scripts/backfill_ohlcv_activity.py` is run and the features table is rebuilt.

DB state (validated 2026-06-06): 3,059,275 rows · 0 duplicates · 2020-08-11 → 2026-06-06 · all <1% null.

### Targets (2)

| Column | Direction | Horizon | Percentile |
|---|---|---|---|
| `trg_l_fw60_q90` | long | 60 min | 90th |
| `trg_s_fw60_q10` | short | 60 min | 10th |

---

### Group 1 — Momentum (10 features)

Inherited from shared base set. Windows are BCH-era (14 bars).
Capture directional pressure and oscillator state.

| Feature | Note |
|---|---|
| `feat_rsi_14` | |
| `feat_roc_14` | |
| `feat_roc_140` | 140-bar baseline |
| `feat_stoch_k_14` | |
| `feat_stoch_d_14` | |
| `feat_cci_20` | |
| `feat_williams_r_14` | Redundant with stoch_k (r=1.0) — exclude from v2 model |
| `feat_adx_14` | |
| `feat_adx_pos_14` | |
| `feat_adx_neg_14` | |

---

### Group 2 — Trend (9 features)

| Feature | Note |
|---|---|
| `feat_macd_12_26` | |
| `feat_macd_signal_12_26_9` | High corr with macd_12_26 (r=0.95) |
| `feat_macd_diff` | |
| `feat_sma_ratio_14` | |
| `feat_sma_ratio_140` | |
| `feat_ema_ratio_14` | High corr with sma_ratio_14 (r=0.98) |
| `feat_ema_ratio_140` | High corr with sma_ratio_140 (r=0.98) |
| `feat_wma_ratio_14` | Redundant with ema_ratio_14 (r=0.97) — exclude from v2 |
| `feat_kama_ratio_10_2_30` | |

---

### Group 3 — Volatility (7 features)

| Feature | Note |
|---|---|
| `feat_bb_width_14` | |
| `feat_bb_position_14` | |
| `feat_bb_width_140` | |
| `feat_bb_position_140` | |
| `feat_atr_14` | High corr with natr_14 (r=0.97) — exclude from v2 |
| `feat_natr_14` | |
| `feat_hist_vol_20` | High corr with returns_std_14 (r=0.96) |

---

### Group 4 — Volume (7 features)

| Feature | Note |
|---|---|
| `feat_volume_sma_14` | |
| `feat_volume_ratio_14` | |
| `feat_obv` | |
| `feat_obv_roc_14` | |
| `feat_mfi_14` | |
| `feat_ad_line` | Near-duplicate of obv (r=0.9999) — exclude from v2 |
| `feat_cmf_20` | |

---

### Group 5 — Price Action (8 features)

| Feature | Note |
|---|---|
| `feat_returns_log` | |
| `feat_returns_sma_14` | Duplicate of roc_14 (r=1.0) — exclude from v2 |
| `feat_returns_std_14` | |
| `feat_returns_skew_14` | |
| `feat_returns_kurt_14` | |
| `feat_hml_range` | |
| `feat_ohlc_range` | Duplicate of hml_range (r=1.0) — exclude from v2 |
| `feat_close_position` | |

---

### Group 6 — Market Structure (6 features)

| Feature | Note |
|---|---|
| `feat_higher_high_count_5` | |
| `feat_higher_low_count_5` | |
| `feat_lower_high_count_5` | |
| `feat_lower_low_count_5` | |
| `feat_swing_high_5` | |
| `feat_swing_low_5` | |

---

### Group 7 — Return & Distance (15 features) — NEW

1h-aware close-to-close returns and price displacement relative to recent
high/low anchors.

| Feature | Windows |
|---|---|
| `feat_return_10/30/60` | Close-to-close return over window bars |
| `feat_return_z_10/30/60` | Return normalized by rolling log-return std |
| `feat_dist_rolling_high_10/30/60` | Close / rolling high − 1 |
| `feat_dist_rolling_low_10/30/60` | Close / rolling low − 1 |
| `feat_rolling_drawdown_10/30/60` | Close / rolling peak − 1 |

---

### Group 8 — Volatility & Activity Regime Rank (10 features) — NEW

Rolling percentile rank and acceleration features for volatility and volume
regime context. Computed from plain OHLCV columns.

| Feature | Source | Windows |
|---|---|---|
| `feat_natr_rank_20/60` | natr_14 | 20, 60 |
| `feat_hist_vol_rank_20/60` | hist_vol_20 | 20, 60 |
| `feat_bb_width_rank_20/60` | bb_width_14 | 20, 60 |
| `feat_range_expansion_10_30` | high-low range | 10 vs 30 |
| `feat_volume_rank_10/30` | base volume | 10, 30 |
| `feat_volume_accel_10_30` | volume SMA ratio | 10 vs 30 |

Additional rank features added after backfill (+5): `quote_volume_rank_10/30`,
`trade_count_rank_10/30`, `trade_count_accel_10_30`.

---

### Group 9 — Candle Shape (10 features) — NEW

Body/wick decomposition of individual and rolling 1m candle geometry.

| Feature | Description |
|---|---|
| `feat_body_ratio` | abs(close-open) / (high-low) |
| `feat_signed_body_ratio` | (close-open) / (high-low), signed |
| `feat_upper_wick_ratio` | upper wick / (high-low) |
| `feat_lower_wick_ratio` | lower wick / (high-low) |
| `feat_body_ratio_sma_10/30` | rolling mean of body_ratio |
| `feat_signed_body_sma_10/30` | rolling mean of signed_body_ratio |
| `feat_wick_imbalance_sma_10/30` | rolling mean of (lower_wick - upper_wick) / range |

---

### Group 10 — Trend Slope (3 features) — NEW

EMA slope and short/medium directional agreement.

| Feature | Description |
|---|---|
| `feat_ema_slope_10` | 1-bar EMA change / close, window=10 |
| `feat_ema_slope_30` | 1-bar EMA change / close, window=30 |
| `feat_directional_agreement_10_30` | sign(return_10) × sign(return_30) |

---

### Group 11 — Momentum Interaction (6 features) — NEW

Momentum change and volume/volatility-confirmed return signals.
Computed from plain OHLCV columns.

| Feature | Description |
|---|---|
| `feat_rsi_delta_5` | RSI − RSI shifted 5 bars |
| `feat_roc_delta_5` | ROC − ROC shifted 5 bars |
| `feat_vol_adj_return_10/30` | 1-bar return / rolling log-return std |
| `feat_volume_confirmed_return_10/30` | 1-bar return × volume percentile rank |

Additional interaction feature added after backfill (+2):
`taker_flow_confirmed_return_10/30`.

---

### Group 12 — Time & Session (8 features) — NEW

Deterministic UTC calendar context. No rolling windows.

| Feature | Description |
|---|---|
| `feat_hour_sin/cos` | UTC hour encoded as sine/cosine (period 24) |
| `feat_dayofweek_sin/cos` | Day of week encoded as sine/cosine (period 7) |
| `feat_weekend` | 1 if Saturday or Sunday (UTC), else 0 |
| `feat_session_asia` | 1 if UTC hour in [0, 8) |
| `feat_session_europe` | 1 if UTC hour in [7, 16) |
| `feat_session_us` | 1 if UTC hour in [13, 22) |

---

### Group 13 — Activity Ratios (11 features, NULL until backfill) — NEW

Requires `quote_volume`, `trades`, `taker_buy_base`, `taker_buy_quote` in
the OHLCV table. Run `scripts/backfill_ohlcv_activity.py --asset-id solusdt_fw60`
then rebuild features to populate these.

| Feature | Description | Windows |
|---|---|---|
| `feat_quote_volume_ratio_10/30/60` | quote_volume / rolling mean | 10, 30, 60 |
| `feat_trade_count_ratio_10/30/60` | trades / rolling mean | 10, 30, 60 |
| `feat_avg_trade_quote_30` | rolling quote_volume sum / trade count | 30 |
| `feat_taker_buy_base_ratio_10/30` | taker_buy_base sum / volume sum | 10, 30 |
| `feat_taker_buy_quote_ratio_10/30` | taker_buy_quote sum / quote_volume sum | 10, 30 |

---

### Feature Count Summary

| Group | Count | Status |
|---|---|---|
| Momentum | 10 | Live |
| Trend | 9 | Live |
| Volatility | 7 | Live |
| Volume | 7 | Live |
| Price Action | 8 | Live |
| Market Structure | 6 | Live |
| Return & Distance | 15 | Live |
| Regime Rank (OHLCV-only) | 10 | Live |
| Candle Shape | 10 | Live |
| Trend Slope | 3 | Live |
| Momentum Interaction (OHLCV-only) | 6 | Live |
| Time & Session | 8 | Live |
| Activity Ratios | 11 | Pending backfill |
| Regime Rank (activity-based) | 5 | Pending backfill |
| Interaction (taker flow) | 2 | Pending backfill |
| **Total live** | **99** | |
| **Total after backfill** | **117** | |

---

## Redundancy Map — Known Duplicates in Legacy Set

These should be excluded from the `solusdt_fw60_local_v2` training profile
but remain in the features table for BCH compatibility.

| Remove | Keep | Correlation |
|---|---|---|
| `feat_returns_sma_14` | `feat_roc_14` | 1.000 |
| `feat_ohlc_range` | `feat_hml_range` | 1.000 |
| `feat_williams_r_14` | `feat_stoch_k_14` | 1.000 |
| `feat_ad_line` | `feat_obv` | 0.9999 |
| `feat_atr_14` | `feat_natr_14` | 0.974 |
| `feat_wma_ratio_14` | `feat_ema_ratio_14` | 0.967 |

---

## Potential Future Variables

All candidates below are derivable from local 1m OHLCV data (including the
retained Binance kline fields). No cross-coin data required.

### F1 — Multi-Timeframe Resampled Features

Resample 1m bars to 5m, 15m, 30m, 1h and compute standard indicators on the
resampled series. Assign the resampled value to every 1m bar within the
resampled interval.

Candidates:
- RSI, ROC, ADX, MACD on 5m, 15m, 30m, 60m bars
- Resampled BB position and width
- Resampled volume ratio (current bar vs N-bar mean on resampled frame)
- Resampled candle direction (bullish/bearish at the 5m or 15m level)

Value: captures coarser-granularity context that 1m rolling windows miss.

### F2 — VWAP and Fair-Value Distance

Rolling VWAP computed from quote_volume (available after backfill):

- `feat_vwap_<window>`: sum(quote_volume) / sum(volume) over window
- `feat_dist_vwap_<window>`: (close − vwap) / close
- `feat_vwap_slope_<window>`: vwap change over short lag / close
- `feat_price_above_vwap_<window>`: binary flag

Windows: 10, 30, 60, 240. Can also use 1-session VWAP (reset at UTC 00:00).

Value: VWAP is the canonical intraday fair value proxy. Distance from VWAP
is one of the most widely used microstructure features.

### F3 — Garman-Klass and Yang-Zhang Volatility Estimators

Use the full OHLC to estimate volatility more efficiently than close-only
historical vol:

- `feat_gk_vol_<window>`: Garman-Klass estimator using high, low, close
- `feat_yz_vol_<window>`: Yang-Zhang estimator adding open-to-close and
  overnight components
- `feat_parkinson_vol_<window>`: Parkinson estimator (high-low only)

Value: lower-noise volatility estimates for the same window length compared
to close-only std. Useful for vol-adjustment and regime features.

### F4 — Order Flow Imbalance (after backfill)

Derived from taker buy vs total volume:

- `feat_ofi_<window>`: rolling sum of (taker_buy_base − sell_base) /
  total_volume, where sell_base = volume − taker_buy_base
- `feat_ofi_cumulative_<window>`: cumulative order flow over window
- `feat_ofi_acceleration_<short>_<medium>`: short OFI vs medium OFI mean

Value: taker buy/sell imbalance is a strong short-term price-pressure signal.
Distinct from simple volume because it captures directional aggression.

### F5 — Autocorrelation and Mean-Reversion Measures

- `feat_return_autocorr_<lag>_<window>`: rolling Pearson correlation of
  return_t with return_{t-lag}, over window bars. Use lags 1, 5, 15, 60.
- `feat_variance_ratio_<short>_<long>`: var(short-period return) /
  (short/long × var(long-period return)) — values near 1 = random walk,
  <1 = mean reversion, >1 = trend.

Value: detects local momentum vs mean-reversion regime, which changes with
market conditions.

### F6 — Rolling Drawdown and Recovery Statistics

- `feat_max_drawdown_<window>`: maximum drawdown within the window
- `feat_time_since_high_<window>`: bars since the rolling window high
- `feat_time_since_low_<window>`: bars since the rolling window low
- `feat_recovery_ratio_<window>`: (close − rolling_low) / (rolling_high − rolling_low)

Value: time-since-high/low captures whether a move is fresh or aging.

### F7 — Candlestick Pattern Flags

Binary flags derived purely from OHLC:

- `feat_doji_<threshold>`: body_ratio < threshold (e.g. 0.1)
- `feat_hammer`: lower_wick_ratio > 0.6 and body near top
- `feat_shooting_star`: upper_wick_ratio > 0.6 and body near bottom
- `feat_engulfing_bull`: current body engulfs previous bearish body
- `feat_engulfing_bear`: current body engulfs previous bullish body
- `feat_inside_bar`: high < prev_high and low > prev_low
- `feat_outside_bar`: high > prev_high and low < prev_low
- `feat_consecutive_bull_<n>`: n consecutive bullish closes
- `feat_consecutive_bear_<n>`: n consecutive bearish closes

Value: pattern flags capture local price action structure that rolling
averages smooth over.

### F8 — Gap and Open Relationship

- `feat_open_vs_close`: (open − prev_close) / prev_close — gap at bar open
- `feat_open_strength_<window>`: rolling sum of sign(open_vs_close) — gap
  bias over window
- `feat_close_vs_open_norm`: (close − open) / ATR — bar direction in ATR units
- `feat_intrabar_range_pct`: (high − low) / close — normalized 1m range

Value: open-to-close and gap features capture intrabar dynamics not visible
in close-only indicators.

### F9 — Entropy and Randomness Measures

- `feat_return_entropy_<window>`: approximate entropy of the return series
  over window (quantized into bins). Measures predictability.
- `feat_price_path_efficiency_<window>`: abs(return_window) / sum of
  abs(1-bar returns) — efficiency ratio. High = trending, low = noisy.
- `feat_fractal_dimension_<window>`: estimated fractal dimension of the
  price series over window.

Value: these measure HOW returns are distributed in time, not just their
magnitude. An efficiency ratio near 1 indicates a clean directional move.

### F10 — Support and Resistance Proximity

- `feat_dist_pivot_high_<window>`: close distance from the highest swing high
  within window, normalized by ATR
- `feat_dist_pivot_low_<window>`: close distance from the lowest swing low
  within window, normalized by ATR
- `feat_round_level_proximity`: distance from nearest round number
  (10, 50, 100, 500 USDT) normalized by ATR
- `feat_prev_session_high_proximity`: distance from previous UTC-day high
- `feat_prev_session_low_proximity`: distance from previous UTC-day low

Value: market participants react to price levels. Distance from recent pivots
and round numbers has well-documented microstructure significance.

### F11 — Realized Skewness and Tail Risk

- `feat_realized_skew_<window>`: rolling skewness of 1-bar returns
  (already have returns_skew_14, extend with 30, 60 windows)
- `feat_realized_kurt_<window>`: rolling kurtosis (already have 14,
  extend with 30, 60)
- `feat_downside_vol_<window>`: std of negative returns only
- `feat_upside_vol_<window>`: std of positive returns only
- `feat_vol_ratio_up_down_<window>`: upside_vol / downside_vol — asymmetry

Value: asymmetric volatility is a known predictor of tail events and
directional biases.

### F12 — Price Acceleration and Second Derivative

- `feat_return_accel_<window>`: rolling slope of returns (second derivative
  of price) — is momentum increasing or decreasing?
- `feat_ema_accel_<window>`: second difference of EMA, normalized
- `feat_rsi_velocity_<window>`: RSI change rate over window
- `feat_momentum_curvature_<short>_<medium>`: second difference of
  medium-window return

Value: captures whether a move is accelerating into or exhausting itself
before the 1h target window.

### F13 — Ichimoku Kinko Hyo Components

Cloud-based trend and momentum indicator using rolling midpoints.
All components normalized as ratios to close price (ML-friendly).

- `feat_tenkan_ratio`: (rolling_high_9 + rolling_low_9) / 2 / close − 1
- `feat_kijun_ratio`: (rolling_high_26 + rolling_low_26) / 2 / close − 1
- `feat_senkou_b_ratio`: (rolling_high_52 + rolling_low_52) / 2 / close − 1
- `feat_tenkan_kijun_delta`: (tenkan − kijun) / close — short vs medium bias
- `feat_price_vs_tenkan`: (close − tenkan) / close
- `feat_price_vs_kijun`: (close − kijun) / close
- `feat_ichimoku_cloud_thickness`: (senkou_a − senkou_b) / close

Value: Ichimoku captures trend direction, momentum, and support/resistance
in a single framework. Cloud thickness is a volatility-of-structure proxy.
The 9/26/52 periods map to ~9min, ~26min, ~52min — well within the 1h horizon.

### F14 — Donchian Channel

Channel-based features from rolling absolute high/low over windows 10, 30, 60.
Distinct from BB (based on price extremes, not std deviation).

- `feat_donchian_width_<window>`: (rolling_high − rolling_low) / close
- `feat_donchian_breakout_<window>`: 1 if close = rolling_high (upper breakout)
- `feat_donchian_breakdown_<window>`: 1 if close = rolling_low (lower breakout)
- `feat_donchian_position_<window>`: (close − rolling_low) / (rolling_high − rolling_low)
  — channel position [0, 1], equivalent to recovery_ratio but named separately

Value: channel width measures range contraction/expansion without std smoothing.
Breakout flags capture the exact moment of range violation.

### F15 — Linear Regression Slope, R² and Residual

Rolling OLS regression of close price on bar index. Vectorized via
convolution (fast even on 3M rows).

- `feat_lr_slope_<window>`: LR slope normalized by close (trend strength
  and direction, window = 10, 30, 60)
- `feat_lr_r2_<window>`: R² of the regression (how well is price trending
  vs. noisy?)
- `feat_lr_residual_<window>`: (close − predicted_close) / close (distance
  from regression line — overextension or underextension)

Value: R² distinguishes trending from ranging regimes objectively. LR slope
is a cleaner trend measure than EMA slope (no weight decay artifacts).
Residual signals mean-reversion potential.

### F16 — Session-Relative Position

Intraday position relative to UTC calendar day and week anchors.

- `feat_day_open_return`: (close − first_open_of_UTC_day) / first_open
- `feat_day_range_position`: (close − day_low) / (day_high − day_low)
  — where is price in today's range? [0=at day low, 1=at day high]
- `feat_bars_into_session_norm`: bars elapsed since UTC 00:00 / 1440
- `feat_weekly_open_return`: (close − Monday_open) / Monday_open

Value: captures intraday mean-reversion potential and session progression
not visible in rolling windows. A price at 0.9 of the day range near session
close has different implications than the same price early in the session.

---

## Notes on Remaining Gaps

After F1–F16, the remaining meaningful OHLCV-derivable variable classes are:

- **Entropy / fractal dimension** (F9): computationally expensive
  (`rolling().apply()` on 3M rows). Profile before adding; skip for now.
- **Multi-timeframe resampled indicators** (F1): architecturally distinct
  from rolling features — requires a separate resampling pass. Implement
  as a dedicated module.
- **VWAP / OFI** (F2, F4): depend on `quote_volume` backfill.
- **Autocorrelation** (F5): included in second-batch implementation below.
- **Tail risk extensions** (F11): included in second-batch.
- **Price acceleration** (F12): included in second-batch.
- **Gap features** (F8): included in second-batch.
- **ATR-normalized S/R distances** (F10): included in second-batch.

---

## Implementation Notes for Future Batches

- Multi-timeframe resampling (F1) should be implemented as a separate
  resampling pass in `sync_features` or a dedicated module, not by adding
  more rolling window configs.
- VWAP features (F2) depend on backfilled `quote_volume`. Implement after
  backfill is confirmed complete.
- Pattern flags (F7) are fast to compute (single-pass, no rolling) — good
  candidate for a standalone `_add_pattern_features` helper.
- Entropy and fractal dimension (F9) use `rolling().apply()` which is
  slow. Profile before adding to full history rebuild.
- All timestamp references must stay UTC `YYYY-MM-DD HH:MM:SS`. No local
  time anywhere.
- Every new feature must start with `feat_`. Every new target with `trg_`.

---

## Recommended Execution Order (Remaining)

1. Run `scripts/backfill_ohlcv_activity.py --asset-id solusdt_fw60` to
   populate `quote_volume`, `trades`, `taker_buy_base`, `taker_buy_quote`
   in `solusdt_1m`.
2. Rebuild features with `--features-only` (no `--drop` needed; new columns
   are added automatically via `ensure_table_columns`).
3. Run `scripts/feature_audit.py --asset-id solusdt_fw60` to validate null
   rates and correlations across all 117 features.
4. Train `lgbm_solusdt_l_fw60_q90_local_v2` and
   `lgbm_solusdt_s_fw60_q10_local_v2` candidates.
5. Compare PR AUC and calibration vs v1 champions.
6. Freeze `solusdt_fw60_local_v2` profile removing confirmed redundant
   features from the training feature list.
7. Implement VWAP (F2) and order flow imbalance (F4) as the next batch
   after backfill is confirmed.
