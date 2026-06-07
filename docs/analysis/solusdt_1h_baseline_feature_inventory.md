# SOLUSDT 1h Baseline Feature Inventory

Generated: 2026-06-06

## Database State

### solusdt_1m (OHLCV)

| Property | Value |
|---|---|
| Rows | 3,059,275 |
| Range | 2020-08-11 06:00:00 → 2026-06-06 17:58:00 |
| Columns | open_time, open, high, low, close, volume |
| Missing kline fields | quote_volume, trades, taker_buy_base, taker_buy_quote |

Binance klines provide `quote_volume`, `trades`, `taker_buy_base`, and `taker_buy_quote`
but the current `sync_ohlcv.py` drops them before storing. These are needed for
the activity and interaction feature groups.

### solusdt_1m_features

| Property | Value |
|---|---|
| Rows | 3,059,261 |
| Range | 2020-08-11 06:00:00 → 2026-06-06 17:58:00 |
| Total columns | 51 (open_time, close, 2 targets, 47 feat_) |
| Target columns | trg_l_fw60_q90, trg_s_fw60_q10 |
| Null rate (all 47 features) | 0.0% |

## Current Feature Groups (47 features)

### Momentum (9 features)
- `feat_rsi_14`
- `feat_roc_14`, `feat_roc_140`
- `feat_stoch_k_14`, `feat_stoch_d_14`
- `feat_cci_20`
- `feat_williams_r_14`
- `feat_adx_14`, `feat_adx_pos_14`, `feat_adx_neg_14`

### Trend (9 features)
- `feat_macd_12_26`, `feat_macd_signal_12_26_9`, `feat_macd_diff`
- `feat_sma_ratio_14`, `feat_sma_ratio_140`
- `feat_ema_ratio_14`, `feat_ema_ratio_140`
- `feat_wma_ratio_14`
- `feat_kama_ratio_10_2_30`

### Volatility (7 features)
- `feat_bb_width_14`, `feat_bb_position_14`
- `feat_bb_width_140`, `feat_bb_position_140`
- `feat_atr_14`, `feat_natr_14`
- `feat_hist_vol_20`

### Volume (7 features)
- `feat_volume_sma_14`, `feat_volume_ratio_14`
- `feat_obv`, `feat_obv_roc_14`
- `feat_mfi_14`
- `feat_ad_line`
- `feat_cmf_20`

### Price Action (8 features)
- `feat_returns_log`
- `feat_returns_sma_14`, `feat_returns_std_14`, `feat_returns_skew_14`, `feat_returns_kurt_14`
- `feat_hml_range`, `feat_ohlc_range`
- `feat_close_position`

### Market Structure (6 features)
- `feat_higher_high_count_5`, `feat_higher_low_count_5`
- `feat_lower_high_count_5`, `feat_lower_low_count_5`
- `feat_swing_high_5`, `feat_swing_low_5`

## Redundancy Analysis (last 50,000 rows)

### Perfect-correlation pairs (r = 1.000)

| Feature A | Feature B | r | Note |
|---|---|---|---|
| feat_roc_14 | feat_returns_sma_14 | 1.000 | Same formula, different name |
| feat_stoch_k_14 | feat_williams_r_14 | 1.000 | Linear transform of each other |
| feat_hml_range | feat_ohlc_range | 1.000 | Same numerator, near-identical denominator |
| feat_obv | feat_ad_line | 0.9999 | Both price-volume accumulators |

### High-correlation pairs (r > 0.95)

| Feature A | Feature B | r | Note |
|---|---|---|---|
| feat_sma_ratio_14 | feat_ema_ratio_14 | 0.982 | Same window, similar smoothing |
| feat_sma_ratio_140 | feat_ema_ratio_140 | 0.982 | Same window, similar smoothing |
| feat_atr_14 | feat_natr_14 | 0.974 | ATR vs ATR/close — same signal |
| feat_sma_ratio_14 | feat_wma_ratio_14 | 0.974 | Same window, different smoothing |
| feat_ema_ratio_14 | feat_wma_ratio_14 | 0.967 | Same window, different smoothing |
| feat_hist_vol_20 | feat_returns_std_14 | 0.955 | Both realized volatility |
| feat_macd_12_26 | feat_macd_signal_12_26_9 | 0.953 | MACD and its own signal |

### Recommended removals from the stable SOL profile

The following are clear formula-duplicates and should be excluded from new
`solusdt_fw60_local_v2` profile (existing shared profile keeps them for BCH compatibility):

- `feat_returns_sma_14` — duplicate of `feat_roc_14`
- `feat_ohlc_range` — duplicate of `feat_hml_range`
- `feat_ad_line` — near-duplicate of `feat_obv`
- `feat_atr_14` — near-duplicate of `feat_natr_14` (keep `feat_natr_14`)
- `feat_williams_r_14` — duplicate of `feat_stoch_k_14`
- `feat_wma_ratio_14` — near-duplicate of `feat_ema_ratio_14`
- Consider keeping only one of `feat_sma_ratio_14` / `feat_ema_ratio_14`

## Coverage Gaps (identified for expansion)

1. **Kline activity fields absent** — quote_volume, trades, taker buy flow not stored
2. **No 1h-aware rolling windows** — existing windows (14, 140) are BCH-oriented
3. **No percentile/rank features** — volatility and activity levels have no context
4. **Candle shape features basic** — no body/wick ratio decomposition
5. **No trend slope** — EMA/SMA slope and directional agreement absent
6. **No return-distance features** — no explicit distance from rolling high/low
7. **No interaction features** — no vol-adjusted or volume-confirmed return
8. **No time/session features** — deterministic calendar context absent
