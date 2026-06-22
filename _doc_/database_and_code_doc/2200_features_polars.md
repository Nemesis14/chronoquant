# _features_polars.py — Feature Computation Engine

`src/data_handling/sync_tables/_features_polars.py`

A fő feature számítási motor. 30+ indikátor csoport, Polars LazyFrame API, t-1 lag kötelezően, numpy helpers a rolling statisztikákhoz. Ez a modul a lookahead bias elleni legfontosabb védelmi vonal.

---

## `compute_features_polars(df_ohlcv, indicators, feat_prefix, available_activity, targets_cfg)`

**Célja:** OHLCV DataFrame-ből feature DataFrame generálása a megadott indikátor konfigurációk alapján.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `df_ohlcv` | `pl.DataFrame` | Input OHLCV sorok (`open_time`, `open`, `high`, `low`, `close`, `volume`) |
| `indicators` | `dict` | Indikátor konfigurációk (`config/features.json` alapján) |
| `feat_prefix` | `str` | Feature oszlopok prefix-e (pl. `"feat_"`) |
| `available_activity` | `list` | Aktivitás metaadatok (ritkán használt) |
| `targets_cfg` | `list` | Target konfiguráció (forward window info) |

**Visszatérési érték:** `pl.DataFrame` — eredeti OHLCV oszlopok + összes `feat_*` oszlop.

**Belső lépések:**
1. LazyFrame-re konvertálás
2. Minden engedélyezett indikátor csoport `_add_*_pl()` függvénye meghívva
3. `_apply_t1_lag_pl(lf, p)` — t-1 lag alkalmazása
4. `_clean_features_pl(lf, p)` — végtelen értékek (`inf`) null-ra cserélése
5. `.collect()` → `pl.DataFrame`

---

## `T_MINUS_1_SKIP`

```python
T_MINUS_1_SKIP: frozenset[str] = frozenset({
    "feat_bars_into_session_norm",
    "feat_hour_sin",
    "feat_hour_cos",
    "feat_dayofweek_sin",
    "feat_dayofweek_cos",
    "feat_weekend",
    "feat_session_asia",
    "feat_session_europe",
    "feat_session_us",
})
```

**Célja:** P2 (időindex-alapú) feature-ök, amelyek **nem** tolódnak el 1 barral. Ezek a feature-ök kizárólag az `open_time` timestampből számítódnak — nem tartalmaznak jövőbeli OHLCV adatot, ezért nem kell a lookahead bias elleni lag.

---

## `_apply_t1_lag_pl(lf, p)`

**Célja:** Minden `feat_*` oszlop shift(1) — kivéve `T_MINUS_1_SKIP` tagjai.

**Mechanizmus:** `lf.with_columns([pl.col(c).shift(1).alias(c) for c in feat_cols if c not in T_MINUS_1_SKIP])`

Az eltolás után az első sor összes OHLCV-alapú feature-je `null` lesz.

---

## Indikátor csoportok

### Momentum (`_add_momentum_pl`)

| Feature | Leírás |
|---------|--------|
| `feat_rsi_{n}` | Relative Strength Index (RSI) |
| `feat_roc_{n}` | Rate of Change |
| `feat_stoch_k_{n}`, `feat_stoch_d_{n}` | Stochastic Oscillator |
| `feat_williams_r_{n}` | Williams %R |
| `feat_cci_{n}` | Commodity Channel Index |

---

### Trend (`_add_trend_pl`)

| Feature | Leírás |
|---------|--------|
| `feat_sma_ratio_{n}` | close / SMA(n) |
| `feat_ema_ratio_{n}` | close / EMA(n) |
| `feat_wma_ratio_{n}` | close / WMA(n) |
| `feat_kama_ratio_{n}_{fast}_{slow}` | close / KAMA (Kaufman Adaptive MA) |
| `feat_macd_{fast}_{slow}` | MACD vonal |
| `feat_macd_signal_{fast}_{slow}_{signal}` | MACD szignálvonal |
| `feat_macd_diff` | MACD hisztogram (macd - signal) |
| `feat_adx_{n}` | Average Directional Index |
| `feat_adx_pos_{n}` | +DI irány indikátor |
| `feat_adx_neg_{n}` | -DI irány indikátor |

---

### Volatility (`_add_volatility_pl`)

| Feature | Leírás |
|---------|--------|
| `feat_bb_width_{n}` | Bollinger Band szélesség: `(upper-lower)/close` |
| `feat_bb_position_{n}` | Ár pozíció a sávon belül: `(close-lower)/(upper-lower)` |
| `feat_atr_{n}` | Average True Range (Wilder EWM) |
| `feat_natr_{n}` | Normalized ATR: `atr/close` |
| `feat_hist_vol_{n}` | Historikus volatilitás: rolling std(log returns) |

---

### Volume (`_add_volume_pl`)

| Feature | Leírás |
|---------|--------|
| `feat_volume_sma_{n}` | Forgalom mozgóátlag |
| `feat_volume_ratio_{n}` | Forgalom / SMA(n) forgalom |
| `feat_obv` | On-Balance Volume (kumulatív) |
| `feat_obv_roc_{n}` | OBV Rate of Change |
| `feat_mfi_{n}` | Money Flow Index |
| `feat_ad_line` | Accumulation/Distribution vonal |
| `feat_cmf_{n}` | Chaikin Money Flow |

---

### Price Action (`_add_price_action_pl`)

| Feature | Leírás |
|---------|--------|
| `feat_returns_log` | Log return: `ln(close/prev_close)` |
| `feat_returns_sma_{n}` | Log return rolling SMA |
| `feat_returns_std_{n}` | Log return rolling STD |
| `feat_returns_skew_{n}` | Log return rolling skewness |
| `feat_returns_kurt_{n}` | Log return rolling kurtosis |
| `feat_hml_range` | `(high-low)/close` |
| `feat_ohlc_range` | `(high-low)/((open+close)/2)` |
| `feat_close_position` | `(close-low)/(high-low)` |

---

### Market Structure (`_add_market_structure_pl`)

SR szintek, drawdown, trend slope, regime rank.

---

### Activity (`_add_activity_pl`)

Kereskedési aktivitás metrikák: trades normalizálva, taker flow.

---

### Return Distance (`_add_return_distance_pl`)

Távolság SMA-tól és Bollinger Band-ektől.

---

### Regime Rank (`_add_regime_rank_pl`)

Rolling percentilis rank, `_rolling_rank_arr` numpy helper-rel.

---

### Candle Shape (`_add_candle_shape_pl`)

Gyertya morfológia: body ratio, wick ratio, doji flag.

---

### Trend Slope (`_add_trend_slope_pl`)

Lineáris regresszió slope különböző ablakokra.

---

### Interaction (`_add_interaction_pl`)

Feature kombinációk: RSI × trend, volume × momentum.

---

### Time / Session (`_add_time_session_pl`, `_add_session_relative_pl`)

**P2 feature-ök — T_MINUS_1_SKIP tagjai (nem kapnak t-1 lag-ot):**

| Feature | Leírás |
|---------|--------|
| `feat_hour_sin`, `feat_hour_cos` | Nap körkörös kódolása |
| `feat_dayofweek_sin`, `feat_dayofweek_cos` | Hét napja körkörös kódolása |
| `feat_weekend` | Hétvége flag (0/1) |
| `feat_session_asia`, `feat_session_europe`, `feat_session_us` | Kereskedési szesszió flag |
| `feat_bars_into_session_norm` | Szesszión belüli pozíció (normalizált, 0–1) |
| `feat_day_range_position` | Ár pozíció a napi expanding range-en belül |
| `feat_day_open_return` | Visszatérés a nap nyitóárához képest |
| `feat_weekly_open_return` | Visszatérés a hét nyitóárához képest |

---

### Egyéb csoportok

| Csoport | Kulcs feature-ök |
|---------|--------|
| `_add_gk_volatility_pl` | `feat_parkinson_vol_{10,30,60}`, `feat_gk_vol_{10,30,60}` |
| `_add_autocorr_pl` | `feat_return_autocorr_lag{1,5}_{30,60}`, `feat_variance_ratio_10_60` |
| `_add_drawdown_timing_pl` | `feat_recovery_ratio_{n}`, `feat_max_drawdown_{n}`, `feat_time_since_high_{n}`, `feat_time_since_low_{n}` |
| `_add_pattern_flags_pl` | `feat_doji`, `feat_hammer`, `feat_shooting_star`, `feat_inside_bar`, `feat_outside_bar`, `feat_engulf_bull`, `feat_engulf_bear`, `feat_bull_bars_ratio_{n}` |
| `_add_gap_pl` | `feat_gap_open`, `feat_gap_open_abs_sma_{10,30}` |
| `_add_efficiency_pl` | `feat_efficiency_ratio_{10,30,60}` |
| `_add_sr_levels_pl` | `feat_atr_dist_high_{n}`, `feat_atr_dist_low_{n}`, `feat_prev_session_high_dist`, `feat_prev_session_low_dist` |
| `_add_tail_risk_pl` | `feat_pos_return_mean_{n}`, `feat_neg_return_mean_{n}`, `feat_return_asymmetry_{n}` |
| `_add_extended_accel_pl` | `feat_rsi_delta_{n}`, `feat_roc_delta_{n}`, `feat_return_momentum_delta_{n}` |
| `_add_ichimoku_pl` | `feat_tenkan_ratio`, `feat_kijun_ratio`, `feat_senkou_b_ratio`, `feat_ichimoku_cloud_thickness` |
| `_add_donchian_pl` | `feat_donchian_width_{n}`, `feat_donchian_position_{n}`, `feat_donchian_breakout_{n}` |
| `_add_lr_pl` | `feat_lr_slope_{n}`, `feat_lr_r2_{n}`, `feat_lr_residual_{n}` |

---

## Numpy Helpers

Rolling statisztikák, amelyek nem elérhetők a Polars natív API-ban:

| Függvény | Leírás |
|----------|--------|
| `_rolling_rank_arr(arr, window)` | Rolling percentilis rank |
| `_time_since_high_arr(arr, window)` | Barak száma az utolsó high óta |
| `_time_since_low_arr(arr, window)` | Barak száma az utolsó low óta |
| `_rolling_skew_arr(arr, window)` | Rolling skewness |
| `_rolling_kurt_arr(arr, window)` | Rolling kurtosis |
| `_kama_numpy(arr, fast, slow, er_period)` | Kaufman Adaptive Moving Average |

Ezeket `pl.Series.to_numpy()` + `pl.lit(result)` mintával integrálja a LazyFrame pipeline-ba.

---

## `_clean_features_pl(lf, p)`

Végtelen értékek (`float("inf")`, `float("-inf")`) null-ra cserélése minden `feat_*` oszlopban a `.collect()` előtt. Megelőzi, hogy a modeling layer `inf` értékeket kapjon a DuckDB-ből.
