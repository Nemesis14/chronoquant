# Feature készlet áttekintés — változók, csoportok, ablakméretk

Adatforrás: `config/features.json`, `models/lgbm_solusdt_*_v4/features.json`, `src/database/sync_tables/_features_polars.py`

**Összesen: 202 feature**, 24 csoport, 1 perces OHLCV gyertyán (SOLUSDT).

---

## Csoportok és backward ablakok

| # | Csoport | Db | Backward ablak(ok) |
|---|---------|----|--------------------|
| 1 | Momentum | 9 | w=14 (RSI, Stoch, ADX), w=20 (CCI), w=14/140 (ROC) |
| 2 | Trend | 8 | w=14, 140 (SMA, EMA ratio), w=10 (KAMA), fast=12/slow=26/sig=9 (MACD) |
| 3 | Volatility | 12 | w=14, 140 (BB), w=14 (NATR), w=20 (hist_vol), w=10, 30, 60 (GK vol, Parkinson) |
| 4 | Volume | 6 | w=14 (vol_sma, vol_ratio, OBV_roc, MFI), w=20 (CMF), kumulatív (OBV) |
| 5 | Price Action | 6 | w=14 (returns std/skew/kurt), ablak nélkül (log return, hml_range, close_pos) |
| 6 | Market Structure | 6 | w=5 (swing high/low, trend counts) |
| 7 | Activity | 11 | w=10, 30 (taker flow), w=10, 30, 60 (quote vol ratio, trade count ratio), w=30 (avg trade) |
| 8 | Return Distance | 15 | w=10, 30, 60 (return, return_z, dist_high, dist_low, rolling_drawdown) |
| 9 | Regime Rank | 15 | w=10, 30 (vol/quote_vol/trade rank, accel), w=20, 60 (natr/hist_vol/bb_width rank), cross=10/30 (range_expansion) |
| 10 | Candle Shape | 9 | ablak nélkül (body_ratio, wick_ratio raw), w=10, 30 (sma variants) |
| 11 | Trend Slope | 3 | w=10, 30 (EMA slope, directional agreement) |
| 12 | Interaction | 12 | w=5, 10, 30 (RSI/ROC delta), w=10, 30 (vol_adj_return, vol/taker confirmed return) |
| 13 | Time / Session | 12 | nincs backward ablak — determinisztikus (óra, nap, szesszió, heti nyitó) |
| 14 | Autocorrelation | 5 | lag=1/5, w=30, 60 (return autocorr), cross=10/60 (variance ratio) |
| 15 | Drawdown & Timing | 12 | w=10, 30, 60 (recovery ratio, max drawdown, time since high/low) |
| 16 | Pattern Flags | 10 | ablak nélkül (doji, hammer, engulf stb.), w=10, 30, 60 (bull_bars_ratio) |
| 17 | Gap | 3 | ablak nélkül (gap_open), w=10, 30 (abs sma) |
| 18 | Efficiency | 3 | w=10, 30, 60 (Kaufman efficiency ratio) |
| 19 | SR Levels | 8 | w=10, 30, 60 (ATR dist high/low), **prev session H/L (1440 bar shift)** |
| 20 | Tail Risk | 9 | w=10, 30, 60 (pos/neg return mean, return asymmetry) |
| 21 | Extended Accel | 2 | w=10, 30 (return momentum delta) |
| 22 | Ichimoku | 7 | built-in: 9, 26, 52 (tenkan, kijun, senkou_b, cloud thickness, delta) |
| 23 | Donchian | 9 | w=10, 30, 60 (width, position, breakout) |
| 24 | Linear Regression | 9 | w=10, 30, 60 (slope, R², residual) |

---

## Domináns backward ablakok (valós időben)

| Ablak (bar) | Valós idő | Hol jelenik meg |
|-------------|-----------|-----------------|
| w=5 | 5 perc | Market Structure, Interaction |
| w=10 | 10 perc | Activity, Return Dist, Regime Rank, Drawdown, Donchian, LR, Efficiency, Candle, Interaction, SR Levels, Tail Risk |
| w=12/26/9 | 12–26 perc | MACD |
| w=14 | 14 perc | Momentum (RSI, Stoch, ADX), Trend (SMA, EMA), Volatility (BB, NATR), Volume |
| w=20 | 20 perc | CCI, Volatility (hist_vol), Regime Rank |
| w=30 | 30 perc | Activity, Return Dist, Regime Rank, Drawdown, Donchian, LR, Candle, Interaction, Tail Risk |
| w=52 | 52 perc | Ichimoku (Senkou B) |
| w=60 | 1 óra | Return Dist, Regime Rank, Autocorr, Drawdown, Donchian, LR, Efficiency, Tail Risk |
| w=140 | 2 óra 20 perc | Trend (SMA, EMA ratio — széles), Volatility (BB wide) — **ez a legnagyobb rolling ablak** |
| **1440 bar shift** | **24 óra** | **prev_session_high_dist, prev_session_low_dist** — ld. warning lent |

---

## ⚠️ WARNING — Hosszú warmup feature-ök

### `feat_prev_session_high_dist` és `feat_prev_session_low_dist`

**Warmup igény: 1441 bar = 24 óra 1 perc**

Implementáció ([`_features_polars.py:1227`](src/database/sync_tables/_features_polars.py#L1227)):
```python
high.max().over("_tmp_day")         # teljes naptári nap max/min — minden sorra azonos aznap belül
pl.col("_tmp_dh").shift(1440)       # 1440 pozícióval visszatol = előző kalendáris nap
```

- Az `over("_tmp_day")` a nap teljes (00:00–23:59 UTC) max/min-jét adja minden sorhoz
- A `shift(1440)` visszatolja az előző lezárt napra → **nincs future leak**
- Ezután jön a globális t-1 lag (1 bar) → összesen **1441 bar null** az elején
- Feltételezés: **pontosan 1440 bar/nap** — gap esetén a shift nem a nap határán landol

### Összehasonlítás a többi feature warmup-jával

| Feature(ek) | Warmup (bar) | Valós idő |
|-------------|-------------|-----------|
| Legtöbb rolling feature | 10–60 | 10–60 perc |
| Ichimoku Senkou B | 52 | 52 perc |
| SMA/EMA/BB w=140 | 140 | 2 óra 20 perc |
| **prev_session_high/low_dist** | **1441** | **24 óra 1 perc** |

---

## Feature leak audit — összefoglalás

| Csoport | Leak? | Megjegyzés |
|---------|-------|------------|
| Minden rolling indikátor | ✅ Nincs | t-1 lag globálisan alkalmazva (`_apply_t1_lag_pl`) |
| `prev_session_high/low_dist` | ✅ Nincs | `shift(1440)` → előző lezárt nap |
| `day_open_return` | ✅ Nincs | nap első nyitójától (00:00 UTC), expanding |
| `day_range_position` | ✅ Nincs | `cum_max/cum_min` expanding, intraday csak múlt bárok |
| `weekly_open_return` | ✅ Nincs | hétfő első nyitójától, expanding |
| Time/Session determinisztikus | ✅ Nincs | csak `open_time` timestamp-ből → T_MINUS_1_SKIP tag, nincs t-1 lag szükséges |

---

## Tennivaló / döntési pont

A `prev_session_high_dist` és `prev_session_low_dist` feature-öket **az első modell verzióból kiszűrni** javasolt, ha:
- A sample az első 1441 barnál kezdődik, de ezek null-jai torzíthatják az imputation-t
- Egyszerűbb baseline-t szeretnénk napi jellegű kontextus nélkül

Alternatíva: megtartani, de a sample `lookback_end_ts`-t legalább 1441 barnyi offset-tel számolni.
