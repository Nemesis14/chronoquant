## Cél

A `lgbm_solusdt_s_fw60_2101_2607` (short) modell összes feature-jének prediktív erejét vizsgáljuk felügyelt (döntési fa alapú) binninggel (max 10 bin / feature). Minden feature-re:

- optimális binekre osztjuk az értékeket, és bin-enként a target átlagot mutatjuk,
- IV (Information Value) és MI (Mutual Information) értéket számolunk az összehasonlíthatóságért.

**Target:** `short_mfe_fw60` (short MFE 60 perces ablakban). Negatív értékek = profitábilis short mozgás.

**Szelekció:** (1) quality szűrő (null > 1%, inf > 0,1%, variancia < 1e-8), (2) MI > 0.001, (3) csoportonként |r| ≥ 0.98 duplikátumok közül a magasabb MI marad.

**Struktúra:** egy szekció = egy feature-csoport (leírás, IV/MI tábla, korrelációs mátrix, bin-diagramok), a végén az összefoglaló.

**Quality szűrő:** 2 feature kizárva | maradék: 206

**Adatbetöltés:** train sorok: 44,563 | feature-ök: 206 | csoportok: 17 | Other (nem mappelt): 0

Snapshot: `solusdt_fw60_2101_2607__49466cd2`

## Price Action

**Ármozgás (Price Action)** — az ár nyers mozgásából számolt statisztikai jellemzők.

- Log-return és annak gördülő átlaga/szórása/ferdesége/kurtózisa

- OHLC range-arányok: az egyetlen gyertya (high-low) tartománya normalizálva az árhoz — nem gördülő ablak

- Return-Z: az aktuális hozam normalizálva a gördülő volatilitáshoz

- Vol-adj-return: hozam osztva gördülő szórással (Sharpe-proxy egyedi bárra)

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `close_position` | (close - low) / (high - low) - zaras helyzete az egyetlen gyertya tartomanyaban (0=aljan, 1=csucsnal) |
| `hml_range` | (high - low) / close - egyetlen gyertya tartomanya close-szal normalizalva (nem gordulo ablak) |
| `ohlc_range` | (high - low) / ((open+close)/2) - egyetlen gyertya tartomanya az OHLC-atlaghoz normalizalva |
| `return_10` | (close - close[-10]) / close[-10] - 10-bari %-os arvaltozas |
| `return_30` | (close - close[-30]) / close[-30] - 30-bari %-os arvaltozas |
| `return_60` | (close - close[-60]) / close[-60] - 60-bari %-os arvaltozas |
| `return_z_10` | ret1 / rolling_std(log_ret, 10) - return normalizalva 10-bari volatilitassal |
| `return_z_30` | ret1 / rolling_std(log_ret, 30) - return normalizalva 30-bari volatilitassal |
| `return_z_60` | ret1 / rolling_std(log_ret, 60) - return normalizalva 60-bari volatilitassal |
| `returns_kurt_14` | log-return 14-bari gordulo kurtosisa - Fisher, excess (>0 = vastag farok) |
| `returns_log` | ln(close / close[-1]) - egyperiodusos log-return |
| `returns_skew_14` | log-return 14-bari gordulo ferdesege - Fisher, bias-corrected |
| `returns_sma_14` | log-return 14-bari gordulo atlaga - rovid trend proxy |
| `returns_std_14` | log-return 14-bari gordulo szorasa (ddof=1) - lokalis volatilitas |
| `vol_adj_return_10` | ret1 / rolling_std(log_ret, 10) - volatilitas-korrigalt return (Sharpe-proxy per bar) |
| `vol_adj_return_30` | ret1 / rolling_std(log_ret, 30) - volatilitas-korrigalt return (Sharpe-proxy per bar) |

{{table:iv_price_action}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_price_action}}

### feat_close_position

{{chart:bin_close_position}}

### feat_hml_range

{{chart:bin_hml_range}}

### feat_return_10

{{chart:bin_return_10}}

### feat_return_30

{{chart:bin_return_30}}

### feat_return_60

{{chart:bin_return_60}}

### feat_returns_log

{{chart:bin_returns_log}}

### feat_returns_sma_14

{{chart:bin_returns_sma_14}}

### feat_returns_std_14

{{chart:bin_returns_std_14}}

## Volatility

**Volatilitás jellemzők** — az ármozgás szórás- és range-alapú mérői.

- **Bollinger-sávok**: bb_width = (upper - lower) / close, bb_position = (close - lower) / (upper - lower)

- **ATR/NATR**: Wilder-EWM alapú True Range átlag; NATR = ATR / close (normalizált)

- **Hist-vol**: log-return gördülő szórása W bárra (ddof=1)

- **Parkinson-vol**: sqrt(mean((ln(H/L))^2 / (4*ln2))) — csak H/L-t használ, pontosabb, mint a close-alapú vol

- **Garman-Klass-vol**: sqrt(mean(0.5*(ln(H/L))^2 - (2*ln2-1)*(ln(C/O))^2)) — OHLC-alapú, még pontosabb

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `atr_14` | ATR-14: Wilder EWM alapu True Range atlag - TR=max(H-L, /H-C[-1]/, /L-C[-1]/) |
| `bb_position_14` | (close - BB_lower_14) / (BB_upper_14 - BB_lower_14) - pozicio a savon belul (0=alap, 1=teteje) |
| `bb_position_140` | (close - BB_lower_140) / (BB_upper_140 - BB_lower_140) - pozicio a savon belul (0=alap, 1=teteje) |
| `bb_width_14` | (BB_upper - BB_lower) / close - Bollinger-sav szelessege (14-bar SMA +/- 2*sigma) |
| `bb_width_140` | (BB_upper - BB_lower) / close - Bollinger-sav szelessege (140-bar SMA +/- 2*sigma) |
| `gk_vol_10` | sqrt(mean(0.5*(ln(H/L))^2 - (2*ln2-1)*(ln(C/O))^2, 10)) - Garman-Klass OHLC vol-becslő |
| `gk_vol_30` | sqrt(mean(0.5*(ln(H/L))^2 - (2*ln2-1)*(ln(C/O))^2, 30)) - Garman-Klass OHLC vol-becslő |
| `gk_vol_60` | sqrt(mean(0.5*(ln(H/L))^2 - (2*ln2-1)*(ln(C/O))^2, 60)) - Garman-Klass OHLC vol-becslő |
| `hist_vol_20` | rolling_std(log_ret, 20, ddof=1) - historikus volatilitas 20 barra |
| `natr_14` | NATR-14: ATR_14 / close - normalizalt ATR (osszehasonlithato idosoron at) |
| `parkinson_vol_10` | sqrt(mean((ln(H/L))^2 / (4*ln2), 10)) - Parkinson range-alapu vol-becslő |
| `parkinson_vol_30` | sqrt(mean((ln(H/L))^2 / (4*ln2), 30)) - Parkinson range-alapu vol-becslő |
| `parkinson_vol_60` | sqrt(mean((ln(H/L))^2 / (4*ln2), 60)) - Parkinson range-alapu vol-becslő |

{{table:iv_volatility}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_volatility}}

### feat_atr_14

{{chart:bin_atr_14}}

### feat_bb_position_14

{{chart:bin_bb_position_14}}

### feat_bb_position_140

{{chart:bin_bb_position_140}}

### feat_bb_width_14

{{chart:bin_bb_width_14}}

### feat_bb_width_140

{{chart:bin_bb_width_140}}

### feat_bb_width_rank_60

{{chart:bin_bb_width_rank_60}}

### feat_gk_vol_10

{{chart:bin_gk_vol_10}}

### feat_gk_vol_30

{{chart:bin_gk_vol_30}}

### feat_gk_vol_60

{{chart:bin_gk_vol_60}}

### feat_hist_vol_20

{{chart:bin_hist_vol_20}}

### feat_hist_vol_rank_60

{{chart:bin_hist_vol_rank_60}}

### feat_natr_14

{{chart:bin_natr_14}}

## Volume

**Volume jellemzők** — a kereskedési volumen szintje, trendje és flow-összetétele.

- **volume_ratio**: aktuális volume / W-bári SMA — normalizált aktivitás szint

- **OBV**: On-Balance Volume = sum(sign(delta_close) * volume) — kumulált irányjelzett forgalom

- **MFI**: Money Flow Index — 100 - 100/(1 + pos_MF/neg_MF), ahol MF = TP * volume

- **A/D Line**: sum(CLV * volume), ahol CLV = ((C-L)-(H-C))/(H-L) — felhalmozás/elosztás

- **CMF**: Chaikin Money Flow = sum(MFV, W) / sum(V, W) — rövid távú flow (-1..+1)

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `ad_line` | sum(CLV*V) - A/D Line — felhalmozas/elosztás, CLV=((C-L)-(H-C))/(H-L) |
| `cmf_20` | sum(CLV*V,20) / sum(V,20) - Chaikin Money Flow (-1..+1) |
| `mfi_14` | MFI-14: 100-100/(1+pos_MF/neg_MF) - TP*V alapu RSI-analog (0-100) |
| `obv` | sum(sign(delta_close) * volume) - On-Balance Volume: kumulalt iranyjelzett forgalom |
| `obv_roc_14` | OBV.pct_change(14) - OBV 14-bari Rate of Change |
| `volume_accel_10_30` | mean(volume,10) / mean(volume,30) - volume gyorsulasa: rovid/kozepes SMA arany |
| `volume_confirmed_return_10` | ret1 * volume_rank_10 - return * volume percentile rank (volume-erositett hozam) |
| `volume_confirmed_return_30` | ret1 * volume_rank_30 - return * volume percentile rank (volume-erositett hozam) |
| `volume_rank_10` | percentile rank of volume over 10 bars - 0=legkisebb, 1=legnagyobb |
| `volume_rank_30` | percentile rank of volume over 30 bars - 0=legkisebb, 1=legnagyobb |
| `volume_ratio_14` | volume / rolling_mean(volume,14) - aktualis volume / 14-bari SMA (>1 = atlag felett) |
| `volume_sma_14` | rolling_mean(volume, 14) - volume 14-bari SMA-ja (abszolut) |

{{table:iv_volume}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_volume}}

### feat_ad_line

{{chart:bin_ad_line}}

### feat_cmf_20

{{chart:bin_cmf_20}}

### feat_obv

{{chart:bin_obv}}

### feat_obv_roc_14

{{chart:bin_obv_roc_14}}

### feat_volume_confirmed_return_10

{{chart:bin_volume_confirmed_return_10}}

### feat_volume_confirmed_return_30

{{chart:bin_volume_confirmed_return_30}}

### feat_volume_rank_10

{{chart:bin_volume_rank_10}}

### feat_volume_ratio_14

{{chart:bin_volume_ratio_14}}

### feat_volume_sma_14

{{chart:bin_volume_sma_14}}

## Gap

**Gap jellemzők** — a nyitóár és az előző zárár közötti ugrás (rés) mértéke.
Kripto-piacokon a gap kisebb, de hír-eseményeknél és likvidáció-kaszkádoknál szignifikáns lehet.
A gap_open_abs_sma a gap abszolút értékének gördülő átlaga, ezzel gap-volatilitás proxy-ként használható.
Ablakméret: 10 és 30 bár.

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `gap_open` | (open - prev_close) / prev_close - nyitogap merteke (relativ) |

{{table:iv_gap}}

### feat_gap_open

{{chart:bin_gap_open}}

## Candle Pattern

**Gyertyamintázat (Candle Pattern)** — az OHLC adatokból számolt gyertyaalak-jellemzők.
Az egyes gyertyák test/kanóc arányait, irányát és ismert technikai mintázatokat (doji, hammer, engulfing) kódolják.
A gördülő átlag változatok (SMA) a mintázat hosszabb távú fennállását mutatják.
Alap képletek: range = high - low, body = |close - open|,
upper_wick = high - max(open, close), lower_wick = min(open, close) - low.

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `body_ratio` | /close - open/ / (high - low) - test/tartomany arany (0-1) |
| `body_ratio_sma_10` | /body//range 10-bari gordulo atlaga |
| `body_ratio_sma_30` | /body//range 30-bari gordulo atlaga |
| `bull_bars_ratio_10` | (close > open) barainak aranya az utolso 10 barban (0-1) |
| `bull_bars_ratio_30` | (close > open) barainak aranya az utolso 30 barban (0-1) |
| `bull_bars_ratio_60` | (close > open) barainak aranya az utolso 60 barban (0-1) |
| `doji` | /body//range < 0.1 => doji gyertya (1/0) - egyforma nyito es zaro |
| `engulf_bear` | Bearish engulfing: elozo bullish bar teljes befoglalasa (1/0) |
| `engulf_bull` | Bullish engulfing: elozo bearish bar teljes befoglalasa (1/0) |
| `hammer` | lower_wick/range > 0.6 ES /body//range < 0.3 ES bullish => kalapacs (1/0) |
| `inside_bar` | high < prev_high ES low > prev_low => inside bar (belso gyertya) (1/0) |
| `lower_wick_ratio` | lower_wick / (high - low) - also kanoc aranya (0-1) |
| `outside_bar` | high > prev_high ES low < prev_low => outside bar (kulso gyertya) (1/0) |
| `shooting_star` | upper_wick/range > 0.6 ES /body//range < 0.3 ES bearish => shooting star (1/0) |
| `signed_body_ratio` | (close - open) / (high - low) - iranyjelzett test-arany (-1..+1) |
| `signed_body_sma_10` | (close-open)/range 10-bari gordulo atlaga - irany-konszisztencia |
| `signed_body_sma_30` | (close-open)/range 30-bari gordulo atlaga - irany-konszisztencia |
| `upper_wick_ratio` | upper_wick / (high - low) - felso kanoc aranya (0-1) |
| `wick_imbalance_sma_10` | (lower_wick - upper_wick)/range 10-bari atlaga - pozitiv = bullish kanoc-aszimmetria |
| `wick_imbalance_sma_30` | (lower_wick - upper_wick)/range 30-bari atlaga - pozitiv = bullish kanoc-aszimmetria |

{{table:iv_candle_pattern}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_candle_pattern}}

### feat_body_ratio

{{chart:bin_body_ratio}}

### feat_body_ratio_sma_10

{{chart:bin_body_ratio_sma_10}}

### feat_body_ratio_sma_30

{{chart:bin_body_ratio_sma_30}}

### feat_bull_bars_ratio_30

{{chart:bin_bull_bars_ratio_30}}

### feat_bull_bars_ratio_60

{{chart:bin_bull_bars_ratio_60}}

### feat_lower_wick_ratio

{{chart:bin_lower_wick_ratio}}

### feat_signed_body_ratio

{{chart:bin_signed_body_ratio}}

### feat_signed_body_sma_30

{{chart:bin_signed_body_sma_30}}

### feat_upper_wick_ratio

{{chart:bin_upper_wick_ratio}}

### feat_wick_imbalance_sma_10

{{chart:bin_wick_imbalance_sma_10}}

## Time/Session

**Idő és session jellemzők** — determinisztikus időstruktúra-kódolás, session-relatív pozíciók.
A kör-kódolás (circular encoding) biztosítja, hogy az éjfél és hajnal közel essen egymáshoz
a feature-térben is. Session flag-ek UTC-időzóna alapján.
Ezek a feature-ök nem kapnak t-1 laget (P2 determinisztikus indexek — az időbélyeg alapján
pontosan kiszámíthatók, nem tartalmaznak jövőbeli infót).

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `bars_into_session_norm` | cumcount_within_day / 1440, klippelve 0-1 - a nap hanyadán vagyunk |
| `day_open_return` | (close - aznapi_elso_open) / aznapi_elso_open - return az aznapi nyitoartol |
| `day_range_position` | (close - exp_low) / (exp_high - exp_low) - ar helyzete a nap kumulalt csocs-melypont tartomanyan |
| `dayofweek_cos` | cos(2*pi * napszam / 7) - het napjanak kor-kodolasa (koszinusz) |
| `dayofweek_sin` | sin(2*pi * napszam / 7) - het napjanak kor-kodolasa (szinusz) |
| `hour_cos` | cos(2*pi * ora / 24) - napszak kor-kodolasa (koszinusz) |
| `hour_sin` | sin(2*pi * ora / 24) - napszak kor-kodolasa (szinusz) |
| `session_asia` | 0h <= UTC_ora < 8h => azs session (1/0) |
| `session_europe` | 7h <= UTC_ora < 16h => europai session (1/0) |
| `session_us` | 13h <= UTC_ora < 22h => US session (1/0) |
| `weekend` | weekday >= 5 => hetvege (1/0) - szombat=5, vasarnap=6 |
| `weekly_open_return` | (close - hetfoi_open) / hetfoi_open - return a heti nyitoartol |

{{table:iv_time_session}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_time_session}}

### feat_bars_into_session_norm

{{chart:bin_bars_into_session_norm}}

### feat_day_open_return

{{chart:bin_day_open_return}}

### feat_day_range_position

{{chart:bin_day_range_position}}

### feat_dayofweek_cos

{{chart:bin_dayofweek_cos}}

### feat_dayofweek_sin

{{chart:bin_dayofweek_sin}}

### feat_hour_cos

{{chart:bin_hour_cos}}

### feat_hour_sin

{{chart:bin_hour_sin}}

### feat_weekend

{{chart:bin_weekend}}

### feat_weekly_open_return

{{chart:bin_weekly_open_return}}

## Taker Flow

**Taker flow és kereskedési aktivitás** — agresszív vevők/eladók jelenlétének mérése.
A Binance futures API-ból elérhető taker_buy_base, quote_volume és trades adatokból számolt arányok.
A taker flow azt méri, hogy a kötések mekkora hányada "taker" (piaci megbízás) és melyik irányba mutat.
Gördülő összegen számolva (rolling sum), ablakméret: 5, 10, 30, 60 bár.

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `avg_trade_quote_30` | sum(quote_volume,30) / sum(trades,30) - atlagos kotesmeret USD-ben (30 bar) |
| `quote_volume_ratio_10` | quote_volume / rolling_mean(quote_volume,10) - USD-forgalom / 10-bari SMA |
| `quote_volume_ratio_30` | quote_volume / rolling_mean(quote_volume,30) - USD-forgalom / 30-bari SMA |
| `quote_volume_ratio_60` | quote_volume / rolling_mean(quote_volume,60) - USD-forgalom / 60-bari SMA |
| `taker_buy_base_ratio_10` | sum(taker_buy_base,10) / sum(volume,10) - taker vasarlasok aranya (0-1) |
| `taker_buy_base_ratio_30` | sum(taker_buy_base,30) / sum(volume,30) - taker vasarlasok aranya (0-1) |
| `taker_buy_quote_ratio_10` | sum(taker_buy_quote,10) / sum(quote_volume,10) - taker buy USD-arany (0-1) |
| `taker_buy_quote_ratio_30` | sum(taker_buy_quote,30) / sum(quote_volume,30) - taker buy USD-arany (0-1) |
| `taker_flow_confirmed_return_10` | ret1 * taker_buy_ratio_10 - return * taker-buy arany (iranyjelzett flow-erositets) |
| `taker_flow_confirmed_return_30` | ret1 * taker_buy_ratio_30 - return * taker-buy arany (iranyjelzett flow-erositets) |
| `trade_count_accel_10_30` | mean(trades,10) / mean(trades,30) - kotesszam gyorsulasa (rovid/kozepes SMA arany) |
| `trade_count_rank_10` | percentile rank of trades over 10 bars - 0=legkisebb, 1=legnagyobb (gordulo) |
| `trade_count_rank_30` | percentile rank of trades over 30 bars - 0=legkisebb, 1=legnagyobb (gordulo) |
| `trade_count_ratio_10` | trades / rolling_mean(trades,10) - kotesszam / 10-bari SMA (>1 = atlag felett) |
| `trade_count_ratio_30` | trades / rolling_mean(trades,30) - kotesszam / 30-bari SMA (>1 = atlag felett) |
| `trade_count_ratio_60` | trades / rolling_mean(trades,60) - kotesszam / 60-bari SMA (>1 = atlag felett) |

{{table:iv_taker_flow}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_taker_flow}}

### feat_avg_trade_quote_30

{{chart:bin_avg_trade_quote_30}}

### feat_quote_volume_rank_10

{{chart:bin_quote_volume_rank_10}}

### feat_quote_volume_ratio_10

{{chart:bin_quote_volume_ratio_10}}

### feat_quote_volume_ratio_30

{{chart:bin_quote_volume_ratio_30}}

### feat_taker_buy_quote_ratio_10

{{chart:bin_taker_buy_quote_ratio_10}}

### feat_taker_buy_quote_ratio_30

{{chart:bin_taker_buy_quote_ratio_30}}

### feat_taker_flow_confirmed_return_10

{{chart:bin_taker_flow_confirmed_return_10}}

### feat_trade_count_rank_10

{{chart:bin_trade_count_rank_10}}

### feat_trade_count_rank_30

{{chart:bin_trade_count_rank_30}}

### feat_trade_count_ratio_10

{{chart:bin_trade_count_ratio_10}}

### feat_trade_count_ratio_30

{{chart:bin_trade_count_ratio_30}}

## Momentum

**Momentum jellemzők** — a leggyakoribb technikai momentum-indikátorok.

- **RSI**: Wilder-EWM alapú relatív erőindex (0-100), com = W-1

- **ROC**: Rate of Change = %-os árváltozás W bár alatt

- **Stochastic %K/%D**: az ár helyzete a W-bári HH-LL tartományban, %D = %K 3-bári SMA-ja

- **Williams %R**: inverz Stochastic (-100..0)

- **CCI**: Typical Price és annak SMA-jától való MAD-normalizált eltérés

- **ADX/+DI/-DI**: Wilder-EWM alapú iránymutató erőindex (+DI = felfelé, -DI = lefelé)

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `cci_20` | CCI-20: (TP - SMA(TP,20)) / (0.015 * MAD(TP,20)) - TP=(H+L+C)/3 |
| `roc_14` | ROC-14: (close / close[-14] - 1) * 100 - 14-bari ar-ROC |
| `roc_140` | ROC-140: (close / close[-140] - 1) * 100 - 140-bari ar-ROC |
| `rsi_14` | RSI-14: 100 - 100/(1 + avg_gain/avg_loss) - Wilder EWM, com=13, min_samples=14 |
| `stoch_d_14` | Stochastic %D-14: %K_14 3-bari SMA-ja (signal) |
| `stoch_k_14` | Stochastic %K-14: (close - LL_14) / (HH_14 - LL_14) * 100 |
| `williams_r_14` | Williams %R-14: -(HH_14 - close) / (HH_14 - LL_14) * 100 - -100..0 |

{{table:iv_momentum}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_momentum}}

### feat_cci_20

{{chart:bin_cci_20}}

### feat_roc_14

{{chart:bin_roc_14}}

### feat_roc_140

{{chart:bin_roc_140}}

### feat_rsi_14

{{chart:bin_rsi_14}}

### feat_stoch_k_14

{{chart:bin_stoch_k_14}}

## Trend

**Trend jellemzők** — mozgóátlag-alapú trend-indikátorok.

- **SMA/EMA/WMA/KAMA ratio**: close / MA — árpozíció a mozgóátlaghoz képest (1.0 = pontosan MA-n)

- **KAMA** (Kaufman Adaptive MA): az Efficiency Ratio alapján adaptív simítási faktort alkalmaz;
  gyors/lassú EMA közt interpolál a trendszerűség függvényében

- **MACD**: EMA_fast - EMA_slow, signal = MACD EWM-je, diff = MACD - signal (hisztogram)

- **ADX**: iránymutató mozgás erőssége

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `adx_14` | ADX-14: Wilder-EWM alapu DX simítasa - irányjelzett mozgáserő-index |
| `adx_neg_14` | -DI-14: 100 * EWM(-DM,14) / EWM(TR,14) - lefele iranyulo mozgas ereje |
| `adx_pos_14` | +DI-14: 100 * EWM(+DM,14) / EWM(TR,14) - felfele iranyulo mozgas ereje |
| `ema_ratio_14` | close / EMA_14 - ar / 14-bari EMA (span=14, adjust=False) |
| `ema_ratio_140` | close / EMA_140 - ar / 140-bari EMA (span=140, adjust=False) |
| `kama_ratio_10_2_30` | close / KAMA(W=10,fast=2,slow=30) - Kaufman Adaptive MA — ER alapu adaptiv EMA |
| `macd_12_26` | EMA_12 - EMA_26 - MACD vonal (fast=12, slow=26) |
| `macd_diff` | MACD - signal - MACD hisztogram (ero es iranyjelzo) |
| `macd_signal_12_26_9` | EWM(MACD, span=9) - MACD signal vonal (12/26/9) |
| `sma_ratio_14` | close / SMA_14 - ar / 14-bari egyszeru mozgoatlag (1 = pontosan SMA-n) |
| `sma_ratio_140` | close / SMA_140 - ar / 140-bari egyszeru mozgoatlag (1 = pontosan SMA-n) |
| `wma_ratio_14` | close / WMA_14 - ar / 14-bari linearis sulyú WMA (sulyok: 1..14) |

{{table:iv_trend}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_trend}}

### feat_adx_14

{{chart:bin_adx_14}}

### feat_adx_neg_14

{{chart:bin_adx_neg_14}}

### feat_adx_pos_14

{{chart:bin_adx_pos_14}}

### feat_ema_ratio_14

{{chart:bin_ema_ratio_14}}

### feat_kama_ratio_10_2_30

{{chart:bin_kama_ratio_10_2_30}}

### feat_macd_12_26

{{chart:bin_macd_12_26}}

### feat_macd_diff

{{chart:bin_macd_diff}}

### feat_macd_signal_12_26_9

{{chart:bin_macd_signal_12_26_9}}

### feat_sma_ratio_140

{{chart:bin_sma_ratio_140}}

### feat_wma_ratio_14

{{chart:bin_wma_ratio_14}}

## Autocorrelation

**Autokorrelációs jellemzők** — a log-return-sorozat önkorrelációja és varianciaarány-teszt.
Azt méri, hogy az egymást követő hozamok mennyire "emlékeznek" egymásra.
Pozitív autokorreláció → trend-tendencia, negatív → mean-reversion.
A variancia-arány (variance ratio) a random walk-hipotézis tesztje:
1.0 = véletlen bolyongás, >1 = trend, <1 = mean-reversion tendencia.
Lag-értékek: 1 és 5 bár; gördülő ablak: 30 és 60 bár.

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `return_autocorr_lag1_30` | Pearson-korr: ret(t) * ret(t-1) gordulo 30-bari ablakban - -1..+1 |
| `return_autocorr_lag1_60` | Pearson-korr: ret(t) * ret(t-1) gordulo 60-bari ablakban - -1..+1 |
| `return_autocorr_lag5_30` | Pearson-korr: ret(t) * ret(t-5) gordulo 30-bari ablakban - -1..+1 |
| `return_autocorr_lag5_60` | Pearson-korr: ret(t) * ret(t-5) gordulo 60-bari ablakban - -1..+1 |
| `variance_ratio_10_60` | Var(ret_10) / (Var(ret1)*10) — 60-bari ablak - 1.0=random walk, >1=trend, <1=mean-rev |

{{table:iv_autocorrelation}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_autocorrelation}}

### feat_return_autocorr_lag1_30

{{chart:bin_return_autocorr_lag1_30}}

### feat_return_autocorr_lag1_60

{{chart:bin_return_autocorr_lag1_60}}

### feat_return_autocorr_lag5_60

{{chart:bin_return_autocorr_lag5_60}}

## Market Structure

**Piaci struktúra (Market Structure)** — az ár pozíciója a közelmúltbeli csúcsokhoz és mélypontokhoz képest,
drawdown, recovery és hatékonysági mutatók.
ATR-normalizált távolságok a W-periódusos rolling high/low-tól, drawdown és recovery mérők,
az előző napi session high/low-tól vett ATR-távolság, valamint efficiency ratio
(mennyire "egyenes" az ármozgás, 0 = kaotikus, 1 = tökéletes trend).
Ablakméret: 10, 30, 60 bár.

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `atr_dist_high_10` | (rolling_max_10(H) - close) / ATR14 - tavolsag a 10-bari csucstol ATR-egysegben |
| `atr_dist_high_30` | (rolling_max_30(H) - close) / ATR14 - tavolsag a 30-bari csucstol ATR-egysegben |
| `atr_dist_high_60` | (rolling_max_60(H) - close) / ATR14 - tavolsag a 60-bari csucstol ATR-egysegben |
| `atr_dist_low_10` | (close - rolling_min_10(L)) / ATR14 - tavolsag a 10-bari melyponttol ATR-egysegben |
| `atr_dist_low_30` | (close - rolling_min_30(L)) / ATR14 - tavolsag a 30-bari melyponttol ATR-egysegben |
| `atr_dist_low_60` | (close - rolling_min_60(L)) / ATR14 - tavolsag a 60-bari melyponttol ATR-egysegben |
| `dist_rolling_high_10` | close / rolling_max_10(H) - 1 - ar tavolsaga a 10-bari high-tol (<=0) |
| `dist_rolling_high_30` | close / rolling_max_30(H) - 1 - ar tavolsaga a 30-bari high-tol (<=0) |
| `dist_rolling_high_60` | close / rolling_max_60(H) - 1 - ar tavolsaga a 60-bari high-tol (<=0) |
| `dist_rolling_low_10` | close / rolling_min_10(L) - 1 - ar tavolsaga a 10-bari low-tol (>=0) |
| `dist_rolling_low_30` | close / rolling_min_30(L) - 1 - ar tavolsaga a 30-bari low-tol (>=0) |
| `dist_rolling_low_60` | close / rolling_min_60(L) - 1 - ar tavolsaga a 60-bari low-tol (>=0) |
| `efficiency_ratio_10` | /close-close[-10]/ / sum(/ret1/, 10) - mennyire egyenes a mozgas (0=kaotikus, 1=tokeletes trend) |
| `efficiency_ratio_30` | /close-close[-30]/ / sum(/ret1/, 30) - mennyire egyenes a mozgas (0=kaotikus, 1=tokeletes trend) |
| `efficiency_ratio_60` | /close-close[-60]/ / sum(/ret1/, 60) - mennyire egyenes a mozgas (0=kaotikus, 1=tokeletes trend) |
| `max_drawdown_10` | (rolling_max_10(H) - rolling_min_10(L)) / rolling_max_10 - range / csocs arany 10 barban |
| `max_drawdown_30` | (rolling_max_30(H) - rolling_min_30(L)) / rolling_max_30 - range / csocs arany 30 barban |
| `max_drawdown_60` | (rolling_max_60(H) - rolling_min_60(L)) / rolling_max_60 - range / csocs arany 60 barban |
| `prev_session_high_dist` | (elozo_napi_max - close) / ATR14 - tavolsag az elozo napi csucstol |
| `prev_session_low_dist` | (close - elozo_napi_min) / ATR14 - tavolsag az elozo napi melyponttol |
| `range_expansion_10_30` | mean(H-L, 10) / mean(H-L, 30) - rovid/kozepes range arany — >1 = bovulo volatilitas |
| `recovery_ratio_10` | (close - rolling_min_10) / (rolling_max_10 - rolling_min_10) - recovery arany 10 barban |
| `recovery_ratio_30` | (close - rolling_min_30) / (rolling_max_30 - rolling_min_30) - recovery arany 30 barban |
| `recovery_ratio_60` | (close - rolling_min_60) / (rolling_max_60 - rolling_min_60) - recovery arany 60 barban |
| `rolling_drawdown_10` | close / rolling_max_10(close) - 1 - aktualis drawdown a 10-bari csocstol (<=0) |
| `rolling_drawdown_30` | close / rolling_max_30(close) - 1 - aktualis drawdown a 30-bari csocstol (<=0) |
| `rolling_drawdown_60` | close / rolling_max_60(close) - 1 - aktualis drawdown a 60-bari csocstol (<=0) |
| `time_since_high_10` | (W-1 - argmax(H, 10)) / 10 - normalizalt ido a 10-bari csocs ota (0=most, 1=legelején volt) |
| `time_since_high_30` | (W-1 - argmax(H, 30)) / 30 - normalizalt ido a 30-bari csocs ota (0=most, 1=legelején volt) |
| `time_since_high_60` | (W-1 - argmax(H, 60)) / 60 - normalizalt ido a 60-bari csocs ota (0=most, 1=legelején volt) |
| `time_since_low_10` | (W-1 - argmin(L, 10)) / 10 - normalizalt ido a 10-bari melypont ota |
| `time_since_low_30` | (W-1 - argmin(L, 30)) / 30 - normalizalt ido a 30-bari melypont ota |
| `time_since_low_60` | (W-1 - argmin(L, 60)) / 60 - normalizalt ido a 60-bari melypont ota |

{{table:iv_market_structure}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_market_structure}}

### feat_atr_dist_low_30

{{chart:bin_atr_dist_low_30}}

### feat_dist_rolling_high_10

{{chart:bin_dist_rolling_high_10}}

### feat_dist_rolling_high_30

{{chart:bin_dist_rolling_high_30}}

### feat_dist_rolling_high_60

{{chart:bin_dist_rolling_high_60}}

### feat_dist_rolling_low_10

{{chart:bin_dist_rolling_low_10}}

### feat_dist_rolling_low_30

{{chart:bin_dist_rolling_low_30}}

### feat_dist_rolling_low_60

{{chart:bin_dist_rolling_low_60}}

### feat_efficiency_ratio_10

{{chart:bin_efficiency_ratio_10}}

### feat_max_drawdown_10

{{chart:bin_max_drawdown_10}}

### feat_max_drawdown_30

{{chart:bin_max_drawdown_30}}

### feat_max_drawdown_60

{{chart:bin_max_drawdown_60}}

### feat_prev_session_high_dist

{{chart:bin_prev_session_high_dist}}

### feat_range_expansion_10_30

{{chart:bin_range_expansion_10_30}}

### feat_recovery_ratio_10

{{chart:bin_recovery_ratio_10}}

### feat_recovery_ratio_30

{{chart:bin_recovery_ratio_30}}

### feat_recovery_ratio_60

{{chart:bin_recovery_ratio_60}}

### feat_rolling_drawdown_10

{{chart:bin_rolling_drawdown_10}}

### feat_time_since_high_30

{{chart:bin_time_since_high_30}}

### feat_time_since_high_60

{{chart:bin_time_since_high_60}}

### feat_time_since_low_10

{{chart:bin_time_since_low_10}}

### feat_time_since_low_60

{{chart:bin_time_since_low_60}}

## Swing

**Swing struktúra (Swing)** — higher high/low és lower high/low darabszáma W bár alatt,
swing pontok és irányzategyezés mérői.
A directional_agreement azt mutatja, hogy a rövid és közepes trend iránya egyezik-e:
+1 = mindkét irány azonos (trendkonszisztencia), -1 = ellentétes (potenciális fordulat).
Ablakméret: 10, 30, 60 bár.

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `directional_agreement_10_30` | sign(ret_10) * sign(ret_30) - 10 es 30 bari return iranyanak egyezese (+1/-1) |
| `higher_high_count_5` | high > prev_high esetek szama az utolso 5 barban |
| `higher_low_count_5` | low > prev_low esetek szama az utolso 5 barban |
| `lower_high_count_5` | high < prev_high esetek szama az utolso 5 barban |
| `lower_low_count_5` | low < prev_low esetek szama az utolso 5 barban |
| `swing_high_5` | high >= rolling_max(H, 5) => 5-bari swing high (1/0) |
| `swing_low_5` | low <= rolling_min(L, 5) => 5-bari swing low (1/0) |

{{table:iv_swing}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_swing}}

### feat_higher_high_count_5

{{chart:bin_higher_high_count_5}}

### feat_lower_high_count_5

{{chart:bin_lower_high_count_5}}

### feat_lower_low_count_5

{{chart:bin_lower_low_count_5}}

### feat_swing_high_5

{{chart:bin_swing_high_5}}

## Tail Risk

**Farok-kockázat (Tail Risk)** — az ár felfele és lefele irányú szélsőértékeinek aszimmetriája W bárra.
A return_asymmetry megmutatja, hogy a pozitív vagy a negatív irányú mozgások dominálnak-e.
Értéke > 1.0 => bullish aszimmetria (felfele nagyobb átlagos mozgás),
értéke < 1.0 => bearish aszimmetria.
Ablakméret: 10, 30, 60 bár.

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `neg_return_mean_10` | rolling_mean(/min(ret,0)/, 10) - negativ hozamok abszolut 10-bari atlaga (downside) |
| `neg_return_mean_30` | rolling_mean(/min(ret,0)/, 30) - negativ hozamok abszolut 30-bari atlaga (downside) |
| `neg_return_mean_60` | rolling_mean(/min(ret,0)/, 60) - negativ hozamok abszolut 60-bari atlaga (downside) |
| `pos_return_mean_10` | rolling_mean(max(ret,0), 10) - pozitiv hozamok 10-bari atlaga (upside intenzitas) |
| `pos_return_mean_30` | rolling_mean(max(ret,0), 30) - pozitiv hozamok 30-bari atlaga (upside intenzitas) |
| `pos_return_mean_60` | rolling_mean(max(ret,0), 60) - pozitiv hozamok 60-bari atlaga (upside intenzitas) |
| `return_asymmetry_10` | pos_return_mean_10 / neg_return_mean_10 - >1 = bullish aszimmetria, <1 = bearish |
| `return_asymmetry_30` | pos_return_mean_30 / neg_return_mean_30 - >1 = bullish aszimmetria, <1 = bearish |
| `return_asymmetry_60` | pos_return_mean_60 / neg_return_mean_60 - >1 = bullish aszimmetria, <1 = bearish |

{{table:iv_tail_risk}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_tail_risk}}

### feat_neg_return_mean_10

{{chart:bin_neg_return_mean_10}}

### feat_neg_return_mean_30

{{chart:bin_neg_return_mean_30}}

### feat_neg_return_mean_60

{{chart:bin_neg_return_mean_60}}

### feat_pos_return_mean_10

{{chart:bin_pos_return_mean_10}}

### feat_pos_return_mean_30

{{chart:bin_pos_return_mean_30}}

### feat_pos_return_mean_60

{{chart:bin_pos_return_mean_60}}

### feat_return_asymmetry_30

{{chart:bin_return_asymmetry_30}}

### feat_return_asymmetry_60

{{chart:bin_return_asymmetry_60}}

## Ichimoku

**Ichimoku Cloud** — japán trendkövető rendszer 9/26/52-es periódusokkal.

- **Tenkan-sen** (Konverzió-vonal): (9-bár max + 9-bár min) / 2 — rövid trend

- **Kijun-sen** (Alap-vonal): (26-bár max + 26-bár min) / 2 — közepes trend

- **Senkou Span B** (Cloud határ): (52-bár max + 52-bár min) / 2

- A feature-ök az árhoz normalizált távolságokat adják vissza: (vonal / close) - 1

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `ichimoku_cloud_thickness` | ((tenkan+kijun)/2 - senkou_b) / close - felho vastagsaga (pozitiv = bullish cloud) |
| `kijun_ratio` | kijun_sen / close - 1 - Kijun-sen = (26-bar max + 26-bar min) / 2 |
| `price_vs_kijun` | (close - kijun) / close - ar vs. Kijun-sen pozicio |
| `price_vs_tenkan` | (close - tenkan) / close - ar vs. Tenkan-sen pozicio |
| `senkou_b_ratio` | senkou_b / close - 1 - Senkou Span B = (52-bar max + 52-bar min) / 2 |
| `tenkan_kijun_delta` | (tenkan - kijun) / close - TK-kulonbseg (cloud bias iranyjelzo) |
| `tenkan_ratio` | tenkan_sen / close - 1 - Tenkan-sen = (9-bar max + 9-bar min) / 2 |

{{table:iv_ichimoku}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_ichimoku}}

### feat_ichimoku_cloud_thickness

{{chart:bin_ichimoku_cloud_thickness}}

### feat_price_vs_kijun

{{chart:bin_price_vs_kijun}}

### feat_price_vs_tenkan

{{chart:bin_price_vs_tenkan}}

### feat_senkou_b_ratio

{{chart:bin_senkou_b_ratio}}

### feat_tenkan_kijun_delta

{{chart:bin_tenkan_kijun_delta}}

## Donchian

**Donchian-csatorna (Donchian)** — az ár elhelyezkedése és a csatorna szélessége W bár alatt.
A Donchian-csatorna felső határa a W-periódusos gördülő maximum (rolling max),
alsó határa a gördülő minimum (rolling min).
Kitörés-jelzőként és range-volatilitás mérőként egyaránt használatos.
Ablakméret: 10, 30, 60 bár.

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `donchian_breakout_10` | close >= rolling_max_10(H) => 10-bari Donchian kitorés (1/0) |
| `donchian_breakout_30` | close >= rolling_max_30(H) => 30-bari Donchian kitorés (1/0) |
| `donchian_breakout_60` | close >= rolling_max_60(H) => 60-bari Donchian kitorés (1/0) |
| `donchian_position_10` | (close - rolling_min_10) / (rolling_max_10 - rolling_min_10) - pozicio a csatornan (0=aljan, 1=tetején) |
| `donchian_position_30` | (close - rolling_min_30) / (rolling_max_30 - rolling_min_30) - pozicio a csatornan (0=aljan, 1=tetején) |
| `donchian_position_60` | (close - rolling_min_60) / (rolling_max_60 - rolling_min_60) - pozicio a csatornan (0=aljan, 1=tetején) |
| `donchian_width_10` | (rolling_max_10(H) - rolling_min_10(L)) / close - csatorna szelessege close-normalizalva |
| `donchian_width_30` | (rolling_max_30(H) - rolling_min_30(L)) / close - csatorna szelessege close-normalizalva |
| `donchian_width_60` | (rolling_max_60(H) - rolling_min_60(L)) / close - csatorna szelessege close-normalizalva |

{{table:iv_donchian}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_donchian}}

### feat_donchian_breakout_10

{{chart:bin_donchian_breakout_10}}

### feat_donchian_position_10

{{chart:bin_donchian_position_10}}

### feat_donchian_position_30

{{chart:bin_donchian_position_30}}

### feat_donchian_width_10

{{chart:bin_donchian_width_10}}

### feat_donchian_width_30

{{chart:bin_donchian_width_30}}

### feat_donchian_width_60

{{chart:bin_donchian_width_60}}

## Trend Slope

**Trend meredeksége (Trend Slope)** — EMA változásának sebessége és lineáris regressziós jellemzők.

- **ema_slope_W**: (EMA_W(t) - EMA_W(t-1)) / close — pillanatnyi meredekség, normalizálva

- **lr_slope_W**: OLS lineáris regresszió meredeksége W bárra illesztve, close-szal normalizálva

- **lr_r2_W**: OLS R2 — mennyire "lineáris" a trend (0 = semmi, 1 = tökéletes)

- **lr_residual_W**: (close - OLS_hat) / close — ár eltérése a trendvonaltól

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `ema_slope_10` | (EMA_10(t) - EMA_10(t-1)) / close - EMA 10 pillanatnyi meredeksege (close-normalizalva) |
| `ema_slope_30` | (EMA_30(t) - EMA_30(t-1)) / close - EMA 30 pillanatnyi meredeksege (close-normalizalva) |
| `lr_r2_10` | OLS R2 10-barra - illeszkedés minősége (0=semmi, 1=tokéletes linearis trend) |
| `lr_r2_30` | OLS R2 30-barra - illeszkedés minősége (0=semmi, 1=tokéletes linearis trend) |
| `lr_r2_60` | OLS R2 60-barra - illeszkedés minősége (0=semmi, 1=tokéletes linearis trend) |
| `lr_residual_10` | (close - OLS_hat_10) / close - ar eltérese a 10-bari regressziós egyenestol |
| `lr_residual_30` | (close - OLS_hat_30) / close - ar eltérese a 30-bari regressziós egyenestol |
| `lr_residual_60` | (close - OLS_hat_60) / close - ar eltérese a 60-bari regressziós egyenestol |
| `lr_slope_10` | OLS_slope_10 / close - 10-bari linearis regresszio meredeksege, close-normalizalva |
| `lr_slope_30` | OLS_slope_30 / close - 30-bari linearis regresszio meredeksege, close-normalizalva |
| `lr_slope_60` | OLS_slope_60 / close - 60-bari linearis regresszio meredeksege, close-normalizalva |

{{table:iv_trend_slope}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_trend_slope}}

### feat_ema_slope_10

{{chart:bin_ema_slope_10}}

### feat_ema_slope_30

{{chart:bin_ema_slope_30}}

### feat_lr_r2_30

{{chart:bin_lr_r2_30}}

### feat_lr_residual_10

{{chart:bin_lr_residual_10}}

### feat_lr_residual_30

{{chart:bin_lr_residual_30}}

### feat_lr_residual_60

{{chart:bin_lr_residual_60}}

### feat_lr_slope_10

{{chart:bin_lr_slope_10}}

### feat_lr_slope_30

{{chart:bin_lr_slope_30}}

### feat_lr_slope_60

{{chart:bin_lr_slope_60}}

## Accel

**Gyorsulás (Accel)** — momentum-indikátorok változásának sebessége (2. derivált).
Azt méri, hogy az RSI, ROC és a 10-bári return-momentum az előző W bárhoz képest mennyit változott.
Gyorsuló pozitív momentum erősödő trendet, gyorsuló negatív értékek lassulást vagy fordulatot jelezhetnek.
Ablakméret: 10 és 30 bár.

| Feature | Leírás & Képlet |
|:--------|:----------------|
| `return_momentum_delta_10` | ret10(t) - ret10(t-10) - ahol ret10 = close/close[-10]-1, 10-bari valtozasa |
| `return_momentum_delta_30` | ret10(t) - ret10(t-30) - ahol ret10 = close/close[-10]-1, 30-bari valtozasa |
| `roc_delta_10` | ROC14(t) - ROC14(t-10) - 14-periodusos ROC ertekenek 10-bari valtozasa |
| `roc_delta_30` | ROC14(t) - ROC14(t-30) - 14-periodusos ROC ertekenek 30-bari valtozasa |
| `roc_delta_5` | ROC14(t) - ROC14(t-5) - 14-periodusos ROC ertekenek 5-bari valtozasa |
| `rsi_delta_10` | RSI(t) - RSI(t-10) - Wilder EWM RSI-14 ertekenek 10-bari valtozasa |
| `rsi_delta_30` | RSI(t) - RSI(t-30) - Wilder EWM RSI-14 ertekenek 30-bari valtozasa |
| `rsi_delta_5` | RSI(t) - RSI(t-5) - Wilder EWM RSI-14 ertekenek 5-bari valtozasa |

{{table:iv_accel}}

### Korrelációs mátrix (átment feature-ök)

{{chart:corr_accel}}

### feat_return_momentum_delta_10

{{chart:bin_return_momentum_delta_10}}

### feat_return_momentum_delta_30

{{chart:bin_return_momentum_delta_30}}

### feat_roc_delta_10

{{chart:bin_roc_delta_10}}

### feat_roc_delta_30

{{chart:bin_roc_delta_30}}

### feat_roc_delta_5

{{chart:bin_roc_delta_5}}

### feat_rsi_delta_30

{{chart:bin_rsi_delta_30}}

## Összefoglaló - feature szelekció eredménye

### Csoport bajnokok (17 csoport)

{{table:group_champions}}

### Top 10 feature (MI szerint)

{{table:top10_features}}

### Összes feature MI szerint rendezve (208 db)

{{table:all_features}}

### Feature szelekció eredménye

{{table:selection_counts}}

## Értelmezés

- A 208 feature-ből **137** került a végső halmazba (55 MI, 14 duplikátum, 2 quality kizárás).
- Legerősebb jelek (MI): `natr_14`, `gk_vol_30`, `gk_vol_60`, `gk_vol_10`, `hist_vol_20`.
- Az IV/MI értékek **train** soron készültek, így a valid periódusról nem hordoznak információt; a szelekció stabilitását a hyperparaméter-keresés riportja (`03_hyperparameter_search`) valid mutatói ellenőrzik.
