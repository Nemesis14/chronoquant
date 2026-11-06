"""Feature-group metadata for the feature-engineering report (moved verbatim from the notebook).

``GROUP_MAP`` maps feature-name prefixes to groups (ORDER MATTERS: more specific prefixes first),
``GROUP_ORDER`` is the logical report order, ``GROUP_DOCS`` the per-group description and
``feature_description`` the one-line formula/description of a single feature.
"""

from __future__ import annotations

import re as _re

GROUP_MAP = {
    # Accel — delta prefixek a feat_rsi/roc/return_ előtt
    "feat_rsi_delta": "Accel", "feat_roc_delta": "Accel",
    "feat_return_momentum_delta": "Accel",
    # Interaction — a feat_rsi/feat_volume_ általánosabb prefixek előtt
    "feat_rsi_x_trend": "Interaction", "feat_volume_x_momentum": "Interaction",
    # Momentum
    "feat_rsi": "Momentum", "feat_roc": "Momentum", "feat_stoch_k": "Momentum",
    "feat_stoch_d": "Momentum", "feat_williams_r": "Momentum", "feat_cci": "Momentum",
    # Trend Slope bővítés — feat_ema_slope_ a feat_ema_ratio előtt
    "feat_ema_slope_": "Trend Slope",
    # Trend
    "feat_sma_ratio": "Trend", "feat_ema_ratio": "Trend", "feat_wma_ratio": "Trend",
    "feat_kama_ratio": "Trend", "feat_macd_signal": "Trend", "feat_macd_diff": "Trend",
    "feat_macd": "Trend", "feat_adx_pos": "Trend", "feat_adx_neg": "Trend",
    "feat_adx": "Trend",
    # Volatility — feat_atr_dist_ → Market Structure, feat_atr → Volatility
    "feat_bb_width": "Volatility", "feat_bb_position": "Volatility",
    "feat_atr_dist_high": "Market Structure", "feat_atr_dist_low": "Market Structure",
    "feat_atr": "Volatility", "feat_natr": "Volatility", "feat_hist_vol": "Volatility",
    "feat_parkinson_vol": "Volatility", "feat_gk_vol": "Volatility",
    # Volume — specifikusabb prefix előbb
    "feat_volume_confirmed_return_": "Volume",
    "feat_volume_accel_": "Volume",
    "feat_volume_rank_": "Volume",
    "feat_volume_sma": "Volume", "feat_volume_ratio": "Volume",
    "feat_obv_roc": "Volume", "feat_obv": "Volume",
    "feat_mfi": "Volume", "feat_ad_line": "Volume", "feat_cmf": "Volume",
    # Price Action — feat_returns_ (specifikus) a feat_return_ (általános) előtt
    "feat_returns_log": "Price Action", "feat_returns_sma": "Price Action",
    "feat_returns_std": "Price Action", "feat_returns_skew": "Price Action",
    "feat_returns_kurt": "Price Action",
    "feat_hml_range": "Price Action", "feat_ohlc_range": "Price Action",
    "feat_close_position": "Price Action",
    # Market Structure — specifikus session dist prefixek először
    "feat_prev_session_high_dist": "Market Structure",
    "feat_prev_session_low_dist": "Market Structure",
    "feat_rolling_drawdown_": "Market Structure",
    "feat_dist_rolling_": "Market Structure",
    "feat_range_expansion_": "Market Structure",
    "feat_recovery_ratio": "Market Structure", "feat_max_drawdown": "Market Structure",
    "feat_time_since_high": "Market Structure", "feat_time_since_low": "Market Structure",
    "feat_efficiency_ratio": "Market Structure",
    # Regime / Stats
    "feat_regime_rank": "Regime Rank", "feat_return_autocorr": "Autocorrelation",
    "feat_variance_ratio": "Autocorrelation",
    # Candle Pattern — specifikus wick prefixek a feat_wick_ratio előtt
    "feat_lower_wick_ratio": "Candle Pattern",
    "feat_upper_wick_ratio": "Candle Pattern",
    "feat_signed_body_": "Candle Pattern",
    "feat_wick_imbalance_": "Candle Pattern",
    "feat_doji": "Candle Pattern", "feat_hammer": "Candle Pattern",
    "feat_shooting_star": "Candle Pattern", "feat_inside_bar": "Candle Pattern",
    "feat_outside_bar": "Candle Pattern", "feat_engulf_bull": "Candle Pattern",
    "feat_engulf_bear": "Candle Pattern", "feat_bull_bars_ratio": "Candle Pattern",
    "feat_body_ratio": "Candle Pattern", "feat_wick_ratio": "Candle Pattern",
    # Trend Slope / LR
    "feat_lr_slope": "Trend Slope", "feat_lr_r2": "Trend Slope",
    "feat_lr_residual": "Trend Slope", "feat_trend_slope": "Trend Slope",
    # Tail Risk
    "feat_pos_return_mean": "Tail Risk", "feat_neg_return_mean": "Tail Risk",
    "feat_return_asymmetry": "Tail Risk",
    # Gap
    "feat_gap_open_abs_sma": "Gap", "feat_gap_open": "Gap",
    # Ichimoku — specifikus prefixek először
    "feat_tenkan_kijun_": "Ichimoku",
    "feat_price_vs_": "Ichimoku",
    "feat_tenkan_ratio": "Ichimoku", "feat_kijun_ratio": "Ichimoku",
    "feat_senkou_b_ratio": "Ichimoku", "feat_ichimoku_cloud_thickness": "Ichimoku",
    # Donchian
    "feat_donchian_width": "Donchian", "feat_donchian_position": "Donchian",
    "feat_donchian_breakout": "Donchian",
    # Time / Session
    "feat_hour_sin": "Time/Session", "feat_hour_cos": "Time/Session",
    "feat_dayofweek_sin": "Time/Session", "feat_dayofweek_cos": "Time/Session",
    "feat_weekend": "Time/Session", "feat_session_asia": "Time/Session",
    "feat_session_europe": "Time/Session", "feat_session_us": "Time/Session",
    "feat_bars_into_session_norm": "Time/Session", "feat_day_range_position": "Time/Session",
    "feat_day_open_return": "Time/Session", "feat_weekly_open_return": "Time/Session",
    # Return Distance
    "feat_dist_sma": "Return Distance", "feat_dist_bb": "Return Distance",
    # Price Action bővítés — feat_return_ UTÁNA (feat_returns_ és egyéb spec. prefixek már fent)
    "feat_return_z_": "Price Action",
    "feat_vol_adj_return_": "Price Action",
    "feat_return_": "Price Action",
    # Swing — új csoport
    "feat_higher_high_": "Swing",
    "feat_higher_low_": "Swing",
    "feat_lower_high_": "Swing",
    "feat_lower_low_": "Swing",
    "feat_swing_": "Swing",
    "feat_directional_agreement_": "Swing",
    # Taker Flow — új csoport
    "feat_taker_": "Taker Flow",
    "feat_avg_trade_": "Taker Flow",
    "feat_trade_count_": "Taker Flow",
    "feat_quote_volume_": "Taker Flow",
}




GROUP_ORDER = [
    "Price Action",    # alap: log-return, range-arányok
    "Volatility",      # alap: ATR, BB, hist-vol — Market Structure is használja
    "Volume",          # alap: OBV, MFI, CMF
    "Gap",             # alap: nyitógap
    "Candle Pattern",  # alap: OHLC gyertyaalak
    "Time/Session",    # alap: időkódolás
    "Taker Flow",      # alap: exchange-specifikus taker adatok
    "Momentum",        # 1. szint: RSI, ROC, Stochastic — Accel is ezeket használja
    "Trend",           # 1. szint: MA-ratio, MACD — Trend Slope ezekre épül
    "Autocorrelation", # 1. szint: return autokorrelációk
    "Market Structure",# 2. szint: ATR-norm. távolságok, drawdown (használja Volatility ATR-jét)
    "Swing",           # 2. szint: higher high/low számolás
    "Tail Risk",       # 2. szint: return aszimmetria
    "Ichimoku",        # 2. szint: komplex trendrendszer (9/26/52)
    "Donchian",        # 2. szint: csatorna-alapú pozíció
    "Trend Slope",     # 3. szint: EMA/LR meredekség — Trend MA-ra épül
    "Accel",           # 3. szint: RSI/ROC delta — Momentum-ra épül
]



def assign_group(feat_name: str) -> str:
    """Group of a ``feat_*`` column (``"Other"`` if no prefix matches)."""
    for prefix, group in GROUP_MAP.items():
        if feat_name.startswith(prefix):
            return group
    return "Other"


GROUP_DOCS = {
    "Accel": """\
**Gyorsulás (Accel)** — momentum-indikátorok változásának sebessége (2. derivált).
Azt méri, hogy az RSI, ROC és a 10-bári return-momentum az előző W bárhoz képest mennyit változott.
Gyorsuló pozitív momentum erősödő trendet, gyorsuló negatív értékek lassulást vagy fordulatot jelezhetnek.
Ablakméret: 10 és 30 bár.""",

    "Autocorrelation": """\
**Autokorrelációs jellemzők** — a log-return-sorozat önkorrelációja és varianciaarány-teszt.
Azt méri, hogy az egymást követő hozamok mennyire "emlékeznek" egymásra.
Pozitív autokorreláció → trend-tendencia, negatív → mean-reversion.
A variancia-arány (variance ratio) a random walk-hipotézis tesztje:
1.0 = véletlen bolyongás, >1 = trend, <1 = mean-reversion tendencia.
Lag-értékek: 1 és 5 bár; gördülő ablak: 30 és 60 bár.""",

    "Candle Pattern": """\
**Gyertyamintázat (Candle Pattern)** — az OHLC adatokból számolt gyertyaalak-jellemzők.
Az egyes gyertyák test/kanóc arányait, irányát és ismert technikai mintázatokat (doji, hammer, engulfing) kódolják.
A gördülő átlag változatok (SMA) a mintázat hosszabb távú fennállását mutatják.
Alap képletek: range = high - low, body = |close - open|,
upper_wick = high - max(open, close), lower_wick = min(open, close) - low.""",

    "Donchian": """\
**Donchian-csatorna (Donchian)** — az ár elhelyezkedése és a csatorna szélessége W bár alatt.
A Donchian-csatorna felső határa a W-periódusos gördülő maximum (rolling max),
alsó határa a gördülő minimum (rolling min).
Kitörés-jelzőként és range-volatilitás mérőként egyaránt használatos.
Ablakméret: 10, 30, 60 bár.""",

    "Gap": """\
**Gap jellemzők** — a nyitóár és az előző zárár közötti ugrás (rés) mértéke.
Kripto-piacokon a gap kisebb, de hír-eseményeknél és likvidáció-kaszkádoknál szignifikáns lehet.
A gap_open_abs_sma a gap abszolút értékének gördülő átlaga, ezzel gap-volatilitás proxy-ként használható.
Ablakméret: 10 és 30 bár.""",

    "Ichimoku": """\
**Ichimoku Cloud** — japán trendkövető rendszer 9/26/52-es periódusokkal.
- **Tenkan-sen** (Konverzió-vonal): (9-bár max + 9-bár min) / 2 — rövid trend
- **Kijun-sen** (Alap-vonal): (26-bár max + 26-bár min) / 2 — közepes trend
- **Senkou Span B** (Cloud határ): (52-bár max + 52-bár min) / 2
- A feature-ök az árhoz normalizált távolságokat adják vissza: (vonal / close) - 1""",

    "Market Structure": """\
**Piaci struktúra (Market Structure)** — az ár pozíciója a közelmúltbeli csúcsokhoz és mélypontokhoz képest,
drawdown, recovery és hatékonysági mutatók.
ATR-normalizált távolságok a W-periódusos rolling high/low-tól, drawdown és recovery mérők,
az előző napi session high/low-tól vett ATR-távolság, valamint efficiency ratio
(mennyire "egyenes" az ármozgás, 0 = kaotikus, 1 = tökéletes trend).
Ablakméret: 10, 30, 60 bár.""",

    "Momentum": """\
**Momentum jellemzők** — a leggyakoribb technikai momentum-indikátorok.
- **RSI**: Wilder-EWM alapú relatív erőindex (0-100), com = W-1
- **ROC**: Rate of Change = %-os árváltozás W bár alatt
- **Stochastic %K/%D**: az ár helyzete a W-bári HH-LL tartományban, %D = %K 3-bári SMA-ja
- **Williams %R**: inverz Stochastic (-100..0)
- **CCI**: Typical Price és annak SMA-jától való MAD-normalizált eltérés
- **ADX/+DI/-DI**: Wilder-EWM alapú iránymutató erőindex (+DI = felfelé, -DI = lefelé)""",

    "Price Action": """\
**Ármozgás (Price Action)** — az ár nyers mozgásából számolt statisztikai jellemzők.
- Log-return és annak gördülő átlaga/szórása/ferdesége/kurtózisa
- OHLC range-arányok: az egyetlen gyertya (high-low) tartománya normalizálva az árhoz — nem gördülő ablak
- Return-Z: az aktuális hozam normalizálva a gördülő volatilitáshoz
- Vol-adj-return: hozam osztva gördülő szórással (Sharpe-proxy egyedi bárra)""",

    "Swing": """\
**Swing struktúra (Swing)** — higher high/low és lower high/low darabszáma W bár alatt,
swing pontok és irányzategyezés mérői.
A directional_agreement azt mutatja, hogy a rövid és közepes trend iránya egyezik-e:
+1 = mindkét irány azonos (trendkonszisztencia), -1 = ellentétes (potenciális fordulat).
Ablakméret: 10, 30, 60 bár.""",

    "Tail Risk": """\
**Farok-kockázat (Tail Risk)** — az ár felfele és lefele irányú szélsőértékeinek aszimmetriája W bárra.
A return_asymmetry megmutatja, hogy a pozitív vagy a negatív irányú mozgások dominálnak-e.
Értéke > 1.0 => bullish aszimmetria (felfele nagyobb átlagos mozgás),
értéke < 1.0 => bearish aszimmetria.
Ablakméret: 10, 30, 60 bár.""",

    "Taker Flow": """\
**Taker flow és kereskedési aktivitás** — agresszív vevők/eladók jelenlétének mérése.
A Binance futures API-ból elérhető taker_buy_base, quote_volume és trades adatokból számolt arányok.
A taker flow azt méri, hogy a kötések mekkora hányada "taker" (piaci megbízás) és melyik irányba mutat.
Gördülő összegen számolva (rolling sum), ablakméret: 5, 10, 30, 60 bár.""",

    "Time/Session": """\
**Idő és session jellemzők** — determinisztikus időstruktúra-kódolás, session-relatív pozíciók.
A kör-kódolás (circular encoding) biztosítja, hogy az éjfél és hajnal közel essen egymáshoz
a feature-térben is. Session flag-ek UTC-időzóna alapján.
Ezek a feature-ök nem kapnak t-1 laget (P2 determinisztikus indexek — az időbélyeg alapján
pontosan kiszámíthatók, nem tartalmaznak jövőbeli infót).""",

    "Trend": """\
**Trend jellemzők** — mozgóátlag-alapú trend-indikátorok.
- **SMA/EMA/WMA/KAMA ratio**: close / MA — árpozíció a mozgóátlaghoz képest (1.0 = pontosan MA-n)
- **KAMA** (Kaufman Adaptive MA): az Efficiency Ratio alapján adaptív simítási faktort alkalmaz;
  gyors/lassú EMA közt interpolál a trendszerűség függvényében
- **MACD**: EMA_fast - EMA_slow, signal = MACD EWM-je, diff = MACD - signal (hisztogram)
- **ADX**: iránymutató mozgás erőssége""",

    "Trend Slope": """\
**Trend meredeksége (Trend Slope)** — EMA változásának sebessége és lineáris regressziós jellemzők.
- **ema_slope_W**: (EMA_W(t) - EMA_W(t-1)) / close — pillanatnyi meredekség, normalizálva
- **lr_slope_W**: OLS lineáris regresszió meredeksége W bárra illesztve, close-szal normalizálva
- **lr_r2_W**: OLS R2 — mennyire "lineáris" a trend (0 = semmi, 1 = tökéletes)
- **lr_residual_W**: (close - OLS_hat) / close — ár eltérése a trendvonaltól""",

    "Volatility": """\
**Volatilitás jellemzők** — az ármozgás szórás- és range-alapú mérői.
- **Bollinger-sávok**: bb_width = (upper - lower) / close, bb_position = (close - lower) / (upper - lower)
- **ATR/NATR**: Wilder-EWM alapú True Range átlag; NATR = ATR / close (normalizált)
- **Hist-vol**: log-return gördülő szórása W bárra (ddof=1)
- **Parkinson-vol**: sqrt(mean((ln(H/L))^2 / (4*ln2))) — csak H/L-t használ, pontosabb, mint a close-alapú vol
- **Garman-Klass-vol**: sqrt(mean(0.5*(ln(H/L))^2 - (2*ln2-1)*(ln(C/O))^2)) — OHLC-alapú, még pontosabb""",

    "Volume": """\
**Volume jellemzők** — a kereskedési volumen szintje, trendje és flow-összetétele.
- **volume_ratio**: aktuális volume / W-bári SMA — normalizált aktivitás szint
- **OBV**: On-Balance Volume = sum(sign(delta_close) * volume) — kumulált irányjelzett forgalom
- **MFI**: Money Flow Index — 100 - 100/(1 + pos_MF/neg_MF), ahol MF = TP * volume
- **A/D Line**: sum(CLV * volume), ahol CLV = ((C-L)-(H-C))/(H-L) — felhalmozás/elosztás
- **CMF**: Chaikin Money Flow = sum(MFV, W) / sum(V, W) — rövid távú flow (-1..+1)""",
}


def _feat_desc(feat: str) -> str:
    """1-soros keplet + leiras egy feature-nevhez (prefix-pattern alapjan)."""
    f = feat.replace("feat_", "")

    # Accel
    if m := _re.fullmatch(r"rsi_delta_(\d+)", f):
        return f"RSI(t) - RSI(t-{m[1]}) | Wilder EWM RSI-14 ertekenek {m[1]}-bari valtozasa"
    if m := _re.fullmatch(r"roc_delta_(\d+)", f):
        return f"ROC14(t) - ROC14(t-{m[1]}) | 14-periodusos ROC ertekenek {m[1]}-bari valtozasa"
    if m := _re.fullmatch(r"return_momentum_delta_(\d+)", f):
        return f"ret10(t) - ret10(t-{m[1]}) | ahol ret10 = close/close[-10]-1, {m[1]}-bari valtozasa"

    # Autocorrelation
    if m := _re.fullmatch(r"return_autocorr_lag(\d+)_(\d+)", f):
        return f"Pearson-korr: ret(t) * ret(t-{m[1]}) gordulo {m[2]}-bari ablakban | -1..+1"
    if m := _re.fullmatch(r"variance_ratio_(\d+)_(\d+)", f):
        return f"Var(ret_{m[1]}) / (Var(ret1)*{m[1]}) — {m[2]}-bari ablak | 1.0=random walk, >1=trend, <1=mean-rev"

    # Candle Pattern
    if f == "doji":
        return "|body|/range < 0.1 => doji gyertya (1/0) | egyforma nyito es zaro"
    if f == "hammer":
        return "lower_wick/range > 0.6 ES |body|/range < 0.3 ES bullish => kalapacs (1/0)"
    if f == "shooting_star":
        return "upper_wick/range > 0.6 ES |body|/range < 0.3 ES bearish => shooting star (1/0)"
    if f == "inside_bar":
        return "high < prev_high ES low > prev_low => inside bar (belso gyertya) (1/0)"
    if f == "outside_bar":
        return "high > prev_high ES low < prev_low => outside bar (kulso gyertya) (1/0)"
    if f == "engulf_bull":
        return "Bullish engulfing: elozo bearish bar teljes befoglalasa (1/0)"
    if f == "engulf_bear":
        return "Bearish engulfing: elozo bullish bar teljes befoglalasa (1/0)"
    if f == "body_ratio":
        return "|close - open| / (high - low) | test/tartomany arany (0-1)"
    if f in ("signed_body_ratio", "signed_body"):
        return "(close - open) / (high - low) | iranyjelzett test-arany (-1..+1)"
    if m := _re.fullmatch(r"body_ratio_sma_(\d+)", f):
        return f"|body|/range {m[1]}-bari gordulo atlaga"
    if m := _re.fullmatch(r"signed_body_sma_(\d+)", f):
        return f"(close-open)/range {m[1]}-bari gordulo atlaga | irany-konszisztencia"
    if m := _re.fullmatch(r"wick_imbalance_sma_(\d+)", f):
        return f"(lower_wick - upper_wick)/range {m[1]}-bari atlaga | pozitiv = bullish kanoc-aszimmetria"
    if f == "upper_wick_ratio":
        return "upper_wick / (high - low) | felso kanoc aranya (0-1)"
    if f == "lower_wick_ratio":
        return "lower_wick / (high - low) | also kanoc aranya (0-1)"
    if m := _re.fullmatch(r"bull_bars_ratio_(\d+)", f):
        return f"(close > open) barainak aranya az utolso {m[1]} barban (0-1)"

    # Donchian
    if m := _re.fullmatch(r"donchian_width_(\d+)", f):
        return f"(rolling_max_{m[1]}(H) - rolling_min_{m[1]}(L)) / close | csatorna szelessege close-normalizalva"
    if m := _re.fullmatch(r"donchian_position_(\d+)", f):
        return f"(close - rolling_min_{m[1]}) / (rolling_max_{m[1]} - rolling_min_{m[1]}) | pozicio a csatornan (0=aljan, 1=tetején)"
    if m := _re.fullmatch(r"donchian_breakout_(\d+)", f):
        return f"close >= rolling_max_{m[1]}(H) => {m[1]}-bari Donchian kitorés (1/0)"

    # Gap
    if f == "gap_open":
        return "(open - prev_close) / prev_close | nyitogap merteke (relativ)"
    if m := _re.fullmatch(r"gap_open_abs_sma_(\d+)", f):
        return f"|gap_open| {m[1]}-bari gordulo atlaga | gap-volatilitas proxy"

    # Ichimoku
    if f == "tenkan_ratio":
        return "tenkan_sen / close - 1 | Tenkan-sen = (9-bar max + 9-bar min) / 2"
    if f == "kijun_ratio":
        return "kijun_sen / close - 1 | Kijun-sen = (26-bar max + 26-bar min) / 2"
    if f == "senkou_b_ratio":
        return "senkou_b / close - 1 | Senkou Span B = (52-bar max + 52-bar min) / 2"
    if f in ("tenkan_kijun_delta",) or f.startswith("tenkan_kijun_"):
        return "(tenkan - kijun) / close | TK-kulonbseg (cloud bias iranyjelzo)"
    if f.startswith("price_vs_tenkan"):
        return "(close - tenkan) / close | ar vs. Tenkan-sen pozicio"
    if f.startswith("price_vs_kijun"):
        return "(close - kijun) / close | ar vs. Kijun-sen pozicio"
    if f == "ichimoku_cloud_thickness":
        return "((tenkan+kijun)/2 - senkou_b) / close | felho vastagsaga (pozitiv = bullish cloud)"

    # Market Structure
    if m := _re.fullmatch(r"atr_dist_high_(\d+)", f):
        return f"(rolling_max_{m[1]}(H) - close) / ATR14 | tavolsag a {m[1]}-bari csucstol ATR-egysegben"
    if m := _re.fullmatch(r"atr_dist_low_(\d+)", f):
        return f"(close - rolling_min_{m[1]}(L)) / ATR14 | tavolsag a {m[1]}-bari melyponttol ATR-egysegben"
    if f == "prev_session_high_dist":
        return "(elozo_napi_max - close) / ATR14 | tavolsag az elozo napi csucstol"
    if f == "prev_session_low_dist":
        return "(close - elozo_napi_min) / ATR14 | tavolsag az elozo napi melyponttol"
    if m := _re.fullmatch(r"recovery_ratio_(\d+)", f):
        return f"(close - rolling_min_{m[1]}) / (rolling_max_{m[1]} - rolling_min_{m[1]}) | recovery arany {m[1]} barban"
    if m := _re.fullmatch(r"max_drawdown_(\d+)", f):
        return f"(rolling_max_{m[1]}(H) - rolling_min_{m[1]}(L)) / rolling_max_{m[1]} | range / csocs arany {m[1]} barban"
    if m := _re.fullmatch(r"time_since_high_(\d+)", f):
        return f"(W-1 - argmax(H, {m[1]})) / {m[1]} | normalizalt ido a {m[1]}-bari csocs ota (0=most, 1=legelején volt)"
    if m := _re.fullmatch(r"time_since_low_(\d+)", f):
        return f"(W-1 - argmin(L, {m[1]})) / {m[1]} | normalizalt ido a {m[1]}-bari melypont ota"
    if m := _re.fullmatch(r"rolling_drawdown_(\d+)", f):
        return f"close / rolling_max_{m[1]}(close) - 1 | aktualis drawdown a {m[1]}-bari csocstol (<=0)"
    if m := _re.fullmatch(r"dist_rolling_high_(\d+)", f):
        return f"close / rolling_max_{m[1]}(H) - 1 | ar tavolsaga a {m[1]}-bari high-tol (<=0)"
    if m := _re.fullmatch(r"dist_rolling_low_(\d+)", f):
        return f"close / rolling_min_{m[1]}(L) - 1 | ar tavolsaga a {m[1]}-bari low-tol (>=0)"
    if m := _re.fullmatch(r"range_expansion_(\d+)_(\d+)", f):
        return f"mean(H-L, {m[1]}) / mean(H-L, {m[2]}) | rovid/kozepes range arany — >1 = bovulo volatilitas"
    if m := _re.fullmatch(r"efficiency_ratio_(\d+)", f):
        return f"|close-close[-{m[1]}]| / sum(|ret1|, {m[1]}) | mennyire egyenes a mozgas (0=kaotikus, 1=tokeletes trend)"

    # Momentum
    if m := _re.fullmatch(r"rsi_(\d+)", f):
        w = int(m[1])
        return f"RSI-{w}: 100 - 100/(1 + avg_gain/avg_loss) | Wilder EWM, com={w-1}, min_samples={w}"
    if m := _re.fullmatch(r"roc_(\d+)", f):
        return f"ROC-{m[1]}: (close / close[-{m[1]}] - 1) * 100 | {m[1]}-bari ar-ROC"
    if m := _re.fullmatch(r"stoch_k_(\d+)", f):
        return f"Stochastic %K-{m[1]}: (close - LL_{m[1]}) / (HH_{m[1]} - LL_{m[1]}) * 100"
    if m := _re.fullmatch(r"stoch_d_(\d+)", f):
        return f"Stochastic %D-{m[1]}: %K_{m[1]} 3-bari SMA-ja (signal)"
    if m := _re.fullmatch(r"williams_r_(\d+)", f):
        return f"Williams %R-{m[1]}: -(HH_{m[1]} - close) / (HH_{m[1]} - LL_{m[1]}) * 100 | -100..0"
    if m := _re.fullmatch(r"cci_(\d+)", f):
        return f"CCI-{m[1]}: (TP - SMA(TP,{m[1]})) / (0.015 * MAD(TP,{m[1]})) | TP=(H+L+C)/3"
    if m := _re.fullmatch(r"adx_pos_(\d+)", f):
        return f"+DI-{m[1]}: 100 * EWM(+DM,{m[1]}) / EWM(TR,{m[1]}) | felfele iranyulo mozgas ereje"
    if m := _re.fullmatch(r"adx_neg_(\d+)", f):
        return f"-DI-{m[1]}: 100 * EWM(-DM,{m[1]}) / EWM(TR,{m[1]}) | lefele iranyulo mozgas ereje"
    if m := _re.fullmatch(r"adx_(\d+)", f):
        return f"ADX-{m[1]}: Wilder-EWM alapu DX simítasa | irányjelzett mozgáserő-index"

    # Price Action
    if f == "returns_log":
        return "ln(close / close[-1]) | egyperiodusos log-return"
    if m := _re.fullmatch(r"returns_sma_(\d+)", f):
        return f"log-return {m[1]}-bari gordulo atlaga | rovid trend proxy"
    if m := _re.fullmatch(r"returns_std_(\d+)", f):
        return f"log-return {m[1]}-bari gordulo szorasa (ddof=1) | lokalis volatilitas"
    if m := _re.fullmatch(r"returns_skew_(\d+)", f):
        return f"log-return {m[1]}-bari gordulo ferdesege | Fisher, bias-corrected"
    if m := _re.fullmatch(r"returns_kurt_(\d+)", f):
        return f"log-return {m[1]}-bari gordulo kurtosisa | Fisher, excess (>0 = vastag farok)"
    if f == "hml_range":
        return "(high - low) / close | egyetlen gyertya tartomanya close-szal normalizalva (nem gordulo ablak)"
    if f == "ohlc_range":
        return "(high - low) / ((open+close)/2) | egyetlen gyertya tartomanya az OHLC-atlaghoz normalizalva"
    if f == "close_position":
        return "(close - low) / (high - low) | zaras helyzete az egyetlen gyertya tartomanyaban (0=aljan, 1=csucsnal)"
    if m := _re.fullmatch(r"return_z_(\d+)", f):
        return f"ret1 / rolling_std(log_ret, {m[1]}) | return normalizalva {m[1]}-bari volatilitassal"
    if m := _re.fullmatch(r"vol_adj_return_(\d+)", f):
        return f"ret1 / rolling_std(log_ret, {m[1]}) | volatilitas-korrigalt return (Sharpe-proxy per bar)"
    if m := _re.fullmatch(r"return_(\d+)", f):
        return f"(close - close[-{m[1]}]) / close[-{m[1]}] | {m[1]}-bari %-os arvaltozas"

    # Swing
    if m := _re.fullmatch(r"higher_high_count_(\d+)", f):
        return f"high > prev_high esetek szama az utolso {m[1]} barban"
    if m := _re.fullmatch(r"higher_low_count_(\d+)", f):
        return f"low > prev_low esetek szama az utolso {m[1]} barban"
    if m := _re.fullmatch(r"lower_high_count_(\d+)", f):
        return f"high < prev_high esetek szama az utolso {m[1]} barban"
    if m := _re.fullmatch(r"lower_low_count_(\d+)", f):
        return f"low < prev_low esetek szama az utolso {m[1]} barban"
    if m := _re.fullmatch(r"swing_high_(\d+)", f):
        return f"high >= rolling_max(H, {m[1]}) => {m[1]}-bari swing high (1/0)"
    if m := _re.fullmatch(r"swing_low_(\d+)", f):
        return f"low <= rolling_min(L, {m[1]}) => {m[1]}-bari swing low (1/0)"
    if m := _re.fullmatch(r"directional_agreement_(\d+)_(\d+)", f):
        return f"sign(ret_{m[1]}) * sign(ret_{m[2]}) | {m[1]} es {m[2]} bari return iranyanak egyezese (+1/-1)"

    # Tail Risk
    if m := _re.fullmatch(r"pos_return_mean_(\d+)", f):
        return f"rolling_mean(max(ret,0), {m[1]}) | pozitiv hozamok {m[1]}-bari atlaga (upside intenzitas)"
    if m := _re.fullmatch(r"neg_return_mean_(\d+)", f):
        return f"rolling_mean(|min(ret,0)|, {m[1]}) | negativ hozamok abszolut {m[1]}-bari atlaga (downside)"
    if m := _re.fullmatch(r"return_asymmetry_(\d+)", f):
        return f"pos_return_mean_{m[1]} / neg_return_mean_{m[1]} | >1 = bullish aszimmetria, <1 = bearish"

    # Taker Flow
    if m := _re.fullmatch(r"taker_buy_base_ratio_(\d+)", f):
        return f"sum(taker_buy_base,{m[1]}) / sum(volume,{m[1]}) | taker vasarlasok aranya (0-1)"
    if m := _re.fullmatch(r"taker_buy_quote_ratio_(\d+)", f):
        return f"sum(taker_buy_quote,{m[1]}) / sum(quote_volume,{m[1]}) | taker buy USD-arany (0-1)"
    if m := _re.fullmatch(r"trade_count_ratio_(\d+)", f):
        return f"trades / rolling_mean(trades,{m[1]}) | kotesszam / {m[1]}-bari SMA (>1 = atlag felett)"
    if m := _re.fullmatch(r"avg_trade_quote_(\d+)", f):
        return f"sum(quote_volume,{m[1]}) / sum(trades,{m[1]}) | atlagos kotesmeret USD-ben ({m[1]} bar)"
    if m := _re.fullmatch(r"quote_volume_ratio_(\d+)", f):
        return f"quote_volume / rolling_mean(quote_volume,{m[1]}) | USD-forgalom / {m[1]}-bari SMA"
    if m := _re.fullmatch(r"trade_count_accel_(\d+)_(\d+)", f):
        return f"mean(trades,{m[1]}) / mean(trades,{m[2]}) | kotesszam gyorsulasa (rovid/kozepes SMA arany)"
    if m := _re.fullmatch(r"trade_count_rank_(\d+)", f):
        return f"percentile rank of trades over {m[1]} bars | 0=legkisebb, 1=legnagyobb (gordulo)"
    if m := _re.fullmatch(r"taker_flow_confirmed_return_(\d+)", f):
        return f"ret1 * taker_buy_ratio_{m[1]} | return * taker-buy arany (iranyjelzett flow-erositets)"

    # Time / Session
    if f == "hour_sin":
        return "sin(2*pi * ora / 24) | napszak kor-kodolasa (szinusz)"
    if f == "hour_cos":
        return "cos(2*pi * ora / 24) | napszak kor-kodolasa (koszinusz)"
    if f == "dayofweek_sin":
        return "sin(2*pi * napszam / 7) | het napjanak kor-kodolasa (szinusz)"
    if f == "dayofweek_cos":
        return "cos(2*pi * napszam / 7) | het napjanak kor-kodolasa (koszinusz)"
    if f == "weekend":
        return "weekday >= 5 => hetvege (1/0) | szombat=5, vasarnap=6"
    if f == "session_asia":
        return "0h <= UTC_ora < 8h => azs session (1/0)"
    if f == "session_europe":
        return "7h <= UTC_ora < 16h => europai session (1/0)"
    if f == "session_us":
        return "13h <= UTC_ora < 22h => US session (1/0)"
    if f == "bars_into_session_norm":
        return "cumcount_within_day / 1440, klippelve 0-1 | a nap hanyadán vagyunk"
    if f == "day_range_position":
        return "(close - exp_low) / (exp_high - exp_low) | ar helyzete a nap kumulalt csocs-melypont tartomanyan"
    if f == "day_open_return":
        return "(close - aznapi_elso_open) / aznapi_elso_open | return az aznapi nyitoartol"
    if f == "weekly_open_return":
        return "(close - hetfoi_open) / hetfoi_open | return a heti nyitoartol"

    # Trend
    if m := _re.fullmatch(r"sma_ratio_(\d+)", f):
        return f"close / SMA_{m[1]} | ar / {m[1]}-bari egyszeru mozgoatlag (1 = pontosan SMA-n)"
    if m := _re.fullmatch(r"ema_ratio_(\d+)", f):
        return f"close / EMA_{m[1]} | ar / {m[1]}-bari EMA (span={m[1]}, adjust=False)"
    if m := _re.fullmatch(r"wma_ratio_(\d+)", f):
        return f"close / WMA_{m[1]} | ar / {m[1]}-bari linearis sulyú WMA (sulyok: 1..{m[1]})"
    if m := _re.fullmatch(r"kama_ratio_(\d+)_(\d+)_(\d+)", f):
        return f"close / KAMA(W={m[1]},fast={m[2]},slow={m[3]}) | Kaufman Adaptive MA — ER alapu adaptiv EMA"
    if m := _re.fullmatch(r"macd_(\d+)_(\d+)", f):
        return f"EMA_{m[1]} - EMA_{m[2]} | MACD vonal (fast={m[1]}, slow={m[2]})"
    if m := _re.fullmatch(r"macd_signal_(\d+)_(\d+)_(\d+)", f):
        return f"EWM(MACD, span={m[3]}) | MACD signal vonal ({m[1]}/{m[2]}/{m[3]})"
    if f == "macd_diff":
        return "MACD - signal | MACD hisztogram (ero es iranyjelzo)"

    # Trend Slope
    if m := _re.fullmatch(r"ema_slope_(\d+)", f):
        return f"(EMA_{m[1]}(t) - EMA_{m[1]}(t-1)) / close | EMA {m[1]} pillanatnyi meredeksege (close-normalizalva)"
    if m := _re.fullmatch(r"lr_slope_(\d+)", f):
        return f"OLS_slope_{m[1]} / close | {m[1]}-bari linearis regresszio meredeksege, close-normalizalva"
    if m := _re.fullmatch(r"lr_r2_(\d+)", f):
        return f"OLS R2 {m[1]}-barra | illeszkedés minősége (0=semmi, 1=tokéletes linearis trend)"
    if m := _re.fullmatch(r"lr_residual_(\d+)", f):
        return f"(close - OLS_hat_{m[1]}) / close | ar eltérese a {m[1]}-bari regressziós egyenestol"

    # Volatility
    if m := _re.fullmatch(r"bb_width_(\d+)", f):
        return f"(BB_upper - BB_lower) / close | Bollinger-sav szelessege ({m[1]}-bar SMA +/- 2*sigma)"
    if m := _re.fullmatch(r"bb_position_(\d+)", f):
        return f"(close - BB_lower_{m[1]}) / (BB_upper_{m[1]} - BB_lower_{m[1]}) | pozicio a savon belul (0=alap, 1=teteje)"
    if m := _re.fullmatch(r"atr_(\d+)", f):
        return f"ATR-{m[1]}: Wilder EWM alapu True Range atlag | TR=max(H-L, |H-C[-1]|, |L-C[-1]|)"
    if m := _re.fullmatch(r"natr_(\d+)", f):
        return f"NATR-{m[1]}: ATR_{m[1]} / close | normalizalt ATR (osszehasonlithato idosoron at)"
    if m := _re.fullmatch(r"hist_vol_(\d+)", f):
        return f"rolling_std(log_ret, {m[1]}, ddof=1) | historikus volatilitas {m[1]} barra"
    if m := _re.fullmatch(r"parkinson_vol_(\d+)", f):
        return f"sqrt(mean((ln(H/L))^2 / (4*ln2), {m[1]})) | Parkinson range-alapu vol-becslő"
    if m := _re.fullmatch(r"gk_vol_(\d+)", f):
        return f"sqrt(mean(0.5*(ln(H/L))^2 - (2*ln2-1)*(ln(C/O))^2, {m[1]})) | Garman-Klass OHLC vol-becslő"

    # Volume
    if m := _re.fullmatch(r"volume_sma_(\d+)", f):
        return f"rolling_mean(volume, {m[1]}) | volume {m[1]}-bari SMA-ja (abszolut)"
    if m := _re.fullmatch(r"volume_ratio_(\d+)", f):
        return f"volume / rolling_mean(volume,{m[1]}) | aktualis volume / {m[1]}-bari SMA (>1 = atlag felett)"
    if f == "obv":
        return "sum(sign(delta_close) * volume) | On-Balance Volume: kumulalt iranyjelzett forgalom"
    if m := _re.fullmatch(r"obv_roc_(\d+)", f):
        return f"OBV.pct_change({m[1]}) | OBV {m[1]}-bari Rate of Change"
    if m := _re.fullmatch(r"mfi_(\d+)", f):
        return f"MFI-{m[1]}: 100-100/(1+pos_MF/neg_MF) | TP*V alapu RSI-analog (0-100)"
    if f == "ad_line":
        return "sum(CLV*V) | A/D Line — felhalmozas/elosztás, CLV=((C-L)-(H-C))/(H-L)"
    if m := _re.fullmatch(r"cmf_(\d+)", f):
        return f"sum(CLV*V,{m[1]}) / sum(V,{m[1]}) | Chaikin Money Flow (-1..+1)"
    if m := _re.fullmatch(r"volume_confirmed_return_(\d+)", f):
        return f"ret1 * volume_rank_{m[1]} | return * volume percentile rank (volume-erositett hozam)"
    if m := _re.fullmatch(r"volume_accel_(\d+)_(\d+)", f):
        return f"mean(volume,{m[1]}) / mean(volume,{m[2]}) | volume gyorsulasa: rovid/kozepes SMA arany"
    if m := _re.fullmatch(r"volume_rank_(\d+)", f):
        return f"percentile rank of volume over {m[1]} bars | 0=legkisebb, 1=legnagyobb"

    return ""


def feature_description(feat: str) -> str:
    """One-line formula + description of a feature (empty string if unknown)."""
    return _feat_desc(feat)
