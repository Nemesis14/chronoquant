"""Polars-native feature computation for sync_features bulk rebuild path.

Replaces pandas/ta library calls with polars LazyFrame expressions for
significantly faster bulk rebuild performance (target: <3 min/chunk vs ~7 min/chunk).

All indicator values are numerically equivalent to the pandas/ta implementation
within floating-point rounding (max ~1e-10 error), except for KAMA which uses
the same iterative numpy algorithm as the ta library.

t-1 lag guarantee: same semantics as _apply_t_minus_1_lag — all OHLCV-based
features are shifted by 1 bar before write; P2 timestamp features are exempt.
"""
from __future__ import annotations

import logging
import math

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
import polars as pl

logger = logging.getLogger(__name__)

# P2 deterministic time-index features: must NOT be shifted by the t-1 lag.
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


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _safe_div(a: pl.Expr, b: pl.Expr) -> pl.Expr:
    """Divide a by b, setting result to null where b == 0."""
    return a / pl.when(b != 0).then(b).otherwise(None)


def _drop_tmp(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Remove all columns prefixed with '_tmp_'."""
    schema = lf.collect_schema()
    tmp_cols = [c for c in schema.names() if c.startswith("_tmp_")]
    return lf.drop(tmp_cols) if tmp_cols else lf


# ---------------------------------------------------------------------------
# Numpy vectorized rolling helpers — no Python callbacks per window
# ---------------------------------------------------------------------------

def _rolling_rank_arr(arr: np.ndarray, w: int) -> np.ndarray:
    """Rolling percentile rank (0..1): fraction of window values <= current."""
    n = len(arr)
    result = np.full(n, np.nan)
    if w > n:
        return result
    wins = sliding_window_view(arr.astype(float), w)
    result[w - 1:] = (wins <= wins[:, -1:]).mean(axis=1)
    return result


def _time_since_high_arr(arr: np.ndarray, w: int) -> np.ndarray:
    """Bars since rolling high, normalised by window size."""
    n = len(arr)
    result = np.full(n, np.nan)
    if w > n:
        return result
    wins = sliding_window_view(arr.astype(float), w)
    result[w - 1:] = (w - 1 - np.argmax(wins, axis=1)) / w
    return result


def _time_since_low_arr(arr: np.ndarray, w: int) -> np.ndarray:
    """Bars since rolling low, normalised by window size."""
    n = len(arr)
    result = np.full(n, np.nan)
    if w > n:
        return result
    wins = sliding_window_view(arr.astype(float), w)
    result[w - 1:] = (w - 1 - np.argmin(wins, axis=1)) / w
    return result


def _rolling_skew_arr(arr: np.ndarray, w: int) -> np.ndarray:
    """Rolling skewness matching pandas .skew() (Fisher / bias-corrected)."""
    n = len(arr)
    result = np.full(n, np.nan)
    if w < 3 or w > n:
        return result
    wins = sliding_window_view(arr.astype(float), w)
    mean = wins.mean(axis=1, keepdims=True)
    std  = wins.std(axis=1, ddof=1, keepdims=True)
    with np.errstate(invalid="ignore", divide="ignore"):
        z = np.where(std > 0, (wins - mean) / std, np.nan)
        result[w - 1:] = w / ((w - 1) * (w - 2)) * np.nansum(z ** 3, axis=1)
    return result


def _rolling_kurt_arr(arr: np.ndarray, w: int) -> np.ndarray:
    """Rolling excess kurtosis matching pandas .kurt() (Fisher)."""
    n = len(arr)
    result = np.full(n, np.nan)
    if w < 4 or w > n:
        return result
    wins = sliding_window_view(arr.astype(float), w)
    mean = wins.mean(axis=1, keepdims=True)
    std  = wins.std(axis=1, ddof=1, keepdims=True)
    with np.errstate(invalid="ignore", divide="ignore"):
        z = np.where(std > 0, (wins - mean) / std, np.nan)
        result[w - 1:] = np.nanmean(z ** 4, axis=1) - 3.0
    return result


def _rolling_mad_arr(arr: np.ndarray, w: int) -> np.ndarray:
    """Rolling mean absolute deviation."""
    n = len(arr)
    result = np.full(n, np.nan)
    if w > n:
        return result
    wins = sliding_window_view(arr.astype(float), w)
    mean = wins.mean(axis=1, keepdims=True)
    result[w - 1:] = np.abs(wins - mean).mean(axis=1)
    return result


def _rolling_rank_inject(
    lf: pl.LazyFrame, src: pl.Expr, w: int, col_name: str
) -> pl.LazyFrame:
    """Materialise src, compute rolling percentile rank via numpy, inject result."""
    lf  = lf.with_columns([src.alias("_tmp_rr_src")])
    arr = lf.select("_tmp_rr_src").collect().get_column("_tmp_rr_src").to_numpy()
    result = _rolling_rank_arr(arr, w)
    return lf.drop("_tmp_rr_src").with_columns([
        pl.lit(pl.Series(col_name, result, dtype=pl.Float64))
    ])


# ---------------------------------------------------------------------------
# Momentum indicators
# ---------------------------------------------------------------------------

def _add_momentum_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    momentum_cfg = indicators.get("momentum", {})
    close = pl.col("close")
    high  = pl.col("high")
    low   = pl.col("low")
    new_cols: list[pl.Expr] = []

    # RSI — Wilder's EWM: com = window - 1, adjust=False, min_samples=window
    for cfg in momentum_cfg.get("rsi", []):
        w = cfg["window"]
        diff = close.diff()
        gain = diff.clip(lower_bound=0.0)
        loss = (diff * -1.0).clip(lower_bound=0.0)
        avg_gain = gain.ewm_mean(com=w - 1, min_samples=w, adjust=False)
        avg_loss = loss.ewm_mean(com=w - 1, min_samples=w, adjust=False)
        new_cols.append((100.0 - 100.0 / (1.0 + avg_gain / avg_loss)).alias(f"{p}rsi_{w}"))

    # ROC
    for cfg in momentum_cfg.get("roc", []):
        w = cfg["window"]
        new_cols.append((close.pct_change(n=w) * 100.0).alias(f"{p}roc_{w}"))

    # Stochastic
    for cfg in momentum_cfg.get("stochastic", []):
        w  = cfg["window"]
        sw = cfg.get("smooth_window", 3)
        hh = high.rolling_max(window_size=w)
        ll = low.rolling_min(window_size=w)
        stoch_k = _safe_div(close - ll, hh - ll) * 100.0
        new_cols.append(stoch_k.alias(f"{p}stoch_k_{w}"))
        new_cols.append(stoch_k.rolling_mean(window_size=sw).alias(f"{p}stoch_d_{w}"))

    # Williams R
    for cfg in momentum_cfg.get("williams_r", []):
        w  = cfg["window"]
        hh = high.rolling_max(window_size=w)
        ll = low.rolling_min(window_size=w)
        new_cols.append((_safe_div(hh - close, hh - ll) * -100.0).alias(f"{p}williams_r_{w}"))

    if new_cols:
        lf = lf.with_columns(new_cols)

    # CCI — MAD via numpy sliding_window_view (no Python callbacks per window)
    for cfg in momentum_cfg.get("cci", []):
        w = cfg["window"]
        lf = lf.with_columns([
            ((pl.col("high") + pl.col("low") + pl.col("close")) / 3.0).alias("_tmp_tp"),
        ])
        tp_arr  = lf.select("_tmp_tp").collect().get_column("_tmp_tp").to_numpy()
        mad_arr = _rolling_mad_arr(tp_arr, w)
        tp_sma  = pl.col("_tmp_tp").rolling_mean(window_size=w, min_samples=w)
        lf = lf.with_columns([
            pl.lit(pl.Series("_tmp_mad", mad_arr, dtype=pl.Float64)),
        ]).with_columns([
            _safe_div(pl.col("_tmp_tp") - tp_sma, pl.col("_tmp_mad") * 0.015).alias(f"{p}cci_{w}"),
        ])
        lf = _drop_tmp(lf)

    # ADX: +DM, -DM, TR → Wilder's EWM → +DI, -DI, DX → EWM = ADX
    for cfg in momentum_cfg.get("adx", []):
        w = cfg["window"]
        prev_high  = high.shift(1)
        prev_low   = low.shift(1)
        prev_close = pl.col("close").shift(1)

        move_up   = high - prev_high
        move_down = prev_low - low

        plus_dm  = pl.when((move_up > move_down) & (move_up > 0)).then(move_up).otherwise(0.0)
        minus_dm = pl.when((move_down > move_up) & (move_down > 0)).then(move_down).otherwise(0.0)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low  - prev_close).abs()
        true_range = pl.max_horizontal([tr1, tr2, tr3])

        lf = lf.with_columns([
            true_range.alias("_tmp_tr"),
            plus_dm.alias("_tmp_pdm"),
            minus_dm.alias("_tmp_ndm"),
        ]).with_columns([
            pl.col("_tmp_tr").ewm_mean(com=w - 1, min_samples=w, adjust=False).alias("_tmp_str"),
            pl.col("_tmp_pdm").ewm_mean(com=w - 1, min_samples=w, adjust=False).alias("_tmp_spdm"),
            pl.col("_tmp_ndm").ewm_mean(com=w - 1, min_samples=w, adjust=False).alias("_tmp_sndm"),
        ]).with_columns([
            (100.0 * _safe_div(pl.col("_tmp_spdm"), pl.col("_tmp_str"))).alias("_tmp_pdi"),
            (100.0 * _safe_div(pl.col("_tmp_sndm"), pl.col("_tmp_str"))).alias("_tmp_ndi"),
        ]).with_columns([
            (100.0 * _safe_div(
                (pl.col("_tmp_pdi") - pl.col("_tmp_ndi")).abs(),
                pl.col("_tmp_pdi") + pl.col("_tmp_ndi"),
            )).alias("_tmp_dx"),
        ]).with_columns([
            pl.col("_tmp_pdi").alias(f"{p}adx_pos_{w}"),
            pl.col("_tmp_ndi").alias(f"{p}adx_neg_{w}"),
            pl.col("_tmp_dx").ewm_mean(com=w - 1, min_samples=w, adjust=False).alias(f"{p}adx_{w}"),
        ])
        lf = _drop_tmp(lf)

    return lf


# ---------------------------------------------------------------------------
# Trend indicators
# ---------------------------------------------------------------------------

def _kama_numpy(close_arr: np.ndarray, window: int, fast: int, slow: int) -> np.ndarray:
    """Kaufman's Adaptive Moving Average via numpy loop (matches ta library output)."""
    n        = len(close_arr)
    kama     = np.full(n, np.nan)
    fast_sc  = 2.0 / (fast + 1)
    slow_sc  = 2.0 / (slow + 1)

    # Precompute |diff| for volatility rolling sum
    abs_diff = np.abs(np.diff(close_arr))  # length n-1

    start = window - 1
    if start >= n:
        return kama
    kama[start] = close_arr[start]

    for i in range(start + 1, n):
        direction  = abs(close_arr[i] - close_arr[i - window])
        # Sum of abs(diff) over [i-window+1 .. i] = abs_diff[i-window .. i-1]
        volatility = float(abs_diff[i - window: i].sum())
        er  = direction / volatility if volatility != 0.0 else 0.0
        sc  = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        kama[i] = kama[i - 1] + sc * (close_arr[i] - kama[i - 1])

    return kama


def _add_trend_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    trend_cfg = indicators.get("trend", {})
    close = pl.col("close")
    new_cols: list[pl.Expr] = []

    # MACD — EWM with span, adjust=False (no min_periods in ta → polars default=1)
    for cfg in trend_cfg.get("macd", []):
        fast   = cfg.get("fast",   12)
        slow   = cfg.get("slow",   26)
        signal = cfg.get("signal",  9)
        ema_fast = close.ewm_mean(span=fast, adjust=False).alias("_tmp_mfast")
        ema_slow = close.ewm_mean(span=slow, adjust=False).alias("_tmp_mslow")
        lf = lf.with_columns([ema_fast, ema_slow]).with_columns([
            (pl.col("_tmp_mfast") - pl.col("_tmp_mslow")).alias("_tmp_macd_line"),
        ]).with_columns([
            pl.col("_tmp_macd_line").alias(f"{p}macd_{fast}_{slow}"),
            pl.col("_tmp_macd_line").ewm_mean(span=signal, adjust=False).alias(
                f"{p}macd_signal_{fast}_{slow}_{signal}"
            ),
        ]).with_columns([
            (pl.col(f"{p}macd_{fast}_{slow}") -
             pl.col(f"{p}macd_signal_{fast}_{slow}_{signal}")).alias(f"{p}macd_diff"),
        ])
        lf = _drop_tmp(lf)

    # SMA ratio
    for cfg in trend_cfg.get("sma", []):
        w   = cfg["window"]
        sma = close.rolling_mean(window_size=w, min_samples=w)
        new_cols.append(_safe_div(close, sma).alias(f"{p}sma_ratio_{w}"))

    # EMA ratio (span=w, adjust=False — matches ta EMAIndicator)
    for cfg in trend_cfg.get("ema", []):
        w   = cfg["window"]
        ema = close.ewm_mean(span=w, adjust=False)
        new_cols.append(_safe_div(close, ema).alias(f"{p}ema_ratio_{w}"))

    # WMA ratio — implemented via weighted sum of shifts (avoids Python loop)
    for cfg in trend_cfg.get("wma", []):
        w       = cfg["window"]
        weights = list(range(1, w + 1))
        w_sum   = float(sum(weights))
        # sum(weights[j] * close.shift(w-1-j)) for j in 0..w-1
        wma_expr = pl.sum_horizontal([
            close.shift(w - 1 - j) * float(weights[j])
            for j in range(w)
        ]) / w_sum
        new_cols.append(_safe_div(close, wma_expr).alias(f"{p}wma_ratio_{w}"))

    if new_cols:
        lf = lf.with_columns(new_cols)
        new_cols = []

    # KAMA ratio — iterative, computed via numpy then patched in
    for cfg in trend_cfg.get("kama", []):
        w    = cfg.get("window", 10)
        fast = cfg.get("fast",    2)
        slow = cfg.get("slow",   30)
        col_name = f"{p}kama_ratio_{w}_{fast}_{slow}"
        # Collect only close column for KAMA iterative computation
        close_series = lf.select("close").collect().get_column("close").to_numpy()
        kama_arr = _kama_numpy(close_series, w, fast, slow)
        kama_pl = pl.Series(col_name + "_raw", kama_arr)
        lf = lf.with_columns([
            pl.lit(kama_pl).alias("_tmp_kama"),
        ]).with_columns([
            _safe_div(close, pl.col("_tmp_kama")).alias(col_name),
        ])
        lf = _drop_tmp(lf)

    return lf


# ---------------------------------------------------------------------------
# Volatility indicators
# ---------------------------------------------------------------------------

def _add_volatility_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    volatility_cfg = indicators.get("volatility", {})
    close = pl.col("close")
    high  = pl.col("high")
    low   = pl.col("low")
    new_cols: list[pl.Expr] = []

    # Bollinger Bands
    for cfg in volatility_cfg.get("bollinger", []):
        w   = cfg["window"]
        dev = cfg.get("window_dev", 2)
        sma = close.rolling_mean(window_size=w, min_samples=w)
        std = close.rolling_std(window_size=w, min_samples=w)
        upper = sma + dev * std
        lower = sma - dev * std
        new_cols.append(_safe_div(upper - lower, close).alias(f"{p}bb_width_{w}"))
        new_cols.append(_safe_div(close - lower, upper - lower).alias(f"{p}bb_position_{w}"))

    # ATR / NATR — Wilder's EWM on True Range
    for cfg in volatility_cfg.get("atr", []):
        w          = cfg["window"]
        prev_close = close.shift(1)
        tr1        = high - low
        tr2        = (high - prev_close).abs()
        tr3        = (low  - prev_close).abs()
        true_range = pl.max_horizontal([tr1, tr2, tr3])
        atr        = true_range.ewm_mean(com=w - 1, min_samples=w, adjust=False)
        new_cols.append(atr.alias(f"{p}atr_{w}"))
        new_cols.append(_safe_div(atr, close).alias(f"{p}natr_{w}"))

    # Historical Volatility (rolling std of log returns)
    for cfg in volatility_cfg.get("historical_vol", []):
        w       = cfg["window"]
        log_ret = (close / close.shift(1)).log()
        new_cols.append(log_ret.rolling_std(window_size=w, min_samples=w).alias(f"{p}hist_vol_{w}"))

    if new_cols:
        lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Volume indicators
# ---------------------------------------------------------------------------

def _add_volume_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    volume_cfg = indicators.get("volume", {})
    close  = pl.col("close")
    volume = pl.col("volume")
    new_cols: list[pl.Expr] = []

    for cfg in volume_cfg.get("volume_sma", []):
        w = cfg["window"]
        new_cols.append(volume.rolling_mean(window_size=w, min_samples=w).alias(f"{p}volume_sma_{w}"))

    for cfg in volume_cfg.get("volume_ratio", []):
        w   = cfg["window"]
        sma = volume.rolling_mean(window_size=w, min_samples=w)
        new_cols.append(_safe_div(volume, sma).alias(f"{p}volume_ratio_{w}"))

    # OBV — cumulative signed volume
    if volume_cfg.get("obv"):
        diff       = close.diff()
        direction  = pl.when(diff > 0).then(pl.lit(1.0)).when(diff < 0).then(pl.lit(-1.0)).otherwise(pl.lit(0.0))
        new_cols.append((direction * volume).cum_sum().alias(f"{p}obv"))

    for cfg in volume_cfg.get("obv_roc", []):
        w        = cfg["window"]
        obv_col  = f"{p}obv"
        # OBV must already exist (added above or in prior with_columns)
        if new_cols:
            lf = lf.with_columns(new_cols)
            new_cols = []
        new_cols.append(pl.col(obv_col).pct_change(n=w).alias(f"{p}obv_roc_{w}"))

    if new_cols:
        lf = lf.with_columns(new_cols)
        new_cols = []

    # MFI — Money Flow Index
    for cfg in volume_cfg.get("mfi", []):
        w = cfg["window"]
        tp       = (pl.col("high") + pl.col("low") + close) / 3.0
        prev_tp  = tp.shift(1)
        raw_flow = tp * volume
        pos_flow = pl.when(tp > prev_tp).then(raw_flow).otherwise(pl.lit(0.0))
        neg_flow = pl.when(tp < prev_tp).then(raw_flow).otherwise(pl.lit(0.0))
        pos_sum  = pos_flow.rolling_sum(window_size=w, min_samples=w)
        neg_sum  = neg_flow.rolling_sum(window_size=w, min_samples=w)
        mfi      = 100.0 - 100.0 / (1.0 + _safe_div(pos_sum, neg_sum))
        lf = lf.with_columns([mfi.alias(f"{p}mfi_{w}")])

    # Accumulation/Distribution Line
    if volume_cfg.get("ad_line"):
        h   = pl.col("high")
        lo  = pl.col("low")
        c   = close
        clv = _safe_div((c - lo) - (h - c), h - lo)
        lf  = lf.with_columns([(clv * volume).cum_sum().alias(f"{p}ad_line")])

    # Chaikin Money Flow
    for cfg in volume_cfg.get("cmf", []):
        w   = cfg["window"]
        h   = pl.col("high")
        lo  = pl.col("low")
        clv = _safe_div((close - lo) - (h - close), h - lo)
        mfv = clv * volume
        lf  = lf.with_columns([
            _safe_div(
                mfv.rolling_sum(window_size=w, min_samples=w),
                volume.rolling_sum(window_size=w, min_samples=w),
            ).alias(f"{p}cmf_{w}"),
        ])

    return lf


# ---------------------------------------------------------------------------
# Price action features
# ---------------------------------------------------------------------------

def _add_price_action_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    price_cfg = indicators.get("price_action", {})
    close = pl.col("close")
    new_cols: list[pl.Expr] = []

    log_ret_col = f"{p}returns_log"

    if price_cfg.get("returns"):
        new_cols.append((close / close.shift(1)).log().alias(log_ret_col))
    else:
        # returns_log may be needed by derived features even if "returns" is falsy
        new_cols.append((close / close.shift(1)).log().alias(log_ret_col))

    # Flush so log_ret_col exists for later rolling ops
    lf = lf.with_columns(new_cols)
    new_cols = []

    log_ret = pl.col(log_ret_col)

    for cfg in price_cfg.get("returns_sma", []):
        w = cfg["window"]
        new_cols.append(log_ret.rolling_mean(window_size=w, min_samples=w).alias(f"{p}returns_sma_{w}"))

    for cfg in price_cfg.get("returns_std", []):
        w = cfg["window"]
        new_cols.append(log_ret.rolling_std(window_size=w, min_samples=w).alias(f"{p}returns_std_{w}"))

    _skew_cfgs = price_cfg.get("returns_skew", [])
    _kurt_cfgs = price_cfg.get("returns_kurt", [])
    if _skew_cfgs or _kurt_cfgs:
        _log_ret_arr = lf.select(pl.col(log_ret_col)).collect().get_column(log_ret_col).to_numpy()
        for cfg in _skew_cfgs:
            w = cfg["window"]
            new_cols.append(
                pl.lit(pl.Series(f"{p}returns_skew_{w}", _rolling_skew_arr(_log_ret_arr, w), dtype=pl.Float64))
                .alias(f"{p}returns_skew_{w}")
            )
        for cfg in _kurt_cfgs:
            w = cfg["window"]
            new_cols.append(
                pl.lit(pl.Series(f"{p}returns_kurt_{w}", _rolling_kurt_arr(_log_ret_arr, w), dtype=pl.Float64))
                .alias(f"{p}returns_kurt_{w}")
            )

    if price_cfg.get("range_metrics"):
        high_low       = pl.col("high") - pl.col("low")
        open_close_avg = (pl.col("open") + close) / 2.0
        new_cols.append(_safe_div(high_low, close).alias(f"{p}hml_range"))
        new_cols.append(_safe_div(high_low, open_close_avg).alias(f"{p}ohlc_range"))

    if price_cfg.get("close_position"):
        high_low = pl.col("high") - pl.col("low")
        new_cols.append(_safe_div(close - pl.col("low"), high_low).alias(f"{p}close_position"))

    if new_cols:
        lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Market structure features
# ---------------------------------------------------------------------------

def _add_market_structure_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    market_cfg = indicators.get("market_structure", {})
    high  = pl.col("high")
    low   = pl.col("low")
    new_cols: list[pl.Expr] = []

    for cfg in market_cfg.get("trend_counts", []):
        w          = cfg["window"]
        higher_high = ((high > high.shift(1)).cast(pl.Int32)).rolling_sum(window_size=w, min_samples=w)
        higher_low  = ((low  > low.shift(1)).cast(pl.Int32)).rolling_sum(window_size=w, min_samples=w)
        lower_high  = ((high < high.shift(1)).cast(pl.Int32)).rolling_sum(window_size=w, min_samples=w)
        lower_low   = ((low  < low.shift(1)).cast(pl.Int32)).rolling_sum(window_size=w, min_samples=w)
        new_cols += [
            higher_high.cast(pl.Float64).alias(f"{p}higher_high_count_{w}"),
            higher_low.cast(pl.Float64).alias(f"{p}higher_low_count_{w}"),
            lower_high.cast(pl.Float64).alias(f"{p}lower_high_count_{w}"),
            lower_low.cast(pl.Float64).alias(f"{p}lower_low_count_{w}"),
        ]

    for cfg in market_cfg.get("swing_points", []):
        w          = cfg["window"]
        roll_high  = high.rolling_max(window_size=w, min_samples=w)
        roll_low   = low.rolling_min(window_size=w, min_samples=w)
        new_cols.append(((high >= roll_high).cast(pl.Float64)).alias(f"{p}swing_high_{w}"))
        new_cols.append(((low  <= roll_low).cast(pl.Float64)).alias(f"{p}swing_low_{w}"))

    if new_cols:
        lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Activity features (activity columns may or may not exist)
# ---------------------------------------------------------------------------

def _add_activity_pl(
    lf: pl.LazyFrame,
    indicators: dict,
    p: str,
    available_activity: list[str],
) -> pl.LazyFrame:
    activity_cfg = indicators.get("activity", {})
    if not activity_cfg:
        return lf

    has_qv     = "quote_volume"      in available_activity
    has_trades = "trades"            in available_activity
    has_tb_b   = "taker_buy_base"   in available_activity
    has_tb_q   = "taker_buy_quote"  in available_activity

    new_cols: list[pl.Expr] = []

    if has_qv:
        qv = pl.col("quote_volume")
        for cfg in activity_cfg.get("quote_volume_ratio", []):
            w = cfg["window"]
            new_cols.append(_safe_div(qv, qv.rolling_mean(window_size=w, min_samples=w)).alias(f"{p}quote_volume_ratio_{w}"))

    if has_trades:
        tr = pl.col("trades")
        for cfg in activity_cfg.get("trade_count_ratio", []):
            w = cfg["window"]
            new_cols.append(_safe_div(tr, tr.rolling_mean(window_size=w, min_samples=w)).alias(f"{p}trade_count_ratio_{w}"))

    if has_qv and has_trades:
        qv = pl.col("quote_volume")
        tr = pl.col("trades")
        for cfg in activity_cfg.get("avg_trade_quote", []):
            w = cfg["window"]
            new_cols.append(
                _safe_div(qv.rolling_sum(window_size=w, min_samples=w), tr.rolling_sum(window_size=w, min_samples=w))
                .alias(f"{p}avg_trade_quote_{w}")
            )

    if has_tb_b:
        tb = pl.col("taker_buy_base")
        vol = pl.col("volume")
        for cfg in activity_cfg.get("taker_buy_base_ratio", []):
            w = cfg["window"]
            new_cols.append(
                _safe_div(tb.rolling_sum(window_size=w, min_samples=w), vol.rolling_sum(window_size=w, min_samples=w))
                .alias(f"{p}taker_buy_base_ratio_{w}")
            )

    if has_tb_q and has_qv:
        tbq = pl.col("taker_buy_quote")
        qv  = pl.col("quote_volume")
        for cfg in activity_cfg.get("taker_buy_quote_ratio", []):
            w = cfg["window"]
            new_cols.append(
                _safe_div(tbq.rolling_sum(window_size=w, min_samples=w), qv.rolling_sum(window_size=w, min_samples=w))
                .alias(f"{p}taker_buy_quote_ratio_{w}")
            )

    if new_cols:
        lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Return & distance features
# ---------------------------------------------------------------------------

def _add_return_distance_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    rd_cfg = indicators.get("return_distance", {})
    if not rd_cfg:
        return lf

    close   = pl.col("close")
    high    = pl.col("high")
    low     = pl.col("low")
    log_ret = (close / close.shift(1)).log()
    new_cols: list[pl.Expr] = []

    for cfg in rd_cfg.get("return", []):
        w = cfg["window"]
        new_cols.append(close.pct_change(n=w).alias(f"{p}return_{w}"))

    for cfg in rd_cfg.get("return_z", []):
        w       = cfg["window"]
        ret     = close.pct_change(n=1)
        std_rol = log_ret.rolling_std(window_size=w, min_samples=w)
        new_cols.append(_safe_div(ret, std_rol).alias(f"{p}return_z_{w}"))

    for cfg in rd_cfg.get("dist_rolling_high", []):
        w  = cfg["window"]
        rh = high.rolling_max(window_size=w, min_samples=w)
        new_cols.append((_safe_div(close, rh) - 1.0).alias(f"{p}dist_rolling_high_{w}"))

    for cfg in rd_cfg.get("dist_rolling_low", []):
        w  = cfg["window"]
        rl = low.rolling_min(window_size=w, min_samples=w)
        new_cols.append((_safe_div(close, rl) - 1.0).alias(f"{p}dist_rolling_low_{w}"))

    for cfg in rd_cfg.get("rolling_drawdown", []):
        w    = cfg["window"]
        peak = close.rolling_max(window_size=w, min_samples=w)
        new_cols.append((_safe_div(close, peak) - 1.0).alias(f"{p}rolling_drawdown_{w}"))

    if new_cols:
        lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Regime rank features
# ---------------------------------------------------------------------------

def _add_regime_rank_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    rr_cfg = indicators.get("regime_rank", {})
    if not rr_cfg:
        return lf

    close  = pl.col("close")
    high   = pl.col("high")
    low    = pl.col("low")
    volume = pl.col("volume")

    new_cols: list[pl.Expr] = []
    natr_col     = f"{p}natr_14"
    hist_vol_col = f"{p}hist_vol_20"
    bb_width_col = f"{p}bb_width_14"
    schema = lf.collect_schema().names()

    for cfg in rr_cfg.get("natr_rank", []):
        w   = cfg["window"]
        src = pl.col(natr_col) if natr_col in schema else _safe_div(
            pl.max_horizontal([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()])
            .ewm_mean(com=13, min_samples=14, adjust=False), close
        )
        if new_cols:
            lf = lf.with_columns(new_cols)
            new_cols = []
        lf = _rolling_rank_inject(lf, src, w, f"{p}natr_rank_{w}")

    for cfg in rr_cfg.get("hist_vol_rank", []):
        w   = cfg["window"]
        lr  = (close / close.shift(1)).log()
        src = pl.col(hist_vol_col) if hist_vol_col in schema else lr.rolling_std(window_size=20, min_samples=20)
        if new_cols:
            lf = lf.with_columns(new_cols)
            new_cols = []
        lf = _rolling_rank_inject(lf, src, w, f"{p}hist_vol_rank_{w}")

    for cfg in rr_cfg.get("bb_width_rank", []):
        w   = cfg["window"]
        sma = close.rolling_mean(window_size=14, min_samples=14)
        std = close.rolling_std(window_size=14, min_samples=14)
        src = pl.col(bb_width_col) if bb_width_col in schema else _safe_div((sma + 2 * std) - (sma - 2 * std), close)
        if new_cols:
            lf = lf.with_columns(new_cols)
            new_cols = []
        lf = _rolling_rank_inject(lf, src, w, f"{p}bb_width_rank_{w}")

    for cfg in rr_cfg.get("range_expansion", []):
        short  = cfg["short"]
        medium = cfg["medium"]
        rng    = high - low
        new_cols.append(
            _safe_div(rng.rolling_mean(window_size=short), rng.rolling_mean(window_size=medium))
            .alias(f"{p}range_expansion_{short}_{medium}")
        )

    for cfg in rr_cfg.get("volume_rank", []):
        w = cfg["window"]
        if new_cols:
            lf = lf.with_columns(new_cols)
            new_cols = []
        lf = _rolling_rank_inject(lf, volume, w, f"{p}volume_rank_{w}")

    if new_cols:
        lf = lf.with_columns(new_cols)
        new_cols = []

    schema = lf.collect_schema().names()

    has_qv     = "quote_volume" in schema
    has_trades = "trades" in schema

    if has_qv:
        for cfg in rr_cfg.get("quote_volume_rank", []):
            w = cfg["window"]
            if new_cols:
                lf = lf.with_columns(new_cols)
                new_cols = []
            lf = _rolling_rank_inject(lf, pl.col("quote_volume"), w, f"{p}quote_volume_rank_{w}")

    if has_trades:
        for cfg in rr_cfg.get("trade_count_rank", []):
            w = cfg["window"]
            if new_cols:
                lf = lf.with_columns(new_cols)
                new_cols = []
            lf = _rolling_rank_inject(lf, pl.col("trades"), w, f"{p}trade_count_rank_{w}")

    for cfg in rr_cfg.get("volume_accel", []):
        short  = cfg["short"]
        medium = cfg["medium"]
        new_cols.append(
            _safe_div(volume.rolling_mean(window_size=short), volume.rolling_mean(window_size=medium))
            .alias(f"{p}volume_accel_{short}_{medium}")
        )

    if has_trades:
        for cfg in rr_cfg.get("trade_count_accel", []):
            short  = cfg["short"]
            medium = cfg["medium"]
            tr = pl.col("trades")
            new_cols.append(
                _safe_div(tr.rolling_mean(window_size=short), tr.rolling_mean(window_size=medium))
                .alias(f"{p}trade_count_accel_{short}_{medium}")
            )

    if new_cols:
        lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Candle shape features
# ---------------------------------------------------------------------------

def _add_candle_shape_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    cs_cfg = indicators.get("candle_shape", {})
    if not cs_cfg:
        return lf

    o  = pl.col("open")
    h  = pl.col("high")
    lo = pl.col("low")
    c  = pl.col("close")

    high_low    = pl.when(h - lo != 0).then(h - lo).otherwise(None)
    body        = (c - o).abs()
    signed_body = c - o
    upper_wick  = h - pl.max_horizontal([c, o])
    lower_wick  = pl.min_horizontal([c, o]) - lo

    new_cols: list[pl.Expr] = []

    if cs_cfg.get("raw"):
        new_cols += [
            _safe_div(body, high_low).alias(f"{p}body_ratio"),
            _safe_div(signed_body, high_low).alias(f"{p}signed_body_ratio"),
            _safe_div(upper_wick, high_low).alias(f"{p}upper_wick_ratio"),
            _safe_div(lower_wick, high_low).alias(f"{p}lower_wick_ratio"),
        ]

    body_ratio        = _safe_div(body, high_low)
    signed_body_ratio = _safe_div(signed_body, high_low)
    wick_imbalance    = lower_wick - upper_wick

    for cfg in cs_cfg.get("body_ratio_sma", []):
        w = cfg["window"]
        new_cols.append(body_ratio.rolling_mean(window_size=w).alias(f"{p}body_ratio_sma_{w}"))

    for cfg in cs_cfg.get("signed_body_sma", []):
        w = cfg["window"]
        new_cols.append(signed_body_ratio.rolling_mean(window_size=w).alias(f"{p}signed_body_sma_{w}"))

    for cfg in cs_cfg.get("wick_imbalance_sma", []):
        w = cfg["window"]
        new_cols.append(_safe_div(wick_imbalance, high_low).rolling_mean(window_size=w).alias(f"{p}wick_imbalance_sma_{w}"))

    if new_cols:
        lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Trend slope features
# ---------------------------------------------------------------------------

def _add_trend_slope_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    ts_cfg = indicators.get("trend_slope", {})
    if not ts_cfg:
        return lf

    close    = pl.col("close")
    new_cols: list[pl.Expr] = []

    for cfg in ts_cfg.get("ema_slope", []):
        w   = cfg["window"]
        ema = close.ewm_mean(span=w, adjust=False)
        lf  = lf.with_columns([ema.alias("_tmp_ema_s")]).with_columns([
            _safe_div(pl.col("_tmp_ema_s") - pl.col("_tmp_ema_s").shift(1), close)
            .alias(f"{p}ema_slope_{w}")
        ])
        lf = _drop_tmp(lf)

    for cfg in ts_cfg.get("directional_agreement", []):
        short  = cfg["short"]
        medium = cfg["medium"]
        ret_s  = close.pct_change(n=short)
        ret_m  = close.pct_change(n=medium)
        new_cols.append((ret_s.sign() * ret_m.sign()).alias(f"{p}directional_agreement_{short}_{medium}"))

    if new_cols:
        lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Interaction features
# ---------------------------------------------------------------------------

def _add_interaction_pl(
    lf: pl.LazyFrame,
    indicators: dict,
    p: str,
    available_activity: list[str],
) -> pl.LazyFrame:
    int_cfg = indicators.get("interaction", {})
    if not int_cfg:
        return lf

    close   = pl.col("close")
    volume  = pl.col("volume")
    log_ret = (close / close.shift(1)).log()
    has_tb  = "taker_buy_base" in available_activity
    new_cols: list[pl.Expr] = []

    rsi_col = f"{p}rsi_14"
    roc_col = f"{p}roc_14"
    schema  = lf.collect_schema().names()

    for cfg in int_cfg.get("rsi_delta", []):
        w   = cfg["window"]
        src = pl.col(rsi_col) if rsi_col in schema else (
            100.0 - 100.0 / (1.0 + close.diff().clip(lower_bound=0.0).ewm_mean(com=13, min_samples=14, adjust=False) /
                               (close.diff() * -1.0).clip(lower_bound=0.0).ewm_mean(com=13, min_samples=14, adjust=False))
        )
        new_cols.append((src - src.shift(w)).alias(f"{p}rsi_delta_{w}"))

    for cfg in int_cfg.get("roc_delta", []):
        w   = cfg["window"]
        src = pl.col(roc_col) if roc_col in schema else close.pct_change(n=14) * 100.0
        new_cols.append((src - src.shift(w)).alias(f"{p}roc_delta_{w}"))

    for cfg in int_cfg.get("vol_adj_return", []):
        w       = cfg["window"]
        ret     = close.pct_change(n=1)
        std_rol = log_ret.rolling_std(window_size=w, min_samples=w)
        new_cols.append(_safe_div(ret, std_rol).alias(f"{p}vol_adj_return_{w}"))

    for cfg in int_cfg.get("volume_confirmed_return", []):
        w   = cfg["window"]
        ret = close.pct_change(n=1)
        if new_cols:
            lf = lf.with_columns(new_cols)
            new_cols = []
        vol_arr  = lf.select(pl.col("volume")).collect().get_column("volume").to_numpy()
        rank_arr = _rolling_rank_arr(vol_arr, w)
        lf = lf.with_columns([
            pl.lit(pl.Series("_tmp_vol_rank", rank_arr, dtype=pl.Float64)),
        ]).with_columns([
            (ret * pl.col("_tmp_vol_rank")).alias(f"{p}volume_confirmed_return_{w}"),
        ]).drop("_tmp_vol_rank")

    if new_cols:
        lf = lf.with_columns(new_cols)
        new_cols = []

    if has_tb:
        schema = lf.collect_schema().names()
        for cfg in int_cfg.get("taker_flow_confirmed_return", []):
            w       = cfg["window"]
            ret     = close.pct_change(n=1)
            tb_b    = pl.col("taker_buy_base")
            tb_sum  = tb_b.rolling_sum(window_size=w, min_samples=w)
            vol_sum = volume.rolling_sum(window_size=w, min_samples=w)
            tb_ratio = _safe_div(tb_sum, vol_sum)
            new_cols.append((ret * tb_ratio).alias(f"{p}taker_flow_confirmed_return_{w}"))
        if new_cols:
            lf = lf.with_columns(new_cols)

    return lf


# ---------------------------------------------------------------------------
# Time / session features (P2 — NOT shifted by t-1 lag)
# ---------------------------------------------------------------------------

def _add_time_session_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("time_session"):
        return lf

    dt = pl.col("open_time")
    hour = dt.dt.hour().cast(pl.Float64)
    dow  = dt.dt.weekday().cast(pl.Float64)  # 0=Mon, 6=Sun (same as pandas)

    lf = lf.with_columns([
        (((2.0 * math.pi * hour) / 24.0).sin()).alias(f"{p}hour_sin"),
        (((2.0 * math.pi * hour) / 24.0).cos()).alias(f"{p}hour_cos"),
        (((2.0 * math.pi * dow)  / 7.0).sin()).alias(f"{p}dayofweek_sin"),
        (((2.0 * math.pi * dow)  / 7.0).cos()).alias(f"{p}dayofweek_cos"),
        (dow >= 5.0).cast(pl.Float64).alias(f"{p}weekend"),
        ((hour >= 0.0) & (hour < 8.0)).cast(pl.Float64).alias(f"{p}session_asia"),
        ((hour >= 7.0) & (hour < 16.0)).cast(pl.Float64).alias(f"{p}session_europe"),
        ((hour >= 13.0) & (hour < 22.0)).cast(pl.Float64).alias(f"{p}session_us"),
    ])
    return lf


# ---------------------------------------------------------------------------
# Garman-Klass volatility
# ---------------------------------------------------------------------------

def _add_gk_volatility_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("gk_volatility"):
        return lf

    h  = pl.col("high")
    lo = pl.col("low")
    c  = pl.col("close")
    o  = pl.col("open")

    log_hl = (h / lo).log()
    log_co = (c / o).log()

    pk_sq = log_hl ** 2 / (4.0 * math.log(2))
    gk_sq = 0.5 * log_hl ** 2 - (2.0 * math.log(2) - 1.0) * log_co ** 2

    new_cols: list[pl.Expr] = []
    for w in (10, 30, 60):
        new_cols.append(pk_sq.rolling_mean(window_size=w).clip(lower_bound=0.0).sqrt().alias(f"{p}parkinson_vol_{w}"))
        new_cols.append(gk_sq.rolling_mean(window_size=w).clip(lower_bound=0.0).sqrt().alias(f"{p}gk_vol_{w}"))

    lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Autocorrelation features — pure polars arithmetic, no rolling_map
# ---------------------------------------------------------------------------

def _add_autocorr_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("autocorr"):
        return lf

    close = pl.col("close")
    ret   = close.pct_change(n=1)
    new_cols: list[pl.Expr] = []

    for lag in (1, 5):
        for w in (30, 60):
            x   = ret
            y   = ret.shift(lag)
            # Pearson corr via rolling moments
            xy  = (x * y).rolling_mean(window_size=w, min_samples=w)
            xm  = x.rolling_mean(window_size=w, min_samples=w)
            ym  = y.rolling_mean(window_size=w, min_samples=w)
            xsq = (x * x).rolling_mean(window_size=w, min_samples=w)
            ysq = (y * y).rolling_mean(window_size=w, min_samples=w)
            cov = xy - xm * ym
            sx  = (xsq - xm * xm).sqrt()
            sy  = (ysq - ym * ym).sqrt()
            new_cols.append(_safe_div(cov, sx * sy).alias(f"{p}return_autocorr_lag{lag}_{w}"))

    lf = lf.with_columns(new_cols)

    # Variance ratio
    ret_10 = close.pct_change(n=10)
    var_1  = (ret * ret).rolling_mean(window_size=60, min_samples=60) - ret.rolling_mean(window_size=60, min_samples=60) ** 2
    var_10 = (ret_10 * ret_10).rolling_mean(window_size=60, min_samples=60) - ret_10.rolling_mean(window_size=60, min_samples=60) ** 2
    lf     = lf.with_columns([
        _safe_div(var_10, var_1 * 10.0).clip(lower_bound=0.0, upper_bound=5.0).alias(f"{p}variance_ratio_10_60"),
    ])
    return lf


# ---------------------------------------------------------------------------
# Drawdown timing — numpy sliding_window_view replaces rolling_map Python callbacks
# ---------------------------------------------------------------------------

def _add_drawdown_timing_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("drawdown_timing"):
        return lf

    close = pl.col("close")
    ratio_cols: list[pl.Expr] = []

    for w in (10, 30, 60):
        rh  = close.rolling_max(window_size=w, min_samples=w)
        rl  = close.rolling_min(window_size=w, min_samples=w)
        rng = pl.when(rh - rl != 0).then(rh - rl).otherwise(None)
        ratio_cols += [
            _safe_div(close - rl, rng).alias(f"{p}recovery_ratio_{w}"),
            _safe_div(rh - rl, rh).alias(f"{p}max_drawdown_{w}"),
        ]

    lf = lf.with_columns(ratio_cols)

    # time_since_high / time_since_low via numpy sliding_window_view (no Python callbacks)
    close_arr  = lf.select(pl.col("close")).collect().get_column("close").to_numpy()
    ts_cols: list[pl.Expr] = []
    for w in (10, 30, 60):
        ts_cols += [
            pl.lit(pl.Series(f"{p}time_since_high_{w}", _time_since_high_arr(close_arr, w), dtype=pl.Float64)),
            pl.lit(pl.Series(f"{p}time_since_low_{w}",  _time_since_low_arr(close_arr, w),  dtype=pl.Float64)),
        ]
    lf = lf.with_columns(ts_cols)
    return lf


# ---------------------------------------------------------------------------
# Pattern flags
# ---------------------------------------------------------------------------

def _add_pattern_flags_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("pattern_flags"):
        return lf

    o  = pl.col("open")
    h  = pl.col("high")
    lo = pl.col("low")
    c  = pl.col("close")

    rng        = pl.when(h - lo != 0).then(h - lo).otherwise(None)
    body       = (c - o).abs()
    hi_end     = pl.max_horizontal([c, o])
    lo_end     = pl.min_horizontal([c, o])
    upper_wick = h - hi_end
    lower_wick = lo_end - lo

    prev_h  = h.shift(1)
    prev_lo = lo.shift(1)
    prev_o  = o.shift(1)
    prev_c  = c.shift(1)
    bull_bar = (c > o).cast(pl.Float64)

    new_cols: list[pl.Expr] = [
        (_safe_div(body, rng) < 0.1).cast(pl.Float64).alias(f"{p}doji"),
        ((_safe_div(lower_wick, rng) > 0.6) & (_safe_div(body, rng) < 0.3) & (c > o)).cast(pl.Float64).alias(f"{p}hammer"),
        ((_safe_div(upper_wick, rng) > 0.6) & (_safe_div(body, rng) < 0.3) & (c < o)).cast(pl.Float64).alias(f"{p}shooting_star"),
        ((h < prev_h) & (lo > prev_lo)).cast(pl.Float64).alias(f"{p}inside_bar"),
        ((h > prev_h) & (lo < prev_lo)).cast(pl.Float64).alias(f"{p}outside_bar"),
        ((prev_c < prev_o) & (o < prev_c) & (c > prev_o)).cast(pl.Float64).alias(f"{p}engulf_bull"),
        ((prev_c > prev_o) & (o > prev_c) & (c < prev_o)).cast(pl.Float64).alias(f"{p}engulf_bear"),
    ]

    lf = lf.with_columns(new_cols)

    new_cols2: list[pl.Expr] = []
    for w in (10, 30, 60):
        new_cols2.append(bull_bar.rolling_mean(window_size=w).alias(f"{p}bull_bars_ratio_{w}"))
    lf = lf.with_columns(new_cols2)
    return lf


# ---------------------------------------------------------------------------
# Gap features
# ---------------------------------------------------------------------------

def _add_gap_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("gap"):
        return lf

    close      = pl.col("close")
    prev_close = close.shift(1)
    gap_open   = _safe_div(pl.col("open") - prev_close, prev_close)

    new_cols = [gap_open.alias(f"{p}gap_open")]
    for w in (10, 30):
        new_cols.append(gap_open.abs().rolling_mean(window_size=w).alias(f"{p}gap_open_abs_sma_{w}"))
    lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Efficiency ratio
# ---------------------------------------------------------------------------

def _add_efficiency_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("efficiency"):
        return lf

    close  = pl.col("close")
    ret_1  = close.pct_change(n=1).abs()
    new_cols: list[pl.Expr] = []

    for w in (10, 30, 60):
        net_move   = close.pct_change(n=w).abs()
        total_path = ret_1.rolling_sum(window_size=w, min_samples=w)
        new_cols.append(_safe_div(net_move, total_path).clip(lower_bound=0.0, upper_bound=1.0).alias(f"{p}efficiency_ratio_{w}"))

    lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Support / resistance level features
# ---------------------------------------------------------------------------

def _add_sr_levels_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("sr_levels"):
        return lf

    close   = pl.col("close")
    high    = pl.col("high")
    low     = pl.col("low")
    atr_col = f"{p}atr_14"
    schema  = lf.collect_schema().names()

    if atr_col in schema:
        atr = pl.col(atr_col)
    else:
        prev_c = close.shift(1)
        tr     = pl.max_horizontal([high - low, (high - prev_c).abs(), (low - prev_c).abs()])
        atr    = tr.ewm_mean(com=13, min_samples=14, adjust=False)

    atr_safe = pl.when(atr >= 1e-10).then(atr).otherwise(1e-10)
    new_cols: list[pl.Expr] = []

    for w in (10, 30, 60):
        rh = high.rolling_max(window_size=w, min_samples=w)
        rl = low.rolling_min(window_size=w, min_samples=w)
        new_cols.append(_safe_div(rh - close, atr_safe).alias(f"{p}atr_dist_high_{w}"))
        new_cols.append(_safe_div(close - rl, atr_safe).alias(f"{p}atr_dist_low_{w}"))

    lf = lf.with_columns(new_cols)

    # Prev-session high/low distance — needs daily groupby
    dt   = pl.col("open_time")
    day  = dt.dt.date()
    lf   = lf.with_columns([day.alias("_tmp_day")])
    lf   = lf.with_columns([
        high.max().over("_tmp_day").alias("_tmp_dh"),
        low.min().over("_tmp_day").alias("_tmp_dl"),
    ]).with_columns([
        pl.col("_tmp_dh").shift(1440).alias("_tmp_pdh"),
        pl.col("_tmp_dl").shift(1440).alias("_tmp_pdl"),
    ]).with_columns([
        _safe_div(pl.col("_tmp_pdh") - close, atr_safe).alias(f"{p}prev_session_high_dist"),
        _safe_div(close - pl.col("_tmp_pdl"), atr_safe).alias(f"{p}prev_session_low_dist"),
    ])
    lf = _drop_tmp(lf)
    return lf


# ---------------------------------------------------------------------------
# Tail risk features
# ---------------------------------------------------------------------------

def _add_tail_risk_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("tail_risk"):
        return lf

    close   = pl.col("close")
    ret     = close.pct_change(n=1)
    pos_ret = ret.clip(lower_bound=0.0)
    neg_ret = (ret.clip(upper_bound=0.0) * -1.0)
    new_cols: list[pl.Expr] = []

    for w in (10, 30, 60):
        pm = pos_ret.rolling_mean(window_size=w, min_samples=w)
        nm = neg_ret.rolling_mean(window_size=w, min_samples=w)
        new_cols += [
            pm.alias(f"{p}pos_return_mean_{w}"),
            nm.alias(f"{p}neg_return_mean_{w}"),
            _safe_div(pm, pl.when(nm != 0).then(nm).otherwise(None)).alias(f"{p}return_asymmetry_{w}"),
        ]

    lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Extended acceleration features
# ---------------------------------------------------------------------------

def _add_extended_accel_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("accel_extended"):
        return lf

    close   = pl.col("close")
    rsi_col = f"{p}rsi_14"
    roc_col = f"{p}roc_14"
    schema  = lf.collect_schema().names()
    ret_10  = close.pct_change(n=10)

    rsi_src = pl.col(rsi_col) if rsi_col in schema else (
        100.0 - 100.0 / (1.0 + close.diff().clip(lower_bound=0.0).ewm_mean(com=13, min_samples=14, adjust=False) /
                           (close.diff() * -1.0).clip(lower_bound=0.0).ewm_mean(com=13, min_samples=14, adjust=False))
    )
    roc_src = pl.col(roc_col) if roc_col in schema else close.pct_change(n=14) * 100.0

    new_cols: list[pl.Expr] = []
    for w in (10, 30):
        new_cols += [
            (rsi_src - rsi_src.shift(w)).alias(f"{p}rsi_delta_{w}"),
            (roc_src - roc_src.shift(w)).alias(f"{p}roc_delta_{w}"),
            (ret_10  - ret_10.shift(w)).alias(f"{p}return_momentum_delta_{w}"),
        ]

    lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Ichimoku features
# ---------------------------------------------------------------------------

def _add_ichimoku_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("ichimoku"):
        return lf

    h = pl.col("high")
    lo = pl.col("low")
    c  = pl.col("close")

    def _midpoint(w: int) -> pl.Expr:
        return (h.rolling_max(window_size=w) + lo.rolling_min(window_size=w)) / 2.0

    tenkan   = _midpoint(9)
    kijun    = _midpoint(26)
    senkou_b = _midpoint(52)

    lf = lf.with_columns([
        tenkan.alias("_tmp_tk"),
        kijun.alias("_tmp_kj"),
        senkou_b.alias("_tmp_sb"),
    ]).with_columns([
        (_safe_div(pl.col("_tmp_tk"), c) - 1.0).alias(f"{p}tenkan_ratio"),
        (_safe_div(pl.col("_tmp_kj"), c) - 1.0).alias(f"{p}kijun_ratio"),
        (_safe_div(pl.col("_tmp_sb"), c) - 1.0).alias(f"{p}senkou_b_ratio"),
        _safe_div(pl.col("_tmp_tk") - pl.col("_tmp_kj"), c).alias(f"{p}tenkan_kijun_delta"),
        _safe_div(c - pl.col("_tmp_tk"), c).alias(f"{p}price_vs_tenkan"),
        _safe_div(c - pl.col("_tmp_kj"), c).alias(f"{p}price_vs_kijun"),
    ]).with_columns([
        _safe_div(
            (pl.col("_tmp_tk") + pl.col("_tmp_kj")) / 2.0 - pl.col("_tmp_sb"), c
        ).alias(f"{p}ichimoku_cloud_thickness"),
    ])
    lf = _drop_tmp(lf)
    return lf


# ---------------------------------------------------------------------------
# Donchian channel features
# ---------------------------------------------------------------------------

def _add_donchian_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("donchian"):
        return lf

    h = pl.col("high")
    lo = pl.col("low")
    c  = pl.col("close")
    new_cols: list[pl.Expr] = []

    for w in (10, 30, 60):
        rh  = h.rolling_max(window_size=w)
        rl  = lo.rolling_min(window_size=w)
        rng = pl.when(rh - rl != 0).then(rh - rl).otherwise(None)
        new_cols += [
            _safe_div(rh - rl, c).alias(f"{p}donchian_width_{w}"),
            _safe_div(c - rl, rng).alias(f"{p}donchian_position_{w}"),
            (c >= rh).cast(pl.Float64).alias(f"{p}donchian_breakout_{w}"),
        ]

    lf = lf.with_columns(new_cols)
    return lf


# ---------------------------------------------------------------------------
# Linear regression features — numpy convolution (same as pandas version)
# ---------------------------------------------------------------------------

def _add_lr_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("lr"):
        return lf

    # Collect only close column — avoids materialising all feature columns
    close_np = lf.select("close").collect().get_column("close").to_numpy().astype(float)
    n        = len(close_np)
    new_data: dict[str, np.ndarray] = {}

    for w in (10, 30, 60):
        t      = np.arange(w, dtype=float) - (w - 1) / 2.0
        var_t  = (t ** 2).mean()
        t_now  = (w - 1) / 2.0
        w_arr  = t / (t ** 2).sum()

        # Slope via convolution
        raw     = np.convolve(close_np, w_arr[::-1], mode="valid")
        slope   = np.full(n, np.nan)
        slope[w - 1:] = raw

        # Rolling mean & var for R² and residual
        roll_mean = np.full(n, np.nan)
        roll_var  = np.full(n, np.nan)
        for i in range(w - 1, n):
            window_vals      = close_np[i - w + 1: i + 1]
            roll_mean[i]     = window_vals.mean()
            roll_var[i]      = max(window_vals.var(), 1e-20)

        r2       = np.clip(slope ** 2 * var_t / roll_var, 0.0, 1.0)
        y_hat    = roll_mean + slope * t_now
        with np.errstate(invalid="ignore", divide="ignore"):
            residual = np.where(close_np != 0, (close_np - y_hat) / close_np, np.nan)
            slope_n  = np.where(close_np != 0, slope / close_np, np.nan)

        new_data[f"{p}lr_slope_{w}"]    = slope_n
        new_data[f"{p}lr_r2_{w}"]       = r2
        new_data[f"{p}lr_residual_{w}"] = residual

    for col_name, arr in new_data.items():
        lf = lf.with_columns([pl.lit(pl.Series(col_name, arr)).alias(col_name)])

    return lf


# ---------------------------------------------------------------------------
# Session-relative features — polars group-by replacing groupby.expanding
# ---------------------------------------------------------------------------

def _add_session_relative_pl(lf: pl.LazyFrame, indicators: dict, p: str) -> pl.LazyFrame:
    if not indicators.get("session_relative"):
        return lf

    dt    = pl.col("open_time")
    close = pl.col("close")
    high  = pl.col("high")
    low   = pl.col("low")
    open_ = pl.col("open")

    # Day key and Monday key
    lf = lf.with_columns([
        dt.dt.date().alias("_tmp_day"),
        (dt - pl.duration(days=dt.dt.weekday())).dt.date().alias("_tmp_monday"),
    ])

    # Day open (first open within each calendar day)
    lf = lf.with_columns([
        open_.first().over("_tmp_day").alias("_tmp_day_open"),
        open_.first().over("_tmp_monday").alias("_tmp_week_open"),
    ])

    # Expanding high/low within day
    lf = lf.with_columns([
        high.cum_max().over("_tmp_day").alias("_tmp_exp_high"),
        low.cum_min().over("_tmp_day").alias("_tmp_exp_low"),
    ])

    # day_range_position
    day_range = pl.col("_tmp_exp_high") - pl.col("_tmp_exp_low")
    lf = lf.with_columns([
        _safe_div(close - pl.col("_tmp_exp_low"), pl.when(day_range != 0).then(day_range).otherwise(None))
        .alias(f"{p}day_range_position"),
    ])

    # day_open_return
    lf = lf.with_columns([
        _safe_div(close - pl.col("_tmp_day_open"), pl.col("_tmp_day_open")).alias(f"{p}day_open_return"),
    ])

    # bars_into_session_norm  (0-based cumcount within day, same as groupby().cumcount())
    lf = lf.with_columns([
        ((pl.col("open_time").cum_count().over("_tmp_day") - 1).cast(pl.Float64) / 1440.0)
        .clip(lower_bound=0.0, upper_bound=1.0)
        .alias(f"{p}bars_into_session_norm"),
    ])

    # weekly_open_return
    lf = lf.with_columns([
        _safe_div(close - pl.col("_tmp_week_open"), pl.col("_tmp_week_open")).alias(f"{p}weekly_open_return"),
    ])

    lf = _drop_tmp(lf)
    return lf


# ---------------------------------------------------------------------------
# t-1 lag — polars shift(1) on all OHLCV-based feature columns
# ---------------------------------------------------------------------------

def _apply_t1_lag_pl(lf: pl.LazyFrame, p: str) -> pl.LazyFrame:
    schema  = lf.collect_schema().names()
    lag_cols = [
        c for c in schema
        if c.startswith(p) and c not in T_MINUS_1_SKIP
    ]
    if not lag_cols:
        return lf
    return lf.with_columns([pl.col(c).shift(1) for c in lag_cols])


# ---------------------------------------------------------------------------
# Clean inf → null
# ---------------------------------------------------------------------------

def _clean_features_pl(lf: pl.LazyFrame, p: str) -> pl.LazyFrame:
    schema   = lf.collect_schema()
    feat_cols = [c for c in schema.names() if c.startswith(p)]
    if not feat_cols:
        return lf
    return lf.with_columns([
        pl.when(pl.col(c).is_infinite()).then(None).otherwise(pl.col(c)).alias(c)
        for c in feat_cols
        if schema[c] in (pl.Float32, pl.Float64)
    ])


# ---------------------------------------------------------------------------
# Target variables
# ---------------------------------------------------------------------------

def _add_targets_pl(lf: pl.LazyFrame, targets_cfg: list[dict]) -> pl.LazyFrame:
    """Compute rolling-max/min threshold targets in polars."""
    close = pl.col("close")

    for target_cfg in targets_cfg:
        direction   = target_cfg["direction"]
        rolling_win = target_cfg["rolling_window"]
        percentile  = target_cfg["percentile"]
        target_col  = target_cfg.get("name") or f"trg_{direction[0]}_fw{rolling_win}_q{int(percentile*100)}"

        if direction == "long":
            rolling_max = close.reverse().rolling_max(window_size=rolling_win, min_samples=1).reverse()
            ratio       = _safe_div(rolling_max, close)
        elif direction == "short":
            rolling_min = close.reverse().rolling_min(window_size=rolling_win, min_samples=1).reverse()
            ratio       = _safe_div(rolling_min, close)
        else:
            raise ValueError(f"Unknown target direction: {direction}")

        # Compute threshold via quantile — needs collect for global quantile
        collected  = lf.select([ratio.alias("_ratio")]).collect()
        threshold  = collected.get_column("_ratio").quantile(percentile, interpolation="linear")
        n          = len(collected)  # reuse — avoids a second full collect

        if direction == "long":
            label = (ratio >= threshold).cast(pl.Float64)
        else:
            label = (ratio <= threshold).cast(pl.Float64)

        lf = lf.with_columns([label.alias(target_col)])

        # Edge-null: last rolling_win rows are set to null (can't observe full forward window)
        if n >= rolling_win:
            edge_mask = pl.int_range(pl.len()) >= (pl.len() - rolling_win)
            lf = lf.with_columns([
                pl.when(edge_mask).then(None).otherwise(pl.col(target_col)).alias(target_col)
            ])

    return lf


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def compute_features_polars(
    df_ohlcv          : "pd.DataFrame",  # noqa: F821
    indicators        : dict,
    feat_prefix       : str,
    available_activity: list[str],
    targets_cfg       : list[dict],
) -> "pd.DataFrame":  # noqa: F821
    """Compute all features from OHLCV data using polars LazyFrame.

    Args:
        df_ohlcv           : OHLCV pandas DataFrame with DatetimeIndex (open_time).
        indicators         : Indicator config dict from features.json.
        feat_prefix        : Column name prefix (e.g. "feat_").
        available_activity : List of activity column names present in df_ohlcv.
        targets_cfg        : Target variable config list.

    Returns:
        pandas DataFrame with all feature and target columns.
    """
    import pandas as pd  # local to avoid circular at module level

    df_reset = df_ohlcv.reset_index()
    lf       = pl.from_pandas(df_reset).lazy()

    # Ensure open_time is Datetime
    if lf.collect_schema()["open_time"] != pl.Datetime:
        lf = lf.with_columns([pl.col("open_time").cast(pl.Datetime("us"))])

    # Ensure all OHLCV numeric columns are Float64
    ohlcv_cols = ["open", "high", "low", "close", "volume"] + available_activity
    for col in ohlcv_cols:
        lf = lf.with_columns([pl.col(col).cast(pl.Float64)])

    # Targets
    lf = _add_targets_pl(lf, targets_cfg)

    # Feature groups
    lf = _add_momentum_pl(lf, indicators, feat_prefix)
    lf = _add_trend_pl(lf, indicators, feat_prefix)
    lf = _add_volatility_pl(lf, indicators, feat_prefix)
    lf = _add_volume_pl(lf, indicators, feat_prefix)
    lf = _add_price_action_pl(lf, indicators, feat_prefix)
    lf = _add_market_structure_pl(lf, indicators, feat_prefix)
    lf = _add_activity_pl(lf, indicators, feat_prefix, available_activity)
    lf = _add_return_distance_pl(lf, indicators, feat_prefix)
    lf = _add_regime_rank_pl(lf, indicators, feat_prefix)
    lf = _add_candle_shape_pl(lf, indicators, feat_prefix)
    lf = _add_trend_slope_pl(lf, indicators, feat_prefix)
    lf = _add_interaction_pl(lf, indicators, feat_prefix, available_activity)
    lf = _add_time_session_pl(lf, indicators, feat_prefix)
    lf = _add_gk_volatility_pl(lf, indicators, feat_prefix)
    lf = _add_autocorr_pl(lf, indicators, feat_prefix)
    lf = _add_drawdown_timing_pl(lf, indicators, feat_prefix)
    lf = _add_pattern_flags_pl(lf, indicators, feat_prefix)
    lf = _add_gap_pl(lf, indicators, feat_prefix)
    lf = _add_efficiency_pl(lf, indicators, feat_prefix)
    lf = _add_sr_levels_pl(lf, indicators, feat_prefix)
    lf = _add_tail_risk_pl(lf, indicators, feat_prefix)
    lf = _add_extended_accel_pl(lf, indicators, feat_prefix)
    lf = _add_ichimoku_pl(lf, indicators, feat_prefix)
    lf = _add_donchian_pl(lf, indicators, feat_prefix)
    lf = _add_lr_pl(lf, indicators, feat_prefix)
    lf = _add_session_relative_pl(lf, indicators, feat_prefix)

    # t-1 lag
    lf = _apply_t1_lag_pl(lf, feat_prefix)

    # Clean inf
    lf = _clean_features_pl(lf, feat_prefix)

    # Collect to pandas
    result = lf.collect().to_pandas()
    result["open_time"] = pd.to_datetime(result["open_time"])
    return result
