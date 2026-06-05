# =============================================================================
# Compute technical indicators and target variables for feature engineering
# =============================================================================
# Purpose:
#  - Load raw OHLCV data from database
#  - Compute configured target variables
#  - Generate configured technical indicators with 'feat_' prefix
#  - Insert feature rows into database idempotently by open_time
# =============================================================================

from datetime import timedelta

import numpy as np
import pandas as pd
import ta

import utils
from db.table_ops import drop_existing_open_times, ensure_table_columns, sqlite_connect


# =============================================================================
# _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series
# =============================================================================
# Purpose:
#  - Divide two series while converting zero denominators to missing values
# =============================================================================
def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0, np.nan)


# =============================================================================
# _add_momentum_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None
# =============================================================================
# Purpose:
#  - Add configured momentum features in-place
# =============================================================================
def _add_momentum_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None:
    momentum_cfg = indicators.get("momentum", {})

    for rsi_cfg in momentum_cfg.get("rsi", []):
        window = rsi_cfg["window"]
        df[f"{prefix}rsi_{window}"] = ta.momentum.RSIIndicator(
            close=df["close"],
            window=window,
        ).rsi()

    for roc_cfg in momentum_cfg.get("roc", []):
        window = roc_cfg["window"]
        df[f"{prefix}roc_{window}"] = ta.momentum.ROCIndicator(
            close=df["close"],
            window=window,
        ).roc()

    for stoch_cfg in momentum_cfg.get("stochastic", []):
        window        = stoch_cfg["window"]
        smooth_window = stoch_cfg.get("smooth_window", 3)
        stoch = ta.momentum.StochasticOscillator(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            window=window,
            smooth_window=smooth_window,
        )
        df[f"{prefix}stoch_k_{window}"] = stoch.stoch()
        df[f"{prefix}stoch_d_{window}"] = stoch.stoch_signal()

    for cci_cfg in momentum_cfg.get("cci", []):
        window = cci_cfg["window"]
        df[f"{prefix}cci_{window}"] = ta.trend.CCIIndicator(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            window=window,
        ).cci()

    for williams_cfg in momentum_cfg.get("williams_r", []):
        window = williams_cfg["window"]
        df[f"{prefix}williams_r_{window}"] = ta.momentum.WilliamsRIndicator(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            lbp=window,
        ).williams_r()

    for adx_cfg in momentum_cfg.get("adx", []):
        window = adx_cfg["window"]
        adx = ta.trend.ADXIndicator(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            window=window,
        )
        df[f"{prefix}adx_{window}"] = adx.adx()
        df[f"{prefix}adx_pos_{window}"] = adx.adx_pos()
        df[f"{prefix}adx_neg_{window}"] = adx.adx_neg()


# =============================================================================
# _add_trend_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None
# =============================================================================
# Purpose:
#  - Add configured trend features in-place
# =============================================================================
def _add_trend_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None:
    trend_cfg = indicators.get("trend", {})

    for macd_cfg in trend_cfg.get("macd", []):
        fast   = macd_cfg.get("fast", 12)
        slow   = macd_cfg.get("slow", 26)
        signal = macd_cfg.get("signal", 9)
        macd = ta.trend.MACD(
            close=df["close"],
            window_fast=fast,
            window_slow=slow,
            window_sign=signal,
        )
        df[f"{prefix}macd_{fast}_{slow}"] = macd.macd()
        df[f"{prefix}macd_signal_{fast}_{slow}_{signal}"] = macd.macd_signal()
        df[f"{prefix}macd_diff"] = macd.macd_diff()

    for sma_cfg in trend_cfg.get("sma", []):
        window = sma_cfg["window"]
        sma = ta.trend.SMAIndicator(close=df["close"], window=window).sma_indicator()
        df[f"{prefix}sma_ratio_{window}"] = _safe_div(df["close"], sma)

    for ema_cfg in trend_cfg.get("ema", []):
        window = ema_cfg["window"]
        ema = ta.trend.EMAIndicator(close=df["close"], window=window).ema_indicator()
        df[f"{prefix}ema_ratio_{window}"] = _safe_div(df["close"], ema)

    for wma_cfg in trend_cfg.get("wma", []):
        window = wma_cfg["window"]
        wma = ta.trend.WMAIndicator(close=df["close"], window=window).wma()
        df[f"{prefix}wma_ratio_{window}"] = _safe_div(df["close"], wma)

    for kama_cfg in trend_cfg.get("kama", []):
        window = kama_cfg.get("window", 10)
        fast   = kama_cfg.get("fast", 2)
        slow   = kama_cfg.get("slow", 30)
        kama = ta.momentum.KAMAIndicator(
            close=df["close"],
            window=window,
            pow1=fast,
            pow2=slow,
        ).kama()
        df[f"{prefix}kama_ratio_{window}_{fast}_{slow}"] = _safe_div(df["close"], kama)


# =============================================================================
# _add_volatility_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None
# =============================================================================
# Purpose:
#  - Add configured volatility features in-place
# =============================================================================
def _add_volatility_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None:
    volatility_cfg = indicators.get("volatility", {})

    for bb_cfg in volatility_cfg.get("bollinger", []):
        window     = bb_cfg["window"]
        window_dev = bb_cfg.get("window_dev", 2)
        bb = ta.volatility.BollingerBands(
            close=df["close"],
            window=window,
            window_dev=window_dev,
        )
        upper = bb.bollinger_hband()
        lower = bb.bollinger_lband()
        df[f"{prefix}bb_width_{window}"] = _safe_div(upper - lower, df["close"])
        df[f"{prefix}bb_position_{window}"] = bb.bollinger_pband()

    for atr_cfg in volatility_cfg.get("atr", []):
        window = atr_cfg["window"]
        atr = ta.volatility.AverageTrueRange(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            window=window,
        ).average_true_range()
        df[f"{prefix}atr_{window}"] = atr
        df[f"{prefix}natr_{window}"] = _safe_div(atr, df["close"])

    returns_log = np.log(_safe_div(df["close"], df["close"].shift(1)))
    for hist_vol_cfg in volatility_cfg.get("historical_vol", []):
        window = hist_vol_cfg["window"]
        df[f"{prefix}hist_vol_{window}"] = returns_log.rolling(window).std()


# =============================================================================
# _add_volume_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None
# =============================================================================
# Purpose:
#  - Add configured volume features in-place
# =============================================================================
def _add_volume_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None:
    volume_cfg = indicators.get("volume", {})

    for volume_sma_cfg in volume_cfg.get("volume_sma", []):
        window = volume_sma_cfg["window"]
        df[f"{prefix}volume_sma_{window}"] = df["volume"].rolling(window).mean()

    for volume_ratio_cfg in volume_cfg.get("volume_ratio", []):
        window = volume_ratio_cfg["window"]
        volume_sma = df["volume"].rolling(window).mean()
        df[f"{prefix}volume_ratio_{window}"] = _safe_div(df["volume"], volume_sma)

    if volume_cfg.get("obv"):
        df[f"{prefix}obv"] = ta.volume.OnBalanceVolumeIndicator(
            close=df["close"],
            volume=df["volume"],
        ).on_balance_volume()

    for obv_roc_cfg in volume_cfg.get("obv_roc", []):
        window = obv_roc_cfg["window"]
        obv_col = f"{prefix}obv"
        if obv_col not in df.columns:
            df[obv_col] = ta.volume.OnBalanceVolumeIndicator(
                close=df["close"],
                volume=df["volume"],
            ).on_balance_volume()
        df[f"{prefix}obv_roc_{window}"] = df[obv_col].pct_change(periods=window)

    for mfi_cfg in volume_cfg.get("mfi", []):
        window = mfi_cfg["window"]
        df[f"{prefix}mfi_{window}"] = ta.volume.MFIIndicator(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            volume=df["volume"],
            window=window,
        ).money_flow_index()

    if volume_cfg.get("ad_line"):
        df[f"{prefix}ad_line"] = ta.volume.AccDistIndexIndicator(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            volume=df["volume"],
        ).acc_dist_index()

    for cmf_cfg in volume_cfg.get("cmf", []):
        window = cmf_cfg["window"]
        df[f"{prefix}cmf_{window}"] = ta.volume.ChaikinMoneyFlowIndicator(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            volume=df["volume"],
            window=window,
        ).chaikin_money_flow()


# =============================================================================
# _add_price_action_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None
# =============================================================================
# Purpose:
#  - Add configured price-action features in-place
# =============================================================================
def _add_price_action_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None:
    price_cfg = indicators.get("price_action", {})

    if price_cfg.get("returns"):
        df[f"{prefix}returns_log"] = np.log(_safe_div(df["close"], df["close"].shift(1)))

    returns_col = f"{prefix}returns_log"
    if returns_col not in df.columns:
        df[returns_col] = np.log(_safe_div(df["close"], df["close"].shift(1)))

    for returns_sma_cfg in price_cfg.get("returns_sma", []):
        window = returns_sma_cfg["window"]
        df[f"{prefix}returns_sma_{window}"] = df[returns_col].rolling(window).mean()

    for returns_std_cfg in price_cfg.get("returns_std", []):
        window = returns_std_cfg["window"]
        df[f"{prefix}returns_std_{window}"] = df[returns_col].rolling(window).std()

    for returns_skew_cfg in price_cfg.get("returns_skew", []):
        window = returns_skew_cfg["window"]
        df[f"{prefix}returns_skew_{window}"] = df[returns_col].rolling(window).skew()

    for returns_kurt_cfg in price_cfg.get("returns_kurt", []):
        window = returns_kurt_cfg["window"]
        df[f"{prefix}returns_kurt_{window}"] = df[returns_col].rolling(window).kurt()

    if price_cfg.get("range_metrics"):
        high_low = df["high"] - df["low"]
        open_close_avg = (df["open"] + df["close"]) / 2
        df[f"{prefix}hml_range"] = _safe_div(high_low, df["close"])
        df[f"{prefix}ohlc_range"] = _safe_div(high_low, open_close_avg)

    if price_cfg.get("close_position"):
        high_low = df["high"] - df["low"]
        df[f"{prefix}close_position"] = _safe_div(df["close"] - df["low"], high_low)


# =============================================================================
# _add_market_structure_features(...)
# =============================================================================
# Purpose:
#  - Add configured market-structure features in-place
# =============================================================================
def _add_market_structure_features(df: pd.DataFrame, indicators: dict, prefix: str) -> None:
    market_cfg = indicators.get("market_structure", {})

    for trend_cfg in market_cfg.get("trend_counts", []):
        window = trend_cfg["window"]
        higher_high = (df["high"] > df["high"].shift(1)).astype(int)
        higher_low  = (df["low"] > df["low"].shift(1)).astype(int)
        lower_high  = (df["high"] < df["high"].shift(1)).astype(int)
        lower_low   = (df["low"] < df["low"].shift(1)).astype(int)
        df[f"{prefix}higher_high_count_{window}"] = higher_high.rolling(window).sum()
        df[f"{prefix}higher_low_count_{window}"] = higher_low.rolling(window).sum()
        df[f"{prefix}lower_high_count_{window}"] = lower_high.rolling(window).sum()
        df[f"{prefix}lower_low_count_{window}"] = lower_low.rolling(window).sum()

    for swing_cfg in market_cfg.get("swing_points", []):
        window = swing_cfg["window"]
        rolling_high = df["high"].rolling(window).max()
        rolling_low  = df["low"].rolling(window).min()
        df[f"{prefix}swing_high_{window}"] = (df["high"] >= rolling_high).astype(int)
        df[f"{prefix}swing_low_{window}"] = (df["low"] <= rolling_low).astype(int)


# =============================================================================
# _clean_feature_values(df: pd.DataFrame, prefix: str) -> None
# =============================================================================
# Purpose:
#  - Replace infinite feature values with missing values in-place
# =============================================================================
def _clean_feature_values(df: pd.DataFrame, prefix: str) -> None:
    feat_cols = [col for col in df.columns if col.startswith(prefix)]
    df[feat_cols] = df[feat_cols].replace([np.inf, -np.inf], np.nan)


# =============================================================================
# sync_features(...) -> None
# =============================================================================
# Purpose:
#  - Fetch raw OHLCV data from [start_time - lookback, end]
#  - Compute configured target variables
#  - Generate all configured technical indicators with 'feat_' prefix
#  - Insert rows into features table
# Parameters:
#  - start_time: "YYYY-MM-DD HH:MM:SS" (UTC)
#  - lookback_bars: minutes to look back for feature computation
#  - end_time: optional "YYYY-MM-DD HH:MM:SS" upper bound for controlled rebuilds
#  - asset_id: optional asset id from config/assets.json
# =============================================================================
def sync_features(
    start_time: str,
    lookback_bars: int = 240,
    end_time: str | None = None,
    asset_id: str | None = None,
) -> None:
    # -------------------------------------------------------------------------
    # Load configuration
    # -------------------------------------------------------------------------
    db_cfg      = utils.load_asset_config(asset_id)
    feat_cfg    = utils.load_features_config(asset_id=asset_id)
    db_path     = db_cfg["database"]["db_path"]
    table_ohlcv = db_cfg["database"]["tables"]["ohlcv"]
    table_feat  = db_cfg["database"]["tables"]["features"]
    cfg_feat    = feat_cfg["database"]["features"]
    targets_cfg = cfg_feat.get("targets", [])

    # -------------------------------------------------------------------------
    # Fetch raw OHLCV data
    # -------------------------------------------------------------------------
    fetch_start = (
        pd.to_datetime(start_time) - timedelta(minutes=lookback_bars)
    ).strftime("%Y-%m-%d %H:%M:%S")

    with sqlite_connect(db_path) as conn:
        df = pd.read_sql_query(
            f"""
            SELECT open_time, open, high, low, close, volume
            FROM {table_ohlcv}
            WHERE open_time >= ?
                AND (? IS NULL OR open_time <= ?)
            ORDER BY open_time ASC
            """,
            conn,
            params=(fetch_start, end_time, end_time),
        )

    if df.empty:
        print(f"No OHLCV rows found since {fetch_start}")
        return

    df["open_time"] = pd.to_datetime(df["open_time"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.set_index("open_time", inplace=True)

    # -------------------------------------------------------------------------
    # Compute target variables
    # -------------------------------------------------------------------------
    for target_cfg in targets_cfg:
        direction   = target_cfg["direction"]
        rolling_win = target_cfg["rolling_window"]
        percentile  = target_cfg["percentile"]
        target_col  = utils.target_name_from_config(target_cfg)

        if direction == "long":
            rolling_max   = df["close"][::-1].rolling(rolling_win, min_periods=1).max()[::-1]
            ratio_long    = _safe_div(rolling_max, df["close"])
            threshold     = ratio_long.quantile(percentile)
            df[target_col] = (ratio_long >= threshold).astype(int)

        elif direction == "short":
            rolling_min    = df["close"][::-1].rolling(rolling_win, min_periods=1).min()[::-1]
            ratio_short    = _safe_div(rolling_min, df["close"])
            threshold      = ratio_short.quantile(percentile)
            df[target_col] = (ratio_short <= threshold).astype(int)

        else:
            raise ValueError(f"Unknown target direction: {direction}")

    # -------------------------------------------------------------------------
    # Generate technical indicators with 'feat_' prefix
    # -------------------------------------------------------------------------
    indicators  = cfg_feat["indicators"]
    feat_prefix = "feat_"

    _add_momentum_features(df, indicators, feat_prefix)
    _add_trend_features(df, indicators, feat_prefix)
    _add_volatility_features(df, indicators, feat_prefix)
    _add_volume_features(df, indicators, feat_prefix)
    _add_price_action_features(df, indicators, feat_prefix)
    _add_market_structure_features(df, indicators, feat_prefix)
    _clean_feature_values(df, feat_prefix)

    # -------------------------------------------------------------------------
    # Prepare and insert into database
    # -------------------------------------------------------------------------
    df_reset = df.reset_index()
    start_dt = pd.to_datetime(start_time)
    df_reset = df_reset[df_reset["open_time"] >= start_dt].copy()
    if end_time is not None:
        end_dt = pd.to_datetime(end_time)
        df_reset = df_reset[df_reset["open_time"] <= end_dt].copy()
    df_reset["open_time"] = df_reset["open_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    feat_cols    = [col for col in df_reset.columns if col.startswith(feat_prefix)]
    target_cols  = utils.target_columns_from_config(feat_cfg)
    cols_to_keep = ["open_time", "close"] + target_cols + feat_cols
    df_final     = df_reset[cols_to_keep].copy()
    ensure_table_columns(db_path, table_feat, df_final)
    df_final     = drop_existing_open_times(df_final, db_path, table_feat)

    if df_final.empty:
        print(f"No new feature rows to insert into '{table_feat}'")
        return

    with sqlite_connect(db_path) as conn:
        df_final.to_sql(table_feat, conn, index=False, if_exists="append", chunksize=50_000)

    print(f"OK: Computed {len(df_final)} feature rows into '{table_feat}'")
