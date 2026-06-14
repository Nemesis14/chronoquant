"""Feature generation tests for the data pipeline.

Verifies DuckDB-backed feature sync, SOL feature profile selection, and
look-ahead prevention for t-1 lagged OHLCV-derived features.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pytest

import data_pipeline.sync_features as sync_features_module
from data_pipeline._features_polars import T_MINUS_1_SKIP, compute_features_polars
from store.duckdb_query import query_range
from store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

# %% Test data builders


def _build_ohlcv(rows: int = 500) -> pd.DataFrame:
    """Build deterministic OHLCV rows for DuckDB-backed sync tests."""
    open_time = pd.date_range("2024-01-01 00:00:00", periods=rows, freq="min")
    base      = pd.Series(range(rows), dtype="float64")
    close     = 100 + base * 0.01 + (base % 17) * 0.03
    open_     = close.shift(1).fillna(close.iloc[0])
    high      = pd.concat([open_, close], axis=1).max(axis=1) + 0.5
    low       = pd.concat([open_, close], axis=1).min(axis=1) - 0.5
    volume    = 1000 + (base % 29) * 10

    return pd.DataFrame(
        {
            "open_time" : open_time.strftime("%Y-%m-%d %H:%M:%S"),
            "open"      : open_,
            "high"      : high,
            "low"       : low,
            "close"     : close,
            "volume"    : volume,
        }
    )


def _seed_ohlcv(data_dir: Path, rows: int = 500) -> None:
    """Write deterministic OHLCV rows into an isolated DuckDB store."""
    conn = get_connection(str(data_dir))
    try:
        ensure_tables(conn)
        insert_ohlcv(conn, _build_ohlcv(rows))
    finally:
        conn.close()


def _asset_cfg(data_dir: Path) -> dict:
    """Return the minimal asset config needed by sync_features tests."""
    return {
        "database": {
            "data_dir"         : str(data_dir),
            "asset_id"         : "solusdt_fw60",
            "features_profile" : "solusdt_fw60",
        }
    }


def _features_cfg() -> dict:
    """Return a controlled feature config for sync_features smoke tests."""
    targets_cfg = [
        {
            "direction"      : "long",
            "name"           : "trg_l_fw60_q90",
            "rolling_window" : 60,
            "percentile"     : 0.9,
        },
        {
            "direction"      : "short",
            "name"           : "trg_s_fw60_q10",
            "rolling_window" : 60,
            "percentile"     : 0.1,
        },
    ]
    indicators = {
        "momentum"         : {
            "rsi"        : [{"window": 14}],
            "roc"        : [{"window": 14}, {"window": 140}],
            "stochastic" : [{"window": 14, "smooth_window": 3}],
            "cci"        : [{"window": 20}],
            "williams_r" : [{"window": 14}],
            "adx"        : [{"window": 14}],
        },
        "trend"            : {
            "macd" : [{"fast": 12, "slow": 26, "signal": 9}],
            "sma"  : [{"window": 14}, {"window": 140}],
            "ema"  : [{"window": 14}, {"window": 140}],
            "wma"  : [{"window": 14}],
            "kama" : [{"window": 10, "fast": 2, "slow": 30}],
        },
        "volatility"       : {
            "bollinger"      : [
                {"window": 14, "window_dev": 2},
                {"window": 140, "window_dev": 2},
            ],
            "atr"            : [{"window": 14}],
            "historical_vol" : [{"window": 20}],
        },
        "volume"           : {
            "volume_sma"   : [{"window": 14}],
            "volume_ratio" : [{"window": 14}],
            "obv"          : [{}],
            "obv_roc"      : [{"window": 14}],
            "mfi"          : [{"window": 14}],
            "ad_line"      : [{}],
            "cmf"          : [{"window": 20}],
        },
        "price_action"     : {
            "returns"      : [{"type": "log"}],
            "returns_sma"  : [{"window": 14}],
            "returns_std"  : [{"window": 14}],
            "returns_skew" : [{"window": 14}],
            "returns_kurt" : [{"window": 14}],
            "range_metrics": [{}],
            "close_position": [{}],
        },
        "market_structure" : {
            "trend_counts" : [{"window": 5}],
            "swing_points" : [{"window": 5}],
        },
    }
    return {"database": {"features": {"targets": targets_cfg, "indicators": indicators}}}


def _build_ohlcv_indexed(rows: int = 1600) -> pl.DataFrame:
    """Build deterministic OHLCV data as a Polars DataFrame with open_time column."""
    open_time = pd.date_range("2024-01-01 00:00:00", periods=rows, freq="min")
    base      = np.arange(rows, dtype="float64")
    close     = 100.0 + base * 0.01 + (base % 17) * 0.03
    open_     = np.roll(close, 1)
    high      = np.maximum(open_, close) + 0.5
    low       = np.minimum(open_, close) - 0.5
    volume    = 1000.0 + (base % 29) * 10

    open_[0] = close[0]
    if rows > 720:
        high[720] = high[720] + 5.0

    return pl.DataFrame({
        "open_time" : open_time.tolist(),
        "open"      : open_,
        "high"      : high,
        "low"       : low,
        "close"     : close,
        "volume"    : volume,
    })


def _compute_session_features_with_lag(df: pl.DataFrame) -> pl.DataFrame:
    """Compute session-relative features through the current Polars path."""
    return compute_features_polars(
        df_ohlcv           = df,
        indicators         = {"session_relative": True},
        feat_prefix        = "feat_",
        available_activity = [],
        targets_cfg        = [],
    )


# %% Feature sync tests


def test_sync_features_expanded_columns_and_idempotency(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify feature sync writes expected columns and stays idempotent."""
    data_dir = tmp_path / "data"
    _seed_ohlcv(data_dir)

    monkeypatch.setattr(
        sync_features_module.utils,
        "load_asset_config",
        lambda asset_id=None: _asset_cfg(data_dir),
    )
    monkeypatch.setattr(
        sync_features_module.utils,
        "load_features_config",
        lambda asset_id=None: _features_cfg(),
    )

    sync_features_module.sync_features(
        start_time = "2024-01-01 04:00:00",
        end_time   = "2024-01-01 06:00:00",
    )
    sync_features_module.sync_features(
        start_time = "2024-01-01 04:00:00",
        end_time   = "2024-01-01 06:00:00",
    )

    df = query_range(str(data_dir), "features")

    expected_features = {
        "feat_rsi_14",
        "feat_roc_140",
        "feat_stoch_k_14",
        "feat_cci_20",
        "feat_williams_r_14",
        "feat_adx_14",
        "feat_macd_12_26",
        "feat_macd_signal_12_26_9",
        "feat_sma_ratio_140",
        "feat_ema_ratio_140",
        "feat_wma_ratio_14",
        "feat_kama_ratio_10_2_30",
        "feat_bb_width_140",
        "feat_bb_position_140",
        "feat_atr_14",
        "feat_natr_14",
        "feat_hist_vol_20",
        "feat_volume_sma_14",
        "feat_volume_ratio_14",
        "feat_obv",
        "feat_obv_roc_14",
        "feat_mfi_14",
        "feat_ad_line",
        "feat_cmf_20",
        "feat_returns_log",
        "feat_returns_sma_14",
        "feat_returns_std_14",
        "feat_returns_skew_14",
        "feat_returns_kurt_14",
        "feat_hml_range",
        "feat_ohlc_range",
        "feat_close_position",
        "feat_higher_high_count_5",
        "feat_higher_low_count_5",
        "feat_lower_high_count_5",
        "feat_lower_low_count_5",
        "feat_swing_high_5",
        "feat_swing_low_5",
    }

    assert expected_features.issubset(set(df.columns))
    assert df["open_time"].is_unique
    assert len(df) == 121
    assert df[list(expected_features)].notna().sum().sum() > 0


def test_sync_features_uses_sol_feature_profile(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify SOLUSDT feature sync uses only the SOL fw60 target profile."""
    data_dir = tmp_path / "data"
    _seed_ohlcv(data_dir)

    monkeypatch.setattr(
        sync_features_module.utils,
        "load_asset_config",
        lambda asset_id=None: _asset_cfg(data_dir),
    )
    monkeypatch.setattr(
        sync_features_module.utils,
        "load_features_config",
        lambda asset_id=None: _features_cfg(),
    )

    sync_features_module.sync_features(
        start_time = "2024-01-01 00:00:00",
        end_time   = "2024-01-01 01:00:00",
        asset_id   = "solusdt_fw60",
    )

    df = query_range(str(data_dir), "features")

    assert "trg_l_fw60_q90" in df.columns
    assert "trg_l_fw240_q90" not in df.columns
    assert "trg_s_fw240_q10" not in df.columns
    assert "feat_rsi_14" in df.columns
    assert df["open_time"].is_unique
    assert len(df) == 61

    non_null = df["trg_l_fw60_q90"].dropna()
    assert set(non_null.unique()).issubset({0, 1})
    assert df["trg_l_fw60_q90"].isna().sum() == 60


# %% Leak-prevention tests


def test_day_range_position_no_intraday_future_leak() -> None:
    """Verify day range position does not use future same-day bars."""
    ohlcv  = _build_ohlcv_indexed(rows=1600)
    cutoff = 700

    feats_full  = _compute_session_features_with_lag(ohlcv)
    feats_trunc = _compute_session_features_with_lag(ohlcv[:cutoff + 1])

    col = "feat_day_range_position"
    assert col in feats_full.columns, f"{col} missing"

    arr_full  = feats_full[col].to_numpy()
    arr_trunc = feats_trunc[col].to_numpy()

    for i in range(1, cutoff - 1):
        v_full  = arr_full[i]
        v_trunc = arr_trunc[i]
        if np.isnan(v_full) and np.isnan(v_trunc):
            continue
        assert abs(v_full - v_trunc) < 1e-10, (
            f"feat_day_range_position differs at row {i}: full={v_full} trunc={v_trunc}"
        )


def test_ohlcv_features_independent_of_appended_future_bars() -> None:
    """Verify OHLCV-derived features do not change when future bars are appended."""
    ohlcv_short = _build_ohlcv_indexed(rows=300)
    ohlcv_long  = _build_ohlcv_indexed(rows=400)

    def _compute(df: pl.DataFrame) -> pl.DataFrame:
        """Compute a small OHLCV-derived feature set through Polars."""
        indicators = {
            "momentum"          : {"rsi": [{"window": 14}], "roc": [{"window": 14}]},
            "trend"             : {"sma": [{"window": 14}]},
            "volatility"        : {"bollinger": [{"window": 14, "window_dev": 2}]},
            "price_action"      : {"returns": [{"type": "log"}]},
        }
        return compute_features_polars(
            df_ohlcv           = df,
            indicators         = indicators,
            feat_prefix        = "feat_",
            available_activity = [],
            targets_cfg        = [],
        )

    f_short = _compute(ohlcv_short)
    f_long  = _compute(ohlcv_long)

    shared_cols = [
        c for c in f_short.columns
        if c.startswith("feat_") and c not in T_MINUS_1_SKIP
    ]
    compare_end = len(f_short) - 2
    for col in shared_cols:
        short_arr   = f_short[col].to_numpy()[1:compare_end]
        long_arr    = f_long[col].to_numpy()[1:compare_end]
        both_nan    = np.isnan(short_arr) & np.isnan(long_arr)
        both_valid  = ~np.isnan(short_arr) & ~np.isnan(long_arr)
        close_match = both_valid & (np.abs(short_arr - long_arr) < 1e-10)
        mismatches  = ~(both_nan | close_match)
        assert not mismatches.any(), (
            f"{col}: {mismatches.sum()} mismatches when extra future bars are added"
        )


def test_deterministic_time_features_stable_across_dataset_sizes() -> None:
    """Verify timestamp-derived P2 features do not depend on dataset size."""
    ohlcv_short = _build_ohlcv_indexed(rows=200)
    ohlcv_long  = _build_ohlcv_indexed(rows=350)

    def _compute_time_feats(df: pl.DataFrame) -> pl.DataFrame:
        """Compute session and time-index features through Polars."""
        indicators = {"session_relative": True, "time_session": True}
        return compute_features_polars(
            df_ohlcv           = df,
            indicators         = indicators,
            feat_prefix        = "feat_",
            available_activity = [],
            targets_cfg        = [],
        )

    f_short = _compute_time_feats(ohlcv_short)
    f_long  = _compute_time_feats(ohlcv_long)

    time_only_cols = [
        c for c in f_short.columns
        if c in T_MINUS_1_SKIP and c != "feat_bars_into_session_norm"
    ]
    assert time_only_cols, "No time-index feature columns found"

    n = len(ohlcv_short)
    for col in time_only_cols:
        short_arr   = f_short[col].to_numpy()[:n]
        long_arr    = f_long[col].to_numpy()[:n]
        both_nan    = np.isnan(short_arr) & np.isnan(long_arr)
        both_valid  = ~np.isnan(short_arr) & ~np.isnan(long_arr)
        close_match = both_valid & (np.abs(short_arr - long_arr) < 1e-10)
        mismatches  = ~(both_nan | close_match)
        assert not mismatches.any(), (
            f"{col}: {mismatches.sum()} mismatches - P2 feature must be timestamp-only"
        )
