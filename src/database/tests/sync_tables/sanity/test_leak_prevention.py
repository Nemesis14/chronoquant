"""Sanity tests for look-ahead leak prevention in feature computation.

Verifies that OHLCV-derived and session-relative features do not depend on
bars that would not yet be available at the time of prediction. These tests
use only synthetic data — no real database required.
"""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from database.sync_tables._features_polars import T_MINUS_1_SKIP, compute_features_polars

pytestmark = pytest.mark.sanity


# %% Test data builders


def _build_ohlcv(rows: int = 1600) -> pl.DataFrame:
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


def _compute_session_features(df: pl.DataFrame) -> pl.DataFrame:
    return compute_features_polars(
        df_ohlcv           = df,
        indicators         = {"session_relative": True},
        feat_prefix        = "feat_",
        available_activity = [],
        targets_cfg        = [],
    )


# %% Tests


def test_day_range_position_no_intraday_future_leak() -> None:
    """feat_day_range_position must not change when future bars are appended."""
    ohlcv  = _build_ohlcv(rows=1600)
    cutoff = 700

    feats_full  = _compute_session_features(ohlcv)
    feats_trunc = _compute_session_features(ohlcv[:cutoff + 1])

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
            f"feat_day_range_position differs at row {i}: "
            f"full={v_full} trunc={v_trunc}"
        )


def test_ohlcv_features_independent_of_appended_future_bars() -> None:
    """OHLCV-derived features must not change when future rows are appended."""
    ohlcv_short = _build_ohlcv(rows=300)
    ohlcv_long  = _build_ohlcv(rows=400)

    def _compute(df: pl.DataFrame) -> pl.DataFrame:
        indicators = {
            "momentum"     : {"rsi": [{"window": 14}], "roc": [{"window": 14}]},
            "trend"        : {"sma": [{"window": 14}]},
            "volatility"   : {"bollinger": [{"window": 14, "window_dev": 2}]},
            "price_action" : {"returns": [{"type": "log"}]},
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
        short_arr  = f_short[col].to_numpy()[1:compare_end]
        long_arr   = f_long[col].to_numpy()[1:compare_end]
        both_nan   = np.isnan(short_arr) & np.isnan(long_arr)
        both_valid = ~np.isnan(short_arr) & ~np.isnan(long_arr)
        close_match = both_valid & (np.abs(short_arr - long_arr) < 1e-10)
        mismatches  = ~(both_nan | close_match)
        assert not mismatches.any(), (
            f"{col}: {mismatches.sum()} mismatches when extra future bars are added"
        )


def test_deterministic_time_features_stable_across_dataset_sizes() -> None:
    """Timestamp-derived features must be identical regardless of dataset length."""
    ohlcv_short = _build_ohlcv(rows=200)
    ohlcv_long  = _build_ohlcv(rows=350)

    def _compute_time_feats(df: pl.DataFrame) -> pl.DataFrame:
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
        short_arr  = f_short[col].to_numpy()[:n]
        long_arr   = f_long[col].to_numpy()[:n]
        both_nan   = np.isnan(short_arr) & np.isnan(long_arr)
        both_valid = ~np.isnan(short_arr) & ~np.isnan(long_arr)
        close_match = both_valid & (np.abs(short_arr - long_arr) < 1e-10)
        mismatches  = ~(both_nan | close_match)
        assert not mismatches.any(), (
            f"{col}: {mismatches.sum()} mismatches — must be timestamp-only"
        )
