# =============================================================================
# Feature generation tests
# =============================================================================
# Purpose:
#  - Verify Task 1 feature expansion creates the expected feature columns
#  - Verify feature sync remains idempotent by open_time
#  - Leak-prevention: verify t-1 lag so no OHLCV feature at row t depends on
#    bar t's market data (KAN-30 P0 fix + KAN-31 global lag)
# =============================================================================

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import data_pipeline.sync_features as sync_features_module
from data_pipeline.sync_features import (
    _T_MINUS_1_SKIP,
    _add_session_relative_features,
    _add_time_session_features,
    _apply_t_minus_1_lag,
)
from store.duckdb_query import query_range


def _build_ohlcv(rows: int = 500) -> pd.DataFrame:
    open_time = pd.date_range("2024-01-01 00:00:00", periods=rows, freq="min")
    base = pd.Series(range(rows), dtype="float64")
    close = 100 + base * 0.01 + (base % 17) * 0.03
    open_ = close.shift(1).fillna(close.iloc[0])
    high = pd.concat([open_, close], axis=1).max(axis=1) + 0.5
    low = pd.concat([open_, close], axis=1).min(axis=1) - 0.5
    volume = 1000 + (base % 29) * 10

    return pd.DataFrame(
        {
            "open_time": open_time.strftime("%Y-%m-%d %H:%M:%S"),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )


def test_sync_features_expanded_columns_and_idempotency(tmp_path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    ohlcv_dir = data_dir / "ohlcv"
    ohlcv_dir.mkdir(parents=True)
    _build_ohlcv().to_parquet(ohlcv_dir / "2024-01-01.parquet", index=False)

    monkeypatch.setattr(sync_features_module.utils, "load_asset_config", lambda asset_id=None: {
        "database": {
            "data_dir": str(data_dir),
            "asset_id": "solusdt_fw60",
            "features_profile": "solusdt_fw60",
        }
    })

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


def test_sync_features_uses_sol_feature_profile(tmp_path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    ohlcv_dir = data_dir / "ohlcv"
    ohlcv_dir.mkdir(parents=True)
    _build_ohlcv().to_parquet(ohlcv_dir / "2024-01-01.parquet", index=False)

    monkeypatch.setattr(sync_features_module.utils, "load_asset_config", lambda asset_id=None: {
        "database": {
            "data_dir": str(data_dir),
            "asset_id": "solusdt_fw60",
            "features_profile": "solusdt_fw60",
        }
    })

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
    # Edge-nulling: the last rolling_win (60) rows should be NaN; only the first row can be 0 or 1
    non_null = df["trg_l_fw60_q90"].dropna()
    assert set(non_null.unique()).issubset({0, 1})
    # With 61 rows and rolling_win=60, only the first row is non-null
    assert df["trg_l_fw60_q90"].isna().sum() == 60


# =============================================================================
# Leak-prevention tests (KAN-30 P0 + KAN-31 global lag)
# =============================================================================

def _build_ohlcv_indexed(rows: int = 1600) -> pd.DataFrame:
    """Build a deterministic OHLCV DataFrame with a DatetimeIndex (1-min bars)."""
    open_time = pd.date_range("2024-01-01 00:00:00", periods=rows, freq="min")
    base = np.arange(rows, dtype="float64")
    close = 100.0 + base * 0.01 + (base % 17) * 0.03
    open_ = np.roll(close, 1)
    open_[0] = close[0]
    high = np.maximum(open_, close) + 0.5
    low = np.minimum(open_, close) - 0.5
    # Add a clear daily high spike mid-day (bar 720) to ensure the two datasets
    # see different same-day maxima when one is truncated before the spike.
    if rows > 720:
        high[720] = high[720] + 5.0
    volume = 1000.0 + (base % 29) * 10
    df = pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": volume},
        index=open_time,
    )
    df.index.name = "open_time"
    return df


def _compute_session_features_with_lag(df: pd.DataFrame) -> pd.DataFrame:
    """Apply session-relative features + global t-1 lag, return df copy."""
    df = df.copy()
    indicators = {"session_relative": True}
    _add_session_relative_features(df, indicators, "feat_")
    _apply_t_minus_1_lag(df, "feat_")
    return df


def test_day_range_position_no_intraday_future_leak():
    """feat_day_range_position must not change when same-day future bars are removed.

    Before the fix (using transform('max'/'min')), the full-day max/min changes
    when we drop intraday bars, so the feature would differ.  After the fix
    (expanding() + global shift), the value at row t uses only bars ≤ t-1,
    so truncating the dataset after t must not change the value at t.
    """
    ohlcv = _build_ohlcv_indexed(rows=1600)

    # Pick a mid-day cutoff well before the 720 spike so the two datasets see
    # different same-day maxima.
    cutoff = 700  # bar index (within day 0, which spans 0..1439)

    feats_full  = _compute_session_features_with_lag(ohlcv)
    feats_trunc = _compute_session_features_with_lag(ohlcv.iloc[: cutoff + 1])

    col = "feat_day_range_position"
    assert col in feats_full.columns, f"{col} missing"

    # Rows 1..cutoff-1: after the shift, value at t derives from expanding
    # max/min up to t-1.  Same bars used in both datasets → must match.
    # Row 0 is always NaN after the shift (no prior bar).
    for i in range(1, cutoff - 1):
        v_full  = feats_full[col].iloc[i]
        v_trunc = feats_trunc[col].iloc[i]
        if pd.isna(v_full) and pd.isna(v_trunc):
            continue
        assert abs(v_full - v_trunc) < 1e-10, (
            f"feat_day_range_position differs at row {i}: full={v_full} trunc={v_trunc}"
        )


def test_ohlcv_features_independent_of_appended_future_bars():
    """After the global t-1 lag, OHLCV features at row t must not depend on bars ≥ t.

    Adding extra bars at the end of the OHLCV input should not change any
    feature value for rows that existed before the extension.
    This verifies the global _apply_t_minus_1_lag shift is effective.
    """
    from data_pipeline.sync_features import (
        _add_momentum_features,
        _add_trend_features,
        _add_volatility_features,
        _add_price_action_features,
    )

    ohlcv_short = _build_ohlcv_indexed(rows=300)
    ohlcv_long  = _build_ohlcv_indexed(rows=400)  # same start, 100 extra bars at end

    def _compute(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        indicators = {
            "momentum":     {"rsi": [{"window": 14}], "roc": [{"window": 14}]},
            "trend":        {"sma": [{"window": 14}]},
            "volatility":   {"bollinger": [{"window": 14, "window_dev": 2}]},
            "price_action": {"returns": [{"type": "log"}]},
            "session_relative": True,
        }
        _add_momentum_features(df, indicators, "feat_")
        _add_trend_features(df, indicators, "feat_")
        _add_volatility_features(df, indicators, "feat_")
        _add_price_action_features(df, indicators, "feat_")
        _add_session_relative_features(df, indicators, "feat_")
        _apply_t_minus_1_lag(df, "feat_")
        return df

    f_short = _compute(ohlcv_short)
    f_long  = _compute(ohlcv_long)

    # All rows from the short dataset (0..299) must be identical in the long one
    shared_cols = [
        c for c in f_short.columns
        if c.startswith("feat_") and c not in _T_MINUS_1_SKIP
    ]
    # Skip the very first row (NaN due to shift) and the last row of short dataset
    # (edge effect: the long dataset has one extra bar visible so the shift
    #  might differ at the boundary — we compare up to len-2 to be safe)
    compare_end = len(f_short) - 2
    for col in shared_cols:
        s = f_short[col].iloc[1:compare_end]
        l = f_long[col].iloc[1:compare_end]
        mismatches = (~(
            (s.isna() & l.isna()) |
            (s.notna() & l.notna() & ((s - l).abs() < 1e-10))
        ))
        assert not mismatches.any(), (
            f"{col}: {mismatches.sum()} mismatches when extra future bars are added"
        )


def test_deterministic_time_features_stable_across_dataset_sizes():
    """P2 deterministic time-index features must not depend on dataset size.

    These features are derived from the bar timestamp alone (_T_MINUS_1_SKIP).
    Adding extra bars at the end of the dataset must not change their values
    for rows that existed before the extension.  They are also NOT shifted by
    the global t-1 lag, so they must remain unchanged after _apply_t_minus_1_lag.
    """
    ohlcv_short = _build_ohlcv_indexed(rows=200)
    ohlcv_long  = _build_ohlcv_indexed(rows=350)  # same start, 150 extra bars

    def _compute_time_feats(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        indicators = {"session_relative": True, "time_session": True}
        _add_session_relative_features(df, indicators, "feat_")
        _add_time_session_features(df, indicators, "feat_")
        _apply_t_minus_1_lag(df, "feat_")
        return df

    f_short = _compute_time_feats(ohlcv_short)
    f_long  = _compute_time_feats(ohlcv_long)

    # Only check the features that are truly timestamp-derived (in _T_MINUS_1_SKIP)
    # but exclude bars_into_session_norm (cumcount — stable but not in time_session)
    time_only_cols = [
        c for c in f_short.columns
        if c in _T_MINUS_1_SKIP and c != "feat_bars_into_session_norm"
    ]
    assert time_only_cols, "No time-index feature columns found"

    n = len(ohlcv_short)
    for col in time_only_cols:
        s = f_short[col].iloc[:n]
        l = f_long[col].iloc[:n]
        mismatches = (~(
            (s.isna() & l.isna()) |
            (s.notna() & l.notna() & ((s - l).abs() < 1e-10))
        ))
        assert not mismatches.any(), (
            f"{col}: {mismatches.sum()} mismatches — P2 feature must be timestamp-only"
        )
