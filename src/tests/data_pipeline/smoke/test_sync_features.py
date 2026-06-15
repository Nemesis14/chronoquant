"""Smoke tests for the feature sync pipeline.

Verifies that sync_features writes expected columns, stays idempotent, and
uses the correct asset profile. Uses isolated tmp_path stores, not real data.
"""

from pathlib import Path

import pandas as pd
import pytest

import database.data_pipeline.sync_features as sync_features_module
from database.store.duckdb_query import query_range
from database.store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

pytestmark = pytest.mark.smoke


# %% Test data builders


def _build_ohlcv(rows: int = 500) -> pd.DataFrame:
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
    conn = get_connection(str(data_dir))
    try:
        ensure_tables(conn)
        insert_ohlcv(conn, _build_ohlcv(rows))
    finally:
        conn.close()


def _asset_cfg(data_dir: Path) -> dict:
    return {
        "database": {
            "db_path"          : str(data_dir),
            "asset_id"         : "solusdt_fw60",
            "features_profile" : "solusdt_fw60",
        }
    }


def _features_cfg() -> dict:
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


# %% Tests


def test_sync_features_expanded_columns_and_idempotency(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify feature sync writes expected columns and stays idempotent on re-run."""
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

    df = query_range(str(data_dir), "feat_ohlcv_quant")

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
    """Verify SOLUSDT feature sync uses only the fw60 target profile."""
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

    df = query_range(str(data_dir), "feat_ohlcv_quant")

    # Targets are written to the separate 'target' table by sync_targets, not here
    assert not any(c.startswith("trg_") for c in df.columns), \
        "trg_* columns must not appear in feat_ohlcv_quant"
    assert "feat_rsi_14" in df.columns
    assert df["open_time"].is_unique
    assert len(df) == 61
