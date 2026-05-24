# =============================================================================
# Feature generation tests
# =============================================================================
# Purpose:
#  - Verify Task 1 feature expansion creates the expected feature columns
#  - Verify feature sync remains idempotent by open_time
# =============================================================================

import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import data_pipeline.sync_features as sync_features_module


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
    db_path     = tmp_path / "features_test.db"
    table_ohlcv = "bchusdt_1m"
    table_feat  = "bchusdt_1m_features"

    with sqlite3.connect(db_path) as conn:
        _build_ohlcv().to_sql(table_ohlcv, conn, index=False, if_exists="replace")
        conn.execute(
            f"""
            CREATE TABLE {table_feat} (
                open_time TEXT,
                close REAL,
                trg_l_fw240_q90 INTEGER,
                trg_s_fw240_q10 INTEGER,
                feat_rsi_14 REAL
            )
            """,
        )

    db_cfg = {
        "database": {
            "db_path": str(db_path),
            "tables": {
                "ohlcv": table_ohlcv,
                "features": table_feat,
                "predictions": "bchusdt_1m_predictions",
            },
        },
    }
    monkeypatch.setattr(sync_features_module.utils, "load_db_config", lambda: db_cfg)

    sync_features_module.sync_features(
        start_time = "2024-01-01 04:00:00",
        end_time   = "2024-01-01 06:00:00",
    )
    sync_features_module.sync_features(
        start_time = "2024-01-01 04:00:00",
        end_time   = "2024-01-01 06:00:00",
    )

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table_feat}", conn)

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
