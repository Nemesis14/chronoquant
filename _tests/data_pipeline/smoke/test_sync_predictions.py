"""Smoke tests for the prediction sync pipeline.

Verifies that sync_predictions writes long_pred/short_pred columns, stays
idempotent, and handles champion model resolution. Champion model loading
is mocked — no real model artifacts required.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pytest

import data_pipeline.sync_predictions as sync_predictions_module
from store.duckdb_query import query_range, row_count
from store.duckdb_store import (
    ensure_tables,
    get_connection,
    insert_feat_ohlcv_quant,
    insert_ohlcv,
    insert_target,
)

pytestmark = pytest.mark.smoke

_HORIZON = 60


# %% Mock objects


class _MockModel:
    """Minimal model stub returning constant probability 0.6."""

    feature_names_in_ = None

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        return np.column_stack([np.full(len(X), 0.4), np.full(len(X), 0.6)])


# %% Test data builders


def _build_ohlcv(rows: int = 150) -> pd.DataFrame:
    open_time = pd.date_range("2024-01-01 00:00:00", periods=rows, freq="min")
    close     = pd.Series(100.0 + np.arange(rows) * 0.01)
    open_     = close.shift(1).fillna(close.iloc[0])
    return pd.DataFrame(
        {
            "open_time"       : open_time,
            "open"            : open_,
            "high"            : close + 0.5,
            "low"             : close - 0.5,
            "close"           : close,
            "volume"          : 1000.0,
            "quote_volume"    : 100_000.0,
            "trades"          : 10,
            "taker_buy_base"  : 400.0,
            "taker_buy_quote" : 40_000.0,
        }
    )


def _build_features(ohlcv: pd.DataFrame, feat_cols: list[str]) -> pd.DataFrame:
    rows   = len(ohlcv)
    result = pd.DataFrame(ohlcv[["open_time", "close"]].copy())
    result["available_ts"]    = result["open_time"]
    result["lookback_end_ts"] = result["open_time"]
    for col in feat_cols:
        result[col] = np.random.default_rng(42).uniform(0, 1, rows)
    return result


def _build_target(ohlcv: pd.DataFrame) -> pl.DataFrame:
    return pl.DataFrame({
        "open_time"       : ohlcv["open_time"].values,
        "close"           : ohlcv["close"].values,
        "trg_l_fw60_q90"  : [i % 10 == 0 for i in range(len(ohlcv))],
        "trg_s_fw60_q10"  : [i % 10 == 1 for i in range(len(ohlcv))],
    })


def _seed_store(db_path: Path, feat_cols: list[str]) -> None:
    conn  = get_connection(str(db_path))
    ohlcv = _build_ohlcv()
    try:
        ensure_tables(conn)
        insert_ohlcv(conn, ohlcv)
        insert_feat_ohlcv_quant(conn, _build_features(ohlcv, feat_cols))
        insert_target(conn, _build_target(ohlcv))
    finally:
        conn.close()


def _asset_cfg(db_path: Path) -> dict:
    return {
        "database": {
            "db_path"       : str(db_path),
            "asset_id"      : "solusdt_fw60",
            "features_profile": "solusdt_fw60",
        }
    }


def _features_cfg() -> dict:
    return {
        "database": {
            "features": {
                "targets": [
                    {
                        "direction"      : "long",
                        "name"           : "trg_l_fw60_q90",
                        "rolling_window" : _HORIZON,
                        "percentile"     : 0.9,
                    },
                    {
                        "direction"      : "short",
                        "name"           : "trg_s_fw60_q10",
                        "rolling_window" : _HORIZON,
                        "percentile"     : 0.1,
                    },
                ]
            }
        }
    }


def _models_cfg() -> dict:
    return {
        "models": {
            "lgbm_solusdt_l_fw60_q90_local_v4": {
                "active"     : True,
                "target_name": "trg_l_fw60_q90",
                "trainer"    : "lightgbm_binary",
                "predict"    : {"method": "predict_proba"},
                "paths"      : {
                    "model_dir"    : "models/lgbm_solusdt_l_fw60_q90_local_v4",
                    "model_file"   : "model.pkl",
                    "features_file": "features.json",
                },
            },
            "lgbm_solusdt_s_fw60_q10_local_v4": {
                "active"     : True,
                "target_name": "trg_s_fw60_q10",
                "trainer"    : "lightgbm_binary",
                "predict"    : {"method": "predict_proba"},
                "paths"      : {
                    "model_dir"    : "models/lgbm_solusdt_s_fw60_q10_local_v4",
                    "model_file"   : "model.pkl",
                    "features_file": "features.json",
                },
            },
        }
    }


_FEAT_COLS = ["feat_rsi_14", "feat_roc_14", "feat_sma_14"]


# %% Tests


def test_sync_predictions_writes_long_and_short_columns(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify that sync_predictions writes long_pred and short_pred for all rows."""
    db_path = tmp_path / "asset.duckdb"
    _seed_store(db_path, _FEAT_COLS)

    def _mock_load_artifacts(model_id, model_meta):
        return _MockModel(), _FEAT_COLS

    monkeypatch.setattr(
        sync_predictions_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )
    monkeypatch.setattr(
        sync_predictions_module.utils, "load_features_config",
        lambda asset_id=None: _features_cfg(),
    )
    monkeypatch.setattr(
        sync_predictions_module.utils, "load_models_config",
        lambda: _models_cfg(),
    )
    monkeypatch.setattr(
        sync_predictions_module.utils, "resolve_asset_id",
        lambda asset_id=None: "solusdt_fw60",
    )
    monkeypatch.setattr(
        sync_predictions_module.utils, "champion_models_for_asset",
        lambda model_cfg, asset_id: (
            "lgbm_solusdt_l_fw60_q90_local_v4",
            _models_cfg()["models"]["lgbm_solusdt_l_fw60_q90_local_v4"],
            "lgbm_solusdt_s_fw60_q10_local_v4",
            _models_cfg()["models"]["lgbm_solusdt_s_fw60_q10_local_v4"],
        ),
    )
    monkeypatch.setattr(
        sync_predictions_module, "_load_model_artifacts",
        _mock_load_artifacts,
    )

    sync_predictions_module.sync_predictions(
        start_time = "2024-01-01 00:00:00",
        end_time   = None,
    )

    df = query_range(str(db_path), "predictions")

    assert "long_pred" in df.columns,  "long_pred column missing from predictions"
    assert "short_pred" in df.columns, "short_pred column missing from predictions"
    assert len(df) > 0,                "predictions table is empty"
    assert (df["long_pred"].between(0, 1)).all(),  "long_pred values out of [0,1]"
    assert (df["short_pred"].between(0, 1)).all(), "short_pred values out of [0,1]"


def test_sync_predictions_is_idempotent(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify that running sync_predictions twice does not duplicate rows."""
    db_path = tmp_path / "asset.duckdb"
    _seed_store(db_path, _FEAT_COLS)

    def _mock_load_artifacts(model_id, model_meta):
        return _MockModel(), _FEAT_COLS

    monkeypatch.setattr(
        sync_predictions_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )
    monkeypatch.setattr(
        sync_predictions_module.utils, "load_features_config",
        lambda asset_id=None: _features_cfg(),
    )
    monkeypatch.setattr(
        sync_predictions_module.utils, "load_models_config",
        lambda: _models_cfg(),
    )
    monkeypatch.setattr(
        sync_predictions_module.utils, "resolve_asset_id",
        lambda asset_id=None: "solusdt_fw60",
    )
    monkeypatch.setattr(
        sync_predictions_module.utils, "champion_models_for_asset",
        lambda model_cfg, asset_id: (
            "lgbm_solusdt_l_fw60_q90_local_v4",
            _models_cfg()["models"]["lgbm_solusdt_l_fw60_q90_local_v4"],
            "lgbm_solusdt_s_fw60_q10_local_v4",
            _models_cfg()["models"]["lgbm_solusdt_s_fw60_q10_local_v4"],
        ),
    )
    monkeypatch.setattr(
        sync_predictions_module, "_load_model_artifacts",
        _mock_load_artifacts,
    )

    sync_predictions_module.sync_predictions(start_time="2024-01-01 00:00:00")
    count_after_first = row_count(str(db_path), "predictions")

    sync_predictions_module.sync_predictions(start_time="2024-01-01 00:00:00")
    count_after_second = row_count(str(db_path), "predictions")

    assert count_after_first == count_after_second, (
        f"Row count changed on second run: {count_after_first} → {count_after_second}"
    )
