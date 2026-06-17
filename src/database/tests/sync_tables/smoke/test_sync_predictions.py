"""Smoke tests for the prediction sync pipeline.

Verifies that sync_predictions writes long_pred/short_pred columns, stays
idempotent, and handles champion model resolution. Champion model loading
is mocked — no real model artifacts required.
"""

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pytest

import database.sync_tables.sync_predictions as sync_predictions_module
from database.store.duckdb_query import query_range, row_count
from database.store.duckdb_store import (
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


def _build_ohlcv(rows: int = 150) -> pl.DataFrame:
    close = [100.0 + i * 0.01 for i in range(rows)]
    open_ = [close[0]] + close[:-1]
    return pl.DataFrame(
        {
            "open_time"       : [datetime(2024, 1, 1) + timedelta(minutes=i) for i in range(rows)],
            "open"            : open_,
            "high"            : [c + 0.5 for c in close],
            "low"             : [c - 0.5 for c in close],
            "close"           : close,
            "volume"          : [1000.0] * rows,
            "quote_volume"    : [100_000.0] * rows,
            "trades"          : [10] * rows,
            "taker_buy_base"  : [400.0] * rows,
            "taker_buy_quote" : [40_000.0] * rows,
        }
    )


def _build_features(ohlcv: pl.DataFrame, feat_cols: list[str]) -> pl.DataFrame:
    rows   = len(ohlcv)
    rng    = np.random.default_rng(42)
    result = ohlcv.select(["open_time", "close"]).with_columns([
        pl.col("open_time").alias("available_ts"),
        pl.col("open_time").alias("lookback_end_ts"),
    ])
    for col in feat_cols:
        result = result.with_columns(pl.Series(col, rng.uniform(0, 1, rows)))
    return result


def _build_target(ohlcv: pl.DataFrame) -> pl.DataFrame:
    return pl.DataFrame({
        "open_time"      : ohlcv["open_time"].to_list(),
        "close"          : ohlcv["close"].to_list(),
        "long_mfe_fw60"  : [float(i) * 0.001 for i in range(len(ohlcv))],
        "short_mfe_fw60" : [float(-i) * 0.001 for i in range(len(ohlcv))],
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
                "indicators": {}
            }
        }
    }


def _models_cfg() -> dict:
    return {
        "models": {
            "lgbm_solusdt_l_fw60_q90_local_v4": {
                "active"     : True,
                "target_name": "long_mfe_fw60",
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
                "target_name": "short_mfe_fw60",
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
