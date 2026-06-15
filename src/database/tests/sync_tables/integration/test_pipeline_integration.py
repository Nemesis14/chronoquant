"""Integration tests for the ohlcv → features → target → predictions pipeline.

Verifies cross-layer data flow and timestamp alignment across all four tables.
Uses synthetic data and a mocked model — no real DB or model artifacts required.
"""

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pytest

import database.sync_tables.sync_predictions as sync_predictions_module
from database.store.duckdb_store import (
    ensure_tables,
    get_connection,
    insert_feat_ohlcv_quant,
    insert_ohlcv,
    insert_target,
)

pytestmark = pytest.mark.integration

_HORIZON   = 60
_N_ROWS    = 250
_FEAT_COLS = ["feat_rsi_14", "feat_roc_14", "feat_sma_14"]


# %% Helpers


class _MockModel:
    feature_names_in_ = None

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        return np.column_stack([np.full(len(X), 0.35), np.full(len(X), 0.65)])


def _build_ohlcv(rows: int = _N_ROWS) -> pd.DataFrame:
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


def _build_features(ohlcv: pd.DataFrame) -> pd.DataFrame:
    rng    = np.random.default_rng(42)
    result = pd.DataFrame(ohlcv[["open_time", "close"]].copy())
    result["available_ts"]    = result["open_time"]
    result["lookback_end_ts"] = result["open_time"]
    for col in _FEAT_COLS:
        result[col] = rng.uniform(0, 1, len(ohlcv))
    return result


def _build_target(ohlcv: pd.DataFrame) -> pl.DataFrame:
    return pl.DataFrame({
        "open_time"      : ohlcv["open_time"].values,
        "close"          : ohlcv["close"].values,
        "trg_l_fw60_q90" : [i % 10 == 0 for i in range(len(ohlcv))],
        "trg_s_fw60_q10" : [i % 10 == 1 for i in range(len(ohlcv))],
    })


def _asset_cfg(db_path: Path) -> dict:
    return {
        "database": {
            "db_path"          : str(db_path),
            "asset_id"         : "solusdt_fw60",
            "features_profile" : "solusdt_fw60",
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


# %% Tests


def test_ohlcv_to_predictions_cross_layer_alignment(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """ohlcv → features → target → predictions: all layers share the same open_time set."""
    db_path = tmp_path / "asset.duckdb"
    ohlcv   = _build_ohlcv()

    # --- seed all three upstream layers ---
    conn = get_connection(str(db_path))
    ensure_tables(conn)
    insert_ohlcv(conn, ohlcv)
    insert_feat_ohlcv_quant(conn, _build_features(ohlcv))
    insert_target(conn, _build_target(ohlcv))
    conn.close()

    # --- run sync_predictions with mocked config and model ---
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
        lambda model_id, model_meta: (_MockModel(), _FEAT_COLS),
    )

    sync_predictions_module.sync_predictions(start_time="2024-01-01 00:00:00")

    # --- cross-layer assertions ---
    c = duckdb.connect(str(db_path), read_only=True)
    try:
        _count = c.execute("SELECT COUNT(*) FROM predictions").fetchone()
        pred_count = _count[0] if _count else 0
        assert pred_count > 0, "predictions table is empty after sync"

        # predictions.open_time ⊆ ohlcv.open_time
        _r = c.execute("""
            SELECT COUNT(*) FROM predictions p
            WHERE NOT EXISTS (SELECT 1 FROM ohlcv o WHERE o.open_time = p.open_time)
        """).fetchone()
        assert (_r[0] if _r else 0) == 0, (
            f"{_r[0] if _r else '?'} predictions rows have no matching ohlcv row"
        )

        # predictions.open_time ⊆ feat_ohlcv_quant.open_time
        _r = c.execute("""
            SELECT COUNT(*) FROM predictions p
            WHERE NOT EXISTS (
                SELECT 1 FROM feat_ohlcv_quant f WHERE f.open_time = p.open_time
            )
        """).fetchone()
        assert (_r[0] if _r else 0) == 0, (
            f"{_r[0] if _r else '?'} predictions rows have no matching feat_ohlcv_quant row"
        )

        # predictions.close matches ohlcv.close
        _r = c.execute("""
            SELECT COUNT(*) FROM predictions p
            JOIN ohlcv o ON o.open_time = p.open_time
            WHERE ABS(p.close - o.close) > 1e-8
        """).fetchone()
        assert (_r[0] if _r else 0) == 0, (
            f"{_r[0] if _r else '?'} predictions rows where close != ohlcv.close"
        )

        # scores in [0, 1]
        _r = c.execute("""
            SELECT COUNT(*) FROM predictions
            WHERE long_pred < 0 OR long_pred > 1
               OR short_pred < 0 OR short_pred > 1
        """).fetchone()
        assert (_r[0] if _r else 0) == 0, (
            f"{_r[0] if _r else '?'} predictions rows with scores outside [0, 1]"
        )
    finally:
        c.close()


def test_features_close_matches_ohlcv_close(tmp_path: Path) -> None:
    """feat_ohlcv_quant.close must equal ohlcv.close for every shared open_time."""
    db_path = tmp_path / "asset.duckdb"
    ohlcv   = _build_ohlcv()

    conn = get_connection(str(db_path))
    ensure_tables(conn)
    insert_ohlcv(conn, ohlcv)
    insert_feat_ohlcv_quant(conn, _build_features(ohlcv))
    conn.close()

    c = duckdb.connect(str(db_path), read_only=True)
    try:
        _r = c.execute("""
            SELECT COUNT(*) FROM feat_ohlcv_quant f
            JOIN ohlcv o ON o.open_time = f.open_time
            WHERE ABS(f.close - o.close) > 1e-8
        """).fetchone()
        assert (_r[0] if _r else 0) == 0, (
            f"{_r[0] if _r else '?'} feat_ohlcv_quant rows where close != ohlcv.close"
        )
    finally:
        c.close()


def test_target_open_time_subset_of_ohlcv(tmp_path: Path) -> None:
    """target.open_time must be a subset of ohlcv.open_time."""
    db_path = tmp_path / "asset.duckdb"
    ohlcv   = _build_ohlcv()

    conn = get_connection(str(db_path))
    ensure_tables(conn)
    insert_ohlcv(conn, ohlcv)
    insert_target(conn, _build_target(ohlcv))
    conn.close()

    c = duckdb.connect(str(db_path), read_only=True)
    try:
        _r = c.execute("""
            SELECT COUNT(*) FROM target t
            WHERE NOT EXISTS (SELECT 1 FROM ohlcv o WHERE o.open_time = t.open_time)
        """).fetchone()
        assert (_r[0] if _r else 0) == 0, (
            f"{_r[0] if _r else '?'} target rows have no matching ohlcv row"
        )
    finally:
        c.close()
