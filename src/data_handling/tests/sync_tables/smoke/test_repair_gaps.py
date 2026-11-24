"""Regression tests for epic_076 / t947: sporadic 1m gaps in feat_ohlcv_quant / predictions.

Root cause: a live sync_full job inserted an OHLCV bar, then failed before the
feature write; the next job started features after the *new* OHLCV tail, and the
append-only feat_ohlcv_quant / predictions inserts could never fill the hole.

Covers:
  - sync_features catch-up start (no new hole after a failed job)
  - backfill_feat_ohlcv_quant / sync_features(backfill=True) interior gap fill
  - repair_gaps: detection (dry run) + feature-then-prediction backfill, idempotent
Isolated tmp_path stores; model loading and the registry deploy check are mocked.
"""

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

import data_handling.sync_tables.repair_gaps as repair_module
import data_handling.sync_tables.sync_features as sync_features_module
import data_handling.sync_tables.sync_predictions as sync_predictions_module
from data_handling.store.duckdb_query import query_range_pl
from data_handling.store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

pytestmark = pytest.mark.smoke

_T0        = datetime(2024, 1, 1)
_FEAT_COLS = ["feat_rsi_14", "feat_sma_ratio_14"]
_LONG      = "lgbm_test_l_fw60"
_SHORT     = "lgbm_test_s_fw60"


class _MockModel:
    """Deterministic stub: probability = sigmoid(sum of features)."""

    feature_names_in_ = None

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        p = 1.0 / (1.0 + np.exp(-np.asarray(X).sum(axis=1) / 100.0))
        return np.column_stack([1.0 - p, p])


# %% Fixtures / helpers


def _ts(minutes: int) -> str:
    return (_T0 + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")


def _seed_ohlcv(db_path: Path, rows: int = 300) -> None:
    close = [100 + i * 0.01 + (i % 17) * 0.03 for i in range(rows)]
    open_ = [close[0]] + close[:-1]
    df = pl.DataFrame({
        "open_time" : [_T0 + timedelta(minutes=i) for i in range(rows)],
        "open"      : open_,
        "high"      : [max(o, c) + 0.5 for o, c in zip(open_, close, strict=True)],
        "low"       : [min(o, c) - 0.5 for o, c in zip(open_, close, strict=True)],
        "close"     : close,
        "volume"    : [1000.0 + (i % 29) * 10 for i in range(rows)],
    })
    conn = get_connection(str(db_path))
    try:
        ensure_tables(conn)
        insert_ohlcv(conn, df)
    finally:
        conn.close()


def _delete(db_path: Path, table: str, start: str, end: str) -> None:
    conn = get_connection(str(db_path))
    try:
        conn.execute(f"DELETE FROM {table} WHERE open_time BETWEEN ? AND ?", [start, end])
    finally:
        conn.close()


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "asset.duckdb"
    _seed_ohlcv(db_path)

    utils = sync_features_module.utils  # shared module object for all three modules
    meta = {"target_name": "long_mfe_fw60", "predict": {"method": "predict_proba"}}
    smeta = {"target_name": "short_mfe_fw60", "predict": {"method": "predict_proba"}}
    monkeypatch.setattr(utils, "load_asset_config", lambda asset_id=None: {
        "database": {"db_path": str(db_path), "asset_id": "testusdt"}
    })
    monkeypatch.setattr(utils, "resolve_asset_id", lambda asset_id=None: "testusdt")
    monkeypatch.setattr(utils, "load_features_config", lambda asset_id=None: {
        "database": {"features": {
            "indicators": {"momentum": {"rsi": [{"window": 14}]},
                           "trend": {"sma": [{"window": 14}]}},
            "targets": [{"rolling_window": 60}],
        }}
    })
    monkeypatch.setattr(utils, "load_models_config", lambda: {"models": {}})
    monkeypatch.setattr(
        utils, "champion_models_for_asset",
        lambda cfg, asset_id: (_LONG, meta, _SHORT, smeta),
    )
    monkeypatch.setattr(
        sync_predictions_module, "_load_model_artifacts",
        lambda model_id, model_meta: (_MockModel(), _FEAT_COLS),
    )
    # never touch the real registry from tests
    monkeypatch.setattr(sync_predictions_module, "_detect_pending_deployment", lambda asset_id: None)
    return db_path


def _times(db_path: Path, table: str) -> list[datetime]:
    return query_range_pl(str(db_path), table, columns=["open_time"])["open_time"].to_list()


def _contiguous(times: list[datetime]) -> bool:
    return all(b - a == timedelta(minutes=1) for a, b in zip(times, times[1:], strict=False))


# %% Tests


def test_failed_job_does_not_leave_feature_or_prediction_gap(store: Path) -> None:
    """Replays the live sync_full sequence around a job that failed after the OHLCV insert."""
    # job N: features + predictions up to 02:00 (minute 120)
    sync_features_module.sync_features(_ts(0), end_time=_ts(120))
    sync_predictions_module.sync_predictions(_ts(0), end_time=_ts(120))

    # job N+1 inserted OHLCV 02:01 then failed before sync_features (DuckDB lock).
    # job N+2: sync_full passes start = next(OHLCV tail before the run) = 02:02.
    sync_features_module.sync_features(_ts(122), end_time=_ts(125))
    pred_tail = _times(store, "predictions")[-1]
    sync_predictions_module.sync_predictions(
        (pred_tail + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"), end_time=_ts(125)
    )

    feat = _times(store, "feat_ohlcv_quant")
    pred = _times(store, "predictions")
    assert _T0 + timedelta(minutes=121) in feat, "catch-up must compute the skipped minute"
    assert feat[-1] == _T0 + timedelta(minutes=125)
    assert _contiguous(feat)
    assert _contiguous(pred)
    assert pred[-1] == _T0 + timedelta(minutes=125)


def test_catch_up_noop_when_tail_is_adjacent(store: Path) -> None:
    sync_features_module.sync_features(_ts(0), end_time=_ts(60))
    assert sync_features_module._catch_up_start(str(store), _ts(61)) == _ts(61)
    assert sync_features_module._catch_up_start(str(store), _ts(30)) == _ts(30)
    assert sync_features_module._catch_up_start(str(store), _ts(90)) == _ts(61)


def test_repair_gaps_backfills_features_then_predictions(store: Path) -> None:
    sync_features_module.sync_features(_ts(0), end_time=_ts(200))
    sync_predictions_module.sync_predictions(_ts(0), end_time=_ts(200))
    feat_ref = query_range_pl(str(store), "feat_ohlcv_quant")
    pred_ref = query_range_pl(str(store), "predictions")

    # historical holes: features+predictions 01:30-01:32, a single minute at 02:40,
    # and a prediction-only hole at 03:00
    _delete(store, "feat_ohlcv_quant", _ts(90), _ts(92))
    _delete(store, "predictions", _ts(90), _ts(92))
    _delete(store, "feat_ohlcv_quant", _ts(160), _ts(160))
    _delete(store, "predictions", _ts(160), _ts(160))
    _delete(store, "predictions", _ts(180), _ts(180))

    dry = repair_module.repair_gaps(dry_run=True)
    assert dry.feature_ranges == [(_ts(90), _ts(92)), (_ts(160), _ts(160))]
    assert dry.missing_feature_rows == 4
    assert dry.prediction_ranges == [(_ts(90), _ts(92)), (_ts(160), _ts(160)), (_ts(180), _ts(180))]
    assert dry.missing_prediction_rows == 5
    assert len(_times(store, "feat_ohlcv_quant")) == len(feat_ref) - 4  # dry run wrote nothing

    res = repair_module.repair_gaps()
    assert res.remaining_feature_rows == 0
    assert res.remaining_prediction_rows == 0

    feat = query_range_pl(str(store), "feat_ohlcv_quant")
    pred = query_range_pl(str(store), "predictions")
    assert feat["open_time"].to_list() == feat_ref["open_time"].to_list()
    assert pred["open_time"].to_list() == pred_ref["open_time"].to_list()
    # backfilled values equal the original live computation (deterministic warmup)
    for col in _FEAT_COLS:
        assert np.allclose(feat[col].to_numpy(), feat_ref[col].to_numpy(), equal_nan=True)
    assert np.allclose(pred["long_pred"].to_numpy(), pred_ref["long_pred"].to_numpy())
    assert np.allclose(pred["short_pred"].to_numpy(), pred_ref["short_pred"].to_numpy())

    # idempotent: second run finds nothing
    again = repair_module.repair_gaps()
    assert again.missing_feature_rows == 0
    assert again.missing_prediction_rows == 0
