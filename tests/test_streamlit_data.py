# =============================================================================
# Streamlit dashboard data helper tests
# =============================================================================

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from streamlit_app import data


def test_prediction_history_uses_latest_stored_timestamp(tmp_path, monkeypatch) -> None:
    import pandas as pd
    now = pd.Timestamp.utcnow().floor("s")
    pred_df = pd.DataFrame({
        "open_time": [
            (now - pd.Timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
            (now - pd.Timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
            now.strftime("%Y-%m-%d %H:%M:%S"),
        ],
        "close": [100.0, 101.0, 102.0],
        "long_pred": [0.10, 0.36, 0.20],
        "short_pred": [0.05, 0.10, 0.15],
    })
    monkeypatch.setattr(data, "latest_open_time", lambda d, ds: now)
    monkeypatch.setattr(data, "query_range",
        lambda d, ds, start, end, columns: pred_df if ds == "predictions" else pd.DataFrame())
    monkeypatch.setattr(data.utils, "load_asset_config",
        lambda asset_id=None: {"database": {"data_dir": str(tmp_path / "data")}})

    result = data.prediction_history(lookback_hours=1)

    assert len(result) == 2
    assert result["long_prediction"].tolist() == [0.36, 0.20]


def test_prediction_history_joins_ohlcv_for_candles(tmp_path, monkeypatch) -> None:
    import pandas as pd
    now = pd.Timestamp.utcnow().floor("s")
    t0 = (now - pd.Timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    t1 = now.strftime("%Y-%m-%d %H:%M:%S")
    pred_df = pd.DataFrame({
        "open_time": [t0, t1],
        "close": [100.0, 101.0],
        "long_pred": [0.10, 0.40],
        "short_pred": [0.05, 0.15],
    })
    ohlcv_df = pd.DataFrame({
        "open_time": [t0, t1],
        "open": [99.0, 100.0],
        "high": [102.0, 103.0],
        "low": [98.0, 99.0],
        "close": [100.0, 102.0],
    })

    monkeypatch.setattr(data, "latest_open_time", lambda d, ds: now)
    monkeypatch.setattr(data, "query_range",
        lambda d, ds, start, end, columns: pred_df if ds == "predictions" else ohlcv_df)
    monkeypatch.setattr(data.utils, "load_asset_config",
        lambda asset_id=None: {"database": {"data_dir": str(tmp_path / "data")}})

    result = data.prediction_history(lookback_hours=1)

    assert result[["open", "high", "low", "close"]].iloc[-1].tolist() == [100.0, 103.0, 99.0, 102.0]


def test_prediction_history_uses_explicit_asset_config(tmp_path, monkeypatch) -> None:
    import pandas as pd
    now = pd.Timestamp.utcnow().floor("s")
    t0 = (now - pd.Timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    t1 = now.strftime("%Y-%m-%d %H:%M:%S")
    pred_df = pd.DataFrame({
        "open_time": [t0, t1],
        "close": [200.0, 205.0],
        "long_pred": [0.12, 0.55],
        "short_pred": [0.05, 0.10],
    })

    def load_asset_config(asset_id: str | None = None) -> dict:
        assert asset_id == "solusdt_fw60"
        return {"database": {"data_dir": str(tmp_path / "data"), "asset_id": "solusdt_fw60"}}

    monkeypatch.setattr(data.utils, "load_asset_config", load_asset_config)
    monkeypatch.setattr(data, "latest_open_time", lambda d, ds: now)
    monkeypatch.setattr(data, "query_range",
        lambda d, ds, start, end, columns: pred_df if ds == "predictions" else pd.DataFrame())

    result = data.prediction_history(lookback_hours=1, asset_id="solusdt_fw60")

    assert len(result) == 2
    assert result["long_prediction"].tolist() == [0.12, 0.55]


def test_active_strategy_returns_sol_strategy_for_sol_asset_id() -> None:
    strategies_cfg = {
        "strategies": {
            "other_strat": {"side": "long"},
            "sol_strat": {"side": "long", "asset_id": "solusdt_fw60"},
        }
    }

    strategy_id, scfg = data.active_strategy(strategies_cfg, asset_id="solusdt_fw60")

    assert strategy_id == "sol_strat"
    assert scfg["asset_id"] == "solusdt_fw60"


def test_active_strategy_returns_default_strategy_when_no_asset_id() -> None:
    strategies_cfg = {
        "strategies": {
            "other_strat": {"side": "long"},
            "sol_strat": {"side": "long", "asset_id": "solusdt_fw60"},
        }
    }

    strategy_id, _ = data.active_strategy(strategies_cfg, asset_id=None)

    assert strategy_id == "other_strat"


def test_prediction_history_missing_table_returns_empty_frame(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(data.utils, "load_asset_config",
        lambda asset_id=None: {"database": {"data_dir": str(tmp_path / "nonexistent")}})
    monkeypatch.setattr(data, "latest_open_time", lambda d, ds: None)

    result = data.prediction_history(lookback_hours=1, asset_id="solusdt_fw60")

    assert result.empty


def test_optional_trading_tables_return_empty_frames(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(data, "_db_path", lambda asset_id=None: None)

    assert data.active_position() is None
    assert data.recent_orders().empty
    assert data.recent_errors().empty
