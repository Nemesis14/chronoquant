# =============================================================================
# Strategy backtest tests
# =============================================================================
# Purpose:
#  - Verify deterministic trade simulation behavior on toy OHLCV data
# =============================================================================

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import utils
from evaluation.backtest import simulate_long_probability_strategy


def test_long_strategy_enters_next_bar_and_takes_profit() -> None:
    frame = pd.DataFrame(
        {
            "open_time": pd.date_range("2026-01-01 00:00:00", periods=5, freq="min"),
            "open": [100.0, 100.0, 101.0, 102.0, 103.0],
            "high": [100.2, 102.5, 102.0, 103.0, 104.0],
            "low": [99.8, 99.9, 100.8, 101.5, 102.5],
            "close": [100.0, 102.0, 101.5, 102.8, 103.5],
            "target": [0, 1, 1, 0, 0],
            "prediction": [0.20, 0.05, 0.05, 0.05, 0.05],
        }
    )
    cfg = {
        "side": "long",
        "initial_equity": 1000.0,
        "entry_threshold": 0.18,
        "rearm_threshold": 0.10,
        "exit_threshold": 0.05,
        "min_hold_minutes": 0,
        "max_hold_minutes": 240,
        "take_profit_pct": 0.02,
        "stop_loss_pct": 0.01,
        "trailing_activation_pct": 0.0,
        "trailing_stop_pct": 0.0,
        "cooldown_minutes": 0,
        "fee_bps_per_side": 0.0,
        "slippage_bps_per_side": 0.0,
    }

    trades, _, summary = simulate_long_probability_strategy(frame, cfg)

    assert len(trades) == 1
    assert trades.iloc[0]["entry_time"] == "2026-01-01 00:01:00"
    assert trades.iloc[0]["exit_reason"] == "take_profit"
    assert summary["trade_count"] == 1
    assert summary["winning_trades"] == 1


def test_strategy_rejects_unknown_side() -> None:
    frame = pd.DataFrame(
        {
            "open_time": pd.date_range("2026-01-01", periods=2, freq="min"),
            "open":       [100.0, 100.0],
            "high":       [101.0, 101.0],
            "low":        [99.0,  99.0],
            "close":      [100.0, 100.0],
            "target":     [0, 0],
            "prediction": [0.1, 0.1],
        }
    )
    cfg = {
        "side": "neutral",
        "initial_equity": 1000.0,
        "entry_threshold": 0.5,
        "rearm_threshold": 0.1,
        "exit_threshold": 0.05,
        "min_hold_minutes": 0,
        "max_hold_minutes": 60,
        "take_profit_pct": 0.01,
        "stop_loss_pct": 0.0,
        "trailing_activation_pct": 0.0,
        "trailing_stop_pct": 0.0,
        "cooldown_minutes": 0,
        "fee_bps_per_side": 0.0,
        "slippage_bps_per_side": 0.0,
    }

    with pytest.raises(ValueError, match="Unsupported strategy side"):
        simulate_long_probability_strategy(frame, cfg)


def test_sol_strategy_config_routes_to_sol_asset() -> None:
    strategies = utils.load_strategies_config()["strategies"]

    assert "solusdt_long_fw60_q90_managed_v1" in strategies
    sol = strategies["solusdt_long_fw60_q90_managed_v1"]
    assert sol["asset_id"] == "solusdt_fw60"
    assert sol["model_id"] == "lgbm_solusdt_l_fw60_q90_stable_v1"
    assert sol["side"] == "long"
