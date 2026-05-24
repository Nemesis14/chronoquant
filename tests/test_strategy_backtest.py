# =============================================================================
# Strategy backtest tests
# =============================================================================
# Purpose:
#  - Verify deterministic trade simulation behavior on toy OHLCV data
# =============================================================================

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

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
