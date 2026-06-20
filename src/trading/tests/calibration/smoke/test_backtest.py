"""Smoke tests for trading.calibration.backtest."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytestmark = pytest.mark.smoke

from trading.calibration.backtest import (
    load_oos_frame,
    simulate_long_probability_strategy,
    summarize_trades,
)

# ---------------------------------------------------------------------------
# Shared toy frame factory
# ---------------------------------------------------------------------------

_N = 100
_DEFAULT_CFG_LONG = {
    "side":             "long",
    "entry_threshold":  0.4,
    "rearm_threshold":  0.2,
    "exit_threshold":   0.1,
    "min_hold_minutes": 0,
    "max_hold_minutes": 30,
    "initial_equity":   10000.0,
}

_DEFAULT_CFG_SHORT = {
    "side":             "short",
    "entry_threshold":  0.4,
    "rearm_threshold":  0.2,
    "exit_threshold":   0.1,
    "min_hold_minutes": 0,
    "max_hold_minutes": 30,
    "initial_equity":   10000.0,
}


def _toy_frame(n: int = _N) -> pd.DataFrame:
    times = pd.date_range("2022-01-01", periods=n, freq="1min")
    return pd.DataFrame(
        {
            "open_time":  times,
            "open":       100.0,
            "high":       101.0,
            "low":        99.0,
            "close":      100.0,
            "target":     0.01,
            "prediction": np.linspace(0.1, 0.8, n),
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_simulate_long_returns_tuple_df_df_dict() -> None:
    """simulate_long_probability_strategy returns (trades_df, equity_df, summary dict)."""
    frame = _toy_frame()
    result = simulate_long_probability_strategy(frame, _DEFAULT_CFG_LONG)

    assert isinstance(result, tuple)
    assert len(result) == 3

    trades_df, equity_df, summary = result
    assert isinstance(trades_df, pd.DataFrame)
    assert isinstance(equity_df, pd.DataFrame)
    assert isinstance(summary, dict)


def test_simulate_short_returns_tuple_df_df_dict() -> None:
    """simulate_long_probability_strategy with side=short dispatches to short simulator
    and returns (trades_df, equity_df, summary dict)."""
    frame = _toy_frame()
    result = simulate_long_probability_strategy(frame, _DEFAULT_CFG_SHORT)

    assert isinstance(result, tuple)
    assert len(result) == 3

    trades_df, equity_df, summary = result
    assert isinstance(trades_df, pd.DataFrame)
    assert isinstance(equity_df, pd.DataFrame)
    assert isinstance(summary, dict)


def test_summarize_trades_empty_df() -> None:
    """summarize_trades with empty DataFrame returns trade_count == 0."""
    result = summarize_trades(
        trades_df=pd.DataFrame(),
        equity_df=pd.DataFrame({"open_time": [pd.Timestamp("2022-01-01")], "equity": [10000.0]}),
        initial_equity=10000.0,
        start_time=pd.Timestamp("2022-01-01"),
        end_time=pd.Timestamp("2022-01-02"),
    )
    assert isinstance(result, dict)
    assert result["trade_count"] == 0


def test_summarize_trades_single_trade() -> None:
    """summarize_trades with one winning trade: win_rate is float, max_drawdown <= 0."""
    trades_df = pd.DataFrame(
        [
            {
                "entry_time":        "2022-01-01 00:01:00",
                "entry_signal_time": "2022-01-01 00:00:00",
                "exit_time":         "2022-01-01 00:10:00",
                "exit_reason":       "max_hold",
                "hold_minutes":      9,
                "entry_price":       100.0,
                "exit_price":        101.0,
                "entry_prediction":  0.5,
                "entry_target":      0.01,
                "gross_return":      0.01,
                "net_return":        0.009,
                "equity_after":      10090.0,
            }
        ]
    )
    equity_df = pd.DataFrame(
        {
            "open_time": [pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-01 00:10:00")],
            "equity":    [10000.0, 10090.0],
        }
    )
    result = summarize_trades(
        trades_df=trades_df,
        equity_df=equity_df,
        initial_equity=10000.0,
        start_time=pd.Timestamp("2022-01-01"),
        end_time=pd.Timestamp("2022-01-01 00:10:00"),
    )
    assert isinstance(result["win_rate"], float)
    assert result["max_drawdown"] <= 0.0


def test_load_oos_frame_raises_when_artifacts_missing() -> None:
    """load_oos_frame raises FileNotFoundError when artifacts directory does not exist."""
    with pytest.raises(FileNotFoundError):
        load_oos_frame("nonexistent_model_id_xyzabc")
