"""Sanity tests for trading.calibration.backtest.

Verifies business invariants:
- entries only appear when prediction >= entry_threshold
- equity stays positive throughout a LONG simulation
- trade_count equals trades_df row count
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytestmark = pytest.mark.sanity

from trading.calibration.backtest import load_oos_frame, simulate_long_probability_strategy

# ---------------------------------------------------------------------------
# Shared toy frame factory
# ---------------------------------------------------------------------------

_N = 100

_ENTRY_THRESHOLD = 0.55

_BASE_CFG = {
    "side":             "long",
    "entry_threshold":  _ENTRY_THRESHOLD,
    "rearm_threshold":  0.2,
    "exit_threshold":   -1.0,      # never exit on prediction — forces max_hold exit
    "min_hold_minutes": 0,
    "max_hold_minutes": 10,        # short hold so multiple trades occur
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
            "prediction": np.linspace(0.1, 0.9, n),
        }
    )


# ---------------------------------------------------------------------------
# Sanity invariants
# ---------------------------------------------------------------------------


def test_entries_only_above_threshold() -> None:
    """Every trade entry occurs only when prev prediction >= entry_threshold."""
    frame = _toy_frame()
    trades_df, _eq, _summary = simulate_long_probability_strategy(frame, _BASE_CFG)

    if trades_df.empty:
        pytest.skip("No trades generated — cannot check entry threshold invariant")

    preds = frame.set_index("open_time")["prediction"]

    for _, trade in trades_df.iterrows():
        signal_time = pd.Timestamp(trade["entry_signal_time"])
        signal_pred = preds.get(signal_time)
        if signal_pred is not None:
            assert signal_pred >= _ENTRY_THRESHOLD, (
                f"Trade entered with prediction={signal_pred:.4f} below threshold={_ENTRY_THRESHOLD}"
            )


def test_long_equity_always_positive() -> None:
    """Equity curve never drops to zero or below in a LONG simulation (no stop-loss)."""
    frame = _toy_frame()
    _trades, equity_df, _summary = simulate_long_probability_strategy(frame, _BASE_CFG)

    assert (equity_df["equity"] > 0).all(), "Equity dropped to 0 or below"


def test_trade_count_matches_trades_df_rows() -> None:
    """summary['trade_count'] equals the number of rows in trades_df."""
    frame = _toy_frame()
    trades_df, _eq, summary = simulate_long_probability_strategy(frame, _BASE_CFG)

    assert summary["trade_count"] == len(trades_df), (
        f"summary trade_count={summary['trade_count']} != trades_df rows={len(trades_df)}"
    )


# ---------------------------------------------------------------------------
# Integration test — 2021 model (skippable)
# ---------------------------------------------------------------------------

SAMPLE_OOS = Path("artifacts/lgbm_solusdt_l_fw60_2021/sample_oos.parquet")


def test_calibration_2021_runs() -> None:
    """End-to-end: load 2021 OOS frame and verify it has content and prediction column."""
    if not SAMPLE_OOS.exists():
        pytest.skip("sample_oos.parquet not found")

    frame = load_oos_frame("lgbm_solusdt_l_fw60_2021")
    assert len(frame) > 0
    assert "prediction" in frame.columns
