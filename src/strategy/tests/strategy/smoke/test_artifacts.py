"""Smoke tests for strategy.strategy.artifacts — write/read round-trip."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from strategy.strategy.artifacts import (
    read_strategy_artifact,
    write_realized_outputs,
    write_strategy_artifact,
)

pytestmark = pytest.mark.smoke


_DECISION_PARAMS = {
    "long_entry_pct"   : 0.90,
    "short_entry_pct"  : 0.90,
    "min_edge_gap"     : 0.05,
    "min_hold_minutes" : 5,
    "max_hold_minutes" : 60,
    "cooldown_minutes" : 45,
    "rearm_pct"        : 0.60,
    "conflict_rule"    : "highest_edge",
}

_METRICS = {
    "n_trades"     : 120,
    "total_return" : 0.45,
    "win_rate"     : 0.55,
    "sharpe"       : 1.2,
    "max_drawdown" : -0.05,
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_write_strategy_artifact_creates_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """write_strategy_artifact() writes a readable JSON file."""
    import utils

    monkeypatch.setattr(utils, "_resolve_path", lambda p: str(tmp_path / p) if p != "." else str(tmp_path))

    out_path = write_strategy_artifact(
        session_id      = "test_session",
        long_model_id   = "model_long",
        short_model_id  = "model_short",
        fit_period      = {"start": "2025-05-01", "end": "2026-05-31"},
        decision_params = _DECISION_PARAMS,
        metrics         = _METRICS,
        optuna_best     = {"value": 0.0042, "n_trials": 200},
    )

    assert out_path.exists(), "strategy_artifact.json was not created"

    with open(out_path, encoding="utf-8") as f:
        data = json.load(f)

    assert data["session_id"]  == "test_session"
    assert data["long_model"]  == "model_long"
    assert data["short_model"] == "model_short"


def test_read_strategy_artifact_returns_required_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """read_strategy_artifact() returns dict with all required keys."""
    import utils

    monkeypatch.setattr(utils, "_resolve_path", lambda p: str(tmp_path / p) if p != "." else str(tmp_path))

    write_strategy_artifact(
        session_id      = "test_session",
        long_model_id   = "model_long",
        short_model_id  = "model_short",
        fit_period      = {"start": "2025-05-01", "end": "2026-05-31"},
        decision_params = _DECISION_PARAMS,
        metrics         = _METRICS,
        optuna_best     = {"value": 0.003, "n_trials": 100},
    )

    result = read_strategy_artifact("test_session")

    required_keys = [
        "session_id", "long_model", "short_model",
        "signal_mode", "evaluation_mode",
        "fit_period",
        "rank_lookup_long_path", "rank_lookup_short_path",
        "isotonic_long_path", "isotonic_short_path",
        "decision_params",
        "optuna_best_trial", "metrics", "calibrated_at",
    ]
    for key in required_keys:
        assert key in result, f"Missing required key in artifact: {key}"

    assert result["session_id"]       == "test_session"
    assert result["signal_mode"]      == "rank_first"
    assert result["evaluation_mode"]  == "same_window"

    dp = result["decision_params"]
    assert dp["conflict_rule"]    == "highest_edge"
    assert dp["long_entry_pct"]   == 0.90
    assert dp["short_entry_pct"]  == 0.90
    assert dp["min_edge_gap"]     == 0.05
    assert dp["min_hold_minutes"] == 5
    assert dp["max_hold_minutes"] == 60
    assert dp["cooldown_minutes"] == 45
    assert dp["rearm_pct"]        == 0.60


# ---------------------------------------------------------------------------
# write_realized_outputs tests
# ---------------------------------------------------------------------------


def _make_trade(i: int) -> dict:
    """Return a minimal trade dict for testing."""
    return {
        "entry_time"        : pd.Timestamp(f"2026-01-{i+1:02d} 09:00:00"),
        "exit_time"         : pd.Timestamp(f"2026-01-{i+1:02d} 10:00:00"),
        "direction"         : "long" if i % 2 == 0 else "short",
        "entry_price"       : 100.0 + i,
        "exit_price"        : 101.0 + i,
        "score_pct_at_entry": 0.90,
        "bucket_mean_mfe"   : 0.01 * (i + 1),
        "n_bars"            : 60,
        "hold_minutes"      : 60,
        "exit_reason"       : "max_hold",
    }


def test_write_realized_outputs_empty_trades(tmp_path: Path) -> None:
    """write_realized_outputs() with empty list must not raise and must write valid files."""
    write_realized_outputs(tmp_path, [])

    trades_path = tmp_path / "trades.parquet"
    equity_path = tmp_path / "equity_curve.parquet"
    summary_path = tmp_path / "summary.json"

    assert trades_path.exists(),  "trades.parquet must be written even for 0 trades"
    assert equity_path.exists(),  "equity_curve.parquet must be written even for 0 trades"
    assert summary_path.exists(), "summary.json must be written even for 0 trades"

    trades_df = pd.read_parquet(trades_path)
    equity_df = pd.read_parquet(equity_path)
    assert len(trades_df) == 0
    assert len(equity_df) == 0

    with open(summary_path, encoding="utf-8") as f:
        summary = json.load(f)

    assert summary["n_trades"]       == 0
    assert summary["equity_basis"]   == "mfe_proxy"
    assert "note"                    in summary


def test_write_realized_outputs_with_trades(tmp_path: Path) -> None:
    """write_realized_outputs() with 3 trades produces correct columns and values."""
    trades = [_make_trade(i) for i in range(3)]
    write_realized_outputs(tmp_path, trades)

    trades_df = pd.read_parquet(tmp_path / "trades.parquet")
    equity_df = pd.read_parquet(tmp_path / "equity_curve.parquet")

    # --- trades.parquet columns ---
    expected_trade_cols = {
        "entry_time", "exit_time", "direction",
        "entry_price", "exit_price",
        "hold_minutes", "exit_reason",
        "score_pct_at_entry", "bucket_mean_mfe",
    }
    assert expected_trade_cols == set(trades_df.columns), (
        f"trades.parquet columns mismatch: {set(trades_df.columns)}"
    )
    assert len(trades_df) == 3

    assert trades_df["entry_price"].tolist() == [100.0, 101.0, 102.0]
    assert trades_df["exit_price"].tolist() == [101.0, 102.0, 103.0]

    # --- equity_curve.parquet columns ---
    expected_equity_cols = {"trade_index", "entry_time", "bucket_mean_mfe", "cumulative_mfe"}
    assert expected_equity_cols == set(equity_df.columns), (
        f"equity_curve.parquet columns mismatch: {set(equity_df.columns)}"
    )
    assert len(equity_df) == 3
    # Cumulative MFE should be monotonically increasing (all positive MFEs)
    assert equity_df["cumulative_mfe"].iloc[-1] > equity_df["cumulative_mfe"].iloc[0]

    # --- summary.json ---
    with open(tmp_path / "summary.json", encoding="utf-8") as f:
        summary = json.load(f)

    required_summary_keys = {
        "initial_capital", "final_equity", "n_trades",
        "win_rate", "gross_return", "net_return",
        "equity_basis", "note",
    }
    assert required_summary_keys == set(summary.keys()), (
        f"summary.json keys mismatch: {set(summary.keys())}"
    )
    assert summary["n_trades"]       == 3
    assert summary["equity_basis"]   == "mfe_proxy"
    assert summary["gross_return"]   == summary["net_return"], "gross == net (no fees)"
    assert summary["initial_capital"] == 1.0
