"""Smoke tests for strategy.strategy.optimize."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from strategy.strategy.optimize import _simulate_strategy, optimize_strategy

pytestmark = pytest.mark.smoke


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_eval_df(n: int = 20) -> pd.DataFrame:
    """Return a synthetic eval DataFrame with rank-first columns."""
    open_times = pd.date_range("2026-03-01 09:00:00", periods=n, freq="min")
    return pd.DataFrame({
        "open_time"            : open_times,
        "score_pct_long"       : [0.90] * n,
        "score_pct_short"      : [0.30] * n,
        "bucket_mean_mfe_long" : [0.012] * n,
        "bucket_mean_mfe_short": [0.005] * n,
        "close"                : [100.0 + i for i in range(n)],
    })


# ---------------------------------------------------------------------------
# _simulate_strategy tests
# ---------------------------------------------------------------------------


def test_simulate_strategy_returns_list() -> None:
    """_simulate_strategy() runs without error and returns a list."""
    df = _make_eval_df(20)

    trades = _simulate_strategy(
        df               = df,
        long_entry_pct   = 0.80,
        short_entry_pct  = 0.80,
        min_edge_gap     = 0.05,
        max_hold_minutes = 5,
        cooldown_minutes = 2,
        min_hold_minutes = 1,
        rearm_pct        = 0.50,
    )

    assert isinstance(trades, list), "_simulate_strategy() must return a list"


def test_simulate_strategy_trade_keys() -> None:
    """Each trade dict from _simulate_strategy() contains required keys."""
    df = _make_eval_df(20)

    trades = _simulate_strategy(
        df               = df,
        long_entry_pct   = 0.80,
        short_entry_pct  = 0.95,
        min_edge_gap     = 0.05,
        max_hold_minutes = 5,
        cooldown_minutes = 2,
        min_hold_minutes = 1,
        rearm_pct        = 0.50,
    )

    required_keys = {
        "entry_time",
        "exit_time",
        "direction",
        "score_pct_at_entry",
        "bucket_mean_mfe",
        "n_bars",
        "hold_minutes",
        "exit_reason",
        "entry_price",
        "exit_price",
    }

    for trade in trades:
        missing = required_keys - set(trade.keys())
        assert not missing, f"Trade dict missing keys: {missing}"


def test_simulate_strategy_exit_reason_values() -> None:
    """exit_reason in trade dicts must be one of the three valid values."""
    df = _make_eval_df(50)

    trades = _simulate_strategy(
        df               = df,
        long_entry_pct   = 0.80,
        short_entry_pct  = 0.95,
        min_edge_gap     = 0.05,
        max_hold_minutes = 5,
        cooldown_minutes = 2,
        min_hold_minutes = 1,
        rearm_pct        = 0.50,
    )

    valid_exit_reasons = {"max_hold", "opposite_edge", "signal_decay"}
    for trade in trades:
        assert trade["exit_reason"] in valid_exit_reasons, (
            f"Unexpected exit_reason: {trade['exit_reason']!r}"
        )
        assert trade["hold_minutes"] == trade["n_bars"], (
            "hold_minutes must equal n_bars"
        )


def test_simulate_strategy_no_trades_when_threshold_too_high() -> None:
    """_simulate_strategy() returns empty list when thresholds exceed all scores."""
    df = _make_eval_df(20)

    trades = _simulate_strategy(
        df               = df,
        long_entry_pct   = 0.999,
        short_entry_pct  = 0.999,
        min_edge_gap     = 0.05,
        max_hold_minutes = 5,
        cooldown_minutes = 2,
        min_hold_minutes = 1,
        rearm_pct        = 0.50,
    )

    assert trades == [], "Expected no trades when threshold is above all percentile scores"


# ---------------------------------------------------------------------------
# optimize_strategy integration smoke test
# ---------------------------------------------------------------------------


def _make_strategy_table(n: int = 200) -> pd.DataFrame:
    """Return a synthetic strategy_table with all required rank-first columns."""
    open_times = pd.date_range("2026-01-01 00:00:00", periods=n, freq="min")
    # Alternate high/low to generate enough trades for Optuna
    pct_long  = [0.92 if i % 3 != 0 else 0.40 for i in range(n)]
    pct_short = [0.30 if i % 3 != 0 else 0.40 for i in range(n)]
    return pd.DataFrame({
        "open_time"            : open_times,
        "pred_long_raw"        : [0.005] * n,
        "pred_short_raw"       : [0.003] * n,
        "long_mfe_fw60"        : [0.010] * n,
        "short_mfe_fw60"       : [0.008] * n,
        "score_pct_long"       : pct_long,
        "score_pct_short"      : pct_short,
        "bucket_long"          : [9] * n,
        "bucket_short"         : [3] * n,
        "bucket_mean_mfe_long" : [0.012] * n,
        "bucket_mean_mfe_short": [0.006] * n,
        "bucket_hit_rate_long" : [0.70] * n,
        "bucket_hit_rate_short": [0.55] * n,
        "pred_long_cal"        : [0.011] * n,
        "pred_short_cal"       : [0.007] * n,
        "open"                 : [99.5 + i for i in range(n)],
        "high"                 : [100.5 + i for i in range(n)],
        "low"                  : [99.0 + i for i in range(n)],
        "close"                : [100.0 + i for i in range(n)],
    })


def test_optimize_strategy_artifact_fields(tmp_path: Path) -> None:
    """optimize_strategy() writes an artifact with expected rank-first fields."""
    session_id = "test_session_smoke"
    artifact_dir = tmp_path / "artifacts" / session_id
    artifact_dir.mkdir(parents=True)

    strategy_table = _make_strategy_table(300)
    strategy_table.to_parquet(artifact_dir / "strategy_table.parquet", index=False)

    def _mock_resolve(path: str) -> str:
        return str(tmp_path / path) if path != "." else str(tmp_path)

    with patch("strategy.strategy.optimize.utils._resolve_path", side_effect=_mock_resolve), \
         patch("strategy.strategy.artifacts.utils._resolve_path", side_effect=_mock_resolve):
        result = optimize_strategy(
            session_id     = session_id,
            long_model_id  = "lgbm_solusdt_l_fw60_2021",
            short_model_id = "lgbm_solusdt_s_fw60_2021",
            start          = "2026-01-01",
            end            = "2026-01-01",
            n_trials       = 5,
        )

    artifact_path = artifact_dir / "strategy_artifact.json"
    assert artifact_path.exists(), "strategy_artifact.json must be written"

    with open(artifact_path, encoding="utf-8") as f:
        artifact = json.load(f)

    assert artifact.get("signal_mode")      == "rank_first",    "signal_mode must be rank_first"
    assert artifact.get("evaluation_mode")  == "same_window",   "evaluation_mode must be same_window"

    dp = artifact.get("decision_params", {})
    assert "long_entry_pct"  in dp, "decision_params must contain long_entry_pct"
    assert "conflict_rule"   in dp, "decision_params must contain conflict_rule"
    assert dp["conflict_rule"] == "highest_edge", "conflict_rule must be highest_edge"

    assert "session_id"   in result
    assert "best_params"  in result
    assert "metrics"      in result
    assert "optuna_best"  in result
