"""Smoke tests for trading.live.strategy.evaluate()."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from trading.live.state import COOLDOWN, FLAT, LONG, TradingState
from trading.live.strategy import (
    ENTER_LONG,
    EXIT_LONG,
    HOLD,
    evaluate,
)

pytestmark = pytest.mark.smoke

# ---------------------------------------------------------------------------
# Shared config helpers
# ---------------------------------------------------------------------------

_DECISION_PARAMS = {
    "long_entry_pct"  : 0.90,
    "short_entry_pct" : 0.90,
    "min_edge_gap"    : 0.05,
    "min_hold_minutes": 5,
    "max_hold_minutes": 60,
    "cooldown_minutes": 30,
    "rearm_pct"       : 0.60,
    "conflict_rule"   : "highest_edge",
}

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


def _flat_armed() -> TradingState:
    return TradingState(status=FLAT, armed=True)


def _flat_unarmed() -> TradingState:
    return TradingState(status=FLAT, armed=False)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_flat_armed_above_threshold_returns_enter_long() -> None:
    """FLAT + armed + score_pct_long >= long_entry_pct → ENTER_LONG."""
    state = _flat_armed()
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.95,
        score_pct_short = 0.10,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == ENTER_LONG


def test_flat_armed_both_below_threshold_returns_hold() -> None:
    """FLAT + armed + both percentiles below thresholds → HOLD."""
    state = _flat_armed()
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.50,
        score_pct_short = 0.40,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == HOLD


def test_flat_armed_both_signal_long_edge_wins() -> None:
    """FLAT + armed + both signal, long edge > min_edge_gap → ENTER_LONG."""
    state = _flat_armed()
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.97,
        score_pct_short = 0.91,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == ENTER_LONG


def test_flat_armed_both_signal_edge_gap_insufficient_returns_hold() -> None:
    """FLAT + armed + both signal, edge gap < min_edge_gap → HOLD."""
    state = _flat_armed()
    # edge_long = 0.93 - 0.91 = 0.02 < 0.05
    # edge_short = 0.91 - 0.93 < 0 — edge_long wins but < min_edge_gap
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.93,
        score_pct_short = 0.91,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == HOLD


def test_long_max_hold_exceeded_returns_exit_long() -> None:
    """LONG + hold >= max_hold_minutes → EXIT_LONG."""
    entry = _NOW - timedelta(minutes=60)
    state = TradingState(status=LONG, armed=False, entry_time=entry)
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.95,
        score_pct_short = 0.10,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == EXIT_LONG


def test_long_opposite_short_signal_after_min_hold_returns_exit_long() -> None:
    """LONG + opposite short signal + min_hold elapsed → EXIT_LONG."""
    entry = _NOW - timedelta(minutes=10)  # > min_hold_minutes=5
    state = TradingState(status=LONG, armed=False, entry_time=entry)
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.85,
        score_pct_short = 0.92,  # >= short_entry_pct=0.90
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == EXIT_LONG


def test_long_signal_decay_after_min_hold_returns_exit_long() -> None:
    """LONG + score_pct_long < rearm_pct + min_hold elapsed → EXIT_LONG."""
    entry = _NOW - timedelta(minutes=10)  # > min_hold_minutes=5
    state = TradingState(status=LONG, armed=False, entry_time=entry)
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.40,  # < rearm_pct=0.60
        score_pct_short = 0.30,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == EXIT_LONG


def test_cooldown_active_returns_hold() -> None:
    """COOLDOWN + cooldown_until in the future → HOLD."""
    future = _NOW + timedelta(minutes=30)
    state = TradingState(status=COOLDOWN, armed=False, cooldown_until=future)
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.10,
        score_pct_short = 0.10,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == HOLD


def test_flat_not_armed_returns_hold() -> None:
    """FLAT + not armed → HOLD regardless of percentile scores."""
    state = _flat_unarmed()
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.99,
        score_pct_short = 0.99,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == HOLD
