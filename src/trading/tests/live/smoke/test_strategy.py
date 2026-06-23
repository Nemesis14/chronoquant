"""Smoke tests for trading.live.strategy.evaluate()."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from trading.live.state import FLAT, LONG, SHORT, TradingState
from trading.live.strategy import (
    ENTER_LONG,
    ENTER_SHORT,
    EXIT_LONG,
    EXIT_SHORT,
    HOLD,
    evaluate,
)

pytestmark = pytest.mark.smoke

# ---------------------------------------------------------------------------
# Shared config helpers
# ---------------------------------------------------------------------------

_DECISION_PARAMS = {
    "entry_cutoff"    : 0.97,
    "max_hold_minutes": 60,
    "tp_spec"         : "0.75x_bucket_mean",
    "sl_spec"         : "none",
    "directions"      : ["long", "short"],
    "same_bar_conflict_rule": "sl_first",
}

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


def _flat() -> TradingState:
    return TradingState(status=FLAT)


# ---------------------------------------------------------------------------
# FLAT state tests
# ---------------------------------------------------------------------------


def test_flat_long_above_cutoff_returns_enter_long() -> None:
    """FLAT + score_pct_long >= entry_cutoff → ENTER_LONG."""
    state = _flat()
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.98,
        score_pct_short = 0.50,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == ENTER_LONG


def test_flat_short_above_cutoff_returns_enter_short() -> None:
    """FLAT + (1 - score_pct_short) >= entry_cutoff → ENTER_SHORT.

    score_pct_short = 0.02 → 1 - 0.02 = 0.98 >= 0.97.
    """
    state = _flat()
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.50,
        score_pct_short = 0.02,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == ENTER_SHORT


def test_flat_both_below_cutoff_returns_hold() -> None:
    """FLAT + both below entry_cutoff → HOLD."""
    state = _flat()
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.80,
        score_pct_short = 0.20,   # 1 - 0.20 = 0.80 < 0.97
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == HOLD


def test_flat_both_signal_long_priority() -> None:
    """FLAT + both long and short trigger → ENTER_LONG (long has priority)."""
    state = _flat()
    # long: 0.98 >= 0.97 ✓
    # short: 1 - 0.01 = 0.99 >= 0.97 ✓
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.98,
        score_pct_short = 0.01,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == ENTER_LONG


# ---------------------------------------------------------------------------
# LONG state tests
# ---------------------------------------------------------------------------


def test_long_max_hold_exceeded_returns_exit_long() -> None:
    """LONG + hold >= max_hold_minutes → EXIT_LONG."""
    entry = _NOW - timedelta(minutes=60)
    state = TradingState(status=LONG, entry_time=entry)
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.95,
        score_pct_short = 0.50,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == EXIT_LONG


def test_long_under_max_hold_returns_hold() -> None:
    """LONG + hold < max_hold_minutes → HOLD."""
    entry = _NOW - timedelta(minutes=30)
    state = TradingState(status=LONG, entry_time=entry)
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.60,
        score_pct_short = 0.50,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == HOLD


# ---------------------------------------------------------------------------
# SHORT state tests
# ---------------------------------------------------------------------------


def test_short_max_hold_exceeded_returns_exit_short() -> None:
    """SHORT + hold >= max_hold_minutes → EXIT_SHORT."""
    entry = _NOW - timedelta(minutes=60)
    state = TradingState(status=SHORT, entry_time=entry)
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.50,
        score_pct_short = 0.10,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == EXIT_SHORT


def test_short_under_max_hold_returns_hold() -> None:
    """SHORT + hold < max_hold_minutes → HOLD."""
    entry = _NOW - timedelta(minutes=30)
    state = TradingState(status=SHORT, entry_time=entry)
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.50,
        score_pct_short = 0.20,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == HOLD


# ---------------------------------------------------------------------------
# Edge / boundary tests
# ---------------------------------------------------------------------------


def test_entry_cutoff_exact_boundary_long() -> None:
    """score_pct_long exactly at entry_cutoff → ENTER_LONG (>= is inclusive)."""
    state = _flat()
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.97,
        score_pct_short = 0.50,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == ENTER_LONG


def test_entry_cutoff_exact_boundary_short() -> None:
    """(1 - score_pct_short) exactly at entry_cutoff → ENTER_SHORT."""
    state = _flat()
    decision, _reason = evaluate(
        state,
        score_pct_long  = 0.50,
        score_pct_short = 0.03,   # 1 - 0.03 = 0.97 exactly
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == ENTER_SHORT


def test_unknown_status_returns_hold() -> None:
    """Unknown state.status → HOLD (fallback branch)."""
    state = TradingState(status="MYSTERY")
    decision, reason = evaluate(
        state,
        score_pct_long  = 0.99,
        score_pct_short = 0.01,
        decision_params = _DECISION_PARAMS,
        now             = _NOW,
    )
    assert decision == HOLD
    assert "unknown_status" in reason
