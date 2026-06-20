"""Smoke tests for trading.live.strategy.evaluate()."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytestmark = pytest.mark.smoke

from trading.live.state import COOLDOWN, FLAT, LONG, SHORT, TradingState
from trading.live.strategy import (
    ENTER_LONG,
    ENTER_SHORT,
    EXIT_LONG,
    HOLD,
    evaluate,
)

# ---------------------------------------------------------------------------
# Shared config helpers
# ---------------------------------------------------------------------------

_LONG_CFG = {
    "entry_threshold":  0.5,
    "rearm_threshold":  0.2,
    "exit_threshold":   0.1,
    "min_hold_minutes": 0,
    "max_hold_minutes": 60,
}

_SHORT_CFG = {
    "entry_threshold":  0.5,
    "rearm_threshold":  0.2,
    "exit_threshold":   0.1,
    "min_hold_minutes": 0,
    "max_hold_minutes": 60,
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
    """FLAT + armed + pred_long >= entry_threshold → ENTER_LONG."""
    state = _flat_armed()
    decision, _reason = evaluate(
        state,
        pred_long=0.6,
        pred_short=0.1,
        long_cfg=_LONG_CFG,
        short_cfg=_SHORT_CFG,
        now=_NOW,
    )
    assert decision == ENTER_LONG


def test_flat_armed_below_threshold_returns_hold() -> None:
    """FLAT + armed + both predictions below entry_threshold → HOLD."""
    state = _flat_armed()
    decision, _reason = evaluate(
        state,
        pred_long=0.3,
        pred_short=0.2,
        long_cfg=_LONG_CFG,
        short_cfg=_SHORT_CFG,
        now=_NOW,
    )
    assert decision == HOLD


def test_long_max_hold_exceeded_returns_exit_long() -> None:
    """LONG + hold > max_hold_minutes → EXIT_LONG."""
    entry = _NOW - timedelta(minutes=120)
    state = TradingState(status=LONG, armed=False, entry_time=entry)
    decision, _reason = evaluate(
        state,
        pred_long=0.6,
        pred_short=0.1,
        long_cfg=_LONG_CFG,
        short_cfg=_SHORT_CFG,
        now=_NOW,
    )
    assert decision == EXIT_LONG


def test_cooldown_active_returns_hold() -> None:
    """COOLDOWN + cooldown_until in the future → HOLD."""
    future = _NOW + timedelta(minutes=30)
    state = TradingState(status=COOLDOWN, armed=False, cooldown_until=future)
    decision, _reason = evaluate(
        state,
        pred_long=0.1,
        pred_short=0.1,
        long_cfg=_LONG_CFG,
        short_cfg=_SHORT_CFG,
        now=_NOW,
    )
    assert decision == HOLD


def test_flat_not_armed_returns_hold() -> None:
    """FLAT + not armed → HOLD regardless of predictions."""
    state = _flat_unarmed()
    decision, _reason = evaluate(
        state,
        pred_long=0.9,
        pred_short=0.9,
        long_cfg=_LONG_CFG,
        short_cfg=_SHORT_CFG,
        now=_NOW,
    )
    assert decision == HOLD
