"""Smoke tests for trading.live.state.TradingState."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from trading.live.state import FLAT, LONG, TradingState

pytestmark = pytest.mark.smoke

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_hold_minutes_returns_zero_when_no_entry_time() -> None:
    """TradingState().hold_minutes() returns 0.0 when entry_time is None."""
    state = TradingState()
    assert state.hold_minutes() == 0.0


def test_from_db_no_position_returns_flat_armed() -> None:
    """TradingState.from_db(run_id, None) → FLAT, armed=True."""
    state = TradingState.from_db("run_001", None)
    assert state.status == FLAT
    assert state.armed is True


def test_from_db_with_position_restores_side() -> None:
    """TradingState.from_db with open position dict → state.side matches dict['side']."""
    pos = {
        "position_id": "pos_123",
        "side":        LONG,
        "entry_time":  "2024-01-15T10:00:00",
        "entry_price": 150.0,
        "quantity":    10.0,
    }
    state = TradingState.from_db("run_002", pos)
    assert state.side == LONG
    assert state.status == LONG
    assert state.armed is False


def test_record_trade_result_increments_daily_counter() -> None:
    """record_trade_result increments daily_trade_count."""
    state = TradingState()
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    state.last_trade_date = today
    state.daily_trade_count = 2

    state.record_trade_result(pnl_usdt=50.0)

    assert state.daily_trade_count == 3


def test_record_trade_result_resets_on_new_day() -> None:
    """record_trade_result resets daily counter when date changes."""
    state = TradingState()
    state.last_trade_date = "2020-01-01"   # past date
    state.daily_trade_count = 99

    state.record_trade_result(pnl_usdt=-10.0)

    assert state.daily_trade_count == 1
    assert state.last_trade_date == datetime.now(UTC).strftime("%Y-%m-%d")
