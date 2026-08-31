"""Cost model tests (epic_055 / t401, t402, t403).

The cost model decides which setup the grid search picks, so a silent error
here would not raise -- it would just quietly choose the wrong strategy. These
tests pin the arithmetic, the funding sign convention, and the property that
matters most: with zero costs the net objective must reproduce the old gross
behaviour exactly.
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from strategy.strategy.costs import (
    DEFAULT_COSTS,
    apply_costs,
    funding_lr,
    round_trip_lr,
)

ZERO_COSTS = {
    "maker_bps": 0.0,
    "taker_bps": 0.0,
    "slippage_bps": 0.0,
    "exit_order_type": {"tp": "maker", "sl": "taker", "timeout": "taker"},
}


def _trade(entry: datetime, exit_: datetime, lr: float, reason: str = "timeout") -> dict:
    return {
        "entry_time": entry,
        "exit_time": exit_,
        "fact_log_return": lr,
        "exit_reason": reason,
    }


# %% Fees


def test_taker_taker_round_trip_is_13_bps() -> None:
    """taker 5 + slip 1.5, both sides -> 13 bps = 1.30 USD on 1000 notional."""
    cost = round_trip_lr("sl", DEFAULT_COSTS)
    assert cost == pytest.approx(0.0013)
    assert cost * 1000 == pytest.approx(1.30)


def test_maker_exit_is_cheaper_and_pays_no_slippage() -> None:
    """A resting TP limit pays the maker fee and does not cross the spread."""
    taker_exit = round_trip_lr("timeout", DEFAULT_COSTS)
    maker_exit = round_trip_lr("tp", DEFAULT_COSTS)
    assert maker_exit < taker_exit
    # entry (5 + 1.5) + exit maker 2.0, no exit slippage = 8.5 bps
    assert maker_exit == pytest.approx(0.00085)


def test_unknown_exit_reason_falls_back_to_taker() -> None:
    """An unmapped reason must not silently become free."""
    assert round_trip_lr("something_new", DEFAULT_COSTS) == round_trip_lr(
        "timeout", DEFAULT_COSTS
    )


# %% Funding


def _schedule(times: list[datetime], rates: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"funding_ts": times, "funding_rate": rates})


def test_funding_only_counts_crossed_settlements() -> None:
    base = datetime(2026, 2, 1, 8, 0)
    sched = _schedule([base, base + timedelta(hours=8)], [0.0001, 0.0002])

    # Opened after the 08:00 settlement, closed before the 16:00 one.
    assert funding_lr(base + timedelta(minutes=1), base + timedelta(hours=1),
                      "long", sched) == 0.0
    # Spans the 16:00 settlement only.
    assert funding_lr(base + timedelta(minutes=1), base + timedelta(hours=9),
                      "long", sched) == pytest.approx(0.0002)


def test_funding_settlement_at_exact_entry_is_not_charged() -> None:
    """A position opened exactly at a settlement did not hold through it."""
    base = datetime(2026, 2, 1, 8, 0)
    sched = _schedule([base], [0.0001])
    assert funding_lr(base, base + timedelta(hours=1), "long", sched) == 0.0


def test_funding_sign_is_inverted_for_short() -> None:
    """Positive funding: longs pay, shorts receive."""
    base = datetime(2026, 2, 1, 8, 0)
    sched = _schedule([base + timedelta(hours=1)], [0.0003])
    entry, exit_ = base, base + timedelta(hours=2)
    assert funding_lr(entry, exit_, "long", sched) == pytest.approx(0.0003)
    assert funding_lr(entry, exit_, "short", sched) == pytest.approx(-0.0003)


def test_funding_handles_non_8h_intervals() -> None:
    """SOLUSDT has run 2h and 4h funding intervals; the schedule drives it."""
    base = datetime(2026, 2, 1, 0, 0)
    sched = _schedule(
        [base + timedelta(hours=h) for h in (2, 4, 6)], [0.0001, 0.0001, 0.0001]
    )
    # A 7-hour position crosses all three 2-hourly settlements.
    got = funding_lr(base, base + timedelta(hours=7), "long", sched)
    assert got == pytest.approx(0.0003)


# %% Aggregation


def test_apply_costs_splits_gross_cost_and_funding() -> None:
    base = datetime(2026, 2, 1, 0, 0)
    sched = _schedule([base + timedelta(hours=1)], [0.0002])
    trades = [
        _trade(base, base + timedelta(hours=2), 0.01, "tp"),
        _trade(base + timedelta(hours=3), base + timedelta(hours=4), -0.005, "sl"),
    ]
    gross, cost, funding = apply_costs(trades, "long", sched, DEFAULT_COSTS)

    assert gross == pytest.approx(0.005)
    assert cost == pytest.approx(round_trip_lr("tp", DEFAULT_COSTS)
                                 + round_trip_lr("sl", DEFAULT_COSTS))
    # Only the first trade spans the settlement.
    assert funding == pytest.approx(0.0002)


def test_losing_trade_still_pays_the_fee() -> None:
    """The fee is charged per fill, not per outcome -- this is the whole point."""
    base = datetime(2026, 2, 1, 0, 0)
    trades = [_trade(base, base + timedelta(minutes=30), -0.02, "sl")]
    _, cost, _ = apply_costs(trades, "long", None, DEFAULT_COSTS)
    assert cost > 0


def test_zero_costs_reproduce_gross() -> None:
    """Regression guard for t403: no fees -> net equals gross exactly."""
    base = datetime(2026, 2, 1, 0, 0)
    trades = [
        _trade(base, base + timedelta(minutes=10), 0.004, "tp"),
        _trade(base + timedelta(hours=1), base + timedelta(hours=2), -0.001, "sl"),
    ]
    gross, cost, funding = apply_costs(trades, "long", None, ZERO_COSTS)
    assert cost == 0.0
    assert funding == 0.0
    assert gross - cost - funding == pytest.approx(gross)
