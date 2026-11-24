"""t938: post-hoc live scoring helpers (synthetic data only; no DB access)."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pytest

from reports.builders import scout_live_score as ls
from reports.builders import wave_events as we
from reports.builders.scout_bench.trades import Costs

COSTS = Costs(2.0, 5.0, 1.5, {"tp": "maker", "sl": "taker", "timeout": "taker"})


def _grid(high: list[float], low: list[float], close: list[float] | None = None) -> we.MinuteData:
    n = len(high)
    c = np.asarray(close if close is not None else [100.0] * n, dtype=float)
    return we.MinuteData(m0=1000, open=np.full(n, 100.0), high=np.asarray(high, float), low=np.asarray(low, float),
                         close=c, volume=np.ones(n))


def test_first_touch_tp_sl_and_stop_first() -> None:
    m = _grid([101, 103, 101, 101], [99, 99, 99, 99])
    t = ls.first_touch(m, 1000, 4, tp=102, sl=97)
    assert (t.kind, t.bar, t.price) == ("tp", 1, 102.0)
    m = _grid([101, 103, 101, 101], [99, 96, 99, 99])           # same bar: stop first (conservative)
    assert ls.first_touch(m, 1000, 4, tp=102, sl=97).kind == "sl"


def test_first_touch_timeout_and_open() -> None:
    m = _grid([101] * 4, [99] * 4, [100, 100, 100, 100.5])
    t = ls.first_touch(m, 1000, 4, tp=102, sl=97)
    assert (t.kind, t.price, t.time) == ("timeout", 100.5, we._dt(1004))
    assert ls.first_touch(m, 1000, 8, tp=102, sl=97).kind == "open"     # window not over yet, no touch


def test_net_bps_matches_simulate_event() -> None:
    from reports.builders.scout_bench.trades import simulate_event

    m = _grid([101, 103, 101, 101], [99, 99, 99, 99])
    ref = simulate_event(m, 1000, 4, 102.0, 97.0, COSTS)
    t = ls.first_touch(m, 1000, 4, tp=102, sl=97)
    assert ls.net_bps(100.0, t, COSTS) == pytest.approx(ref["net"] * 1e4)
    assert ls.net_bps(100.0, ls.Touch("open", None, None, None), COSTS) is None


def test_run_refuses_pre_holdout_dates() -> None:
    with pytest.raises(ValueError):
        ls.run("solusdt", datetime(2026, 1, 1))
