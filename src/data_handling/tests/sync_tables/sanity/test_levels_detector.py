"""S/R level + trendline detector tests (epic_071 / t910).

Pins: (1) the detector finds a known horizontal level and a known trendline
in synthetic data, (2) the look-ahead rule -- a run on data truncated at T
yields exactly the objects / state of the full run known at T, and every
``confirmed_at`` is the close of the last confirming scale bar.
"""

from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import utils
from data_handling.sync_tables import _levels_polars as lv

pytestmark = pytest.mark.sanity

T0 = datetime(2024, 1, 1)


def _path_1m(waypoints: list[tuple[float, float]]) -> pl.DataFrame:
    """1m OHLCV of a piecewise-linear price path through (hour, price) waypoints."""
    hours = np.array([w[0] for w in waypoints])
    prices = np.array([w[1] for w in waypoints])
    n_min = int(hours[-1] * 60)
    t_end = np.arange(1, n_min + 1) / 60.0          # minute close, in hours
    t_beg = t_end - 1 / 60.0
    p_beg = np.interp(t_beg, hours, prices)
    p_end = np.interp(t_end, hours, prices)
    return pl.DataFrame({
        "open_time": [T0 + timedelta(minutes=i) for i in range(n_min)],
        "open": p_beg, "high": np.maximum(p_beg, p_end), "low": np.minimum(p_beg, p_end),
        "close": p_end, "volume": np.ones(n_min),
    })


def _random_walk_1m(days: int, seed: int) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    n = days * 1440
    # mean-reverting walk so prices revisit zones (levels) and trend for a while (lines)
    x = np.zeros(n)
    for i in range(1, n):
        x[i] = x[i - 1] * 0.99995 + rng.normal(0, 0.0012)
    close = 100.0 * np.exp(x + 0.15 * np.sin(np.arange(n) / 5000.0))
    open_ = np.concatenate(([close[0]], close[:-1]))
    wig = np.abs(rng.normal(0, 0.0006, n)) * close
    return pl.DataFrame({
        "open_time": [T0 + timedelta(minutes=i) for i in range(n)],
        "open": open_, "high": np.maximum(open_, close) + wig,
        "low": np.minimum(open_, close) - wig, "close": close, "volume": np.ones(n),
    })


def _params(**scales: dict) -> dict:
    p = utils.load_levels_detector_config()
    if scales:
        p["scales"] = scales
    return p


# %% Config


def test_config_block_matches_spec() -> None:
    p = utils.load_levels_detector_config()
    assert p["scales"] == {
        "s": {"bar": "1h", "lookback_days": 7},
        "m": {"bar": "4h", "lookback_days": 30},
        "l": {"bar": "1d", "lookback_days": 180},
    }
    assert p["pivot_n"] == 3 and p["atr_window"] == 14
    assert p["level_tolerance_atr"] == 0.25 and p["break_atr"] == 0.5


# %% Resampling


def test_resample_drops_unclosed_trailing_bar() -> None:
    df = _path_1m([(0, 100.0), (5.5, 105.0)])             # 5h30m of data
    bars = lv.resample_ohlcv(df, "1h")
    assert bars.height == 5
    assert bars["close_time"][-1] == T0 + timedelta(hours=5)
    assert (bars["n_minutes"] == 60).all()


# %% Known objects


def test_finds_known_horizontal_levels() -> None:
    # zigzag 100 <-> 110 every 20h, extremes at mid-hour (no fractal ties)
    wps = [(0.0, 105.0)] + [(20 * k + 10.5, 100.0 if k % 2 == 0 else 110.0) for k in range(10)]
    res = lv.detect_scale(_path_1m(wps), "s", _params())
    prices = sorted(res.levels["price"].to_list())
    assert any(abs(x - 100.0) < 0.2 for x in prices)
    assert any(abs(x - 110.0) < 0.2 for x in prices)
    lvl100 = res.levels.filter((pl.col("price") - 100.0).abs() < 0.2)
    assert int(lvl100["n_touches"].max() or 0) >= 3  # type: ignore[arg-type]
    # bounces off the support are touches labelled 'bounce'
    fwd = lv.forward_label_events(res.bars, res.events, _params())
    sup = fwd.filter((pl.col("obj_type") == "level") & (pl.col("event_type") == "touch")
                     & (pl.col("side") == "support"))
    assert sup.height > 0 and (sup["forward_label"].drop_nulls() == "bounce").all()


def test_finds_known_rising_support_line_and_its_break() -> None:
    # lows on the line 100 + 0.05 * h at h = 40k + 10.5; highs 10 above; then a crash
    wps = [(0.0, 105.0)]
    for k in range(5):
        h_low = 40 * k + 10.5
        wps.append((h_low, 100.0 + 0.05 * (h_low - 10.5)))
        wps.append((h_low + 20, 110.0 + 0.05 * (h_low - 10.5)))
    wps.append((wps[-1][0] + 30, 80.0))
    res = lv.detect_scale(_path_1m(wps), "s", _params())
    sup = res.trendlines.filter(pl.col("kind") == "support")
    assert sup.height >= 1
    best = sup.sort("n_touches", descending=True).row(0, named=True)
    assert best["n_touches"] >= 4
    assert best["slope_per_bar"] == pytest.approx(0.05, rel=0.05)
    assert best["broken_at"] is not None
    brk = res.events.filter((pl.col("obj_id") == best["line_id"]) & (pl.col("event_type") == "break"))
    assert brk.height == 1 and brk["side"][0] == "support"
    # no resistance line: highs are rising
    assert res.trendlines.filter(pl.col("kind") == "resistance").height == 0


# %% Look-ahead


def test_confirmed_at_is_close_of_last_confirming_bar() -> None:
    p = _params()
    res = lv.detect_scale(_random_walk_1m(30, seed=1), "s", p)
    assert res.pivots.height > 0
    n = p["pivot_n"]
    bars = res.bars
    exp = [bars["close_time"][i + n] for i in res.pivots["bar_idx"].to_list()]
    assert res.pivots["confirmed_at"].to_list() == exp
    # gap-free data: = pivot bar open + (n+1) bars
    assert (res.pivots["confirmed_at"] - res.pivots["pivot_time"] == timedelta(hours=n + 1)).all()
    # events exist strictly after their object's confirmation
    conf = pl.concat([
        res.levels.select(pl.col("level_id").alias("obj_id"), "confirmed_at"),
        res.trendlines.select(pl.col("line_id").alias("obj_id"), "confirmed_at"),
    ])
    ev = res.events.join(conf, on="obj_id")
    assert ev.height == res.events.height
    assert (ev["event_time"] > ev["confirmed_at"]).all()


@pytest.mark.parametrize("scale,cfg", [
    ("t", {"bar": "15m", "lookback_days": 2}),
    ("s", {"bar": "1h", "lookback_days": 7}),
])
def test_truncated_run_equals_full_run_as_of_t(scale: str, cfg: dict) -> None:
    p = _params(**{scale: cfg})
    df = _random_walk_1m(45, seed=7)
    full = lv.detect_scale(df, scale, p)
    assert full.levels.height > 3 and full.trendlines.height > 1 and full.events.height > 10

    rng = np.random.default_rng(3)
    cuts = sorted(int(x) for x in rng.integers(10 * 1440, 45 * 1440 - 1, size=6))
    for cut in cuts:
        T = T0 + timedelta(minutes=cut)                       # arbitrary minute, mid-bar
        tr = lv.detect_scale(df.filter(pl.col("open_time") < T), scale, p)

        def known(d: pl.DataFrame, col: str = "confirmed_at", t: datetime = T) -> pl.DataFrame:
            return d.filter(pl.col(col) <= t)

        # the truncated run cannot know anything after T
        assert (tr.pivots["confirmed_at"] <= T).all()
        assert (tr.events["event_time"] <= T).all()
        assert (tr.levels["confirmed_at"] <= T).all()
        assert (tr.trendlines["confirmed_at"] <= T).all()
        # ... and knows exactly what the full run knew at T
        assert_frame_equal(tr.pivots, known(full.pivots))
        assert_frame_equal(tr.events, known(full.events, "event_time"))
        assert_frame_equal(tr.level_touches, known(full.level_touches))
        assert_frame_equal(tr.line_touches, known(full.line_touches))
        assert_frame_equal(lv.levels_as_of(tr, T, active_only=False, params=p),
                           lv.levels_as_of(full, T, active_only=False, params=p))
        assert_frame_equal(lv.trendlines_as_of(tr, T, active_only=False),
                           lv.trendlines_as_of(full, T, active_only=False))


# %% Forward labels


def test_forward_labels_have_expected_vocabulary() -> None:
    p = _params()
    res = lv.detect_scale(_random_walk_1m(30, seed=2), "s", p)
    fwd = lv.forward_label_events(res.bars, res.events, p)
    assert fwd.height == res.events.height
    allowed = {"touch": {"bounce", "break"}, "break": {"follow_through", "false_break"},
               "retest": {"hold", "fail"}}
    for et, labs in allowed.items():
        got = set(fwd.filter(pl.col("event_type") == et)["forward_label"].drop_nulls())
        assert got <= labs
    assert all(c.startswith("forward_") for c in fwd.columns if c not in res.events.columns)


def test_detect_all_scales_returns_every_configured_scale() -> None:
    out = lv.detect_all_scales(_random_walk_1m(20, seed=4), scales=["s", "m"])
    assert set(out) == {"s", "m"}
    assert all(isinstance(r, lv.ScaleResult) for r in out.values())
