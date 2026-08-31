"""Triple-barrier labelling tests (epic_058 / t701).

The label decides what the model learns, so a silent error here would not
raise -- it would just teach the wrong thing. These tests pin the two
conventions that must match the strategy simulator (intrabar touch, same-bar
conflict resolves down) and the boundary behaviour at the end of the series.
"""

import numpy as np
import pytest

from data_handling.sync_tables._triple_barrier import (
    class_distribution,
    horizon_barrier_bps,
    label_column_name,
    triple_barrier_labels,
)


def _flat(n: int, price: float = 100.0) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Perfectly flat series: no barrier can ever be touched."""
    close = np.full(n, price)
    return close, close.copy(), close.copy()


def test_flat_series_is_all_timeout() -> None:
    close, high, low = _flat(50)
    labels = triple_barrier_labels(close, high, low, horizon=10)
    assert np.nansum(np.abs(labels)) == 0.0
    assert (labels[:40] == 0.0).all()


def test_last_horizon_rows_are_nan_not_zero() -> None:
    """0 is a real class (timeout); an incomplete window must be NaN."""
    close, high, low = _flat(50)
    labels = triple_barrier_labels(close, high, low, horizon=10)
    assert np.isnan(labels[-10:]).all()
    assert not np.isnan(labels[:-10]).any()


def test_upper_barrier_touched_first_labels_plus_one() -> None:
    close, high, low = _flat(30)
    high[5] = 101.5  # +150 bps, inside a 100 bps barrier
    labels = triple_barrier_labels(close, high, low, horizon=10, up_bps=100, dn_bps=100)
    # Bars 0..4 have the spike inside their forward window.
    assert (labels[0:5] == 1.0).all()


def test_lower_barrier_touched_first_labels_minus_one() -> None:
    close, high, low = _flat(30)
    low[5] = 98.5
    labels = triple_barrier_labels(close, high, low, horizon=10, up_bps=100, dn_bps=100)
    assert (labels[0:5] == -1.0).all()


def test_earlier_barrier_wins() -> None:
    """Both barriers get touched; the earlier bar decides."""
    close, high, low = _flat(30)
    high[7] = 101.5   # up at bar 7
    low[3] = 98.5     # down at bar 3 -- earlier
    labels = triple_barrier_labels(close, high, low, horizon=10, up_bps=100, dn_bps=100)
    assert labels[0] == -1.0

    close2, high2, low2 = _flat(30)
    high2[3] = 101.5  # up earlier this time
    low2[7] = 98.5
    labels2 = triple_barrier_labels(close2, high2, low2, horizon=10, up_bps=100, dn_bps=100)
    assert labels2[0] == 1.0


def test_same_bar_conflict_resolves_to_lower() -> None:
    """Matches the simulator's sl_first rule: a bar touching both is a loss."""
    close, high, low = _flat(30)
    high[4] = 101.5
    low[4] = 98.5
    labels = triple_barrier_labels(close, high, low, horizon=10, up_bps=100, dn_bps=100)
    assert labels[0] == -1.0


def test_barrier_outside_window_is_ignored() -> None:
    """A touch beyond the vertical barrier must not leak backwards."""
    close, high, low = _flat(40)
    high[20] = 101.5
    labels = triple_barrier_labels(close, high, low, horizon=10, up_bps=100, dn_bps=100)
    # Bar 0's window is bars 1..10 -- the spike at 20 is out of reach.
    assert labels[0] == 0.0
    # Bar 10's window is 11..20 -- it does reach.
    assert labels[10] == 1.0


def test_exact_touch_counts() -> None:
    """>= and <=, not strict: hitting the barrier exactly is a touch."""
    close, high, low = _flat(30)
    high[5] = 101.0  # exactly +100 bps
    labels = triple_barrier_labels(close, high, low, horizon=10, up_bps=100, dn_bps=100)
    assert labels[0] == 1.0


def test_chunking_does_not_change_the_answer() -> None:
    rng = np.random.default_rng(11)
    n = 5_000
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.0008, n)))
    high = close * (1 + np.abs(rng.normal(0, 0.0006, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.0006, n)))

    whole = triple_barrier_labels(close, high, low, horizon=240, chunk_rows=10_000)
    chunked = triple_barrier_labels(close, high, low, horizon=240, chunk_rows=137)
    assert np.allclose(whole, chunked, equal_nan=True)


def test_wider_barriers_produce_more_timeouts() -> None:
    """Sanity on the monotonic relationship the barrier grid relies on."""
    rng = np.random.default_rng(5)
    n = 3_000
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.0008, n)))
    high = close * (1 + np.abs(rng.normal(0, 0.0006, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.0006, n)))

    narrow = class_distribution(triple_barrier_labels(close, high, low, 240, 50, 50))
    wide = class_distribution(triple_barrier_labels(close, high, low, 240, 200, 200))
    assert wide["0"] > narrow["0"]


def test_column_name() -> None:
    assert label_column_name(240, 100) == "tb_label_240_100"
    assert label_column_name(60, 50.0) == "tb_label_60_50"


@pytest.mark.parametrize("horizon", [1, 5, 240])
def test_series_shorter_than_horizon_is_all_nan(horizon: int) -> None:
    close, high, low = _flat(horizon)
    labels = triple_barrier_labels(close, high, low, horizon=horizon)
    assert np.isnan(labels).all()


def test_array_barrier_matches_scalar() -> None:
    """A constant array barrier must reproduce the scalar result exactly.

    This is the guard on the epic_060 change: the per-row path is a superset of
    the scalar path, so if the two ever diverge the volatility-scaled labels
    are not comparable to the fixed-barrier ones they are measured against.
    """
    rng = np.random.default_rng(7)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0, 0.001, 4000)))
    high = close * (1.0 + np.abs(rng.normal(0, 0.0008, 4000)))
    low = close * (1.0 - np.abs(rng.normal(0, 0.0008, 4000)))

    scalar = triple_barrier_labels(close, high, low, horizon=60, up_bps=100.0,
                                   dn_bps=100.0)
    array = triple_barrier_labels(close, high, low, horizon=60,
                                  up_bps=np.full(4000, 100.0),
                                  dn_bps=np.full(4000, 100.0))
    assert np.array_equal(scalar, array, equal_nan=True)


def test_nan_barrier_yields_nan_label() -> None:
    """A bar with no usable barrier must not become a training row."""
    rng = np.random.default_rng(11)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0, 0.001, 500)))
    high, low = close * 1.002, close * 0.998
    width = np.full(500, 50.0)
    width[:100] = np.nan

    out = triple_barrier_labels(close, high, low, horizon=60, up_bps=width,
                                dn_bps=width)
    assert np.isnan(out[:100]).all()
    assert not np.isnan(out[100:440]).any()


def test_horizon_barrier_is_lagged_and_clipped() -> None:
    """The barrier must be knowable at t-1 and must respect its bounds."""
    rng = np.random.default_rng(3)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0, 0.001, 5000)))

    width = horizon_barrier_bps(close, horizon=240, k=1.0, span=1440,
                                min_bps=60.0, max_bps=400.0)
    valid = width[~np.isnan(width)]
    assert valid.size > 0
    assert valid.min() >= 60.0 and valid.max() <= 400.0

    # Doubling volatility from bar 3000 must not move any earlier barrier:
    # that is what "no lookahead" means here.
    bumped = close.copy()
    bumped[3000:] = close[3000:] * np.exp(np.cumsum(rng.normal(0, 0.004, 2000)))
    width2 = horizon_barrier_bps(bumped, horizon=240, k=1.0, span=1440,
                                 min_bps=60.0, max_bps=400.0)
    assert np.array_equal(width[:3000], width2[:3000], equal_nan=True)
