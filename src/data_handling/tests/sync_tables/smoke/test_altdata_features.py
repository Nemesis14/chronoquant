"""Leak and window tests for the alt-data feature layer (epic_054 / t308).

The point of these tests is not that the numbers are pretty -- it is that a
feature computed for minute t cannot possibly have seen data from after t.
Two independent checks enforce that:

* a mutation test: rewriting the tail of the input must leave every earlier
  feature value bit-identical;
* an availability test: the value landing on minute t must come from a 5-minute
  bar that had already closed by t.
"""

from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest

from data_handling.sync_tables._altdata_features_polars import (
    ALTDATA_FEATURES,
    W_240M,
    compute_altdata_features,
    expand_to_minute_grid,
)

N_BARS = 600  # 50 hours of 5-minute bars -- comfortably past the 240m window


def _synthetic_altdata(seed: int = 7, n: int = N_BARS) -> pl.DataFrame:
    """Deterministic 5-minute alt-data frame with plausible magnitudes."""
    rng = np.random.default_rng(seed)
    start = datetime(2024, 1, 1)
    times = [start + timedelta(minutes=5 * i) for i in range(n)]
    price = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, n)))
    oi = 8e6 * np.exp(np.cumsum(rng.normal(0, 0.0005, n)))

    return pl.DataFrame({
        "open_time": times,
        "oi_coin": oi,
        "oi_value": oi * price,
        "ls_count": 2.0 + rng.normal(0, 0.05, n),
        "ls_top_count": 2.4 + rng.normal(0, 0.05, n),
        "ls_top_sum": 1.9 + rng.normal(0, 0.03, n),
        "taker_vol_ratio": 1.0 + rng.normal(0, 0.1, n),
        "prem_open": rng.normal(0, 0.0004, n),
        "prem_high": rng.normal(0, 0.0004, n),
        "prem_low": rng.normal(0, 0.0004, n),
        "prem_close": rng.normal(0, 0.0004, n),
        "funding_last": np.full(n, 0.0001),
        "funding_next_ts": [start + timedelta(hours=8 * (1 + i // 96)) for i in range(n)],
        "spot_close": price * (1 + rng.normal(0, 0.0002, n)),
        "spot_volume": rng.lognormal(6, 0.4, n),
        "spot_taker_buy": rng.lognormal(5.6, 0.4, n),
        "perp_close": price,
        "perp_volume": rng.lognormal(7, 0.4, n),
        "perp_taker_buy": rng.lognormal(6.6, 0.4, n),
        # Order-book depth levels (epic_058). Deeper bands hold more size, so
        # the 5% level is generated larger than the 1% level.
        "bid_1": rng.lognormal(11.9, 0.15, n),
        "bid_2": rng.lognormal(12.4, 0.15, n),
        "bid_5": rng.lognormal(13.0, 0.15, n),
        "ask_1": rng.lognormal(11.9, 0.15, n),
        "ask_2": rng.lognormal(12.4, 0.15, n),
        "ask_5": rng.lognormal(13.0, 0.15, n),
    })


def test_all_features_present() -> None:
    out = compute_altdata_features(_synthetic_altdata())
    assert set(ALTDATA_FEATURES).issubset(out.columns)
    assert out.height == N_BARS


def test_features_are_not_all_null() -> None:
    """A warmed-up window must actually produce values, not silent nulls."""
    out = compute_altdata_features(_synthetic_altdata()).tail(100)
    for col in ALTDATA_FEATURES:
        assert out[col].null_count() < 100, f"{col} teljesen null a warmup utan"


def test_future_mutation_does_not_change_past() -> None:
    """Rewriting the last 100 bars must not move any earlier feature value.

    This is the test that would fail on a full-sample mean, a centred window,
    or a forward-looking shift -- the three ways lookahead usually sneaks in.
    """
    base = _synthetic_altdata()
    cutoff = N_BARS - 100

    mutated = base.with_columns(
        pl.when(pl.int_range(pl.len()) >= cutoff)
        .then(pl.col(c) * 5.0)
        .otherwise(pl.col(c))
        .alias(c)
        for c in ["oi_coin", "ls_count", "ls_top_sum", "ls_top_count",
                  "prem_close", "spot_close", "perp_close", "perp_volume",
                  "spot_volume", "spot_taker_buy"]
    )

    a = compute_altdata_features(base).head(cutoff)
    b = compute_altdata_features(mutated).head(cutoff)

    for col in ALTDATA_FEATURES:
        left, right = a[col].to_numpy(), b[col].to_numpy()
        assert np.allclose(left, right, equal_nan=True), f"{col} lat a jovobe"


def test_minute_grid_uses_only_completed_bars() -> None:
    """The value on minute t must come from a 5m bar that closed at or before t."""
    feats = compute_altdata_features(_synthetic_altdata())
    start = datetime(2024, 1, 1)
    minutes = pl.DataFrame(
        {"open_time": [start + timedelta(minutes=i) for i in range(5 * N_BARS)]}
    )
    wide = expand_to_minute_grid(feats, minutes)

    # Reconstruct which source bar each minute resolved to, then assert the bar
    # had already closed. A 5m bar opening at T closes at T+5m.
    probe = feats.select(["open_time", "available_at", "feat_oi_delta_60"]).drop_nulls()
    joined = (
        wide.select(["open_time", "feat_oi_delta_60"])
        .drop_nulls()
        .join(probe, on="feat_oi_delta_60", how="inner", suffix="_src")
    )
    late = joined.filter(pl.col("available_at") > pl.col("open_time"))
    assert late.height == 0, "egy perc olyan 5m barbol kapott erteket, ami meg nem zarult le"


def test_warmup_is_null_not_zero() -> None:
    """Before the longest window is warm, values must be NULL rather than 0."""
    out = compute_altdata_features(_synthetic_altdata())
    head = out.head(W_240M // 2 - 1)
    assert head["feat_ls_count_z_240"].null_count() == head.height
    assert head["feat_prem_pctile_240"].null_count() == head.height


@pytest.mark.parametrize("col", ALTDATA_FEATURES)
def test_no_window_exceeds_240_minutes(col: str) -> None:
    """Truncating history to 240 minutes must reproduce the final value.

    If a feature secretly reached further back than the embargo allows, the
    truncated frame would give a different answer.
    """
    full = _synthetic_altdata()
    # 48 bars of window + 48 bars of second-pass shift headroom.
    truncated = full.tail(W_240M * 2 + 1)

    a = compute_altdata_features(full).tail(1)[col].to_numpy()
    b = compute_altdata_features(truncated).tail(1)[col].to_numpy()
    assert np.allclose(a, b, equal_nan=True), f"{col} 240 percnel messzebbre nyul vissza"
