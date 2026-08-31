"""Leak-safety tests for t204 attention features (epic_053).

This is the primary deliverable of t204, not the features themselves: a prior
research effort in this project leaked the future into the past through a
90-hour timestamp offset, and these tests exist so that mistake cannot repeat
here unnoticed.

Three properties, each isolated in its own test:

  shift-test  Mutating rows at t > T must not change any output row at t <= T.
  lag-test    A single hour's own raw counts must not affect that same hour's
              output row -- only the *next* hour's row may move.
  warmup-test The trailing-baseline features are NULL (not 0) for a
              subreddit's first ~14 days.

Plus a gap-fill check: an hour absent from the raw table (not just present
with n_items=0) must still be treated as a true zero, not silently dropped
from the output grid.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from modeling.analyst_signal.social_features import (
    ROLL_MIN_SAMPLES_HOURS,
    compute_social_features,
)
from modeling.tests.analyst_signal.smoke.conftest import (
    DENSE_REGIME,
    SPARSE_REGIME,
    make_synthetic_social_hourly,
)

pytestmark = pytest.mark.smoke

START = datetime(2024, 1, 1, 0, 0, 0)


# ---------------------------------------------------------------------------
# Shift-test: the future must not change the past
# ---------------------------------------------------------------------------


def test_shift_future_rows_do_not_change_past_features() -> None:
    end   = START + timedelta(days=90)
    split = START + timedelta(days=50)   # well past the 14-day warmup

    raw = make_synthetic_social_hourly(
        START, end, {"dense": DENSE_REGIME, "sparse": SPARSE_REGIME}, seed=1,
    )
    baseline = compute_social_features(raw)

    # Mutate every row strictly after the split -- a large, deliberately
    # unrealistic spike -- and leave everything at/before the split alone.
    mutated = raw.with_columns(
        pl.when(pl.col("ts_utc") > split)
        .then(pl.col("n_items") * 500 + 5000)
        .otherwise(pl.col("n_items"))
        .alias("n_items")
    )
    mutated = mutated.with_columns(
        pl.when(pl.col("ts_utc") > split)
        .then(pl.col("n_authors") * 50 + 500)
        .otherwise(pl.col("n_authors"))
        .alias("n_authors")
    )
    recomputed = compute_social_features(mutated)

    past_baseline   = baseline.filter(pl.col("ts_utc") <= split).sort(["subreddit", "ts_utc"])
    past_recomputed = recomputed.filter(pl.col("ts_utc") <= split).sort(["subreddit", "ts_utc"])

    assert past_baseline.height > 0
    assert_frame_equal(past_baseline, past_recomputed, check_row_order=True)


# ---------------------------------------------------------------------------
# Lag-test: hour t's own data cannot influence hour t's own output row
# ---------------------------------------------------------------------------


def test_lag_single_hour_spike_does_not_affect_its_own_row() -> None:
    end = START + timedelta(days=60)
    raw = make_synthetic_social_hourly(START, end, {"dense": DENSE_REGIME}, seed=2)

    spike_hour = START + timedelta(days=40, hours=3)  # well past warmup

    baseline = compute_social_features(raw)
    spiked_raw = raw.with_columns(
        pl.when(pl.col("ts_utc") == spike_hour)
        .then(pl.col("n_items") * 1000 + 100_000)
        .otherwise(pl.col("n_items"))
        .alias("n_items")
    )
    spiked = compute_social_features(spiked_raw)

    row_at_spike_baseline = baseline.filter(pl.col("ts_utc") == spike_hour).sort("subreddit")
    row_at_spike_spiked   = spiked.filter(pl.col("ts_utc") == spike_hour).sort("subreddit")
    assert_frame_equal(row_at_spike_baseline, row_at_spike_spiked)

    # The *next* hour's row must move -- otherwise the lag test would pass
    # trivially because the pipeline ignores the spike everywhere.
    next_hour = spike_hour + timedelta(hours=1)
    att_z_before = baseline.filter(pl.col("ts_utc") == next_hour)["att_z"].item()
    att_z_after  = spiked.filter(pl.col("ts_utc") == next_hour)["att_z"].item()
    assert att_z_before is not None
    assert att_z_after is not None
    assert att_z_before != pytest.approx(att_z_after)


# ---------------------------------------------------------------------------
# Warmup-test: NULL during the baseline window, never 0
# ---------------------------------------------------------------------------


def test_warmup_window_is_null_not_zero() -> None:
    end = START + timedelta(days=40)
    raw = make_synthetic_social_hourly(START, end, {"dense": DENSE_REGIME, "sparse": SPARSE_REGIME}, seed=3)
    features = compute_social_features(raw)

    warmup_cutoff = START + timedelta(hours=ROLL_MIN_SAMPLES_HOURS - 1)
    warmup_rows = features.filter(pl.col("ts_utc") < warmup_cutoff)
    assert warmup_rows.height > 0

    for col in ("att_z", "authors_z", "new_author_share"):
        assert warmup_rows[col].null_count() == warmup_rows.height, (
            f"{col}: expected all-null during warmup, got "
            f"{warmup_rows.height - warmup_rows[col].null_count()} non-null rows"
        )
        # And specifically: no row silently became 0.0 instead of null.
        assert (warmup_rows[col].fill_null(-999.0) == 0.0).sum() == 0

    post_warmup = features.filter(pl.col("ts_utc") >= START + timedelta(days=20))
    for col in ("att_z", "authors_z", "new_author_share"):
        assert post_warmup[col].null_count() < post_warmup.height, (
            f"{col}: expected some non-null rows well past warmup"
        )


# ---------------------------------------------------------------------------
# Gap-fill: an hour absent from the raw table reads as a true zero
# ---------------------------------------------------------------------------


def test_missing_hour_is_treated_as_zero_not_dropped() -> None:
    hours = [START + timedelta(hours=i) for i in range(10)]
    raw = pl.DataFrame({
        "ts_utc"          : hours,
        "subreddit"       : "test_sub",
        "kind"            : "comment",
        "source"          : "dump",
        "n_items"         : [5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
        "n_authors"       : [3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
        "n_new_authors"   : [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "n_chars"         : [100] * 10,
        "ingested_at_utc" : datetime(2026, 1, 1),
    }).with_columns(pl.col("ts_utc").cast(pl.Datetime("us")))

    # Drop the row at hour index 5 entirely -- simulating a silent hour that
    # never got a row in social_hourly, as opposed to an explicit 0 row.
    raw_with_gap = raw.filter(pl.col("ts_utc") != hours[5])
    assert raw_with_gap.height == 9

    features = compute_social_features(raw_with_gap)

    # The output grid must still contain the gap hour -- densified to 0
    # activity, not silently absent.
    assert hours[5] in features["ts_utc"].to_list()
    assert features.height == 10
