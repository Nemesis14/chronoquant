"""Feature-behaviour tests for t204 attention features (epic_053).

Complements test_leak_safety.py (leak / lag / warmup) with the remaining t204
acceptance criteria: output shape, per-subreddit independence of the
normalisation, and that a large level break (like the 2023-06 API lockout)
disappears from the normalised output.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from modeling.analyst_signal.social_features import (
    FEATURE_COLUMNS,
    ROLL_WINDOW_HOURS,
    compute_social_features,
)
from modeling.tests.analyst_signal.smoke.conftest import (
    DENSE_REGIME,
    SPARSE_REGIME,
    make_synthetic_social_hourly,
)

pytestmark = pytest.mark.smoke

START = datetime(2024, 1, 1, 0, 0, 0)


def test_output_has_expected_columns_and_hourly_grid() -> None:
    end = START + timedelta(days=20)
    raw = make_synthetic_social_hourly(START, end, {"dense": DENSE_REGIME}, seed=4)
    features = compute_social_features(raw)

    assert list(features.columns) == list(FEATURE_COLUMNS)
    # One row per hour, inclusive of both endpoints.
    expected_rows = int((end - START) / timedelta(hours=1)) + 1
    assert features.height == expected_rows


def test_subreddits_are_normalised_independently() -> None:
    end = START + timedelta(days=70)
    raw_both  = make_synthetic_social_hourly(
        START, end, {"dense": DENSE_REGIME, "sparse": SPARSE_REGIME}, seed=5,
    )
    raw_dense_only = raw_both.filter(pl.col("subreddit") == "dense")

    features_both  = compute_social_features(raw_both).filter(pl.col("subreddit") == "dense")
    features_alone = compute_social_features(raw_dense_only)

    # Whatever happens in the sparse subreddit's regime (rate, breaks, noise)
    # must have zero effect on the dense subreddit's features -- proves the
    # pipeline never pools across subreddits.
    joined = features_both.join(features_alone, on="ts_utc", suffix="_alone")
    assert joined.height == features_both.height
    for col in ("att_z", "authors_z", "new_author_share", "items_per_author", "chars_per_item"):
        left  = joined[col]
        right = joined[f"{col}_alone"]
        both_null = left.is_null() & right.is_null()
        close = ((left - right).abs() < 1e-9) | both_null
        assert close.all(), f"{col}: dense-subreddit features changed when sparse subreddit was present"


def test_level_break_disappears_after_normalisation() -> None:
    """A large, sustained rate change (like the 2023-06 API lockout) must not
    show up as a sustained deviation in att_z once the rolling baseline has
    had a full 30-day window to relearn the new level.
    """
    break_at = START + timedelta(days=60)
    end      = break_at + timedelta(days=60)
    regime: dict = {
        "breaking": {
            "rate_before": 2000.0,
            "rate_after": 130.0,     # ~15x drop, matching the measured
            "break_at": break_at,    # r/CryptoCurrency 2021-05 -> 2025-01 fall
        }
    }
    raw = make_synthetic_social_hourly(START, end, regime, seed=6)
    features = raw.pipe(compute_social_features)

    pre_break_window  = features.filter(
        (pl.col("ts_utc") >= START + timedelta(days=30))
        & (pl.col("ts_utc") < break_at)
    )
    # Evaluate well after break_at + one full 30-day baseline window, so the
    # rolling mean/std have fully purged the old regime.
    post_recovery_start = break_at + timedelta(hours=ROLL_WINDOW_HOURS) + timedelta(days=5)
    post_break_window = features.filter(pl.col("ts_utc") >= post_recovery_start)

    assert pre_break_window.height > 0
    assert post_break_window.height > 0

    pre_mean  = pre_break_window["att_z"].drop_nulls().mean()
    post_mean = post_break_window["att_z"].drop_nulls().mean()
    assert pre_mean is not None
    assert post_mean is not None

    # Both windows should sit close to a 0-centred baseline -- the level
    # break should not leave att_z sitting at a persistent offset in either
    # window.
    assert abs(pre_mean) < 1.0, f"pre-break att_z mean not centred: {pre_mean}"
    assert abs(post_mean) < 1.0, f"post-break att_z mean not centred: {post_mean}"

    # Sanity: confirm the break is real in the raw signal, so the assertions
    # above are actually testing something.
    raw_pre  = raw.filter((pl.col("ts_utc") >= START + timedelta(days=30)) & (pl.col("ts_utc") < break_at))
    raw_post = raw.filter(pl.col("ts_utc") >= post_recovery_start)
    assert raw_pre["n_items"].mean() > 5 * raw_post["n_items"].mean(), (
        "synthetic regime break is too small to be a meaningful test"
    )
