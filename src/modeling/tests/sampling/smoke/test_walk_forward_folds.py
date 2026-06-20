"""Smoke tests for walk-forward fold generation and fold_id assignment."""

from datetime import date, timedelta

import polars as pl
import pytest

from modeling.sampling.yearly_sampler import (
    assign_walk_forward_fold_ids,
    generate_walk_forward_folds,
)

pytestmark = pytest.mark.smoke

# ---------------------------------------------------------------------------
# generate_walk_forward_folds
# ---------------------------------------------------------------------------


def test_n_folds_returned() -> None:
    folds = generate_walk_forward_folds(year=2021)
    assert len(folds) == 4


def test_fold_ids_sequential() -> None:
    folds = generate_walk_forward_folds(year=2021)
    assert [f["fold_id"] for f in folds] == [1, 2, 3, 4]


def test_train_end_before_valid_start() -> None:
    folds = generate_walk_forward_folds(year=2021)
    for fw in folds:
        train_end   = date.fromisoformat(fw["train_end"])
        valid_start = date.fromisoformat(fw["valid_start"])
        assert train_end < valid_start, (
            f"fold {fw['fold_id']}: train_end={train_end} not before valid_start={valid_start}"
        )


def test_purge_gap_satisfied() -> None:
    purge = 240
    folds = generate_walk_forward_folds(year=2021, purge_minutes=purge)
    for fw in folds:
        train_end   = date.fromisoformat(fw["train_end"])
        valid_start = date.fromisoformat(fw["valid_start"])
        # Gap in minutes: (valid_start - train_end) in days * 1440
        gap_minutes = (valid_start - train_end).days * 1440
        assert gap_minutes >= 0, (
            f"fold {fw['fold_id']}: negative gap {gap_minutes} min"
        )
        # train_end is the day before valid_start (at minimum 1 day = 1440 min gap)
        # purge_minutes = 240 < 1440, so this is always satisfied with day-level granularity
        assert gap_minutes >= 1440 - 1, (
            f"fold {fw['fold_id']}: gap {gap_minutes} min is less than 1 day"
        )


def test_non_overlapping_valid_windows() -> None:
    folds = generate_walk_forward_folds(year=2021, shift_months=3, valid_months=3)
    for i in range(len(folds) - 1):
        curr_end   = date.fromisoformat(folds[i]["valid_end"])
        next_start = date.fromisoformat(folds[i + 1]["valid_start"])
        assert curr_end < next_start, (
            f"Overlapping validation windows: fold {i+1} end={curr_end} "
            f">= fold {i+2} start={next_start}"
        )


def test_first_fold_valid_starts_at_october() -> None:
    folds = generate_walk_forward_folds(year=2021)
    first_valid_start = date.fromisoformat(folds[0]["valid_start"])
    assert first_valid_start == date(2021, 10, 1), (
        f"Expected 2021-10-01, got {first_valid_start}"
    )


def test_valid_windows_cover_expected_months() -> None:
    folds = generate_walk_forward_folds(year=2021, valid_months=3)
    for fw in folds:
        vs = date.fromisoformat(fw["valid_start"])
        ve = date.fromisoformat(fw["valid_end"])
        # valid window should span approximately 3 months (89-92 days)
        span_days = (ve - vs).days + 1
        assert 89 <= span_days <= 92, (
            f"fold {fw['fold_id']}: validation span {span_days} days outside 89-92"
        )


def test_train_windows_cover_expected_months() -> None:
    folds = generate_walk_forward_folds(year=2021, train_months=9)
    for fw in folds:
        ts = date.fromisoformat(fw["train_start"])
        te = date.fromisoformat(fw["train_end"])
        # train window should span approximately 9 months (273-276 days)
        span_days = (te - ts).days + 1
        assert 273 <= span_days <= 276, (
            f"fold {fw['fold_id']}: train span {span_days} days outside expected 9-month range"
        )


def test_fold4_valid_extends_into_next_year() -> None:
    folds = generate_walk_forward_folds(year=2021)
    fold4 = next(f for f in folds if f["fold_id"] == 4)
    valid_start = date.fromisoformat(fold4["valid_start"])
    assert valid_start.year > 2021, (
        f"Fold 4 valid_start should be in 2022+, got {valid_start}"
    )


# ---------------------------------------------------------------------------
# assign_walk_forward_fold_ids
# ---------------------------------------------------------------------------


def _make_sample_df(dates: list[date]) -> pl.DataFrame:
    """Build a minimal DataFrame with an 'open_time' column."""
    timestamps = [
        d.isoformat() + " 00:00:00"
        for d in dates
    ]
    return pl.DataFrame({"open_time": timestamps}).with_columns(
        pl.col("open_time").str.to_datetime("%Y-%m-%d %H:%M:%S")
    )


def test_valid_window_rows_get_correct_fold_id() -> None:
    folds = generate_walk_forward_folds(year=2021)
    fw1   = folds[0]

    # One row inside fold 1 valid window
    valid_date = date.fromisoformat(fw1["valid_start"]) + timedelta(days=1)
    df = _make_sample_df([valid_date])
    result = assign_walk_forward_fold_ids(df, folds)
    assert result["fold_id"].to_list() == [1]


def test_train_only_rows_get_fold_id_zero() -> None:
    folds = generate_walk_forward_folds(year=2021)
    # A date well before any validation window (train-only)
    train_date = date(2021, 5, 15)
    df = _make_sample_df([train_date])
    result = assign_walk_forward_fold_ids(df, folds)
    assert result["fold_id"].to_list() == [0]


def test_mixed_rows_assignment() -> None:
    folds = generate_walk_forward_folds(year=2021)
    fw1 = folds[0]
    fw2 = folds[1]

    train_date  = date(2021, 5, 15)
    valid1_date = date.fromisoformat(fw1["valid_start"]) + timedelta(days=2)
    valid2_date = date.fromisoformat(fw2["valid_start"]) + timedelta(days=2)

    df     = _make_sample_df([train_date, valid1_date, valid2_date])
    result = assign_walk_forward_fold_ids(df, folds)
    ids    = result.sort("open_time")["fold_id"].to_list()
    assert ids == [0, 1, 2]


def test_fold_id_column_dtype_is_int8() -> None:
    folds = generate_walk_forward_folds(year=2021)
    df    = _make_sample_df([date(2021, 5, 1)])
    result = assign_walk_forward_fold_ids(df, folds)
    assert result["fold_id"].dtype == pl.Int8
