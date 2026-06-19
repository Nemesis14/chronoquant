"""Smoke tests for select_monthly_validation_weeks."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from modeling.sampling.yearly_sampler import select_monthly_validation_weeks

pytestmark = pytest.mark.smoke

YEAR = 2021


def _make_hourly_df(year: int) -> pl.DataFrame:
    """Minimal hourly DataFrame (one row per hour) for the given year."""
    start = datetime(year, 1, 1, 0, 0, 0)
    n_hours = 366 * 24 if year % 4 == 0 else 365 * 24
    times = [start + timedelta(hours=i) for i in range(n_hours)]
    return pl.DataFrame({"open_time": times}).with_columns(
        pl.col("open_time").cast(pl.Datetime("us"))
    )


def test_returns_exactly_12_weeks() -> None:
    hourly_df = _make_hourly_df(YEAR)
    weeks = select_monthly_validation_weeks(hourly_df, YEAR, seed=42)
    assert len(weeks) == 12, f"Expected 12 weeks, got {len(weeks)}"


def test_each_week_from_different_month() -> None:
    hourly_df = _make_hourly_df(YEAR)
    weeks = select_monthly_validation_weeks(hourly_df, YEAR, seed=42)
    # week_start (Monday) should be in a different month for each week
    months = [ws.month for ws, _ in weeks]
    assert len(set(months)) == 12, f"Duplicate months in validation weeks: {months}"


def test_each_week_is_seven_days() -> None:
    hourly_df = _make_hourly_df(YEAR)
    weeks = select_monthly_validation_weeks(hourly_df, YEAR, seed=42)
    for ws, we in weeks:
        delta = we - ws
        assert delta.days == 6, f"Week {ws}–{we} is {delta.days + 1} days, expected 7"


def test_week_starts_on_monday() -> None:
    hourly_df = _make_hourly_df(YEAR)
    weeks = select_monthly_validation_weeks(hourly_df, YEAR, seed=42)
    for ws, we in weeks:
        assert ws.weekday() == 0, f"Week start {ws} is not a Monday"
        assert we.weekday() == 6, f"Week end {we} is not a Sunday"


def test_same_seed_reproducible() -> None:
    hourly_df = _make_hourly_df(YEAR)
    w1 = select_monthly_validation_weeks(hourly_df, YEAR, seed=42)
    w2 = select_monthly_validation_weeks(hourly_df, YEAR, seed=42)
    assert w1 == w2, "Same seed produced different validation weeks"


def test_different_seed_different_output() -> None:
    hourly_df = _make_hourly_df(YEAR)
    w1 = select_monthly_validation_weeks(hourly_df, YEAR, seed=42)
    w2 = select_monthly_validation_weeks(hourly_df, YEAR, seed=99)
    assert w1 != w2, "Different seeds produced identical validation weeks"


def test_all_starts_within_year() -> None:
    hourly_df = _make_hourly_df(YEAR)
    weeks = select_monthly_validation_weeks(hourly_df, YEAR, seed=42)
    for ws, _ in weeks:
        assert ws.year == YEAR, f"Week start {ws} is not in year {YEAR}"


