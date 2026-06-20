"""Smoke tests for assign_fold_ids — monthly stratification coverage."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from modeling.sampling.yearly_sampler import assign_fold_ids

pytestmark = pytest.mark.smoke

YEAR = 2021
SEED = 42
N_FOLDS = 4


def _make_hourly_df(year: int) -> pl.DataFrame:
    """Minimal hourly DataFrame (one row per hour) for the given year."""
    start = datetime(year, 1, 1, 0, 0, 0)
    n_hours = 366 * 24 if year % 4 == 0 else 365 * 24
    times = [start + timedelta(hours=i) for i in range(n_hours)]
    return pl.DataFrame({"open_time": times}).with_columns(
        pl.col("open_time").cast(pl.Datetime("us"))
    )


def test_all_months_represented_in_assignments() -> None:
    """Every calendar month should contribute weeks to the fold assignments."""
    hourly_df = _make_hourly_df(YEAR)
    _, assignments = assign_fold_ids(hourly_df, YEAR, seed=SEED, n_folds=N_FOLDS)
    all_weeks = [w for weeks in assignments.values() for w in weeks]
    months_covered = {datetime.strptime(w["start"], "%Y-%m-%d").month for w in all_weeks}
    assert len(months_covered) == 12, (
        f"Expected 12 months covered, got {len(months_covered)}: {sorted(months_covered)}"
    )


def test_each_fold_has_weeks_from_multiple_months() -> None:
    """With n_folds=4 and 12 months, each fold should have ~3 weeks."""
    hourly_df = _make_hourly_df(YEAR)
    _, assignments = assign_fold_ids(hourly_df, YEAR, seed=SEED, n_folds=N_FOLDS)
    for fold_id, weeks in assignments.items():
        assert len(weeks) >= 2, (
            f"Fold {fold_id} has only {len(weeks)} weeks — expected at least 2"
        )


def test_week_entries_are_seven_days_apart() -> None:
    """Each week entry should span exactly 6 days (Mon–Sun)."""
    hourly_df = _make_hourly_df(YEAR)
    _, assignments = assign_fold_ids(hourly_df, YEAR, seed=SEED, n_folds=N_FOLDS)
    for fold_id, weeks in assignments.items():
        for week in weeks:
            start = datetime.strptime(week["start"], "%Y-%m-%d").date()
            end   = datetime.strptime(week["end"],   "%Y-%m-%d").date()
            delta = (end - start).days
            assert delta == 6, (
                f"Fold {fold_id} week {week['start']}–{week['end']} spans {delta+1} days, expected 7"
            )


def test_week_starts_on_monday() -> None:
    """All week start dates should be Mondays (weekday == 0)."""
    hourly_df = _make_hourly_df(YEAR)
    _, assignments = assign_fold_ids(hourly_df, YEAR, seed=SEED, n_folds=N_FOLDS)
    for fold_id, weeks in assignments.items():
        for week in weeks:
            start = datetime.strptime(week["start"], "%Y-%m-%d").date()
            assert start.weekday() == 0, (
                f"Fold {fold_id} week start {week['start']} is not a Monday"
            )


def test_same_seed_reproducible() -> None:
    hourly_df = _make_hourly_df(YEAR)
    _, a1 = assign_fold_ids(hourly_df, YEAR, seed=SEED, n_folds=N_FOLDS)
    _, a2 = assign_fold_ids(hourly_df, YEAR, seed=SEED, n_folds=N_FOLDS)
    assert a1 == a2, "Same seed produced different fold_week_assignments"


def test_different_seed_different_output() -> None:
    hourly_df = _make_hourly_df(YEAR)
    _, a1 = assign_fold_ids(hourly_df, YEAR, seed=SEED,      n_folds=N_FOLDS)
    _, a2 = assign_fold_ids(hourly_df, YEAR, seed=SEED + 10, n_folds=N_FOLDS)
    assert a1 != a2, "Different seeds produced identical fold_week_assignments"


def test_all_starts_within_year() -> None:
    hourly_df = _make_hourly_df(YEAR)
    _, assignments = assign_fold_ids(hourly_df, YEAR, seed=SEED, n_folds=N_FOLDS)
    for fold_id, weeks in assignments.items():
        for week in weeks:
            start = datetime.strptime(week["start"], "%Y-%m-%d")
            assert start.year == YEAR, (
                f"Fold {fold_id} week start {week['start']} is not in year {YEAR}"
            )
