"""Smoke tests for assign_fold_ids."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from modeling.sampling.yearly_sampler import assign_fold_ids

pytestmark = pytest.mark.smoke

YEAR = 2021
SEED = 42
N_FOLDS = 4


def _make_hourly_df(year: int) -> pl.DataFrame:
    start = datetime(year, 1, 1, 0, 0, 0)
    n_hours = 365 * 24
    times = [start + timedelta(hours=i) for i in range(n_hours)]
    return pl.DataFrame({"open_time": times}).with_columns(
        pl.col("open_time").cast(pl.Datetime("us"))
    )


def test_fold_id_column_added() -> None:
    df = _make_hourly_df(YEAR)
    result, _ = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    assert "fold_id" in result.columns


def test_fold_id_values_in_range() -> None:
    df = _make_hourly_df(YEAR)
    result, _ = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    values = set(result["fold_id"].unique().to_list())
    assert values <= set(range(1, N_FOLDS + 1)), f"Unexpected fold_id values: {values}"


def test_all_rows_assigned_fold_id() -> None:
    df = _make_hourly_df(YEAR)
    result, _ = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    assert len(result) == len(df), "Row count changed after fold_id assignment"
    assert result["fold_id"].null_count() == 0, "Null fold_ids found"


def test_fold_week_assignments_keys() -> None:
    df = _make_hourly_df(YEAR)
    _, assignments = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    assert set(assignments.keys()) == set(range(1, N_FOLDS + 1))


def test_fold_week_assignments_have_start_end() -> None:
    df = _make_hourly_df(YEAR)
    _, assignments = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    for fold_id, weeks in assignments.items():
        for week in weeks:
            assert "start" in week, f"Missing 'start' in fold {fold_id} week"
            assert "end" in week, f"Missing 'end' in fold {fold_id} week"


def test_same_seed_reproducible() -> None:
    df = _make_hourly_df(YEAR)
    r1, a1 = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    r2, a2 = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    assert r1["fold_id"].to_list() == r2["fold_id"].to_list(), "Same seed not reproducible"
    assert a1 == a2, "Same seed produced different fold_week_assignments"


def test_different_seed_different_output() -> None:
    df = _make_hourly_df(YEAR)
    r1, _ = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    r2, _ = assign_fold_ids(df, YEAR, SEED + 1, N_FOLDS)
    assert r1["fold_id"].to_list() != r2["fold_id"].to_list(), (
        "Different seeds produced identical fold_id assignments"
    )


def test_fold_id_dtype_is_int8() -> None:
    df = _make_hourly_df(YEAR)
    result, _ = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    assert result["fold_id"].dtype == pl.Int8, (
        f"fold_id dtype should be Int8, got {result['fold_id'].dtype}"
    )


def test_all_folds_have_rows() -> None:
    df = _make_hourly_df(YEAR)
    result, _ = assign_fold_ids(df, YEAR, SEED, N_FOLDS)
    fold_counts = result.group_by("fold_id").len()
    assert len(fold_counts) == N_FOLDS, f"Expected {N_FOLDS} folds, got {len(fold_counts)}"
