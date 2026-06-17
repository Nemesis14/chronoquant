"""Smoke tests for assign_segments."""

from datetime import date, datetime, timedelta

import polars as pl
import pytest

from modeling.quantitative.sampling.yearly_sampler import assign_segments

pytestmark = pytest.mark.smoke

PURGE_MINUTES = 240
VALID_WEEKS: list[tuple[date, date]] = [
    (date(2021, 3, 8), date(2021, 3, 14)),   # Mon–Sun
    (date(2021, 7, 5), date(2021, 7, 11)),
]


def _make_hourly_df(year: int) -> pl.DataFrame:
    start = datetime(year, 1, 1, 0, 0, 0)
    n_hours = 365 * 24
    times = [start + timedelta(hours=i) for i in range(n_hours)]
    return pl.DataFrame({"open_time": times}).with_columns(
        pl.col("open_time").cast(pl.Datetime("us"))
    )


def test_segment_values_only_valid_set() -> None:
    df = _make_hourly_df(2021)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES)
    values = set(result["segment"].unique().to_list())
    assert values <= {"train", "valid", "purge"}, f"Unexpected segment values: {values}"


def test_no_overlap_valid_and_train() -> None:
    df = _make_hourly_df(2021)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES)
    valid_times = set(result.filter(pl.col("segment") == "valid")["open_time"].to_list())
    train_times = set(result.filter(pl.col("segment") == "train")["open_time"].to_list())
    overlap = valid_times & train_times
    assert len(overlap) == 0, f"Overlap between valid and train: {len(overlap)} rows"


def test_purge_not_in_train_or_valid() -> None:
    df = _make_hourly_df(2021)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES)
    purge_times = set(result.filter(pl.col("segment") == "purge")["open_time"].to_list())
    train_times = set(result.filter(pl.col("segment") == "train")["open_time"].to_list())
    valid_times = set(result.filter(pl.col("segment") == "valid")["open_time"].to_list())
    assert len(purge_times & train_times) == 0, "Purge rows found in train set"
    assert len(purge_times & valid_times) == 0, "Purge rows found in valid set"


def test_purge_window_boundaries() -> None:
    """Rows exactly at the boundary edge are classified correctly."""
    delta = timedelta(minutes=PURGE_MINUTES)

    week_start, week_end = VALID_WEEKS[0]
    vs = datetime(week_start.year, week_start.month, week_start.day, 0, 0, 0)
    ve = datetime(week_end.year, week_end.month, week_end.day, 23, 59, 0)

    # One row at the pre-purge boundary start, one at pre-purge boundary end
    pre_purge_start = vs - delta
    pre_purge_end = vs - timedelta(hours=1)   # still before vs, within purge window

    # One row right after the post-valid boundary start (should be purge)
    post_purge_start = ve + timedelta(hours=1)  # just after ve, within purge window
    post_purge_end = ve + delta                 # exactly at the post-purge boundary

    # A row deep in train territory (not near any valid week)
    train_row = datetime(2021, 1, 15, 12, 0, 0)

    df = pl.DataFrame({
        "open_time": [pre_purge_start, pre_purge_end, vs, ve, post_purge_start, post_purge_end, train_row],
    }).with_columns(pl.col("open_time").cast(pl.Datetime("us")))

    # Use only the first valid week to isolate the check
    result = assign_segments(df, [VALID_WEEKS[0]], PURGE_MINUTES)
    seg = dict(zip(result["open_time"].to_list(), result["segment"].to_list(), strict=True))

    assert seg[pre_purge_start] == "purge", "pre-purge boundary start should be purge"
    assert seg[pre_purge_end] == "purge", "row just before vs should be purge"
    assert seg[vs] == "valid", "week start should be valid"
    assert seg[ve] == "valid", "week end should be valid"
    assert seg[post_purge_start] == "purge", "row just after ve should be purge"
    assert seg[train_row] == "train", "distant row should be train"


def test_all_rows_assigned() -> None:
    df = _make_hourly_df(2021)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES)
    assert len(result) == len(df), "Row count changed after segment assignment"
    assert result["segment"].null_count() == 0, "Null segments found"


def test_segment_column_added() -> None:
    df = _make_hourly_df(2021)
    assert "segment" not in df.columns
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES)
    assert "segment" in result.columns


def test_fold_id_column_added() -> None:
    df = _make_hourly_df(2021)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES)
    assert "fold_id" in result.columns


def test_fold_id_matches_valid_week_index() -> None:
    df = _make_hourly_df(2021)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES)
    for fold_idx, (week_start, week_end) in enumerate(VALID_WEEKS):
        vs = datetime(week_start.year, week_start.month, week_start.day, 0, 0, 0)
        ve = datetime(week_end.year, week_end.month, week_end.day, 23, 59, 0)
        valid_rows = result.filter(
            (pl.col("open_time") >= vs) & (pl.col("open_time") <= ve)
        )
        assert (valid_rows["fold_id"] == fold_idx).all(), (
            f"Fold {fold_idx} valid rows have wrong fold_id"
        )


def test_train_rows_have_null_fold_id() -> None:
    df = _make_hourly_df(2021)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES)
    train_rows = result.filter(pl.col("segment") == "train")
    assert train_rows["fold_id"].null_count() == len(train_rows), (
        "Train rows should have null fold_id"
    )


def test_test_segment_with_test_start() -> None:
    df = _make_hourly_df(2021)
    test_start = date(2021, 12, 1)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES, test_start=test_start)
    values = set(result["segment"].unique().to_list())
    assert "test" in values, "test segment not found when test_start is set"
    test_dt = datetime(2021, 12, 1, 0, 0, 0)
    test_rows = result.filter(pl.col("open_time") >= test_dt)
    assert (test_rows["segment"] == "test").all(), "Rows >= test_start should be 'test'"


def test_test_rows_have_null_fold_id() -> None:
    df = _make_hourly_df(2021)
    test_start = date(2021, 12, 1)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES, test_start=test_start)
    test_rows = result.filter(pl.col("segment") == "test")
    assert test_rows["fold_id"].null_count() == len(test_rows), (
        "Test rows should have null fold_id"
    )


def test_no_test_segment_without_test_start() -> None:
    df = _make_hourly_df(2021)
    result = assign_segments(df, VALID_WEEKS, PURGE_MINUTES)
    assert "test" not in result["segment"].unique().to_list(), (
        "test segment should not appear when test_start is None"
    )
