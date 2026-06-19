"""Smoke tests for select_hourly_observations."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from modeling.sampling.yearly_sampler import select_hourly_observations

pytestmark = pytest.mark.smoke

YEAR = 2021


def _make_minute_df(year: int, n_days: int = 3) -> pl.DataFrame:
    """Synthetic 1-min DataFrame: n_days worth of minute-level rows."""
    start = datetime(year, 1, 1, 0, 0, 0)
    times = [start + timedelta(minutes=i) for i in range(n_days * 24 * 60)]
    return pl.DataFrame({"open_time": times, "close": [100.0] * len(times)}).with_columns(
        pl.col("open_time").cast(pl.Datetime("us"))
    )


def test_exactly_one_row_per_hour() -> None:
    df = _make_minute_df(YEAR)
    result = select_hourly_observations(df, YEAR, seed=42)

    hours = result.with_columns(pl.col("open_time").dt.truncate("1h").alias("h"))["h"]
    assert hours.n_unique() == len(result), "Duplicate hours found in output"
    assert len(result) == 3 * 24, f"Expected {3 * 24} rows, got {len(result)}"


def test_same_seed_same_output() -> None:
    df = _make_minute_df(YEAR)
    r1 = select_hourly_observations(df, YEAR, seed=42)
    r2 = select_hourly_observations(df, YEAR, seed=42)
    assert r1.equals(r2), "Same seed produced different outputs"


def test_different_seed_different_output() -> None:
    df = _make_minute_df(YEAR)
    r1 = select_hourly_observations(df, YEAR, seed=42)
    r2 = select_hourly_observations(df, YEAR, seed=99)
    # Different seeds should select different minutes (they may collide rarely on small data)
    # but open_time sets should differ
    assert not r1.equals(r2), "Different seeds produced identical outputs"


def test_all_selected_rows_exist_in_input() -> None:
    df = _make_minute_df(YEAR)
    result = select_hourly_observations(df, YEAR, seed=42)

    input_times = set(df["open_time"].to_list())
    output_times = set(result["open_time"].to_list())
    assert output_times.issubset(input_times), "Output contains rows not in the input"


def test_output_sorted_by_open_time() -> None:
    df = _make_minute_df(YEAR)
    result = select_hourly_observations(df, YEAR, seed=42)
    times = result["open_time"].to_list()
    assert times == sorted(times), "Output is not sorted by open_time"


def test_filters_to_requested_year() -> None:
    df_2021 = _make_minute_df(2021, n_days=2)
    df_2022 = _make_minute_df(2022, n_days=2)
    df = pl.concat([df_2021, df_2022])

    result = select_hourly_observations(df, 2021, seed=42)
    years = result["open_time"].dt.year().unique().to_list()
    assert years == [2021], f"Unexpected years in output: {years}"
