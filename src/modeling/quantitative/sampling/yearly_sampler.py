"""Yearly random-hour observation selector and segment assigner.

Pure functions — no IO, no database access, no project imports.
"""

import random
from datetime import date, datetime, timedelta

import polars as pl


def select_hourly_observations(df: pl.DataFrame, year: int, seed: int) -> pl.DataFrame:
    """Select exactly one random minute per hour for a given calendar year.

    Selection is content-based and reproducible: same df + year + seed → same output
    regardless of input row order.

    Args:
        df   : DataFrame with an 'open_time' Datetime column plus payload columns.
        year : Calendar year to filter.
        seed : Reproducibility seed.

    Returns:
        DataFrame with at most 8 760 / 8 784 rows — one per distinct hour present in data.
    """
    return (
        df.filter(pl.col("open_time").dt.year() == year)
        .with_columns(
            pl.col("open_time").dt.truncate("1h").alias("_hour"),
            pl.col("open_time").cast(pl.Int64).hash(seed=seed, seed_1=seed + 1).alias("_rand"),
        )
        .sort(["_hour", "_rand"])
        .unique(subset=["_hour"], keep="first", maintain_order=False)
        .drop(["_hour", "_rand"])
        .sort("open_time")
    )


def select_monthly_validation_weeks(
    hourly_df: pl.DataFrame,
    year: int,
    seed: int,
) -> list[tuple[date, date]]:
    """Select one full Monday–Sunday week per calendar month.

    Args:
        hourly_df : Hourly observations (available for future data-availability checks).
        year      : Calendar year.
        seed      : Reproducibility seed (Python random.Random).

    Returns:
        List of up to 12 (week_start, week_end) tuples — one per month.
        The selected week may extend beyond the month boundary; that is acceptable.
    """
    rng = random.Random(seed)
    weeks: list[tuple[date, date]] = []

    for month in range(1, 13):
        mondays: list[date] = []
        d = date(year, month, 1)
        while d.month == month:
            if d.weekday() == 0:
                mondays.append(d)
            d += timedelta(days=1)

        if mondays:
            monday = rng.choice(mondays)
            weeks.append((monday, monday + timedelta(days=6)))

    return weeks


def assign_segments(
    hourly_df: pl.DataFrame,
    valid_weeks: list[tuple[date, date]],
    purge_minutes: int = 240,
) -> pl.DataFrame:
    """Assign 'train', 'valid', or 'purge' label to every row.

    Rules (evaluated in this priority order):
        valid  : rows within any validation week (Mon 00:00 → Sun 23:59)
        purge  : rows within ±purge_minutes of any validation week boundary
                 (never overlaps with the valid set)
        train  : everything else

    Args:
        hourly_df     : Hourly observations from select_hourly_observations.
        valid_weeks   : Output of select_monthly_validation_weeks.
        purge_minutes : Buffer in minutes around each validation week boundary.

    Returns:
        hourly_df with an added 'segment' Utf8 column.
    """
    delta = timedelta(minutes=purge_minutes)

    valid_expr: pl.Expr = pl.lit(value=False)
    purge_expr: pl.Expr = pl.lit(value=False)

    for week_start, week_end in valid_weeks:
        vs = datetime(week_start.year, week_start.month, week_start.day, 0, 0, 0)
        ve = datetime(week_end.year, week_end.month, week_end.day, 23, 59, 0)

        valid_expr = valid_expr | (
            (pl.col("open_time") >= vs) & (pl.col("open_time") <= ve)
        )
        purge_expr = purge_expr | (
            (pl.col("open_time") >= vs - delta) & (pl.col("open_time") < vs)
        ) | (
            (pl.col("open_time") > ve) & (pl.col("open_time") <= ve + delta)
        )

    return hourly_df.with_columns(
        pl.when(valid_expr)
        .then(pl.lit("valid"))
        .when(purge_expr)
        .then(pl.lit("purge"))
        .otherwise(pl.lit("train"))
        .alias("segment")
    )
