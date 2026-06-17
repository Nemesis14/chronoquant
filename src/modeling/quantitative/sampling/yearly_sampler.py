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
    hourly_df  : pl.DataFrame,
    year       : int,
    seed       : int,
    test_months: int = 0,
) -> list[tuple[date, date]]:
    """Select one full Monday–Sunday week per calendar month.

    Months reserved for test holdout (the last ``test_months`` months of the
    year) are excluded from validation-week selection.

    Args:
        hourly_df   : Hourly observations (available for future data-availability checks).
        year        : Calendar year.
        seed        : Reproducibility seed (Python random.Random).
        test_months : Number of trailing months to exclude (default 0 = all 12 months).

    Returns:
        List of up to ``12 - test_months`` (week_start, week_end) tuples —
        one per non-test month.  The selected week may extend beyond the month
        boundary; that is acceptable.
    """
    rng = random.Random(seed)
    weeks: list[tuple[date, date]] = []

    last_cv_month = 12 - test_months

    for month in range(1, last_cv_month + 1):
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
    hourly_df    : pl.DataFrame,
    valid_weeks  : list[tuple[date, date]],
    purge_minutes: int = 240,
    test_start   : date | None = None,
) -> pl.DataFrame:
    """Assign segment label and fold_id to every row.

    Segment priority (highest wins):
        test  : rows with open_time >= test_start (when test_start is set)
        valid : rows within any validation week (Mon 00:00 → Sun 23:59)
        purge : rows within ±purge_minutes of any validation week boundary
        train : everything else

    fold_id semantics (Int16, nullable):
        valid/purge rows: index of the corresponding validation week (0-based)
        train/test rows: null

    Args:
        hourly_df     : Hourly observations from select_hourly_observations.
        valid_weeks   : Output of select_monthly_validation_weeks.
        purge_minutes : Buffer in minutes around each validation week boundary.
        test_start    : First day of the test holdout period.  Rows on or after
                        this date receive segment='test'.  None disables test rows.

    Returns:
        hourly_df with added 'segment' (Utf8) and 'fold_id' (Int16, nullable) columns.
    """
    delta = timedelta(minutes=purge_minutes)

    # Build segment and fold_id expressions iteratively.
    # Later assignments override earlier ones; valid overrides purge.
    segment_expr: pl.Expr = pl.lit("train")
    fold_id_expr: pl.Expr = pl.lit(None).cast(pl.Int16)

    for fold_idx, (week_start, week_end) in enumerate(valid_weeks):
        vs = datetime(week_start.year, week_start.month, week_start.day, 0, 0, 0)
        ve = datetime(week_end.year, week_end.month, week_end.day, 23, 59, 0)

        pre_purge  = (pl.col("open_time") >= vs - delta) & (pl.col("open_time") < vs)
        post_purge = (pl.col("open_time") > ve) & (pl.col("open_time") <= ve + delta)
        purge_week = pre_purge | post_purge
        valid_week = (pl.col("open_time") >= vs) & (pl.col("open_time") <= ve)

        segment_expr = (
            pl.when(purge_week).then(pl.lit("purge"))
            .otherwise(segment_expr)
        )
        segment_expr = (
            pl.when(valid_week).then(pl.lit("valid"))
            .otherwise(segment_expr)
        )

        # fold_id: assign for purge and valid rows of this fold.
        # valid overrides purge when they overlap (handled by segment_expr above;
        # fold_id is the same for both so the assignment is identical).
        fold_id_expr = (
            pl.when(purge_week | valid_week).then(pl.lit(fold_idx).cast(pl.Int16))
            .otherwise(fold_id_expr)
        )

    # Test segment overrides everything — applied last.
    if test_start is not None:
        test_start_dt = datetime(test_start.year, test_start.month, test_start.day, 0, 0, 0)
        is_test = pl.col("open_time") >= test_start_dt
        segment_expr = pl.when(is_test).then(pl.lit("test")).otherwise(segment_expr)
        fold_id_expr = pl.when(is_test).then(pl.lit(None).cast(pl.Int16)).otherwise(fold_id_expr)

    return hourly_df.with_columns(
        segment_expr.alias("segment"),
        fold_id_expr.alias("fold_id"),
    )
