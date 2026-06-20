"""Yearly random-hour observation selector, fold-id assigner, and walk-forward fold generator.

Pure functions — no IO, no database access, no project imports.
"""

import random
from calendar import monthrange
from datetime import date, timedelta

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


def assign_fold_ids(
    hourly_df: pl.DataFrame,
    year: int,
    seed: int,
    n_folds: int = 4,
) -> tuple[pl.DataFrame, dict[int, list[dict[str, str]]]]:
    """Assign fold_id 1..n_folds to every row based on calendar week.

    For each month, Monday-starting weeks are randomly shuffled and assigned
    fold labels 1..n_folds cyclically. Returns df with 'fold_id' column (Int8)
    and fold_week_assignments dict.

    Args:
        hourly_df : Hourly observations from select_hourly_observations.
        year      : Calendar year.
        seed      : Reproducibility seed.
        n_folds   : Number of folds (default 4).

    Returns:
        (df_with_fold_id, fold_week_assignments)
        fold_week_assignments: {1: [{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}, ...], ...}
    """
    rng = random.Random(seed)

    monday_to_fold: dict[date, int] = {}
    fold_week_assignments: dict[int, list[dict[str, str]]] = {k: [] for k in range(1, n_folds + 1)}

    for month in range(1, 13):
        mondays: list[date] = []
        d = date(year, month, 1)
        while d.month == month:
            if d.weekday() == 0:
                mondays.append(d)
            d += timedelta(days=1)

        if mondays:
            shuffled = mondays.copy()
            rng.shuffle(shuffled)
            for i, monday in enumerate(shuffled):
                fold_id = (i % n_folds) + 1
                monday_to_fold[monday] = fold_id
                week_end = monday + timedelta(days=6)
                fold_week_assignments[fold_id].append({
                    "start": str(monday),
                    "end":   str(week_end),
                })

    # Build date→fold_id covering all 7 days of each assigned week
    date_to_fold: dict[date, int] = {}
    for monday, fold_id in monday_to_fold.items():
        for offset in range(7):
            date_to_fold[monday + timedelta(days=offset)] = fold_id

    fold_map_df = pl.DataFrame({
        "date":    list(date_to_fold.keys()),
        "fold_id": pl.Series(list(date_to_fold.values()), dtype=pl.Int8),
    })

    result = (
        hourly_df
        .with_columns(pl.col("open_time").dt.date().alias("date"))
        .join(fold_map_df, on="date", how="left")
        .with_columns(pl.col("fold_id").fill_null(pl.lit(1, dtype=pl.Int8)))
        .drop("date")
    )

    return result, fold_week_assignments


def generate_walk_forward_folds(
    year         : int,
    train_months : int = 9,
    valid_months : int = 3,
    shift_months : int = 3,
    purge_minutes: int = 240,
    n_folds      : int = 4,
) -> list[dict]:
    """Generate walk-forward fold time windows for a given anchor year.

    The first fold's validation window starts at ``year-10-01`` (October of the
    anchor year), shifted back by ``(n_folds - 1) * shift_months`` months so
    that fold 1 always begins at October of the anchor year.

    Validation windows are non-overlapping when ``shift_months == valid_months``.
    Training windows end ``purge_minutes`` before the validation window starts.

    Args:
        year         : Anchor calendar year (e.g. 2021).
        train_months : Training window length in months.
        valid_months : Validation window length in months.
        shift_months : Months to shift between successive folds.
        purge_minutes: Minutes to exclude around fold boundaries (informational;
                       stored in metadata but applied at search time).
        n_folds      : Number of folds to generate (default 4).

    Returns:
        List of dicts, each with keys:
        ``fold_id``, ``train_start``, ``train_end``,
        ``valid_start``, ``valid_end`` (all dates as "YYYY-MM-DD" strings).
    """
    def _month_start(y: int, m: int) -> date:
        """Return first day of month, wrapping year if needed."""
        while m > 12:
            m -= 12
            y += 1
        while m < 1:
            m += 12
            y -= 1
        return date(y, m, 1)

    def _month_end(y: int, m: int) -> date:
        """Return last day of month, wrapping year if needed."""
        while m > 12:
            m -= 12
            y += 1
        while m < 1:
            m += 12
            y -= 1
        _, last_day = monthrange(y, m)
        return date(y, m, last_day)

    # First fold valid start: October of anchor year (month 10)
    first_valid_month = 10

    folds = []
    for fold_idx in range(n_folds):
        # Valid window start for this fold
        shift_offset     = fold_idx * shift_months
        valid_start_month = first_valid_month + shift_offset
        valid_start      = _month_start(year, valid_start_month)
        valid_end        = _month_end(year, valid_start_month + valid_months - 1)

        # Train window: train_months ending right before valid_start
        # train_end is the last day of the month preceding valid_start by one day
        # We compute: valid_start - 1 day → last day of previous month
        train_end_date   = valid_start - timedelta(days=1)
        # train_start: go back train_months from the month of train_end
        train_end_m      = train_end_date.month
        train_end_y      = train_end_date.year
        train_start_month = train_end_m - train_months + 1
        train_start      = _month_start(train_end_y, train_start_month)

        folds.append({
            "fold_id"    : fold_idx + 1,
            "train_start": str(train_start),
            "train_end"  : str(train_end_date),
            "valid_start": str(valid_start),
            "valid_end"  : str(valid_end),
        })

    return folds


def assign_walk_forward_fold_ids(
    hourly_df        : pl.DataFrame,
    fold_time_windows: list[dict],
) -> pl.DataFrame:
    """Assign fold_id to each row based on walk-forward validation windows.

    Rows falling inside a fold's validation window receive that fold's
    ``fold_id`` (1..n).  All other rows receive ``fold_id = 0`` (train-only).

    Args:
        hourly_df         : Hourly observations (must have 'open_time' column).
        fold_time_windows : Output of ``generate_walk_forward_folds()``.

    Returns:
        DataFrame with 'fold_id' column (Int8) added.
    """
    # Start with fold_id = 0 for all rows
    result = hourly_df.with_columns(pl.lit(0, dtype=pl.Int8).alias("fold_id"))

    for fw in fold_time_windows:
        valid_start = date.fromisoformat(fw["valid_start"])
        valid_end   = date.fromisoformat(fw["valid_end"])
        fold_id     = int(fw["fold_id"])

        result = result.with_columns(
            pl.when(
                (pl.col("open_time").dt.date() >= valid_start)
                & (pl.col("open_time").dt.date() <= valid_end)
            )
            .then(pl.lit(fold_id, dtype=pl.Int8))
            .otherwise(pl.col("fold_id"))
            .alias("fold_id")
        )

    return result
