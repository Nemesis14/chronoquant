"""Leak-safe attention features from hourly Reddit counts (epic_053 / t204).

Turns the raw per-(subreddit, hour) item/author/character counts in
``social_hourly`` into six regime-normalised features that t205 can test for
predictive value against BTC 60-minute magnitude.

Why normalisation is mandatory
-------------------------------
Raw counts are not comparable across time or across subreddits. Measured
comment rates (see t204 ticket Notes) fall 8-16x from 2021-05 to 2025-01 for
most subreddits, while r/CryptoMarkets *rises* over the same window -- so a
single global scale factor cannot work, and neither can a naive rolling
z-score fit on the full sample (that would smuggle the future into the past,
exactly the mistake this project already made once with a 90-hour timestamp
offset). Every normalisation step here is trailing-only and computed
independently per subreddit.

Pipeline (per subreddit, independently -- never pooled across subreddits)
---------------------------------------------------------------------------
1. Aggregate ``kind``/``source`` away: sum counts to one row per
   (subreddit, ts_utc). See ``_aggregate_kind_source`` for the accepted
   approximation this introduces.
2. Densify to a complete hourly grid per subreddit (missing hour = 0 activity,
   a real observation, not a gap -- required so sparse subreddits such as
   r/BitcoinMarkets, which measured 6-12 comments/hour with +/-29% Poisson
   noise, are represented honestly).
3. Deseasonalise ``n_items`` and ``n_authors`` against a trailing,
   *expanding* hour-of-week profile (see "Why expanding, not 30-day" below).
4. Trailing z-score the deseasonalised residual over a 30-day window with a
   14-day minimum warmup (``ROLL_WINDOW_HOURS`` / ``ROLL_MIN_SAMPLES_HOURS``).
   This is also where the 2023-06 API-lockout level break is absorbed: the
   rolling baseline relearns the new level within one window, so a step
   change in the raw series does not appear as a step change in the feature.
5. Apply one global ``shift(1)`` per subreddit across every derived column --
   the same t-1 lag convention used by
   ``data_handling/sync_tables/_features_polars.py``. This is the single
   choke point that guarantees leak-safety: everything upstream of the shift
   is free to use rolling windows that include the current row, because the
   shift moves the whole computation back by one hour before it is ever
   labelled with a timestamp. ``tests/analyst_signal/smoke/test_leak_safety.py``
   verifies this holds by mutating future rows and checking past output is
   bit-identical.

Why expanding, not 30-day, for the hour-of-week profile
---------------------------------------------------------
The ticket's "30-day trailing / 14-day warmup" window is stated once, for
"the baseline". Applied literally to the hour-of-week profile it would give
only ~4 occurrences of a given hour-of-week per window (30 days / 7 days),
which is far too few to average against the same +/-29% Poisson noise the
ticket calls out for sparse subreddits. The 30-day/14-day window is applied
instead to the z-score step (4), which is where the ticket's own wording
("deszezonalizalva ... majd trailing z-score") places it. The hour-of-week
profile itself uses an unbounded expanding trailing mean per
(subreddit, hour_of_week), so it keeps accumulating precision over time
while remaining strictly causal (row t's profile never includes row t's own
contribution beyond itself -- see inline comment in
``_add_seasonal_residual``). This is a recorded assumption, not a spec
requirement; if t205 or a later ticket needs the profile itself capped to a
rolling window, only ``_add_seasonal_residual`` needs to change.

Approximation: authors are summed across kind/source
-------------------------------------------------------
``social_hourly`` stores author counts already aggregated per
(subreddit, kind, source) -- there is no author-level identity available to
this layer. Summing ``n_authors`` across kind/source (comment + submission)
will double-count an author who both commented and submitted in the same
subreddit-hour. This is accepted as the only option the input schema allows;
flagged here and in the t204 ticket Notes for whoever owns t202/t203 next.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

logger = logging.getLogger(__name__)

#: Trailing baseline window for the z-score step: 30 days of hourly rows.
ROLL_WINDOW_HOURS: int = 24 * 30

#: Minimum rows in the trailing window before a z-score is emitted at all:
#: 14 days of hourly rows. Below this the output is NULL, never 0.
ROLL_MIN_SAMPLES_HOURS: int = 24 * 14

#: Raw social_hourly columns this module reads. `kind` and `source` (when
#: present) are summed away in `_aggregate_kind_source` and never appear in
#: the output.
_RAW_COUNT_COLS: tuple[str, ...] = ("n_items", "n_authors", "n_new_authors", "n_chars")

#: Final social_features columns, in output order.
FEATURE_COLUMNS: tuple[str, ...] = (
    "ts_utc",
    "subreddit",
    "att_z",
    "att_accel",
    "authors_z",
    "new_author_share",
    "items_per_author",
    "chars_per_item",
)


# %% Internal helpers


def _safe_div(numerator: pl.Expr, denominator: pl.Expr) -> pl.Expr:
    """Divide two expressions, returning null (not inf/NaN) where denom is 0."""
    return numerator / pl.when(denominator != 0).then(denominator).otherwise(None)


def _aggregate_kind_source(social_hourly: pl.DataFrame) -> pl.DataFrame:
    """Collapse `kind` (and `source`, if present) to one row per subreddit-hour.

    Args:
        social_hourly : Raw rows, one per (ts_utc, subreddit, kind[, source]).

    Returns:
        One row per (subreddit, ts_utc) with counts summed across kind/source.
    """
    missing = [c for c in ("ts_utc", "subreddit", *_RAW_COUNT_COLS) if c not in social_hourly.columns]
    if missing:
        raise ValueError(f"social_hourly is missing required columns: {missing}")

    agg = (
        social_hourly
        .group_by(["subreddit", "ts_utc"])
        .agg([pl.col(c).sum().alias(c) for c in _RAW_COUNT_COLS])
    )
    logger.info(
        "_aggregate_kind_source: %d raw rows -> %d subreddit-hour rows",
        social_hourly.height, agg.height,
    )
    return agg


def _densify_hourly_grid(agg: pl.DataFrame) -> pl.DataFrame:
    """Fill every subreddit's timeline to a complete, gap-free hourly grid.

    A missing hour in `social_hourly` means zero activity, not a gap in
    coverage -- filled with 0, never null, so sparse subreddits are not
    silently interpolated over.

    Args:
        agg : One row per (subreddit, ts_utc), from `_aggregate_kind_source`.

    Returns:
        Same columns, one row per (subreddit, hour) between that subreddit's
        own first and last observed hour, inclusive.
    """
    time_unit  = agg.schema["ts_utc"].time_unit  # type: ignore[union-attr]
    time_zone  = agg.schema["ts_utc"].time_zone  # type: ignore[union-attr]
    bounds     = agg.group_by("subreddit").agg([
        pl.col("ts_utc").min().alias("_min_ts"),
        pl.col("ts_utc").max().alias("_max_ts"),
    ])

    grid_frames: list[pl.DataFrame] = []
    for row in bounds.iter_rows(named=True):
        hours = pl.datetime_range(
            row["_min_ts"], row["_max_ts"],
            interval  = "1h",
            time_unit = time_unit,
            time_zone = time_zone,
            eager     = True,
        )
        grid_frames.append(pl.DataFrame({"subreddit": row["subreddit"], "ts_utc": hours}))
    grid = pl.concat(grid_frames)

    densified = (
        grid
        .join(agg, on=["subreddit", "ts_utc"], how="left")
        .with_columns([pl.col(c).fill_null(0).alias(c) for c in _RAW_COUNT_COLS])
        .sort(["subreddit", "ts_utc"])
    )
    logger.info(
        "_densify_hourly_grid: %d sparse rows -> %d dense rows across %d subreddits",
        agg.height, densified.height, bounds.height,
    )
    return densified


def _add_hour_of_week(df: pl.DataFrame) -> pl.DataFrame:
    """Add a deterministic 0..167 hour-of-week bucket derived from ts_utc.

    Purely calendar-derived (like the OHLCV pipeline's `feat_hour_sin` etc.),
    so this is safe to compute directly on the un-lagged ts_utc -- it carries
    no information about what happened at that hour, only when it is.
    """
    weekday = pl.col("ts_utc").dt.weekday()  # 1 (Mon) .. 7 (Sun)
    hour    = pl.col("ts_utc").dt.hour()
    return df.with_columns(((weekday - 1) * 24 + hour).alias("hour_of_week"))


def _add_seasonal_residual(df: pl.DataFrame, raw_col: str, resid_col: str) -> pl.DataFrame:
    """Subtract a trailing, causal hour-of-week baseline from `raw_col`.

    The baseline at row t is the expanding mean of `raw_col` over all rows
    up to and including t within the same (subreddit, hour_of_week) group.
    Inclusive-of-self is intentional and not a leak: row t's own raw value is
    knowable at row t (it becomes leak-safe only once the pipeline-wide
    t-1 shift is applied at the very end, in `compute_social_features`).

    Args:
        df        : Densified frame, sorted by [subreddit, ts_utc], with
                     hour_of_week already added.
        raw_col    : Column to deseasonalise, e.g. "n_items".
        resid_col : Output column name for the residual.

    Returns:
        `df` with `resid_col` added.
    """
    group = ["subreddit", "hour_of_week"]
    cum_sum   = pl.col(raw_col).cum_sum().over(group)
    cum_count = pl.col(raw_col).cum_count().over(group)
    seasonal_mean = cum_sum / cum_count
    return df.with_columns((pl.col(raw_col) - seasonal_mean).alias(resid_col))


def _add_rolling_zscore(df: pl.DataFrame, col: str, out_col: str) -> pl.DataFrame:
    """Trailing z-score of `col` over a 30-day / 14-day-warmup window, per subreddit.

    Args:
        df      : Frame sorted by [subreddit, ts_utc].
        col     : Input column (already deseasonalised, or a raw ratio).
        out_col : Output column name.

    Returns:
        `df` with `out_col` added; null while fewer than
        ROLL_MIN_SAMPLES_HOURS rows are available in a subreddit's trailing
        window.
    """
    roll_mean = pl.col(col).rolling_mean(
        window_size=ROLL_WINDOW_HOURS, min_samples=ROLL_MIN_SAMPLES_HOURS,
    ).over("subreddit")
    roll_std = pl.col(col).rolling_std(
        window_size=ROLL_WINDOW_HOURS, min_samples=ROLL_MIN_SAMPLES_HOURS,
    ).over("subreddit")
    return df.with_columns(_safe_div(pl.col(col) - roll_mean, roll_std).alias(out_col))


# %% Public entry point


def compute_social_features(social_hourly: pl.DataFrame) -> pl.DataFrame:
    """Compute the six t204 attention features from raw hourly Reddit counts.

    Args:
        social_hourly : Rows matching the social_hourly schema -- at minimum
                         ts_utc, subreddit, n_items, n_authors,
                         n_new_authors, n_chars. `kind` and `source` are
                         accepted if present and summed away.

    Returns:
        One row per (subreddit, ts_utc) -- a complete hourly grid per
        subreddit -- with columns `FEATURE_COLUMNS`. All six features carry
        the project's t-1 lag: the value stored for hour t depends only on
        data through hour t-1. att_z / authors_z / new_author_share are null
        for a subreddit's first ~14 trailing days (warmup); items_per_author
        / chars_per_item are null only where the t-1 lag itself has no prior
        row, or the denominator is 0.
    """
    if social_hourly.is_empty():
        return pl.DataFrame(schema={
            "ts_utc": pl.Datetime("us"), "subreddit": pl.Utf8,
            "att_z": pl.Float64, "att_accel": pl.Float64, "authors_z": pl.Float64,
            "new_author_share": pl.Float64, "items_per_author": pl.Float64,
            "chars_per_item": pl.Float64,
        })

    agg       = _aggregate_kind_source(social_hourly)
    densified = _densify_hourly_grid(agg)
    df        = _add_hour_of_week(densified).sort(["subreddit", "ts_utc"])

    df = _add_seasonal_residual(df, "n_items", "_resid_items")
    df = _add_seasonal_residual(df, "n_authors", "_resid_authors")
    df = _add_rolling_zscore(df, "_resid_items", "_att_z_pre")
    df = _add_rolling_zscore(df, "_resid_authors", "_authors_z_pre")

    df = df.with_columns([
        _safe_div(pl.col("n_new_authors"), pl.col("n_authors")).alias("_new_author_share_raw"),
        _safe_div(pl.col("n_items"), pl.col("n_authors")).alias("_items_per_author_pre"),
        _safe_div(pl.col("n_chars"), pl.col("n_items")).alias("_chars_per_item_pre"),
    ])
    df = _add_rolling_zscore(df, "_new_author_share_raw", "_new_author_share_pre")

    # --- single choke point: t-1 lag applied to every derived column at once ---
    pre_cols   = ["_att_z_pre", "_authors_z_pre", "_new_author_share_pre",
                  "_items_per_author_pre", "_chars_per_item_pre"]
    final_cols = ["att_z", "authors_z", "new_author_share",
                  "items_per_author", "chars_per_item"]
    df = df.sort(["subreddit", "ts_utc"]).with_columns([
        pl.col(pre).shift(1).over("subreddit").alias(final)
        for pre, final in zip(pre_cols, final_cols, strict=True)
    ])

    # att_accel is a diff of the already-lagged att_z -- no further shift needed.
    df = df.with_columns(pl.col("att_z").diff().over("subreddit").alias("att_accel"))

    result = df.select(list(FEATURE_COLUMNS))
    logger.info("compute_social_features: %d output rows", result.height)
    return result


# %% CLI

def main(argv: list[str] | None = None) -> int:
    """Load social_hourly, compute the features, write social_features."""
    import argparse
    import os
    import sys

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    from data_handling.store.analyst_signal_store import (
        ensure_tables,
        get_connection,
        upsert_table,
    )

    parser = argparse.ArgumentParser(description="Build social_features from social_hourly.")
    parser.add_argument("--source", default="dump", choices=["dump", "arctic_shift", "all"],
                        help="Which social_hourly source rows to read.")
    parser.add_argument("--kinds", nargs="+", default=["comment"],
                        choices=["comment", "submission"],
                        help="Which kinds to include. Default: comment only -- "
                             "submissions are too sparse to carry an hourly signal "
                             "(r/BitcoinMarkets posts are zero in 61.8%% of hours).")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    db_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "database", "analyst_signal", "analyst_signal.duckdb",
    )
    conn = get_connection(os.path.abspath(db_path))
    try:
        ensure_tables(conn)
        where  = [] if args.source == "all" else ["source = ?"]
        params = [] if args.source == "all" else [args.source]
        kind_ph = ", ".join("?" for _ in args.kinds)
        where.append(f"kind IN ({kind_ph})")
        params.extend(args.kinds)
        sql = f"SELECT * FROM social_hourly WHERE {' AND '.join(where)}"  # noqa: S608
        raw = pl.from_pandas(conn.execute(sql, params).fetchdf())
        logger.info("social_hourly betoltve: %d sor", raw.height)

        feats = compute_social_features(raw)
        feats = feats.with_columns(
            pl.lit(datetime.now(UTC).replace(tzinfo=None))
              .cast(pl.Datetime("us")).alias("computed_at_utc")
        )
        if args.dry_run:
            logger.info("dry-run: %d sor keszult, nem irom ki", feats.height)
        else:
            n = upsert_table(conn, "social_features", feats)
            logger.info("social_features: %d sor irva", n)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
