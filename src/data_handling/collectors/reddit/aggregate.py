#!/usr/bin/env python3
"""Reddit raw layer -> social_hourly aggregate (epic_053 / t202).

Reads the parquet partitions written by ingest.py and computes an hourly,
count-only attention time series per subreddit/kind. No score-like field is
ever read here -- only id, created_utc, author and text length, per the
leak-rule in the ticket (score/num_comments/upvote_ratio reflect crawl time,
not creation time).

n_new_authors is computed against reddit_seen_authors, a small bookkeeping
table (subreddit, author) -> first_seen_ts_utc that persists across ingest
runs in analyst_signal.duckdb, so an incremental or out-of-order backfill
still yields a correct "first time this author posted here" count.

The hourly grid is filled explicitly: every hour in [start, end) gets a row,
0-filled where nothing happened -- an absent hour is never a missing row.

An optional keyword filter supports the CryptoCurrency -> CryptoCurrency:btc
split called for in the ticket Notes (filtered and unfiltered counts kept as
separate `subreddit` rows so the filter's effect stays measurable).

Usage:
    python -m data_handling.collectors.reddit.aggregate \
        --subreddit BitcoinMarkets --start 2025-01-01 --end 2025-02-01
    python -m data_handling.collectors.reddit.aggregate \
        --subreddit BitcoinMarkets --start 2025-01-01 --end 2025-02-01 --dry-run
"""

import argparse
import logging
import os
import re
import sys
from datetime import UTC, datetime

import duckdb
import polars as pl

import utils
from data_handling.collectors.reddit.ingest import month_windows, raw_dir
from data_handling.store.analyst_signal_store import (
    DB_SUBPATH,
    ensure_tables,
    get_connection,
    upsert_table,
)

logger = logging.getLogger(__name__)

#: Deleted/removed authors count toward n_items (attention happened) but not
#: toward n_authors / n_new_authors -- see ticket "Notes a szureesrol".
_DELETED_AUTHORS = ("[deleted]", "[removed]")


def _is_real_author_expr() -> pl.Expr:
    return pl.col("author").is_not_null() & ~pl.col("author").is_in(list(_DELETED_AUTHORS))


# %% Loading

def _load_partition(subreddit: str, kind: str, month_start: datetime) -> pl.DataFrame:
    """Read one monthly parquet partition, or an empty frame if absent."""
    path = os.path.join(
        raw_dir(), f"subreddit={subreddit}", f"kind={kind}", f"{month_start:%Y-%m}.parquet"
    )
    if not os.path.exists(path):
        return pl.DataFrame()
    return pl.read_parquet(path)


def load_raw_range(subreddit: str, kind: str, start: datetime, end: datetime) -> pl.DataFrame:
    """Concatenate every monthly parquet partition overlapping [start, end).

    Args:
        subreddit : Subreddit name, no r/ prefix.
        kind      : "comment" or "submission".
        start     : Inclusive window start (UTC, tz-aware).
        end       : Exclusive window end (UTC, tz-aware).

    Returns:
        Raw rows clipped to [start, end), empty frame if nothing on disk.
    """
    frames = [
        df for month_start, _ in month_windows(start, end)
        if (df := _load_partition(subreddit, kind, month_start)).height
    ]
    if not frames:
        return pl.DataFrame()
    df = pl.concat(frames, how="vertical")
    start_epoch, end_epoch = int(start.timestamp()), int(end.timestamp())
    return df.filter((pl.col("created_utc") >= start_epoch) & (pl.col("created_utc") < end_epoch))


# %% Aggregation

def aggregate_hourly(
    conn       : duckdb.DuckDBPyConnection,
    subreddit  : str,
    kind       : str,
    start      : datetime,
    end        : datetime,
    label      : str | None = None,
    keyword_re : re.Pattern | None = None,
    source     : str = "dump",
) -> pl.DataFrame:
    """Build social_hourly rows for one subreddit/kind/window.

    Args:
        conn       : Open connection to analyst_signal.duckdb (reddit_seen_authors
                     lookup/update lives here).
        subreddit  : Source subreddit name (raw-layer partition key).
        kind       : "comment" or "submission".
        start      : Inclusive window start (UTC, tz-aware).
        end        : Exclusive window end (UTC, tz-aware).
        label      : social_hourly.subreddit value to write, if different from
                     `subreddit` (e.g. "CryptoCurrency:btc" for the filtered variant).
        keyword_re : If given, keep only rows whose text matches (case-insensitive,
                     word-boundary) -- used for the CryptoCurrency BTC-mention split.
        source     : Which pipeline produced the rows -- "dump" (Watchful1
                     per-subreddit archive) or "arctic_shift" (API). Part of the
                     primary key: the two have different coverage and must stay
                     separable (epic_053 / t203).

    Returns:
        One row per hour in [start, end), 0-filled where nothing happened.
    """
    label = label or subreddit
    df = load_raw_range(subreddit, kind, start, end)

    hours = pl.datetime_range(
        start.replace(tzinfo=None), end.replace(tzinfo=None),
        interval="1h", closed="left", eager=True,
    ).cast(pl.Datetime("us"))
    grid = pl.DataFrame({"ts_utc": hours})

    if df.height:
        text = df["body"] if kind == "comment" else (
            df["title"].fill_null("") + " " + df["selftext"].fill_null("")
        )
        df = df.with_columns(text.alias("_text"))
        if keyword_re is not None:
            keep = df["_text"].map_elements(
                lambda t: bool(keyword_re.search(t or "")), return_dtype=pl.Boolean
            )
            df = df.filter(keep)

    if not df.height:
        out = grid.with_columns([
            pl.lit(0, dtype=pl.Int64).alias("n_items"),
            pl.lit(0, dtype=pl.Int64).alias("n_authors"),
            pl.lit(0, dtype=pl.Int64).alias("n_new_authors"),
            pl.lit(0, dtype=pl.Int64).alias("n_chars"),
        ])
    else:
        df = df.with_columns(
            pl.from_epoch(pl.col("created_utc"), time_unit="s")
              .dt.truncate("1h").cast(pl.Datetime("us")).alias("ts_utc")
        )

        per_hour = df.group_by("ts_utc").agg([
            pl.len().alias("n_items"),
            pl.col("author").filter(_is_real_author_expr()).n_unique().alias("n_authors"),
            pl.col("_text").str.len_chars().fill_null(0).sum().alias("n_chars"),
        ])
        out = grid.join(per_hour, on="ts_utc", how="left").with_columns([
            pl.col("n_items").fill_null(0).cast(pl.Int64),
            pl.col("n_authors").fill_null(0).cast(pl.Int64),
            pl.col("n_chars").fill_null(0).cast(pl.Int64),
        ])

        known = {
            row[0] for row in conn.execute(
                "SELECT author FROM reddit_seen_authors WHERE subreddit = ?", [label]
            ).fetchall()
        }
        first_occurrence = (
            df.filter(_is_real_author_expr())
              .sort("created_utc")
              .unique(subset=["author"], keep="first")
        )
        new_authors = (
            first_occurrence.filter(~pl.col("author").is_in(list(known)))
            if known else first_occurrence
        )
        new_per_hour = new_authors.group_by("ts_utc").agg(pl.len().alias("n_new_authors"))
        out = (
            out.join(new_per_hour, on="ts_utc", how="left")
               .with_columns(pl.col("n_new_authors").fill_null(0).cast(pl.Int64))
        )

        if new_authors.height:
            upsert_table(conn, "reddit_seen_authors", new_authors.select(
                pl.lit(label).alias("subreddit"),
                pl.col("author"),
                pl.col("ts_utc").alias("first_seen_ts_utc"),
            ))

    now = datetime.now(UTC).replace(tzinfo=None)
    return out.select([
        pl.col("ts_utc"),
        pl.lit(label).alias("subreddit"),
        pl.lit(kind).alias("kind"),
        pl.lit(source).alias("source"),
        pl.col("n_items"),
        pl.col("n_authors"),
        pl.col("n_new_authors"),
        pl.col("n_chars"),
        pl.lit(now).cast(pl.Datetime("us")).alias("ingested_at_utc"),
    ])


# %% CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subreddit", required=True, help="Subreddit name, no r/ prefix.")
    parser.add_argument("--start", required=True, help="UTC date, inclusive, YYYY-MM-DD.")
    parser.add_argument("--end", required=True, help="UTC date, exclusive, YYYY-MM-DD.")
    parser.add_argument("--kinds", nargs="+", default=["comment", "submission"],
                        choices=["comment", "submission"])
    parser.add_argument("--source", default="dump", choices=["dump", "arctic_shift"],
                        help="Which pipeline produced the raw layer being aggregated.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and print, but do not write the database.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC)
    end   = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC)

    db_path = os.path.join(utils._repo_root(), DB_SUBPATH)
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        total_written = 0
        for kind in args.kinds:
            out = aggregate_hourly(conn, args.subreddit, kind, start, end,
                                   source=args.source)
            logger.info(
                "%s/%s: %d ora, sum(n_items)=%d, sum(n_authors ora-osszeg)=%d, sum(n_new_authors)=%d, sum(n_chars)=%d",
                args.subreddit, kind, out.height,
                out["n_items"].sum(), out["n_authors"].sum(),
                out["n_new_authors"].sum(), out["n_chars"].sum(),
            )
            if not args.dry_run:
                total_written += upsert_table(conn, "social_hourly", out)
        if not args.dry_run:
            logger.info("social_hourly: %d sor irva", total_written)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
