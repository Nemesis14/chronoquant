#!/usr/bin/env python3
"""Reddit raw-layer ingest: subreddit dumps -> partitioned parquet (epic_053 / t202).

Fetches comments and/or submissions for one subreddit over a date range via
the Arctic Shift API (api.py) and writes them to the raw layer as parquet,
partitioned by subreddit / kind / month. Nothing is aggregated here -- that
is aggregate.py's job. Re-running over an already-fetched range is cheap: the
underlying page cache in api.py makes it request-free, and this module
overwrites the same monthly parquet file (idempotent by construction).

Usage:
    python -m data_handling.collectors.reddit.ingest \
        --subreddit BitcoinMarkets --start 2025-01-01 --end 2025-02-01
    python -m data_handling.collectors.reddit.ingest \
        --subreddit BitcoinMarkets --start 2025-01-01 --end 2025-02-01 --dry-run
"""

import argparse
import logging
import os
import sys
import time
from datetime import UTC, datetime

import polars as pl

import utils
from data_handling.collectors.reddit.api import ArcticShiftClient, RateLimitStop

logger = logging.getLogger(__name__)

RAW_SUBDIR = os.path.join("data", "analyst_signal", "raw", "source=reddit")

COMMENT_SCHEMA = {
    "id":           pl.Utf8,
    "created_utc":  pl.Int64,
    "author":       pl.Utf8,
    "body":         pl.Utf8,
}

SUBMISSION_SCHEMA = {
    "id":           pl.Utf8,
    "created_utc":  pl.Int64,
    "author":       pl.Utf8,
    "title":        pl.Utf8,
    "selftext":     pl.Utf8,
}


# %% Paths

def raw_dir() -> str:
    """Absolute path of the Reddit raw layer."""
    return os.path.join(utils._repo_root(), RAW_SUBDIR)


# %% Calendar

def month_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Break [start, end) into calendar-month (month_start, month_end) pairs.

    Args:
        start : Inclusive window start (UTC, tz-aware).
        end   : Exclusive window end (UTC, tz-aware).

    Returns:
        Ordered list of (month_start, month_end) pairs clipped to [start, end).
    """
    windows: list[tuple[datetime, datetime]] = []
    cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur < end:
        nxt = cur.replace(year=cur.year + 1, month=1) if cur.month == 12 \
            else cur.replace(month=cur.month + 1)
        windows.append((max(cur, start), min(nxt, end)))
        cur = nxt
    return windows


# %% Ingest

def ingest_month(
    client      : ArcticShiftClient,
    subreddit   : str,
    kind        : str,
    month_start : datetime,
    month_end   : datetime,
) -> str:
    """Fetch one subreddit/kind/month window and persist it as one parquet partition.

    Args:
        client      : Configured ArcticShiftClient.
        subreddit   : Subreddit name, no r/ prefix.
        kind        : "comment" or "submission".
        month_start : Inclusive window start (UTC, tz-aware).
        month_end   : Exclusive window end (UTC, tz-aware).

    Returns:
        Path to the written parquet file (may hold zero rows).
    """
    rel_prefix = f"subreddit={subreddit}/kind={kind}/{month_start:%Y-%m}"
    rows = client.fetch_window(kind, subreddit, month_start, month_end, rel_prefix)

    out_dir  = os.path.join(client.raw_dir, f"subreddit={subreddit}", f"kind={kind}")
    out_path = os.path.join(out_dir, f"{month_start:%Y-%m}.parquet")
    os.makedirs(out_dir, exist_ok=True)

    schema = COMMENT_SCHEMA if kind == "comment" else SUBMISSION_SCHEMA
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    df.write_parquet(out_path)
    logger.info("  %s/%s %s: %d sor -> %s",
                subreddit, kind, month_start.strftime("%Y-%m"), df.height, out_path)
    return out_path


def ingest_range(
    client    : ArcticShiftClient,
    subreddit : str,
    kinds     : list[str],
    start     : datetime,
    end       : datetime,
) -> list[str]:
    """Ingest every kind, month by month, over [start, end).

    Args:
        client    : Configured ArcticShiftClient.
        subreddit : Subreddit name, no r/ prefix.
        kinds     : "comment" and/or "submission".
        start     : Inclusive window start (UTC, tz-aware).
        end       : Exclusive window end (UTC, tz-aware).

    Returns:
        Paths of every parquet partition written.
    """
    written: list[str] = []
    for kind in kinds:
        for month_start, month_end in month_windows(start, end):
            written.append(ingest_month(client, subreddit, kind, month_start, month_end))
    return written


# %% CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subreddit", required=True, help="Subreddit name, no r/ prefix.")
    parser.add_argument("--start", required=True, help="UTC date, inclusive, YYYY-MM-DD.")
    parser.add_argument("--end", required=True, help="UTC date, exclusive, YYYY-MM-DD.")
    parser.add_argument("--kinds", nargs="+", default=["comment", "submission"],
                        choices=["comment", "submission"])
    parser.add_argument("--dry-run", action="store_true",
                        help="No HTTP; cached pages are still served.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC)
    end   = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC)
    if end <= start:
        logger.error("--end (%s) nem lehet <= --start (%s)", args.end, args.start)
        return 1

    client = ArcticShiftClient(raw_dir(), dry_run=args.dry_run)
    logger.info("Raw layer: %s  (subreddit=%s, kinds=%s, [%s, %s))",
                client.raw_dir, args.subreddit, args.kinds, args.start, args.end)

    t0 = time.monotonic()
    try:
        written = ingest_range(client, args.subreddit, args.kinds, start, end)
    except RateLimitStop as exc:
        logger.error("%s", exc)
        return 2
    elapsed = time.monotonic() - t0

    logger.info("Kesz. %d partition, %d HTTP keres, %.1fs",
                len(written), client.request_count, elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
