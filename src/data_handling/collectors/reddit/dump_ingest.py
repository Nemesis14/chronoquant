#!/usr/bin/env python3
"""Watchful1 per-subreddit dump -> raw parquet layer (epic_053 / t202).

Reads a zstandard-compressed ndjson dump (one record per line) and writes the
same partitioned parquet layout the Arctic Shift ingest produces, so
`aggregate.py` consumes both without knowing where the rows came from:

    data/analyst_signal/raw/source=reddit/subreddit=<name>/kind=<kind>/<YYYY-MM>.parquet

Only the fields the hourly aggregate needs are kept. This is not an
optimisation -- it is the leak rule enforced at the boundary. The dumps carry
`score`, `ups`, `downs`, `num_comments` and `retrieved_on`, and those score
values are the crawl-time value, not the value when the item was posted. A
sampled r/BitcoinMarkets comment created 2013-04-11 was retrieved 2015-05-14:
using its score against a 2013 bar is a two-year lookahead. Those fields are
dropped here and never reach the raw layer.

The dumps use a large zstd window, so the decompressor is opened with
max_window_size=2**31.

Usage:
    uv run python -m data_handling.collectors.reddit.dump_ingest \
        --dump data/analyst_signal/raw/source=reddit/_dumps/reddit/subreddits25/Bitcoin_comments.zst \
        --subreddit Bitcoin --kind comment
"""

import argparse
import io
import json
import logging
import os
import sys
from datetime import UTC, datetime

import polars as pl
import zstandard

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from data_handling.collectors.reddit.ingest import raw_dir  # noqa: E402

logger = logging.getLogger(__name__)

#: Kept fields per kind. Everything else in the dump is discarded on read.
KIND_FIELDS = {
    "comment"   : ("id", "created_utc", "author", "body"),
    "submission": ("id", "created_utc", "author", "title", "selftext"),
}

#: Rows buffered across all months before a flush. Bounds peak memory on the
#: large dumps (CryptoCurrency comments is ~4.3 GB compressed).
_FLUSH_ROWS = 2_000_000

#: Large-window zstd frames -- the dumps are built with --long=31.
_MAX_WINDOW = 2**31


# %% Reading

def iter_records(dump_path: str, fields: tuple[str, ...]):
    """Stream one dict per line from a zstd-compressed ndjson dump.

    Args:
        dump_path: Path to the .zst dump.
        fields   : Field names to keep; all others are dropped immediately.

    Yields:
        One dict per parseable line, restricted to `fields`.
    """
    dctx = zstandard.ZstdDecompressor(max_window_size=_MAX_WINDOW)
    bad = 0
    with open(dump_path, "rb") as fh:
        text = io.TextIOWrapper(dctx.stream_reader(fh), encoding="utf-8", errors="replace")
        for line in text:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                bad += 1
                continue
            if row.get("created_utc") is None:
                bad += 1
                continue
            yield {k: row.get(k) for k in fields}
    if bad:
        logger.warning("%s: %d sor kihagyva (parse hiba vagy hianyzo created_utc)",
                       os.path.basename(dump_path), bad)


# %% Writing

def _flush(buffer: dict[str, list[dict]], out_dir: str, chunk_no: int) -> int:
    """Write each buffered month to its own chunk parquet. Returns rows written."""
    written = 0
    for month, rows in buffer.items():
        if not rows:
            continue
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{month}__chunk{chunk_no:04d}.parquet")
        pl.DataFrame(rows).write_parquet(path)
        written += len(rows)
    buffer.clear()
    return written


def _merge_chunks(out_dir: str) -> list[str]:
    """Merge per-month chunk files into one parquet per month, then delete chunks.

    Chunking exists because a dump is not guaranteed to be perfectly ordered by
    created_utc; a month may be revisited after a flush. Merging at the end is
    what makes the result independent of the dump's ordering.

    Returns:
        The month keys written.
    """
    chunks: dict[str, list[str]] = {}
    for name in sorted(os.listdir(out_dir)):
        if "__chunk" not in name or not name.endswith(".parquet"):
            continue
        chunks.setdefault(name.split("__chunk")[0], []).append(os.path.join(out_dir, name))

    months = []
    for month, paths in sorted(chunks.items()):
        df = pl.concat([pl.read_parquet(p) for p in paths], how="vertical")
        df = df.unique(subset=["id"], keep="first").sort("created_utc")
        df.write_parquet(os.path.join(out_dir, f"{month}.parquet"))
        for p in paths:
            os.remove(p)
        months.append(month)
    return months


def ingest_dump(dump_path: str, subreddit: str, kind: str) -> tuple[int, list[str]]:
    """Convert one dump file into the partitioned raw parquet layer.

    Args:
        dump_path: Path to the .zst dump.
        subreddit: Raw-layer partition key (no r/ prefix).
        kind     : "comment" or "submission".

    Returns:
        (rows written, month keys written)
    """
    fields  = KIND_FIELDS[kind]
    out_dir = os.path.join(raw_dir(), f"subreddit={subreddit}", f"kind={kind}")
    os.makedirs(out_dir, exist_ok=True)

    buffer: dict[str, list[dict]] = {}
    buffered = total = 0
    chunk_no = 0

    for row in iter_records(dump_path, fields):
        ts = int(row["created_utc"])
        month = datetime.fromtimestamp(ts, UTC).strftime("%Y-%m")
        row["created_utc"] = ts
        buffer.setdefault(month, []).append(row)
        buffered += 1
        if buffered >= _FLUSH_ROWS:
            total += _flush(buffer, out_dir, chunk_no)
            logger.info("  ... %d sor feldolgozva", total)
            chunk_no += 1
            buffered = 0

    total += _flush(buffer, out_dir, chunk_no)
    months = _merge_chunks(out_dir)
    return total, months


# %% CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dump", required=True, help="Path to the .zst dump file.")
    parser.add_argument("--subreddit", required=True, help="Subreddit name, no r/ prefix.")
    parser.add_argument("--kind", required=True, choices=["comment", "submission"])
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    started = datetime.now(UTC)
    rows, months = ingest_dump(args.dump, args.subreddit, args.kind)
    elapsed = (datetime.now(UTC) - started).total_seconds()
    logger.info("%s/%s: %d sor, %d honap, %.1f mp",
                args.subreddit, args.kind, rows, len(months), elapsed)
    if months:
        logger.info("  tartomany: %s .. %s", months[0], months[-1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
