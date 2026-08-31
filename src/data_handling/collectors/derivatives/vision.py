#!/usr/bin/env python3
"""Binance Vision archive downloader for derivatives data (epic_054 / t307).

Fetches the public bulk archive at data.binance.vision and returns normalized
Polars frames. Downloads are cached on disk as the original zip files -- those
zips ARE the raw layer: they are byte-reproducible from a public URL, so there
is no reason to re-materialize them as parquet.

Two archive quirks are handled automatically, because both silently corrupt a
dataset if assumed away:

1. Header rows appeared only in newer files. A 2022 premiumIndexKlines file has
   no header; the 2025 one does. Detected per file from the first token.
2. Spot kline timestamps switched from milliseconds to MICROSECONDS during 2025.
   Futures files stayed on milliseconds. Detected per value by magnitude.

Usage:
    python -m data_handling.collectors.derivatives.vision --probe
    python -m data_handling.collectors.derivatives.vision \
        --kind metrics --start 2022-01-01 --end 2022-01-05
"""

import argparse
import http.client
import io
import logging
import os
import sys
import time
import zipfile
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import polars as pl

import utils

logger = logging.getLogger(__name__)

BASE_URL = "https://data.binance.vision/data"
CACHE_SUBDIR = os.path.join("data", "vision_cache")
MAX_WORKERS = 8
TIMEOUT_S = 120

# Kline layout is shared by every *Klines archive, futures and spot alike.
KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore",
]
METRICS_COLS = [
    "create_time", "symbol", "sum_open_interest", "sum_open_interest_value",
    "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio",
    "count_long_short_ratio", "sum_taker_long_short_vol_ratio",
]
FUNDING_COLS = ["calc_time", "funding_interval_hours", "last_funding_rate"]

# metrics/ starts here for SOLUSDT; 2021-11-30 returns HTTP 404 (verified, t301).
METRICS_FIRST_DAY = date(2021, 12, 1)


# %% Download + cache


def _cache_path(rel: str) -> str:
    return os.path.join(utils._repo_root(), CACHE_SUBDIR, rel.replace("/", os.sep))


def download(rel: str, *, force: bool = False) -> bytes | None:
    """Fetch one archive file, caching it on disk.

    Args:
        rel: Path below BASE_URL, e.g. "futures/um/daily/metrics/SOLUSDT/x.zip".
        force: Re-download even when a cached copy exists.

    Returns:
        Raw zip bytes, or None when the archive holds no such file (HTTP 404).
        A missing day is normal -- the archive skips exchange downtime -- so it
        is reported as None rather than raised.
    """
    path = _cache_path(rel)
    if not force and os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as fh:
            return fh.read()

    url = f"{BASE_URL}/{rel}"
    # A long backfill makes tens of thousands of requests and the host starts
    # dropping connections partway through (WinError 10054, or a 5xx). Those are
    # transient and unrelated to whether the file exists, so they are retried
    # with a widening pause; only a 404 is a real answer, and it means the
    # archive has no such day.
    raw: bytes | None = None
    for attempt in range(5):
        try:
            with urlopen(url, timeout=TIMEOUT_S) as resp:  # noqa: S310 - fixed public host
                raw = resp.read()
            break
        except HTTPError as exc:
            if exc.code == 404:
                logger.debug("not in archive: %s", rel)
                return None
            if attempt == 4:
                raise
        except (URLError, TimeoutError, ConnectionError, http.client.HTTPException):
            if attempt == 4:
                raise
        time.sleep(2 ** attempt)
    if raw is None:
        return None

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(raw)
    return raw


def download_many(rels: list[str], *, force: bool = False) -> dict[str, bytes]:
    """Download several archive files in parallel, skipping the ones that 404."""
    out: dict[str, bytes] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        results = pool.map(lambda r: download(r, force=force), rels)
        for rel, raw in zip(rels, results, strict=True):
            if raw is not None:
                out[rel] = raw
    missing = len(rels) - len(out)
    if missing:
        logger.info("archivumbol hianyzik: %d / %d fajl", missing, len(rels))
    return out


# %% Parsing


def _read_zip_csv(raw: bytes, columns: list[str]) -> pl.DataFrame:
    """Parse the single CSV inside a Vision zip into a frame with `columns`.

    Header presence varies by file vintage, so it is detected rather than
    assumed: when the first field will not parse as a number, row one is a
    header and gets skipped.
    """
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        body = zf.read(zf.namelist()[0])

    first_field = body[:200].split(b",", 1)[0].strip()
    try:
        float(first_field)
        has_header = False
    except ValueError:
        has_header = True

    # Everything is read as text and cast by the caller, on purpose. Inferring
    # dtypes from the first rows breaks across archive vintages: an early month
    # whose volumes happen to be whole numbers gets typed i64, and the parse
    # then dies on the first decimal further down the file. Every loader below
    # casts explicitly anyway, so inference buys nothing and costs robustness.
    return pl.read_csv(
        io.BytesIO(body),
        has_header=False,
        skip_rows=1 if has_header else 0,
        new_columns=columns,
        infer_schema_length=0,
    )


def epoch_to_utc(col: pl.Expr) -> pl.Expr:
    """Convert an epoch column to UTC-naive timestamps, detecting the unit.

    Binance moved spot kline timestamps to microseconds during 2025 while the
    futures archives stayed on milliseconds, so the unit is inferred per value:
    above 1e15 is microseconds, above 1e12 milliseconds, otherwise seconds.
    All three are normalized to microsecond precision so joins across sources
    do not fail on a unit mismatch.
    """
    return (
        pl.when(col > 1_000_000_000_000_000)
        .then(col)
        .when(col > 1_000_000_000_000)
        .then(col * 1_000)
        .otherwise(col * 1_000_000)
        .cast(pl.Datetime("us"))
    )


# %% Date helpers


def _days(start: date, end: date) -> Iterator[date]:
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def _months(start: date, end: date) -> Iterator[tuple[int, int]]:
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


# %% Per-source loaders


def load_metrics(symbol: str, start: date, end: date) -> pl.DataFrame:
    """Daily metrics archive -> 5-minute open interest and positioning frame."""
    start = max(start, METRICS_FIRST_DAY)
    rels = [
        f"futures/um/daily/metrics/{symbol}/{symbol}-metrics-{day:%Y-%m-%d}.zip"
        for day in _days(start, end)
    ]
    blobs = download_many(rels)
    if not blobs:
        return pl.DataFrame()

    frames = [_read_zip_csv(raw, METRICS_COLS) for raw in blobs.values()]
    return (
        pl.concat(frames, how="vertical_relaxed")
        .select(
            pl.col("create_time").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("open_time"),
            pl.col("sum_open_interest").cast(pl.Float64, strict=False).alias("oi_coin"),
            pl.col("sum_open_interest_value").cast(pl.Float64, strict=False).alias("oi_value"),
            pl.col("count_long_short_ratio").cast(pl.Float64, strict=False).alias("ls_count"),
            pl.col("count_toptrader_long_short_ratio").cast(pl.Float64, strict=False).alias("ls_top_count"),
            pl.col("sum_toptrader_long_short_ratio").cast(pl.Float64, strict=False).alias("ls_top_sum"),
            pl.col("sum_taker_long_short_vol_ratio").cast(pl.Float64, strict=False).alias("taker_vol_ratio"),
        )
        .unique(subset=["open_time"], keep="first")
        .sort("open_time")
    )


def load_premium_index(symbol: str, start: date, end: date) -> pl.DataFrame:
    """Monthly premiumIndexKlines (5m) -> premium index OHLC frame."""
    rels = [
        f"futures/um/monthly/premiumIndexKlines/{symbol}/5m/{symbol}-5m-{y:04d}-{m:02d}.zip"
        for y, m in _months(start, end)
    ]
    blobs = download_many(rels)
    if not blobs:
        return pl.DataFrame()

    frames = [_read_zip_csv(raw, KLINE_COLS) for raw in blobs.values()]
    return (
        pl.concat(frames, how="vertical_relaxed")
        .select(
            epoch_to_utc(pl.col("open_time").cast(pl.Int64, strict=False)).alias("open_time"),
            pl.col("open").cast(pl.Float64, strict=False).alias("prem_open"),
            pl.col("high").cast(pl.Float64, strict=False).alias("prem_high"),
            pl.col("low").cast(pl.Float64, strict=False).alias("prem_low"),
            pl.col("close").cast(pl.Float64, strict=False).alias("prem_close"),
        )
        .unique(subset=["open_time"], keep="first")
        .sort("open_time")
    )


def load_funding(symbol: str, start: date, end: date) -> pl.DataFrame:
    """Monthly fundingRate archive -> settled funding rate frame (8h grid)."""
    rels = [
        f"futures/um/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{y:04d}-{m:02d}.zip"
        for y, m in _months(start, end)
    ]
    blobs = download_many(rels)
    if not blobs:
        return pl.DataFrame()

    frames = [_read_zip_csv(raw, FUNDING_COLS) for raw in blobs.values()]
    return (
        pl.concat(frames, how="vertical_relaxed")
        .select(
            epoch_to_utc(pl.col("calc_time").cast(pl.Int64, strict=False)).alias("funding_ts"),
            pl.col("last_funding_rate").cast(pl.Float64, strict=False).alias("funding_rate"),
        )
        .unique(subset=["funding_ts"], keep="first")
        .sort("funding_ts")
    )


def load_spot_klines(symbol: str, start: date, end: date) -> pl.DataFrame:
    """Monthly spot 1m klines -> 5-minute aggregated spot close/volume frame."""
    rels = [
        f"spot/monthly/klines/{symbol}/1m/{symbol}-1m-{y:04d}-{m:02d}.zip"
        for y, m in _months(start, end)
    ]
    blobs = download_many(rels)
    if not blobs:
        return pl.DataFrame()

    frames = [_read_zip_csv(raw, KLINE_COLS) for raw in blobs.values()]
    minute = (
        pl.concat(frames, how="vertical_relaxed")
        .select(
            epoch_to_utc(pl.col("open_time").cast(pl.Int64, strict=False)).alias("open_time"),
            pl.col("close").cast(pl.Float64, strict=False).alias("spot_close"),
            pl.col("volume").cast(pl.Float64, strict=False).alias("spot_volume"),
            pl.col("taker_buy_volume").cast(pl.Float64, strict=False).alias("spot_taker_buy"),
        )
        .unique(subset=["open_time"], keep="first")
        .sort("open_time")
    )

    # Aggregate onto the 5-minute grid the metrics archive uses: last close,
    # summed volume. group_by_dynamic labels each window by its left edge,
    # which matches the metrics create_time convention.
    return minute.group_by_dynamic("open_time", every="5m", label="left").agg(
        pl.col("spot_close").last(),
        pl.col("spot_volume").sum(),
        pl.col("spot_taker_buy").sum(),
    )


# %% CLI


def _probe(symbol: str) -> None:
    """Report archive availability for each source used by this epic."""
    checks = [
        ("metrics 2022-01-01",
         f"futures/um/daily/metrics/{symbol}/{symbol}-metrics-2022-01-01.zip"),
        ("metrics 2021-11-30 (varhatoan hianyzik)",
         f"futures/um/daily/metrics/{symbol}/{symbol}-metrics-2021-11-30.zip"),
        ("premiumIndex 2022-01",
         f"futures/um/monthly/premiumIndexKlines/{symbol}/5m/{symbol}-5m-2022-01.zip"),
        ("fundingRate 2022-01",
         f"futures/um/monthly/fundingRate/{symbol}/{symbol}-fundingRate-2022-01.zip"),
        ("spot klines 2022-01",
         f"spot/monthly/klines/{symbol}/1m/{symbol}-1m-2022-01.zip"),
    ]
    for label, rel in checks:
        raw = download(rel)
        state = f"{len(raw):,} byte" if raw else "NINCS (404)"
        print(f"  {label:44} {state}")


def _force_utf8_stdout() -> None:
    """Windows consoles default to cp1250, which cannot render Polars tables."""
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")


def main() -> None:
    parser = argparse.ArgumentParser(description="Binance Vision derivatives downloader")
    parser.add_argument("--symbol", default="SOLUSDT")
    parser.add_argument("--kind", choices=["metrics", "premium", "funding", "spot"])
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--probe", action="store_true", help="Archive availability check")
    args = parser.parse_args()

    _force_utf8_stdout()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.probe:
        _probe(args.symbol)
        return

    if not (args.kind and args.start and args.end):
        parser.error("--kind, --start and --end are required without --probe")

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC).date()
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC).date()

    loaders = {
        "metrics": load_metrics,
        "premium": load_premium_index,
        "funding": load_funding,
        "spot": load_spot_klines,
    }
    frame = loaders[args.kind](args.symbol, start, end)
    print(frame)
    print(f"sorok: {frame.height:,}")


if __name__ == "__main__":
    main()


BOOK_DEPTH_COLS = ["timestamp", "percentage", "depth", "notional"]

# bookDepth/ starts here for SOLUSDT (verified against the archive listing).
BOOK_DEPTH_FIRST_DAY = date(2023, 1, 1)


def load_book_depth(symbol: str, start: date, end: date) -> pl.DataFrame:
    """Daily bookDepth archive -> 5-minute order-book depth frame.

    The archive stores one row per (snapshot, percentage level), roughly every
    31 seconds at ten levels: -5..-1 percent below the mid (bid side) and
    1..5 percent above (ask side). Depth is in coin units.

    Levels are pivoted to columns and averaged within each 5-minute bucket --
    the mean rather than the last snapshot, because a single snapshot catches
    whatever transient state the book happened to be in.
    """
    start = max(start, BOOK_DEPTH_FIRST_DAY)
    rels = [
        f"futures/um/daily/bookDepth/{symbol}/{symbol}-bookDepth-{day:%Y-%m-%d}.zip"
        for day in _days(start, end)
    ]
    blobs = download_many(rels)
    if not blobs:
        return pl.DataFrame()

    frames = [_read_zip_csv(raw, BOOK_DEPTH_COLS) for raw in blobs.values()]
    raw_df = (
        pl.concat(frames, how="vertical_relaxed")
        .select(
            pl.col("timestamp").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("ts"),
            pl.col("percentage").cast(pl.Int8, strict=False).alias("pct"),
            pl.col("depth").cast(pl.Float64, strict=False).alias("depth"),
        )
        .drop_nulls()
    )

    # Keep the three levels that matter: +/-1% is what a market order eats
    # first, +/-5% describes how fast the book thins out.
    wanted = {-5: "bid_5", -2: "bid_2", -1: "bid_1", 1: "ask_1", 2: "ask_2", 5: "ask_5"}
    pivoted = (
        raw_df.filter(pl.col("pct").is_in(list(wanted)))
        .with_columns(
            pl.col("pct").replace_strict(wanted, return_dtype=pl.Utf8).alias("level")
        )
        .pivot(on="level", index="ts", values="depth", aggregate_function="first")
        .sort("ts")
    )

    level_cols = [c for c in wanted.values() if c in pivoted.columns]
    return (
        pivoted.group_by_dynamic("ts", every="5m", label="left")
        .agg([pl.col(c).mean().alias(c) for c in level_cols])
        .rename({"ts": "open_time"})
        .sort("open_time")
    )
