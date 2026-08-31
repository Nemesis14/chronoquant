#!/usr/bin/env python3
"""Build a survivorship-free multi-perp price universe (epic_062 / t831).

The cross-sectional question -- is the spread between the best and worst perp
wide enough to pay two legs of fees -- cannot be answered on today's top
symbols. A list of "the 30 biggest perps now" silently drops everything that
died in between, and the pairs that die are exactly the ones that moved most.

So the universe is picked from what the archive shows *at the selection date*:

    1. list every symbol Binance Vision has ever published futures klines for
       (986 as of 2026-08, delisted ones included -- the listing is a plain S3
       enumeration, not an exchangeInfo call that only shows live symbols)
    2. rank them by traded quote volume in the selection month, using daily
       bars -- 31 rows per symbol, so ranking ~500 candidates is cheap
    3. keep the top N, and download their hourly bars for the study window

⚠️ Hourly, not 1-minute, on purpose. The probe measures 4-hour forward returns
close-to-close, which hourly bars give exactly; 1m would be ~1000x the download
for no change in the answer. If the probe comes back GO, the modelling stage
needs 1m and that is a separate backfill.

Usage:
    python -m data_handling.collectors.universe.build_universe --rank-only
    python -m data_handling.collectors.universe.build_universe --top 30
"""

import argparse
import logging
import os
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date

import duckdb
import polars as pl

import utils
from data_handling.collectors.derivatives.vision import (
    FUNDING_COLS,
    KLINE_COLS,
    _months,
    _read_zip_csv,
    download_many,
    epoch_to_utc,
)

logger = logging.getLogger(__name__)

LISTING_URL = (
    "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
    "?delimiter=/&prefix=data/futures/um/monthly/klines/"
)
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

DB_REL = os.path.join("database", "_universe", "universe.duckdb")
SELECT_YEAR = 2024
SELECT_MONTH = 1
STUDY_START = date(2024, 1, 1)
STUDY_END = date(2026, 7, 31)
QUOTE = "USDT"

# Rolling universe (epic_064 / t851). A fixed "top 30 as of 2024-01" list
# cannot be pushed back through 2020: a coin is in it because we know it grew
# big later. Re-selecting every six months from the month immediately before
# each date removes that look-ahead, the way an index rebalances.
REBALANCE_MONTHS = (1, 7)
ROLL_FIRST = date(2020, 7, 1)
ROLL_LAST = date(2026, 7, 1)
TOP_N = 30


def list_symbols() -> list[str]:
    """Every symbol the futures kline archive has ever carried."""
    with urllib.request.urlopen(LISTING_URL, timeout=60) as resp:  # noqa: S310
        root = ET.fromstring(resp.read())
    if root.findtext(".//s3:IsTruncated", namespaces=S3_NS) == "true":
        # One page holds ~1000 keys. If Binance ever crosses that the ranking
        # would silently see only part of the market, which is the exact bias
        # this function exists to avoid -- so fail loudly instead.
        raise RuntimeError("az S3 listazas tobb lapos lett -- lapozas kell")
    prefixes = [
        p.text or "" for p in root.findall(".//s3:CommonPrefixes/s3:Prefix", S3_NS)
    ]
    return sorted(p.rstrip("/").rsplit("/", 1)[-1] for p in prefixes)


def perp_symbols() -> list[str]:
    """Perpetual pairs quoted in QUOTE, filtered down from the raw listing.

    Two things get dropped. Dated futures (``BTCUSDT_240329``) are a different
    instrument with an expiry, not a perp. And the archive carries three
    symbols whose names are CJK meme text -- they cannot even be put in an
    HTTP request line, which is how they announced themselves.
    """
    return [
        s for s in list_symbols()
        if s.isascii() and s.isalnum() and s.endswith(QUOTE)
    ]


def rank_by_volume(
    symbols: list[str], year: int = SELECT_YEAR, month: int = SELECT_MONTH
) -> pl.DataFrame:
    """Quote volume traded in the selection month, per symbol.

    Daily bars: one small file each, so the whole market can be ranked without
    downloading a single minute-level archive.
    """
    rels = {
        f"futures/um/monthly/klines/{s}/1d/{s}-1d-{year:04d}-{month:02d}.zip": s
        for s in symbols
    }
    blobs = download_many(list(rels))
    logger.info("rangsorolas: %d / %d szimbolum letezett ekkor", len(blobs), len(rels))

    rows = []
    for rel, raw in blobs.items():
        frame = _read_zip_csv(raw, KLINE_COLS)
        qv = frame["quote_volume"].cast(pl.Float64, strict=False).sum()
        rows.append({"symbol": rels[rel], "quote_volume": float(qv or 0.0),
                     "n_days": frame.height})
    return pl.DataFrame(rows).sort("quote_volume", descending=True)


def load_hourly(symbol: str, start: date, end: date) -> pl.DataFrame:
    """Monthly 1h klines for one symbol over the study window."""
    rels = [
        f"futures/um/monthly/klines/{symbol}/1h/{symbol}-1h-{y:04d}-{m:02d}.zip"
        for y, m in _months(start, end)
    ]
    blobs = download_many(rels)
    if not blobs:
        return pl.DataFrame()

    frames = [_read_zip_csv(raw, KLINE_COLS) for raw in blobs.values()]
    return (
        pl.concat(frames, how="vertical_relaxed")
        .select(
            pl.lit(symbol).alias("symbol"),
            epoch_to_utc(
                pl.col("open_time").cast(pl.Int64, strict=False)
            ).alias("open_time"),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
            pl.col("quote_volume").cast(pl.Float64, strict=False),
        )
        .unique(subset=["open_time"], keep="first")
        .sort("open_time")
    )


def load_funding(symbol: str, start: date, end: date) -> pl.DataFrame:
    """Settled funding rates for one symbol -- 8-hourly, tiny files.

    Unmodelled funding is the single largest hole in a multi-day market-neutral
    backtest. A neutral book is long one perp and short another, so the two
    funding legs partly cancel -- but only partly: the whole point of the book
    is that the two sides are different, and on alt perps the funding
    *differential* can run to tens of basis points a day. It has to be measured,
    not waved away as "roughly offsetting".
    """
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
            pl.lit(symbol).alias("symbol"),
            epoch_to_utc(
                pl.col("calc_time").cast(pl.Int64, strict=False)
            ).alias("funding_ts"),
            pl.col("last_funding_rate").cast(pl.Float64, strict=False)
            .alias("funding_rate"),
        )
        .unique(subset=["funding_ts"], keep="first")
        .sort("funding_ts")
    )


def _rebalance_dates() -> list[date]:
    """Every re-selection date in the study period, oldest first."""
    out = []
    y = ROLL_FIRST.year
    while True:
        for m in REBALANCE_MONTHS:
            d = date(y, m, 1)
            if ROLL_FIRST <= d <= ROLL_LAST:
                out.append(d)
        if y > ROLL_LAST.year:
            break
        y += 1
    return sorted(out)


def _ranking_month(d: date) -> tuple[int, int]:
    """The calendar month immediately before a re-selection date.

    Strictly before: ranking on the month a selection takes effect would let
    the choice depend on data the selection could not have seen.
    """
    return (d.year, d.month - 1) if d.month > 1 else (d.year - 1, 12)


def build_rolling(top: int = TOP_N) -> pl.DataFrame:
    """Top-N by trailing volume at each re-selection date (epic_064 / t851)."""
    symbols = perp_symbols()
    dates = _rebalance_dates()
    logger.info("%d ujravalasztas, %d %s-par jelolt", len(dates), len(symbols), QUOTE)

    rows = []
    for i, d in enumerate(dates):
        y, m = _ranking_month(d)
        ranked = rank_by_volume(symbols, y, m)
        if ranked.is_empty():
            logger.warning("%s: nincs forgalmi adat %04d-%02d-re, kihagyva", d, y, m)
            continue
        chosen = ranked.head(top)
        # An entry stays valid until the next re-selection; the final one runs
        # to the end of the study window.
        valid_to = dates[i + 1] if i + 1 < len(dates) else STUDY_END
        for rank, r in enumerate(chosen.iter_rows(named=True), start=1):
            rows.append({
                "rebalance_date": d,
                "symbol": r["symbol"],
                "rank": rank,
                "quote_volume": r["quote_volume"],
                "valid_from": d,
                "valid_to": valid_to,
            })
        logger.info("%s  (rangsor %04d-%02d)  top1=%s  top30=%s",
                    d, y, m, chosen["symbol"][0], chosen["symbol"][-1])

    return pl.DataFrame(rows)


def store_rolling(frame: pl.DataFrame) -> None:
    """Persist the rolling universe next to the price tables."""
    db_path = os.path.join(utils._repo_root(), DB_REL)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = duckdb.connect(db_path)
    try:
        conn.register("uni_in", frame.to_arrow())
        conn.execute("CREATE OR REPLACE TABLE universe AS SELECT * FROM uni_in")
        conn.unregister("uni_in")
    finally:
        conn.close()


def build(top: int) -> None:
    """Rank the market, take the top N, and store their hourly bars."""
    symbols = perp_symbols()
    logger.info("%s-pár az archivumban: %d", QUOTE, len(symbols))

    ranked = rank_by_volume(symbols)
    chosen = ranked.head(top)
    logger.info(
        "kivalasztva %d szimbolum, forgalom %.0f .. %.0f M USD (%d-%02d)",
        chosen.height,
        chosen["quote_volume"].max() / 1e6,  # type: ignore[operator]
        chosen["quote_volume"].min() / 1e6,  # type: ignore[operator]
        SELECT_YEAR, SELECT_MONTH,
    )

    db_path = os.path.join(utils._repo_root(), DB_REL)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = duckdb.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_1h (
                symbol VARCHAR, open_time TIMESTAMP,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                volume DOUBLE, quote_volume DOUBLE,
                PRIMARY KEY (symbol, open_time)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS funding (
                symbol VARCHAR, funding_ts TIMESTAMP, funding_rate DOUBLE,
                PRIMARY KEY (symbol, funding_ts)
            )
        """)
        conn.register("ranked_in", ranked.to_arrow())
        conn.execute("CREATE OR REPLACE TABLE selection AS SELECT * FROM ranked_in")
        conn.unregister("ranked_in")

        for row in chosen.iter_rows(named=True):
            symbol = row["symbol"]
            frame = load_hourly(symbol, STUDY_START, STUDY_END)
            if frame.is_empty():
                logger.warning("%s: nincs orai adat, kihagyva", symbol)
                continue
            conn.register("incoming", frame.to_arrow())
            conn.execute("DELETE FROM ohlcv_1h WHERE symbol = ?", [symbol])
            conn.execute("INSERT INTO ohlcv_1h SELECT * FROM incoming")
            conn.unregister("incoming")
            # A pair that stops early was delisted mid-study. That is data, not
            # an error: dropping it is how survivorship bias gets in.
            fund = load_funding(symbol, STUDY_START, STUDY_END)
            if not fund.is_empty():
                conn.register("fund_in", fund.to_arrow())
                conn.execute("DELETE FROM funding WHERE symbol = ?", [symbol])
                conn.execute("INSERT INTO funding SELECT * FROM fund_in")
                conn.unregister("fund_in")

            last = frame["open_time"].max()
            logger.info("%-14s %6d ora, %5d funding, utolso: %s",
                        symbol, frame.height, fund.height, last)

        total = conn.execute(
            "SELECT count(DISTINCT symbol), count(*) FROM ohlcv_1h"
        ).fetchone()
        logger.info("kesz: %s szimbolum, %s sor", total[0], f"{total[1]:,}")  # type: ignore[index]
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-perp univerzum epites")
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--rolling", action="store_true",
                        help="Gorgetett univerzum-valasztas (epic_064): felevente "
                             "ujravalasztott top 30 a megelozo honap forgalmabol.")
    parser.add_argument("--rank-only", action="store_true",
                        help="Csak a rangsort irja ki, nem tolt le orai adatot")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.rolling:
        frame = build_rolling(args.top)
        store_rolling(frame)
        distinct = sorted(set(frame["symbol"].to_list()))
        print(f"\nujravalasztasok: {frame['rebalance_date'].n_unique()}")
        print(f"osszes bejegyzes: {frame.height}")
        print(f"DISTINCT szimbolum (= letoltendo halmaz): {len(distinct)}")
        print("\n" + ", ".join(distinct))
        return

    if args.rank_only:
        symbols = perp_symbols()
        ranked = rank_by_volume(symbols)
        print(ranked.head(args.top).to_pandas().to_string(index=False))
        return

    build(args.top)


if __name__ == "__main__":
    main()
