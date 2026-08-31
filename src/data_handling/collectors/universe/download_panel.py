#!/usr/bin/env python3
"""Download the multi-coin panel (epic_064 / t852).

Picks the N largest perps that are trading *today* and pulls every archive
they have, as far back as each one goes. Deliberately simple: no rolling
re-selection, no point-in-time universe bookkeeping.

⚠️ This carries survivorship bias -- the 30 are chosen because they survived
to 2026, so their history is the history of coins that worked out. At a 24h
horizon the effect is much weaker than it would be over months (a coin dying
is a slow process, and the barrier outcome inside one day barely feels it),
but it is not zero, and any result has to be read with that in mind.

Sources, per symbol, over its whole available range:

    klines 1m    execution ground truth -- entry/exit price, maker fill check
    klines 5m    the modelling grid
    metrics 5m   open interest, long/short ratios, taker ratio (DAILY files)
    premium 5m   premium index
    fundingRate  settled funding

Not downloaded: bookDepth (18 GB, deferred) and spot klines (basis features,
deferred with it).

Usage:
    python -m data_handling.collectors.universe.download_panel --list-only
    python -m data_handling.collectors.universe.download_panel --top 30
"""

import argparse
import logging
import os
import sys
from datetime import date

import duckdb
import polars as pl

import utils
from data_handling.collectors.derivatives.vision import (
    KLINE_COLS,
    _months,
    _read_zip_csv,
    download_many,
    epoch_to_utc,
    load_metrics,
    load_premium_index,
)
from data_handling.collectors.universe.build_universe import (
    DB_REL,
    load_funding,
    perp_symbols,
    rank_by_volume,
)

logger = logging.getLogger(__name__)

# Futures klines start here; earlier months simply 404 and are skipped.
HISTORY_START = date(2019, 9, 1)
HISTORY_END = date(2026, 7, 31)
# The month the "live" ranking is taken from -- the most recent complete one.
RANK_YEAR, RANK_MONTH = 2026, 7


def load_klines(symbol: str, interval: str, start: date, end: date) -> pl.DataFrame:
    """Monthly klines at one interval, over whatever range the archive has."""
    rels = [
        f"futures/um/monthly/klines/{symbol}/{interval}/"
        f"{symbol}-{interval}-{y:04d}-{m:02d}.zip"
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
            pl.col("taker_buy_volume").cast(pl.Float64, strict=False),
            pl.col("count").cast(pl.Int64, strict=False).alias("n_trades"),
        )
        .unique(subset=["open_time"], keep="first")
        .sort("open_time")
    )


def _ddl(conn: duckdb.DuckDBPyConnection) -> None:
    kline_cols = (
        "symbol VARCHAR, open_time TIMESTAMP, open DOUBLE, high DOUBLE,"
        " low DOUBLE, close DOUBLE, volume DOUBLE, quote_volume DOUBLE,"
        " taker_buy_volume DOUBLE, n_trades BIGINT,"
        " PRIMARY KEY (symbol, open_time)"
    )
    conn.execute(f"CREATE TABLE IF NOT EXISTS ohlcv_1m ({kline_cols})")
    conn.execute(f"CREATE TABLE IF NOT EXISTS ohlcv_5m ({kline_cols})")
    # Column names follow what vision.load_metrics / load_premium_index already
    # produce, so the SOLUSDT alt-data feature code reads them unchanged.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metrics_5m (
            symbol VARCHAR, open_time TIMESTAMP,
            oi_coin DOUBLE, oi_value DOUBLE,
            ls_count DOUBLE, ls_top_count DOUBLE, ls_top_sum DOUBLE,
            taker_vol_ratio DOUBLE,
            PRIMARY KEY (symbol, open_time)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS premium_5m (
            symbol VARCHAR, open_time TIMESTAMP,
            prem_open DOUBLE, prem_high DOUBLE, prem_low DOUBLE, prem_close DOUBLE,
            PRIMARY KEY (symbol, open_time)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS funding (
            symbol VARCHAR, funding_ts TIMESTAMP, funding_rate DOUBLE,
            PRIMARY KEY (symbol, funding_ts)
        )
    """)
    # Which symbols are finished, so a restart does not redo them.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS panel_log (
            symbol VARCHAR PRIMARY KEY, done_at TIMESTAMP,
            n_1m BIGINT, n_5m BIGINT, n_metrics BIGINT, n_premium BIGINT,
            n_funding BIGINT, first_bar TIMESTAMP, last_bar TIMESTAMP
        )
    """)


def _replace(
    conn: duckdb.DuckDBPyConnection, table: str, symbol: str, frame: pl.DataFrame
) -> int:
    """Idempotent per-symbol write: drop what is there, insert what came back."""
    if frame.is_empty():
        return 0
    conn.register("incoming", frame.to_arrow())
    conn.execute(f"DELETE FROM {table} WHERE symbol = ?", [symbol])
    conn.execute(f"INSERT INTO {table} SELECT * FROM incoming")
    conn.unregister("incoming")
    return frame.height


def download_symbol(conn: duckdb.DuckDBPyConnection, symbol: str) -> None:
    """Pull every source for one symbol and store it."""
    k1 = load_klines(symbol, "1m", HISTORY_START, HISTORY_END)
    if k1.is_empty():
        logger.warning("%s: nincs 1m kline, kihagyva", symbol)
        return
    k5 = load_klines(symbol, "5m", HISTORY_START, HISTORY_END)

    # The archive range is whatever the 1m klines cover; asking the other
    # sources for more only buys 404s.
    lo = k1["open_time"].min().date()  # type: ignore[union-attr]
    hi = k1["open_time"].max().date()  # type: ignore[union-attr]

    met = load_metrics(symbol, lo, hi)
    if not met.is_empty():
        met = met.select(
            pl.lit(symbol).alias("symbol"), "open_time", "oi_coin", "oi_value",
            "ls_count", "ls_top_count", "ls_top_sum", "taker_vol_ratio",
        )
    prem = load_premium_index(symbol, lo, hi)
    if not prem.is_empty():
        prem = prem.select(
            pl.lit(symbol).alias("symbol"), "open_time",
            "prem_open", "prem_high", "prem_low", "prem_close",
        )
    fund = load_funding(symbol, lo, hi)

    n1 = _replace(conn, "ohlcv_1m", symbol, k1)
    n5 = _replace(conn, "ohlcv_5m", symbol, k5)
    nm = _replace(conn, "metrics_5m", symbol, met)
    np_ = _replace(conn, "premium_5m", symbol, prem)
    nf = _replace(conn, "funding", symbol, fund)

    conn.execute(
        "INSERT OR REPLACE INTO panel_log VALUES (?, now(), ?, ?, ?, ?, ?, ?, ?)",
        [symbol, n1, n5, nm, np_, nf, lo, hi],
    )
    logger.info(
        "%-14s 1m=%-8s 5m=%-7s metrics=%-7s prem=%-7s fund=%-5s  %s .. %s",
        symbol, f"{n1:,}", f"{n5:,}", f"{nm:,}", f"{np_:,}", f"{nf:,}", lo, hi,
    )


def pick_live_top(top: int, rank_months: int = 12, min_age_months: int = 24) -> list[str]:
    """The N largest live perps that actually have a history worth training on.

    Ranking on the latest month alone does not work here. Binance now lists
    perps on tokenized equities, ETFs and metals -- SanDisk, SK Hynix, a
    semiconductor ETF, gold, SpaceX -- and a freshly listed contract's launch
    volume puts it straight into the top 30. Measured on 2026-07, twenty of the
    top thirty had under two years of data and ten had under six months.

    Two changes fix it with one idea. Volume is summed over a trailing window
    rather than one month, so a single launch spike cannot carry a symbol; and
    the symbol must already have existed `min_age_months` ago, which excludes
    every recent listing regardless of how much it trades.
    """
    symbols = perp_symbols()

    # Existence check first: one tiny daily-kline file per symbol, and it
    # shrinks the ranking work to the survivors.
    y, m = RANK_YEAR, RANK_MONTH - min_age_months
    while m <= 0:
        y, m = y - 1, m + 12
    logger.info("kor-szures: letezett-e mar %04d-%02d-ban?", y, m)
    old_enough = set(rank_by_volume(symbols, y, m)["symbol"].to_list())
    logger.info("%d / %d szimbolum eleg regi", len(old_enough), len(symbols))

    candidates = [s for s in symbols if s in old_enough]
    totals: dict[str, float] = {}
    ry, rm = RANK_YEAR, RANK_MONTH
    for _ in range(rank_months):
        month = rank_by_volume(candidates, ry, rm)
        for row in month.iter_rows(named=True):
            totals[row["symbol"]] = totals.get(row["symbol"], 0.0) + row["quote_volume"]
        rm -= 1
        if rm == 0:
            ry, rm = ry - 1, 12

    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)
    return [s for s, _ in ranked[:top]]


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-coin panel letoltes")
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--list-only", action="store_true",
                        help="Csak a kivalasztott szimbolumokat irja ki")
    parser.add_argument("--force", action="store_true",
                        help="A mar kesz szimbolumokat is ujratolti")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    symbols = pick_live_top(args.top)
    print(f"\nkivalasztva {len(symbols)} el-o perp ({RANK_YEAR}-{RANK_MONTH:02d} forgalom):")
    print("  " + ", ".join(symbols) + "\n")
    if args.list_only:
        return

    db_path = os.path.join(utils._repo_root(), DB_REL)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = duckdb.connect(db_path)
    try:
        _ddl(conn)
        done = set()
        if not args.force:
            done = {
                r[0] for r in conn.execute("SELECT symbol FROM panel_log").fetchall()
            }
            if done:
                logger.info("mar kesz, kihagyva: %d szimbolum", len(done))

        for i, symbol in enumerate(symbols, 1):
            if symbol in done:
                continue
            logger.info("[%d/%d] %s", i, len(symbols), symbol)
            download_symbol(conn, symbol)

        rows = conn.execute("""
            SELECT count(*), sum(n_1m), sum(n_5m), sum(n_metrics), min(first_bar)
            FROM panel_log
        """).fetchone()
        if rows:
            print(f"\nkesz: {rows[0]} szimbolum")
            print(f"  ohlcv_1m   {rows[1]:>12,} sor")
            print(f"  ohlcv_5m   {rows[2]:>12,} sor")
            print(f"  metrics_5m {rows[3]:>12,} sor")
            print(f"  legkorabbi bar: {rows[4]}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
