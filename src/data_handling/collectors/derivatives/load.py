#!/usr/bin/env python3
"""Build the altdata_5m table from Binance Vision archives (epic_054 / t307).

Joins the four Vision sources onto one 5-minute grid and upserts them into the
live DuckDB as `altdata_5m`. Nothing is derived here beyond the join and the
funding alignment -- feature computation is t308's job.

Grid convention: `open_time` is the LEFT edge of the 5-minute window, matching
the metrics archive's create_time. The perp columns are aggregated from the
existing 1-minute `ohlcv` table so that perp and alt data never disagree about
what a bar is.

Usage:
    python -m data_handling.collectors.derivatives.load --start 2021-12-01
    python -m data_handling.collectors.derivatives.load --start 2021-12-01 --coverage
"""

import argparse
import logging
import sys
from datetime import UTC, date, datetime

import duckdb
import polars as pl

import utils
from data_handling.collectors.derivatives import vision
from data_handling.store.duckdb_store import get_connection

logger = logging.getLogger(__name__)

TABLE = "altdata_5m"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    open_time        TIMESTAMP PRIMARY KEY,
    oi_coin          DOUBLE,
    oi_value         DOUBLE,
    ls_count         DOUBLE,
    ls_top_count     DOUBLE,
    ls_top_sum       DOUBLE,
    taker_vol_ratio  DOUBLE,
    prem_open        DOUBLE,
    prem_high        DOUBLE,
    prem_low         DOUBLE,
    prem_close       DOUBLE,
    funding_last     DOUBLE,
    funding_next_ts  TIMESTAMP,
    spot_close       DOUBLE,
    spot_volume      DOUBLE,
    spot_taker_buy   DOUBLE,
    perp_close       DOUBLE,
    perp_volume      DOUBLE,
    perp_taker_buy   DOUBLE,
    bid_1            DOUBLE,
    bid_2            DOUBLE,
    bid_5            DOUBLE,
    ask_1            DOUBLE,
    ask_2            DOUBLE,
    ask_5            DOUBLE
)
"""

COLUMNS = [
    "open_time", "oi_coin", "oi_value", "ls_count", "ls_top_count", "ls_top_sum",
    "taker_vol_ratio", "prem_open", "prem_high", "prem_low", "prem_close",
    "funding_last", "funding_next_ts", "spot_close", "spot_volume",
    "spot_taker_buy", "perp_close", "perp_volume", "perp_taker_buy",
    "bid_1", "bid_2", "bid_5", "ask_1", "ask_2", "ask_5",
]


def load_perp_5m(conn: duckdb.DuckDBPyConnection, start: date, end: date) -> pl.DataFrame:
    """Aggregate the existing 1-minute ohlcv table onto the 5-minute grid."""
    rel = conn.execute(
        """
        SELECT time_bucket(INTERVAL '5 minutes', open_time) AS open_time,
               last(close ORDER BY open_time)               AS perp_close,
               sum(volume)                                  AS perp_volume,
               sum(taker_buy_base)                          AS perp_taker_buy
        FROM ohlcv
        WHERE open_time >= ? AND open_time < ?
        GROUP BY 1
        ORDER BY 1
        """,
        [datetime.combine(start, datetime.min.time()),
         datetime.combine(end, datetime.max.time())],
    ).pl()
    return rel


def build(symbol: str, start: date, end: date, asset_id: str = "solusdt") -> pl.DataFrame:
    """Download every source and join them onto one 5-minute frame."""
    logger.info("metrics letoltes...")
    metrics = vision.load_metrics(symbol, start, end)
    if metrics.is_empty():
        raise RuntimeError("metrics archivum ures a kert tartomanyra")

    logger.info("premium index letoltes...")
    premium = vision.load_premium_index(symbol, start, end)
    logger.info("funding letoltes...")
    funding = vision.load_funding(symbol, start, end)
    logger.info("spot klines letoltes...")
    spot = vision.load_spot_klines(symbol, start, end)
    # Order-book depth only exists from 2023-01; earlier rows keep NULL.
    logger.info("bookDepth letoltes (nagy, ~460 KB/nap)...")
    depth = vision.load_book_depth(symbol, start, end)

    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = get_connection(db_path)
    try:
        perp = load_perp_5m(conn, start, end)
    finally:
        conn.close()

    df = metrics
    for other in (premium, spot, perp, depth):
        if not other.is_empty():
            df = df.join(other, on="open_time", how="left")

    # Funding settles on an 8h grid. Attach the last settled rate (backward) and
    # the next settlement time (forward) so t308 can build a countdown feature
    # without hardcoding the 00/08/16 UTC schedule.
    if not funding.is_empty():
        df = df.sort("open_time")
        df = df.join_asof(
            funding.rename({"funding_rate": "funding_last"}).sort("funding_ts"),
            left_on="open_time", right_on="funding_ts", strategy="backward",
        ).drop("funding_ts")
        df = df.join_asof(
            funding.select("funding_ts").rename({"funding_ts": "funding_next_ts"}).sort(
                "funding_next_ts"
            ),
            left_on="open_time", right_on="funding_next_ts", strategy="forward",
        )

    for col in COLUMNS:
        if col not in df.columns:
            dtype = pl.Datetime("us") if col.endswith("_ts") else pl.Float64
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))

    return df.select(COLUMNS).sort("open_time")


def upsert(df: pl.DataFrame, asset_id: str = "solusdt") -> int:
    """Idempotent upsert into altdata_5m keyed on open_time."""
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = get_connection(db_path)
    try:
        conn.execute(DDL)
        conn.register("incoming", df.to_arrow())
        conn.execute(f"DELETE FROM {TABLE} WHERE open_time IN (SELECT open_time FROM incoming)")
        conn.execute(f"INSERT INTO {TABLE} SELECT {', '.join(COLUMNS)} FROM incoming")
        conn.unregister("incoming")
        row = conn.execute(f"SELECT count(*) FROM {TABLE}").fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def coverage(asset_id: str = "solusdt") -> pl.DataFrame:
    """Per-month non-null share of every altdata_5m column."""
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = get_connection(db_path)
    try:
        value_cols = [c for c in COLUMNS if c != "open_time"]
        parts = ", ".join(
            f"round(100.0 * count({c}) / count(*)) AS {c}" for c in value_cols
        )
        return conn.execute(
            f"SELECT strftime(open_time, '%Y-%m') AS ho, count(*) AS sorok, {parts} "
            f"FROM {TABLE} GROUP BY 1 ORDER BY 1"
        ).pl()
    finally:
        conn.close()


def _force_utf8_stdout() -> None:
    """Windows consoles default to cp1250, which cannot render Polars tables."""
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build altdata_5m from Binance Vision")
    parser.add_argument("--symbol", default="SOLUSDT")
    parser.add_argument("--asset-id", default="solusdt")
    parser.add_argument("--start", default="2021-12-01", help="YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD (default: tegnap)")
    parser.add_argument("--coverage", action="store_true", help="Csak lefedettsegi riport")
    args = parser.parse_args()

    _force_utf8_stdout()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.coverage:
        print(coverage(args.asset_id))
        return

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC).date()
    end = (
        datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC).date()
        if args.end
        else datetime.now(UTC).date()
    )

    df = build(args.symbol, start, end, args.asset_id)
    total = upsert(df, args.asset_id)
    logger.info("altdata_5m: %d uj/frissitett sor, osszesen %d", df.height, total)


if __name__ == "__main__":
    main()
