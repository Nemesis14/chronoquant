#!/usr/bin/env python3
"""Feature layer for the multi-coin panel (epic_064 / t853 + t854).

Reuses the single-asset feature code unchanged. Both entry points take one
continuous time series and know nothing about symbols, so this is a per-coin
loop rather than a rewrite:

    compute_features_polars()   212 OHLCV-derived features
    compute_altdata_features()   27 derivatives features

and then one thing that is genuinely new, and only exists because there are
thirty coins: **share features**. What fraction of the panel's turnover and
open interest is this coin, and is that fraction moving? A coin tends to move
when money rotates into it *at the expense of the others*, which no amount of
single-asset history can express.

⚠️ The grid is **hourly**, not 5-minute. That is what the measurement needs and
what the disk allows: training samples one row per coin per hour anyway (two
adjacent minutes share 1439 of their 1440 label minutes), the walk-forward is
hourly, and at a 24h horizon an hourly score is indistinguishable from a
minute-by-minute one. A 5-minute grid would be 12.7 GB against 1.4 GB of free
space; hourly is ~1 GB. The finer grid is a live-scoring concern, and only
worth building if the measurement comes back positive.

Two further consequences, both deliberate:

* Window lengths in `features.json` are in **bars**, so every lookback is now
  in hours. At a 24h horizon that is the right direction -- a 14-minute window
  is noise here, a 14-hour one is not.
* `spot_*` and `bid_*`/`ask_*` inputs are absent (spot klines and bookDepth
  were deferred), so the 9 features built on them come out NULL: basis,
  spot/perp volume ratio, spot taker imbalance and the five depth features.
  The premium index covers much of what basis would say.

Tables written:
    feat_1h        (symbol, open_time) -- per-coin features
    feat_share_1h  (symbol, open_time) -- panel-relative features

Kept apart because the second depends on the whole panel: adding a coin
invalidates every share, but leaves the per-coin features untouched.

Usage:
    python -m data_handling.sync_tables.sync_universe_features
    python -m data_handling.sync_tables.sync_universe_features --shares-only
"""

import argparse
import logging
import os
import sys

import duckdb
import polars as pl

import utils
from data_handling.sync_tables._altdata_features_polars import (
    compute_altdata_features,
    expand_to_minute_grid,
)
from data_handling.sync_tables._features_polars import compute_features_polars

logger = logging.getLogger(__name__)

DB_REL = os.path.join("database", "_universe", "universe.duckdb")
PANEL_START = "2021-12-01"
# Bars of head start so the longest rolling window is warm at PANEL_START.
# The longest window in features.json is 240 bars; 600 leaves margin.
WARMUP_BARS = 600
GRID_MIN = 60

# Columns compute_altdata_features reads that this panel does not have.
MISSING_ALT_INPUTS = (
    "spot_close", "spot_volume", "spot_taker_buy",
    "bid_1", "ask_1", "bid_5", "ask_5",
)


def _conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(
        os.path.join(utils._repo_root(), DB_REL), read_only=read_only
    )


def _ohlcv(conn: duckdb.DuckDBPyConnection, symbol: str) -> pl.DataFrame:
    """One coin's hourly bars, aggregated from the 5-minute table.

    Labelled by the hour it opens, like every other bar in this project, so the
    t-1 shift inside the feature code keeps bar t built from bar t-1 only.
    """
    return conn.execute("""
        SELECT date_trunc('hour', open_time) AS open_time,
               arg_min(open, open_time)  AS open,
               max(high)                 AS high,
               min(low)                  AS low,
               arg_max(close, open_time) AS close,
               sum(volume)               AS volume,
               sum(quote_volume)         AS quote_volume,
               sum(n_trades)             AS trades,
               sum(taker_buy_volume)     AS taker_buy_base
        FROM ohlcv_5m WHERE symbol = ? GROUP BY 1 ORDER BY 1
    """, [symbol]).pl()


def _altdata_frame(conn: duckdb.DuckDBPyConnection, symbol: str) -> pl.DataFrame:
    """The frame compute_altdata_features expects, on its native 5-minute grid.

    Left at 5 minutes on purpose. That module upsamples to `5m` internally and
    its window constants are counts of 5-minute bars, so handing it hourly rows
    would silently produce windows of nulls. The features are computed at their
    native resolution and mapped onto the hourly grid afterwards, by the
    module's own as-of join.

    Missing sources are filled with nulls rather than dropped: the feature code
    then produces null features, which the downstream MI filter removes on its
    own. Silently omitting the columns would raise instead.
    """
    frame = conn.execute("""
        SELECT o.open_time,
               o.close  AS perp_close,
               o.volume AS perp_volume,
               m.oi_coin, m.ls_count, m.ls_top_count, m.ls_top_sum,
               p.prem_close,
               f.funding_rate AS funding_last,
               f.next_ts      AS funding_next_ts
        FROM ohlcv_5m o
        LEFT JOIN metrics_5m m ON m.symbol = o.symbol AND m.open_time = o.open_time
        LEFT JOIN premium_5m p ON p.symbol = o.symbol AND p.open_time = o.open_time
        LEFT JOIN (
            -- The funding rate in force at a bar is the last one settled, and
            -- the next settlement time drives the countdown feature.
            SELECT funding_ts, funding_rate,
                   lead(funding_ts) OVER (ORDER BY funding_ts) AS next_ts
            FROM funding WHERE symbol = ?
        ) f ON o.open_time >= f.funding_ts
           AND (f.next_ts IS NULL OR o.open_time < f.next_ts)
        WHERE o.symbol = ?
        ORDER BY o.open_time
    """, [symbol, symbol]).pl()

    return frame.with_columns(
        [pl.lit(None, dtype=pl.Float64).alias(c) for c in MISSING_ALT_INPUTS]
    )


def build_features(conn: duckdb.DuckDBPyConnection) -> None:
    """Per-coin features for the whole panel."""
    cfg = utils.load_features_config()["database"]["features"]
    indicators = cfg["indicators"]

    symbols = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT symbol FROM ohlcv_5m ORDER BY 1"
        ).fetchall()
    ]

    first = True
    for i, symbol in enumerate(symbols, 1):
        bars = _ohlcv(conn, symbol)
        if bars.height < WARMUP_BARS * 2:
            logger.warning("%s: tul keves bar (%d), kihagyva", symbol, bars.height)
            continue

        feats = compute_features_polars(
            df_ohlcv=bars,
            indicators=indicators,
            feat_prefix="feat_",
            available_activity=["quote_volume", "trades", "taker_buy_base"],
            targets_cfg=[],
        )

        # expand_to_minute_grid is an as-of backward join on `available_at`,
        # so it maps 5-minute features onto any coarser index without leaking:
        # the hour at 10:00 only sees alt-data stamped available before it.
        alt_5m = compute_altdata_features(_altdata_frame(conn, symbol))
        alt_h = expand_to_minute_grid(alt_5m, bars.select("open_time"))
        feats = feats.join(alt_h, on="open_time", how="left")

        out = (
            feats.with_columns(pl.lit(symbol).alias("symbol"))
            .filter(pl.col("open_time") >= pl.lit(PANEL_START).str.to_datetime())
            .select(["symbol", "open_time", *sorted(
                c for c in feats.columns if c.startswith("feat_")
            )])
        )
        # Float32 halves the table for precision no feature needs.
        out = out.with_columns(
            [pl.col(c).cast(pl.Float32) for c in out.columns if c.startswith("feat_")]
        )

        conn.register("feat_in", out.to_arrow())
        if first:
            conn.execute("DROP TABLE IF EXISTS feat_1h")
            conn.execute("CREATE TABLE feat_1h AS SELECT * FROM feat_in")
            first = False
        else:
            conn.execute("DELETE FROM feat_1h WHERE symbol = ?", [symbol])
            conn.execute("INSERT INTO feat_1h SELECT * FROM feat_in")
        conn.unregister("feat_in")

        n_null = sum(
            1 for c in out.columns
            if c.startswith("feat_") and out[c].null_count() == out.height
        )
        logger.info(
            "[%2d/%d] %-14s %d sor, %d feature (%d vegig NULL)",
            i, len(symbols), symbol, out.height,
            len([c for c in out.columns if c.startswith("feat_")]), n_null,
        )


def build_shares(conn: duckdb.DuckDBPyConnection) -> None:
    """Panel-relative features: what share of the pool is this coin.

    Every column is shifted one bar per symbol at the end, the same t-1 rule the
    rest of the feature set follows. The denominator is built from whichever
    coins actually have a bar at that hour, so a coin that had not listed yet
    cannot inflate anyone's share retroactively.
    """
    logger.info("reszarany-blokk: panel betoltese...")
    panel = conn.execute(f"""
        WITH h AS (
            SELECT symbol, date_trunc('hour', open_time) AS open_time,
                   arg_max(close, open_time) AS close,
                   sum(quote_volume)         AS quote_volume
            FROM ohlcv_5m
            WHERE open_time >= (CAST('{PANEL_START}' AS TIMESTAMP)
                                - INTERVAL {WARMUP_BARS * GRID_MIN} MINUTE)
            GROUP BY 1, 2
        ), m AS (
            SELECT symbol, date_trunc('hour', open_time) AS open_time,
                   arg_max(oi_value, open_time) AS oi_value
            FROM metrics_5m GROUP BY 1, 2
        )
        SELECT h.symbol, h.open_time, h.close, h.quote_volume, m.oi_value
        FROM h LEFT JOIN m USING (symbol, open_time)
        ORDER BY h.open_time, h.symbol
    """).pl()
    logger.info("panel: %s sor", f"{panel.height:,}")

    w_1h = 1     # one bar on the hourly grid
    w_24h = 24
    w_7d = 168

    panel = panel.with_columns(
        (pl.col("quote_volume") / pl.col("quote_volume").sum().over("open_time"))
        .alias("vol_share"),
        (pl.col("oi_value") / pl.col("oi_value").sum().over("open_time"))
        .alias("oi_share"),
        (pl.col("close") / pl.col("close").shift(w_24h).over("symbol") - 1.0)
        .alias("_ret24"),
    )
    panel = panel.with_columns(
        # How much of the last day's move was this coin's own, net of whatever
        # the whole panel did -- the cleanest single "is money rotating here"
        # signal available without a rank.
        (pl.col("_ret24") - pl.col("_ret24").mean().over("open_time"))
        .alias("ret_vs_pool_24h"),
    )
    panel = panel.with_columns(
        (pl.col("vol_share") / pl.col("vol_share").shift(w_1h).over("symbol") - 1.0)
        .alias("vol_share_delta_1h"),
        (pl.col("vol_share") / pl.col("vol_share").shift(w_24h).over("symbol") - 1.0)
        .alias("vol_share_delta_24h"),
        (pl.col("oi_share") / pl.col("oi_share").shift(w_24h).over("symbol") - 1.0)
        .alias("oi_share_delta_24h"),
        (
            (pl.col("vol_share")
             - pl.col("vol_share").rolling_mean(w_7d).over("symbol"))
            / pl.col("vol_share").rolling_std(w_7d).over("symbol")
        ).alias("vol_share_z_7d"),
        (
            (pl.col("oi_share")
             - pl.col("oi_share").rolling_mean(w_7d).over("symbol"))
            / pl.col("oi_share").rolling_std(w_7d).over("symbol")
        ).alias("oi_share_z_7d"),
    )

    share_cols = [
        "vol_share", "vol_share_delta_1h", "vol_share_delta_24h", "vol_share_z_7d",
        "oi_share", "oi_share_delta_24h", "oi_share_z_7d", "ret_vs_pool_24h",
    ]
    out = (
        panel.with_columns(
            # t-1 lag, exactly as every other feature in this project.
            [pl.col(c).shift(1).over("symbol").alias(f"feat_{c}") for c in share_cols]
        )
        .filter(pl.col("open_time") >= pl.lit(PANEL_START).str.to_datetime())
        .select(["symbol", "open_time", *[f"feat_{c}" for c in share_cols]])
        .with_columns(
            [pl.col(f"feat_{c}").cast(pl.Float32) for c in share_cols]
        )
    )

    conn.register("share_in", out.to_arrow())
    conn.execute("CREATE OR REPLACE TABLE feat_share_1h AS SELECT * FROM share_in")
    conn.unregister("share_in")
    logger.info("feat_share_1h: %s sor, %d feature", f"{out.height:,}", len(share_cols))


def report(conn: duckdb.DuckDBPyConnection) -> None:
    print("\n=== FEATURE TABLAK ===")
    for t in ("feat_1h", "feat_share_1h"):
        try:
            n, k = conn.execute(
                f"SELECT count(*), count(DISTINCT symbol) FROM {t}"
            ).fetchone()  # type: ignore[misc]
            cols = [r[0] for r in conn.execute(f"DESCRIBE {t}").fetchall()]
            nfeat = len([c for c in cols if c.startswith("feat_")])
            print(f"  {t:<16}{n:>13,} sor  {k:>3} coin  {nfeat:>4} feature")
        except duckdb.CatalogException:
            print(f"  {t:<16}-- nincs meg --")

    print("\n=== VEGIG NULL FEATURE-OK (a kihagyott spot/bookDepth miatt) ===")
    cols = [
        r[0] for r in conn.execute("DESCRIBE feat_1h").fetchall()
        if r[0].startswith("feat_")
    ]
    checks = ", ".join(f'count("{c}") AS "{c}"' for c in cols)
    row = conn.execute(f"SELECT {checks} FROM feat_1h").fetchone()
    dead = [c for c, v in zip(cols, row or [], strict=False) if v == 0]
    print(f"  {len(dead)} / {len(cols)}: {', '.join(dead) if dead else 'nincs'}")

    print("\n=== RESZARANY MINTA (a legutolso bar) ===")
    df = conn.execute("""
        SELECT symbol, feat_vol_share, feat_vol_share_delta_24h,
               feat_oi_share, feat_ret_vs_pool_24h
        FROM feat_share_1h
        WHERE open_time = (SELECT max(open_time) FROM feat_share_1h)
        ORDER BY feat_vol_share DESC NULLS LAST LIMIT 8
    """).df()
    print("  " + df.to_string(index=False).replace("\n", "\n  "))


def main() -> None:
    parser = argparse.ArgumentParser(description="Panel feature-reteg")
    parser.add_argument("--shares-only", action="store_true")
    parser.add_argument("--features-only", action="store_true")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = _conn()
    try:
        if not args.shares_only:
            build_features(conn)
        if not args.features_only:
            build_shares(conn)
        report(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
