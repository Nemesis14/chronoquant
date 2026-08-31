#!/usr/bin/env python3
"""Direction-oriented feature block (epic_064, irany-feature-ok).

Everything measured so far says the same thing: the models keep finding
volatility. Mutual information ranks `natr`, `parkinson_vol`, `returns_std` and
`hist_vol` at the top because they separate the timeout class -- whether a big
move happens at all -- while saying nothing about which way it goes. Every
continuous target made this worse, because squared loss rewards predicting
magnitude.

So this block does two things.

**It throws volatility out of the candidate pool.** Only signed features
survive: something that says "up" or "down", not "a lot" or "a little". Trend
strength goes too -- ADX measures how strong a move is, not its direction,
though its +DI/-DI components are directional and stay.

**It adds positioning.** The derivatives data has been downloaded and unused:
open interest, the long/short account ratios, the top-trader ratios, the taker
buy/sell split and the funding rate. Positioning is the one thing in this panel
that is not another transform of price, and crowding is a directional story --
a market where everyone is already long, that has stopped going up, tends to go
down.

The crowding features follow that literally: funding level alone is not a
signal, but expensive funding *plus* built-up open interest *plus* a price that
has stopped responding is.

⚠️ Funding settles every 8 hours and the metrics archive is 5-minute, so those
features update far more slowly than the price ones. They are forward-filled
onto the hourly grid, which is honest but means several consecutive hours can
carry the same value.

Usage:
    python -m modeling.experiments.direction_features --symbol XRPUSDT --build
"""

import argparse
import logging
import os
import sys

import duckdb
import numpy as np
import polars as pl

import utils
from data_handling.sync_tables._features_polars import _rolling_rank_arr
from modeling.experiments.baseline_features import (
    DB_REL,
    NORM_WINDOW,
    PANEL_START,
)

logger = logging.getLogger(__name__)

TABLE = "feat_dir_1h"

# Pure magnitude: how much moved, not which way. These are what the models kept
# latching onto, so they are excluded from the candidate pool by name.
MAGNITUDE_MARKERS = (
    "natr", "_atr_", "atr_dist", "hist_vol", "gk_vol", "parkinson", "bb_width",
    "returns_std", "returns_kurt", "donchian_width", "hml_range", "ohlc_range",
    "volume_sma", "volume_ratio", "avg_trade_quote", "trade_count",
    "true_range", "vol_of_vol", "range_", "_rank_",
)
# ADX is trend strength; its directional components are not, so they stay.
ADX_KEEP = ("adx_pos", "adx_neg")


def is_directional(col: str) -> bool:
    """True when a feature carries a sign rather than a magnitude."""
    if any(k in col for k in ADX_KEEP):
        return True
    if col.startswith("feat_adx"):
        return False
    return not any(m in col for m in MAGNITUDE_MARKERS)


def _hourly_sources(conn: duckdb.DuckDBPyConnection, symbol: str) -> pl.DataFrame:
    """Price, positioning, premium and funding on one hourly grid."""
    return conn.execute("""
        WITH px AS (
            SELECT date_trunc('hour', open_time) AS open_time,
                   arg_max(close, open_time) AS close,
                   max(high) AS high, min(low) AS low,
                   sum(quote_volume) AS quote_volume,
                   sum(taker_buy_volume) AS taker_buy
            FROM ohlcv_5m WHERE symbol = ? GROUP BY 1
        ), mt AS (
            SELECT date_trunc('hour', open_time) AS open_time,
                   arg_max(oi_value, open_time)  AS oi_value,
                   arg_max(ls_count, open_time)  AS ls_count,
                   arg_max(ls_top_count, open_time) AS ls_top_count,
                   arg_max(ls_top_sum, open_time)   AS ls_top_sum,
                   avg(taker_vol_ratio)          AS taker_ratio
            FROM metrics_5m WHERE symbol = ? GROUP BY 1
        ), pm AS (
            SELECT date_trunc('hour', open_time) AS open_time,
                   avg(prem_close) AS prem
            FROM premium_5m WHERE symbol = ? GROUP BY 1
        )
        SELECT px.*, mt.oi_value, mt.ls_count, mt.ls_top_count, mt.ls_top_sum,
               mt.taker_ratio, pm.prem,
               (SELECT f.funding_rate FROM funding f
                WHERE f.symbol = ? AND f.funding_ts <= px.open_time
                ORDER BY f.funding_ts DESC LIMIT 1) AS funding
        FROM px LEFT JOIN mt USING (open_time) LEFT JOIN pm USING (open_time)
        ORDER BY px.open_time
    """, [symbol] * 4).pl()


def _z(col: pl.Expr, w: int) -> pl.Expr:
    m = col.rolling_mean(w, min_samples=w // 2)
    s = col.rolling_std(w, min_samples=w // 2)
    return pl.when(s > 0).then((col - m) / s).otherwise(None)


def build_block(src: pl.DataFrame) -> pl.DataFrame:
    """The direction block: positioning, crowding, divergence, confirmation."""
    d = src.sort("open_time")
    ret = pl.col("close") / pl.col("close").shift(1) - 1.0

    d = d.with_columns(
        ret.alias("_ret1"),
        (pl.col("close") / pl.col("close").shift(24) - 1.0).alias("_ret24"),
        (pl.col("taker_buy") / pl.col("quote_volume")).alias("_taker_share"),
    )

    d = d.with_columns(
        # --- funding: level, persistence, and the turn -----------------------
        _z(pl.col("funding"), 168).alias("feat_dir_funding_z_168"),
        pl.col("funding").sign().alias("_f_sign"),
        (pl.col("funding") - pl.col("funding").shift(24))
        .alias("feat_dir_funding_delta_24"),
        # --- premium: the same story, but it updates every bar ---------------
        _z(pl.col("prem"), 168).alias("feat_dir_prem_z_168"),
        (pl.col("prem") - pl.col("prem").shift(8)).alias("feat_dir_prem_delta_8"),
        # --- leverage build-up ----------------------------------------------
        (pl.col("oi_value") / pl.col("oi_value").shift(8) - 1.0)
        .alias("feat_dir_oi_delta_8"),
        (pl.col("oi_value") / pl.col("oi_value").shift(24) - 1.0)
        .alias("feat_dir_oi_delta_24"),
        _z(pl.col("oi_value"), 168).alias("feat_dir_oi_z_168"),
        # --- who is positioned which way ------------------------------------
        _z(pl.col("ls_count").log(), 168).alias("feat_dir_ls_retail_z_168"),
        _z(pl.col("ls_top_count").log(), 168).alias("feat_dir_ls_top_z_168"),
        (pl.col("ls_count").log() - pl.col("ls_top_count").log())
        .alias("feat_dir_retail_vs_top"),
        (pl.col("ls_count") / pl.col("ls_count").shift(24) - 1.0)
        .alias("feat_dir_ls_delta_24"),
        # --- aggressive flow -------------------------------------------------
        (pl.col("taker_ratio") - 1.0).alias("feat_dir_taker_imb"),
        _z(pl.col("taker_ratio"), 168).alias("feat_dir_taker_z_168"),
        (pl.col("_taker_share") - pl.col("_taker_share").rolling_mean(24))
        .alias("feat_dir_taker_share_dev_24"),
    )

    d = d.with_columns(
        # How long funding has held its sign -- a fresh flip and a month-old
        # tilt mean opposite things.
        (pl.col("_f_sign") * pl.col("_f_sign").rolling_sum(48, min_samples=1).abs())
        .alias("feat_dir_funding_persist_48"),
        (pl.col("_f_sign") != pl.col("_f_sign").shift(24))
        .cast(pl.Float64).alias("feat_dir_funding_flip_24"),
    )

    d = d.with_columns(
        # --- crowding: expensive, built-up, and going nowhere ----------------
        # The user's hypothesis, written literally. A large positive value says
        # longs are paying up, leverage has grown, and price has not rewarded
        # them -- the setup that resolves downward.
        (
            pl.col("feat_dir_funding_z_168")
            * pl.col("feat_dir_oi_z_168")
            * (1.0 - pl.col("_ret24").abs()
               / (pl.col("_ret24").abs().rolling_mean(168, min_samples=24) + 1e-9))
        ).alias("feat_dir_crowded"),
        # Leverage building without price following is the same idea, simpler.
        (pl.col("feat_dir_oi_delta_24") - pl.col("_ret24"))
        .alias("feat_dir_oi_price_div_24"),
        (pl.col("feat_dir_oi_delta_8") * pl.col("_ret1").sign())
        .alias("feat_dir_oi_signed_flow_8"),
        # --- divergence: new high on weaker internals ------------------------
        (
            (pl.col("close") >= pl.col("close").rolling_max(24, min_samples=12))
            .cast(pl.Float64)
            * (pl.col("feat_dir_oi_delta_24") < 0).cast(pl.Float64)
        ).alias("feat_dir_high_no_oi_24"),
        (
            (pl.col("close") <= pl.col("close").rolling_min(24, min_samples=12))
            .cast(pl.Float64)
            * (pl.col("feat_dir_oi_delta_24") < 0).cast(pl.Float64)
        ).alias("feat_dir_low_no_oi_24"),
    )

    feats = [c for c in d.columns if c.startswith("feat_dir_")]
    # t-1 lag, then the same within-coin rolling percentile the rest of the set
    # uses, so a value means the same thing here as everywhere else.
    d = d.with_columns([pl.col(c).shift(1).alias(c) for c in feats])
    ranked = {}
    for c in feats:
        a = d[c].to_numpy().astype("float64")
        ranked[c] = (a if np.unique(a[~np.isnan(a)]).size <= 3
                     else _rolling_rank_arr(a, NORM_WINDOW))
    d = d.with_columns([pl.Series(c, v) for c, v in ranked.items()])
    return d.select(["open_time", *feats])


def build(symbols: list[str]) -> None:
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL))
    try:
        first = True
        for symbol in symbols:
            block = build_block(_hourly_sources(conn, symbol))
            feats = [c for c in block.columns if c.startswith("feat_dir_")]
            out = (
                block.with_columns(pl.lit(symbol).alias("symbol"))
                .filter(pl.col("open_time") >= pl.lit(PANEL_START).str.to_datetime())
                .select(["symbol", "open_time", *feats])
                .with_columns([pl.col(c).cast(pl.Float32) for c in feats])
            )
            conn.register("in_", out.to_arrow())
            if first:
                conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
                conn.execute(f"CREATE TABLE {TABLE} AS SELECT * FROM in_")
                first = False
            else:
                conn.execute(f"DELETE FROM {TABLE} WHERE symbol = ?", [symbol])
                conn.execute(f"INSERT INTO {TABLE} SELECT * FROM in_")
            conn.unregister("in_")
            nn = sum(1 for c in feats if out[c].null_count() < out.height)
            logger.info("%-14s %d sor, %d irany-feature (%d hasznalhato)",
                        symbol, out.height, len(feats), nn)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Irany-orientalt feature blokk")
    parser.add_argument("--symbol", default="XRPUSDT")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--build", action="store_true")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.build:
        if args.all:
            conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL),
                                  read_only=True)
            syms = [r[0] for r in conn.execute(
                "SELECT DISTINCT symbol FROM ohlcv_5m ORDER BY 1").fetchall()]
            conn.close()
        else:
            syms = [args.symbol]
        build(syms)


if __name__ == "__main__":
    main()
