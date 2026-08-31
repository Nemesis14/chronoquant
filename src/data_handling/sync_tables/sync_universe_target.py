#!/usr/bin/env python3
"""Vol-scaled 24h triple-barrier labels for the multi-coin panel (epic_064 / t855).

Two things make this different from the single-asset labeller.

**The barrier is one shared multiple of each coin's own volatility.** A fixed
width in basis points is a large move on BTC and noise on a memecoin: at 300 bps
BTC times out 56% of the time and 1000BONK only 5%, so the model would learn to
recognise which coin it is looking at rather than which way price will go. One
common `k` applied to each coin's own sigma fixes that -- the widths differ in
basis points, but "+1" means the same amount of relative movement everywhere.
`k` is chosen so the panel-median barrier lands on TARGET_MEDIAN_BPS.

**Isolated price spikes are capped.** The archive contains bars like

    DASHUSDT 2020-04-14 07:35   open 73.65  high 73.99  low 0.10  close 73.09

-- a 99.9% round trip inside one minute on a thin book. Intrabar touch decides
the label, so that single bar would stamp "-1" on all 1440 bars before it. But
real crashes must survive: on 2025-10-10 21:17 seven coins printed 50-85% ranges
in the same minutes, and that is exactly the move a barrier label exists to
capture. The test is therefore co-occurrence, not size: an extreme range on one
coin while the rest of the panel is calm is treated as a bad print and its wick
is capped; the same range across several coins is left untouched. Coins are
counted within a few minutes of each other, not on the exact minute -- a slower
crash rolls through the panel over several bars.

Every capped bar is written to `wick_audit` so the decision stays reviewable.

Usage:
    python -m data_handling.sync_tables.sync_universe_target
    python -m data_handling.sync_tables.sync_universe_target --audit-only
"""

import argparse
import logging
import os
import sys

import duckdb
import numpy as np
import polars as pl

import utils
from data_handling.sync_tables._triple_barrier import (
    class_distribution,
    horizon_barrier_bps,
    triple_barrier_labels,
)

logger = logging.getLogger(__name__)

DB_REL = os.path.join("database", "_universe", "universe.duckdb")
HORIZON_MIN = 1440          # 24 hours
TARGET_MEDIAN_BPS = 300.0   # each coin's median barrier, in its own vol units
VOL_SPAN = 1440
MIN_BPS = 30.0              # alt perps are far more volatile than SOL; the fee
MAX_BPS = 2000.0            # floor can sit lower and the ceiling much higher
PANEL_START = "2021-12-01"  # metrics coverage begins here; the panel is uniform from then

# A bar whose high-low span exceeds this fraction of close is "extreme".
WICK_EXTREME = 0.20
# Extreme on at most this many coins -> treated as a bad print rather than a
# market event. Co-occurrence is checked within a window, not on the exact
# minute: on 2025-10-10 twenty coins spiked in the same minute, but the
# 2021-05-19 crash rolled through BCH at 13:14, ETH at 13:10 and FIL at 13:08.
# An exact-minute test called that real crash isolated.
ISOLATION_MAX_COINS = 1
ISOLATION_WINDOW_MIN = 5
# What an isolated wick gets capped to, as a fraction beyond the bar's body.
WICK_CAP = 0.10


def _conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(
        os.path.join(utils._repo_root(), DB_REL), read_only=read_only
    )


def extreme_minutes(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Every extreme bar in the panel, with how many coins were extreme with it.

    The count is what separates a bad print from a crash, so it is computed
    across the whole panel before any single coin is labelled.
    """
    return conn.execute(f"""
        WITH ext AS (
            SELECT symbol, open_time, open, high, low, close,
                   (high - low) / close AS span
            FROM ohlcv_1m
            WHERE close > 0 AND (high - low) / close > {WICK_EXTREME}
        )
        SELECT e.*, (
            SELECT count(DISTINCT x.symbol) FROM ext x
            WHERE x.open_time
                  BETWEEN e.open_time - INTERVAL {ISOLATION_WINDOW_MIN} MINUTE
                      AND e.open_time + INTERVAL {ISOLATION_WINDOW_MIN} MINUTE
        ) AS n_coins
        FROM ext e
        ORDER BY e.symbol, e.open_time
    """).pl()


def common_k(conn: duckdb.DuckDBPyConnection, symbols: list[str]) -> float:
    """One multiplier for the whole panel, sized so the median barrier is sane.

    Normalising each coin separately onto the same basis-point width is the
    trap: it makes the label mean "a big move" on BTC and "a normal move" on a
    memecoin, which is coin identity dressed up as a target. Taking the median
    across coins keeps a single relative definition while still landing the
    typical barrier near TARGET_MEDIAN_BPS.
    """
    units = []
    for symbol in symbols:
        close = conn.execute(
            "SELECT close FROM ohlcv_1m WHERE symbol = ? ORDER BY open_time",
            [symbol],
        ).pl()["close"].to_numpy().astype("float64")
        if close.size <= VOL_SPAN:
            continue
        unit = horizon_barrier_bps(
            close, horizon=HORIZON_MIN, k=1.0, span=VOL_SPAN,
            min_bps=0.0, max_bps=np.inf,
        )
        units.append(float(np.nanmedian(unit)))
    if not units:
        raise ValueError("nincs eleg adat a kozos k meghatarozasahoz")
    k = TARGET_MEDIAN_BPS / float(np.median(units))
    logger.info(
        "kozos k = %.4f  (coinonkenti egyseg-median %.0f .. %.0f bps, panel-median %.0f)",
        k, min(units), max(units), float(np.median(units)),
    )
    return k


def _cap_wicks(
    frame: pl.DataFrame, isolated: set[np.datetime64]
) -> tuple[np.ndarray, np.ndarray, int]:
    """Return high/low arrays with isolated spikes pulled back to the body."""
    high = frame["high"].to_numpy().astype("float64").copy()
    low = frame["low"].to_numpy().astype("float64").copy()
    if not isolated:
        return high, low, 0

    times = frame["open_time"].to_numpy()
    mask = np.isin(times, np.array(sorted(isolated), dtype=times.dtype))
    if not mask.any():
        return high, low, 0

    op = frame["open"].to_numpy().astype("float64")
    cl = frame["close"].to_numpy().astype("float64")
    body_hi = np.maximum(op, cl)
    body_lo = np.minimum(op, cl)
    # The body is what actually traded; the wick beyond it is the suspect part.
    high[mask] = np.minimum(high[mask], body_hi[mask] * (1.0 + WICK_CAP))
    low[mask] = np.maximum(low[mask], body_lo[mask] * (1.0 - WICK_CAP))
    return high, low, int(mask.sum())


def build(conn: duckdb.DuckDBPyConnection) -> None:
    """Label every coin and store the 5-minute-aligned rows."""
    ext = extreme_minutes(conn)
    isolated_all = ext.filter(pl.col("n_coins") <= ISOLATION_MAX_COINS)
    logger.info(
        "extrem barok: %d, ebbol izolalt (<=%d coin +-%d percen belul): %d",
        ext.height, ISOLATION_MAX_COINS, ISOLATION_WINDOW_MIN, isolated_all.height,
    )

    conn.execute("DROP TABLE IF EXISTS wick_audit")
    conn.register("audit_in", ext.to_arrow())
    conn.execute("CREATE TABLE wick_audit AS SELECT * FROM audit_in")
    conn.unregister("audit_in")

    conn.execute(f"""
        CREATE OR REPLACE TABLE target_5m (
            symbol VARCHAR, open_time TIMESTAMP,
            tb_label_{HORIZON_MIN}_vol TINYINT,
            tb_label_{HORIZON_MIN}_vol_bps REAL,
            fwd_ret_{HORIZON_MIN} REAL,
            PRIMARY KEY (symbol, open_time)
        )
    """)

    symbols = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT symbol FROM ohlcv_1m ORDER BY 1"
        ).fetchall()
    ]
    k = common_k(conn, symbols)

    for i, symbol in enumerate(symbols, 1):
        bars = conn.execute(
            "SELECT open_time, open, high, low, close FROM ohlcv_1m"
            " WHERE symbol = ? ORDER BY open_time",
            [symbol],
        ).pl()
        if bars.height <= HORIZON_MIN:
            logger.warning("%s: tul keves sor, kihagyva", symbol)
            continue

        iso = set(
            isolated_all.filter(pl.col("symbol") == symbol)["open_time"].to_numpy()
        )
        high, low, n_capped = _cap_wicks(bars, iso)
        close = bars["close"].to_numpy().astype("float64")

        width = horizon_barrier_bps(
            close, horizon=HORIZON_MIN, k=k, span=VOL_SPAN,
            min_bps=MIN_BPS, max_bps=MAX_BPS,
        )
        labels = triple_barrier_labels(
            close, high, low, horizon=HORIZON_MIN, up_bps=width, dn_bps=width
        )
        # The plain forward return prices the timeout class, where neither
        # barrier was reached and the position is closed at the clock.
        fwd = np.full(close.size, np.nan)
        fwd[:-HORIZON_MIN] = (
            np.log(close[HORIZON_MIN:] / close[:-HORIZON_MIN]) * 10_000.0
        )

        out = pl.DataFrame({
            "symbol": [symbol] * bars.height,
            "open_time": bars["open_time"],
            f"tb_label_{HORIZON_MIN}_vol": labels,
            f"tb_label_{HORIZON_MIN}_vol_bps": width,
            f"fwd_ret_{HORIZON_MIN}": fwd,
        }).with_columns(
            pl.col(f"tb_label_{HORIZON_MIN}_vol").fill_nan(None),
            pl.col(f"tb_label_{HORIZON_MIN}_vol_bps").fill_nan(None),
            pl.col(f"fwd_ret_{HORIZON_MIN}").fill_nan(None),
        ).filter(
            # Only the 5-minute grid is stored: the label is computed from
            # 1-minute bars for touch accuracy, but the model never sees a
            # finer row than the feature grid.
            (pl.col("open_time").dt.minute() % 5 == 0)
            & (pl.col("open_time") >= pl.lit(PANEL_START).str.to_datetime())
        )

        conn.register("tgt_in", out.to_arrow())
        conn.execute("INSERT INTO target_5m SELECT * FROM tgt_in")
        conn.unregister("tgt_in")

        dist = class_distribution(labels)
        logger.info(
            "[%2d/%d] %-14s korlat med=%.0f bps (p10 %.0f / p90 %.0f)"
            "  wick-capped=%d  +1=%.1f%% -1=%.1f%% 0=%.1f%%  sorok=%d",
            i, len(symbols), symbol,
            float(np.nanmedian(width)), float(np.nanpercentile(width, 10)),
            float(np.nanpercentile(width, 90)), n_capped,
            dist["+1"], dist["-1"], dist["0"], out.height,
        )


def report(conn: duckdb.DuckDBPyConnection) -> None:
    """Class balance and barrier width, per coin and per year."""
    lbl = f"tb_label_{HORIZON_MIN}_vol"
    print("\n=== OSZTALYELOSZLAS COINONKENT ===")
    print(f"  {'symbol':<14}{'n':>10}{'+1':>8}{'-1':>8}{'timeout':>9}"
          f"{'korlat med':>12}")
    for r in conn.execute(f"""
        SELECT symbol, count(*),
               round(100.0*avg(CASE WHEN {lbl}= 1 THEN 1.0 ELSE 0 END),1),
               round(100.0*avg(CASE WHEN {lbl}=-1 THEN 1.0 ELSE 0 END),1),
               round(100.0*avg(CASE WHEN {lbl}= 0 THEN 1.0 ELSE 0 END),1),
               round(median({lbl}_bps),0)
        FROM target_5m WHERE {lbl} IS NOT NULL GROUP BY 1 ORDER BY 1
    """).fetchall():
        print(f"  {r[0]:<14}{r[1]:>10,}{r[2]:>7.1f}%{r[3]:>7.1f}%{r[4]:>8.1f}%"
              f"{r[5]:>12,.0f}")

    print("\n=== OSZTALYELOSZLAS EVENKENT (korszak-stabilitas) ===")
    print(f"  {'ev':>6}{'n':>12}{'+1':>8}{'-1':>8}{'timeout':>9}{'korlat med':>12}")
    for r in conn.execute(f"""
        SELECT strftime(open_time,'%Y'), count(*),
               round(100.0*avg(CASE WHEN {lbl}= 1 THEN 1.0 ELSE 0 END),1),
               round(100.0*avg(CASE WHEN {lbl}=-1 THEN 1.0 ELSE 0 END),1),
               round(100.0*avg(CASE WHEN {lbl}= 0 THEN 1.0 ELSE 0 END),1),
               round(median({lbl}_bps),0)
        FROM target_5m WHERE {lbl} IS NOT NULL GROUP BY 1 ORDER BY 1
    """).fetchall():
        print(f"  {r[0]:>6}{r[1]:>12,}{r[2]:>7.1f}%{r[3]:>7.1f}%{r[4]:>8.1f}%"
              f"{r[5]:>12,.0f}")

    print("\n=== WICK AUDIT ===")
    r = conn.execute(f"""
        SELECT count(*), count(*) FILTER (WHERE n_coins <= {ISOLATION_MAX_COINS})
        FROM wick_audit
    """).fetchone() or (0, 0)
    print(f"  extrem bar osszesen: {r[0]:,}   ebbol izolalt (kapva): {r[1]:,}")
    print("\n  a 10 legnagyobb IZOLALT (kapott) kilenges:")
    for row in conn.execute(f"""
        SELECT symbol, open_time, open, high, low, close, round(span*100,0)
        FROM wick_audit WHERE n_coins <= {ISOLATION_MAX_COINS}
        ORDER BY span DESC LIMIT 10
    """).fetchall():
        print(f"    {row[0]:<14}{row[1]!s:<21} o={row[2]:<12g} h={row[3]:<12g}"
              f" l={row[4]:<12g} c={row[5]:<12g} span={row[6]:.0f}%")
    print("\n  a 10 legnagyobb EGYUTTMOZGO (erintetlen) kilenges:")
    for row in conn.execute(f"""
        SELECT symbol, open_time, low, close, round(span*100,0), n_coins
        FROM wick_audit WHERE n_coins > {ISOLATION_MAX_COINS}
        ORDER BY span DESC LIMIT 10
    """).fetchall():
        print(f"    {row[0]:<14}{row[1]!s:<21} l={row[2]:<12g} c={row[3]:<12g}"
              f" span={row[4]:.0f}%  ekkor {row[5]} coin volt extrem")


def main() -> None:
    parser = argparse.ArgumentParser(description="Panel triple-barrier cimke")
    parser.add_argument("--audit-only", action="store_true",
                        help="Csak a wick-auditot es a riportot futtatja")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = _conn()
    try:
        if not args.audit_only:
            build(conn)
        report(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
