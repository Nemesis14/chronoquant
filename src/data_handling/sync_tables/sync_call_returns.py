#!/usr/bin/env python3
"""Compute drift-corrected forward returns for analyst-signal calls (epic_051 / t4).

Joins the source-agnostic calls table against BTCUSDT spot OHLCV and writes one
row per (call, horizon) into call_returns.

Pipeline position
-----------------
normalize (calls)  ->  sync_call_returns  ->  author scoring (t5)

Why the drift correction
------------------------
BTC drifts upward over the sample, so a zero-skill author who is always long
earns that drift for free while an always-short one loses it. Ranking on raw
return would therefore rank direction bias, not skill. Measured on this data at
the actual call timestamps, the gap between a permabull and a permabear at a
7-day horizon is about 0.85 percentage points with zero skill on either side.

Two independent drift estimates are stored so the result can be checked both ways:

  drift_bucket   Contemporaneous: the mean market return over all calls in the
                 same calendar month at the same horizon. Low variance, and it
                 conditions on the period, which a single pooled mean does not.
                 Applied uniformly to every author, so it encodes no
                 author-specific future information. This is the primary column.

  drift_trailing Strictly leak-free: implied by the trailing 365-day log price
                 change, scaled to the horizon. Noisier, uses only pre-t data.
                 Robustness check for the primary result.

Volatility normalisation
------------------------
BTC volatility varies by an order of magnitude across regimes, so without
scaling, an author's mean is dominated by whichever calls happened to land in
turbulent periods. z divides the excess return by trailing realised volatility
scaled to the horizon, making calls comparable across time.

Usage:
    python -m data_handling.sync_tables.sync_call_returns
    python -m data_handling.sync_tables.sync_call_returns --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, datetime

import duckdb
import polars as pl

import utils
from data_handling.store.analyst_signal_store import (
    DB_SUBPATH,
    ensure_tables,
    get_connection,
    upsert_call_returns,
)

logger = logging.getLogger(__name__)

#: Asset whose OHLCV backs the forward returns.
PRICE_ASSET_ID = "btcusdt"

#: Evaluation horizons in minutes: 1h, 4h, 1d, 3d, 7d.
HORIZONS_MIN = (60, 240, 1440, 4320, 10080)

#: A matched bar may be at most this far from the requested instant. The OHLCV
#: store has Binance maintenance gaps up to 33 hours in 2018, and a stale entry
#: price would silently corrupt the return basis, so such calls get NULL.
MATCH_TOLERANCE_MIN = 60

#: Calendar months with fewer calls than this fall back to the yearly mean, so
#: that a sparse early bucket cannot produce a wild drift estimate.
MIN_BUCKET_CALLS = 20

#: Trailing window for the leak-free drift estimate.
TRAILING_DRIFT_DAYS = 365

#: Trailing window for realised volatility.
TRAILING_VOL_DAYS = 30

_MINUTES_PER_DAY = 1440


# %% SQL

def _build_sql(horizon_min: int) -> str:
    """Return the SQL computing every call_returns column for one horizon.

    Args:
        horizon_min : Forward horizon in minutes.

    Returns:
        A DuckDB SQL statement producing one row per call.
    """
    trailing_min = TRAILING_DRIFT_DAYS * _MINUTES_PER_DAY
    return f"""
    WITH c AS (
        SELECT call_id, direction, ts_utc FROM sig.calls
    ),
    -- last bar at or before the call, and at or before the exit instant
    entry AS (
        SELECT c.call_id, c.direction, c.ts_utc,
               o.open_time AS entry_ts, o.close AS entry_price
        FROM c ASOF LEFT JOIN ohlcv o ON o.open_time <= c.ts_utc
    ),
    ex AS (
        SELECT e.*, o.open_time AS exit_ts, o.close AS exit_price
        FROM entry e ASOF LEFT JOIN ohlcv o
          ON o.open_time <= e.ts_utc + INTERVAL {horizon_min} MINUTE
    ),
    -- price {TRAILING_DRIFT_DAYS} days before the call, for the leak-free drift
    trail AS (
        SELECT x.*, o.close AS past_price
        FROM ex x ASOF LEFT JOIN ohlcv o
          ON o.open_time <= x.ts_utc - INTERVAL {trailing_min} MINUTE
    ),
    base AS (
        SELECT
            call_id, direction, ts_utc, entry_ts, entry_price, exit_ts, exit_price,
            CASE
                WHEN entry_price > 0
                 AND datediff('minute', entry_ts, ts_utc) <= {MATCH_TOLERANCE_MIN}
                 AND datediff('minute', exit_ts,
                              ts_utc + INTERVAL {horizon_min} MINUTE) <= {MATCH_TOLERANCE_MIN}
                THEN (exit_price - entry_price) / entry_price
            END AS r_mkt,
            CASE
                WHEN past_price > 0 AND entry_price > 0
                THEN exp(ln(entry_price / past_price) * {horizon_min} / {trailing_min}) - 1
            END AS drift_trailing,
            date_trunc('month', ts_utc) AS bucket_m,
            date_trunc('year',  ts_utc) AS bucket_y
        FROM trail
    ),
    -- contemporaneous drift: mean market return of the calls in the bucket
    drift AS (
        SELECT
            b.*,
            AVG(r_mkt) OVER (PARTITION BY bucket_m) AS mu_month,
            COUNT(r_mkt) OVER (PARTITION BY bucket_m) AS n_month,
            AVG(r_mkt) OVER (PARTITION BY bucket_y) AS mu_year
        FROM base b
    ),
    vol AS (
        SELECT d.*, v.vol_1m
        FROM drift d ASOF LEFT JOIN daily_vol v ON v.d <= d.ts_utc
    )
    SELECT
        call_id,
        {horizon_min}::INTEGER                        AS horizon_min,
        entry_ts, entry_price, exit_ts, exit_price,
        r_mkt,
        direction * r_mkt                             AS r_raw,
        CASE WHEN n_month >= {MIN_BUCKET_CALLS}
             THEN mu_month ELSE mu_year END           AS drift_bucket,
        drift_trailing,
        direction * (r_mkt - CASE WHEN n_month >= {MIN_BUCKET_CALLS}
                                  THEN mu_month ELSE mu_year END) AS r_excess,
        direction * (r_mkt - drift_trailing)          AS r_excess_trail,
        vol_1m * sqrt({horizon_min})                  AS vol,
        CASE WHEN vol_1m > 0
             THEN direction * (r_mkt - CASE WHEN n_month >= {MIN_BUCKET_CALLS}
                                            THEN mu_month ELSE mu_year END)
                  / (vol_1m * sqrt({horizon_min}))
        END                                           AS z
    FROM vol
    """


_DAILY_VOL_SQL = f"""
CREATE OR REPLACE TEMP TABLE daily_vol AS
WITH lr AS (
    SELECT open_time,
           ln(close) - ln(lag(close) OVER (ORDER BY open_time)) AS r
    FROM ohlcv WHERE close > 0
),
daily AS (
    SELECT date_trunc('day', open_time) AS d,
           SUM(r * r) AS ss,
           COUNT(r)   AS n
    FROM lr WHERE r IS NOT NULL GROUP BY 1
),
rolled AS (
    SELECT d,
           SUM(ss) OVER w AS ss_w,
           SUM(n)  OVER w AS n_w
    FROM daily
    WINDOW w AS (ORDER BY d ROWS BETWEEN {TRAILING_VOL_DAYS} PRECEDING AND 1 PRECEDING)
)
SELECT d, CASE WHEN n_w > 0 THEN sqrt(ss_w / n_w) END AS vol_1m FROM rolled
"""


# %% Compute

def compute(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Compute forward returns for every call at every horizon.

    Args:
        conn : DuckDB connection with the price store open and sig attached.

    Returns:
        Polars DataFrame matching the call_returns schema.
    """
    conn.execute(_DAILY_VOL_SQL)
    now = datetime.now(UTC).replace(tzinfo=None)

    row = conn.execute("SELECT COUNT(*) FROM sig.calls").fetchone()
    n_calls = int(row[0]) if row else 0

    frames: list[pl.DataFrame] = []
    for horizon in HORIZONS_MIN:
        df = conn.execute(_build_sql(horizon)).pl()
        # Every call must survive the joins. A shortfall means an ASOF join
        # silently dropped rows -- which would bias the sample towards the
        # periods that happen to have full price coverage.
        if df.height != n_calls:
            raise RuntimeError(
                f"h={horizon}: {df.height} sor {n_calls} call helyett — "
                f"egy ASOF join sorokat dobott el"
            )
        matched = df.height - df["r_mkt"].null_count()
        logger.info("h=%-6d rows=%-7d matched=%-7d (%.1f%%)",
                    horizon, df.height, matched, 100 * matched / max(df.height, 1))
        frames.append(df)

    out = pl.concat(frames).with_columns(
        pl.lit(now).cast(pl.Datetime("us")).alias("computed_at_utc")
    )
    return out


def _f(value: object) -> float:
    """Coerce a Polars aggregate to float, treating null as 0.0."""
    return float(value) if isinstance(value, (int, float)) else 0.0


def summarise(df: pl.DataFrame) -> None:
    """Print a per-horizon summary of the computed returns."""
    print("\n=== FORWARD RETURN / DRIFT KORREKCIO ===")
    header = (f"{'h':>6s} {'n':>7s} {'match':>7s} {'drift':>9s} "
              f"{'long nyers':>11s} {'short nyers':>12s} "
              f"{'long korr':>10s} {'short korr':>11s} {'|z| atlag':>10s}")
    print(header)
    print("-" * len(header))
    for horizon in HORIZONS_MIN:
        part = df.filter(
            (pl.col("horizon_min") == horizon) & pl.col("r_mkt").is_not_null()
        )
        if not part.height:
            continue
        longs = part.filter(pl.col("r_raw") == pl.col("r_mkt"))
        shorts = part.filter(pl.col("r_raw") != pl.col("r_mkt"))
        total = df.filter(pl.col("horizon_min") == horizon).height
        print(f"{horizon:>6d} {total:>7,d} {part.height:>7,d} "
              f"{_f(part['drift_bucket'].mean()) * 100:>8.3f}% "
              f"{_f(longs['r_raw'].mean()) * 100:>10.3f}% "
              f"{_f(shorts['r_raw'].mean()) * 100:>11.3f}% "
              f"{_f(longs['r_excess'].mean()) * 100:>9.3f}% "
              f"{_f(shorts['r_excess'].mean()) * 100:>10.3f}% "
              f"{_f(part['z'].abs().mean()):>10.3f}")


# %% CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute and report, but do not write the database.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    price_db = utils.load_asset_config(PRICE_ASSET_ID)["database"]["db_path"]
    signal_db = os.path.join(utils._repo_root(), DB_SUBPATH)

    conn = duckdb.connect(price_db, read_only=True)
    try:
        conn.execute(f"ATTACH '{signal_db}' AS sig (READ_ONLY)")
        df = compute(conn)
    finally:
        conn.close()

    summarise(df)

    if args.dry_run:
        logger.info("dry-run: nincs iras")
        return 0

    out = get_connection(signal_db)
    try:
        ensure_tables(out)
        upsert_call_returns(out, df)
        total = out.execute("SELECT COUNT(*) FROM call_returns").fetchone()
        logger.info("call_returns: %d sor  (%s)", total[0] if total else 0, signal_db)
    finally:
        out.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
