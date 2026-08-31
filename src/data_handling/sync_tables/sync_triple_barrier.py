#!/usr/bin/env python3
"""Populate the triple-barrier label column on the target table (epic_058 / t701).

Reads OHLCV, computes the label, and writes it back as an extra column on
`target`. The column joins into `quant_train` automatically -- the target
column discovery there matches on `_mfe_fw` or `tb_label`, so no further wiring
is needed.

Usage:
    python -m data_handling.sync_tables.sync_triple_barrier
    python -m data_handling.sync_tables.sync_triple_barrier --barrier-bps 150
    python -m data_handling.sync_tables.sync_triple_barrier --report
    python -m data_handling.sync_tables.sync_triple_barrier --vol-scaled
"""

import argparse
import contextlib
import logging
import sys
import time

import duckdb
import numpy as np
import polars as pl

import utils
from data_handling.store.duckdb_store import get_connection
from data_handling.sync_tables._triple_barrier import (
    DEFAULT_BARRIER_BPS,
    DEFAULT_HORIZON,
    DEFAULT_VOL_SPAN,
    class_distribution,
    horizon_barrier_bps,
    label_column_name,
    normalising_k,
    triple_barrier_labels,
)

logger = logging.getLogger(__name__)


def _connect_with_retry(db_path: str, attempts: int = 12) -> duckdb.DuckDBPyConnection:
    """Open the live DB, waiting out a transient lock from the dashboard."""
    for _ in range(attempts):
        try:
            return get_connection(db_path)
        except duckdb.IOException:
            time.sleep(5)
    raise RuntimeError(
        "A live DuckDB tartosan zarolt -- fut meg a Streamlit dashboard?"
    )


def sync_triple_barrier(
    asset_id: str | None = None,
    horizon: int = DEFAULT_HORIZON,
    barrier_bps: float = DEFAULT_BARRIER_BPS,
) -> dict[str, float]:
    """Compute and persist the triple-barrier label. Returns the class split."""
    col = label_column_name(horizon, barrier_bps)
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = _connect_with_retry(db_path)
    try:
        bars = conn.execute(
            "SELECT open_time, close, high, low FROM ohlcv ORDER BY open_time"
        ).pl()
        logger.info("ohlcv beolvasva: %d sor", bars.height)

        labels = triple_barrier_labels(
            bars["close"].to_numpy().astype("float64"),
            bars["high"].to_numpy().astype("float64"),
            bars["low"].to_numpy().astype("float64"),
            horizon=horizon,
            up_bps=barrier_bps,
            dn_bps=barrier_bps,
        )
        dist = class_distribution(labels)
        logger.info("cimke eloszlas: %s", dist)

        frame = pl.DataFrame({"open_time": bars["open_time"], col: labels})
        conn.register("tb_incoming", frame.to_arrow())
        # Already present on a re-run; this pass refreshes the values.
        with contextlib.suppress(duckdb.CatalogException):
            conn.execute(f"ALTER TABLE target ADD COLUMN {col} TINYINT")
        conn.execute(
            f'UPDATE target SET {col} = CAST(i."{col}" AS TINYINT) '
            f"FROM tb_incoming i WHERE target.open_time = i.open_time"
        )
        conn.unregister("tb_incoming")
        return dist
    finally:
        conn.close()


def sync_vol_scaled(
    asset_id: str | None = None,
    horizon: int = DEFAULT_HORIZON,
    target_median_bps: float = DEFAULT_BARRIER_BPS,
    span: int = DEFAULT_VOL_SPAN,
) -> dict[str, float]:
    """Volatility-scaled variant of the label (epic_060 / t811).

    Writes two columns rather than one: the label, and the barrier width that
    produced it. The width is not diagnostics -- the walk-forward has to price
    each trade at its own barrier, because a win is worth what the barrier was
    when the trade opened, while the fee stays fixed.

    Both names start with ``tb_label_`` so the quant_train target discovery
    picks them up without further wiring.
    """
    col = f"tb_label_{horizon}_vol"
    bps_col = f"{col}_bps"
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = _connect_with_retry(db_path)
    try:
        bars = conn.execute(
            "SELECT open_time, close, high, low FROM ohlcv ORDER BY open_time"
        ).pl()
        logger.info("ohlcv beolvasva: %d sor", bars.height)

        close = bars["close"].to_numpy().astype("float64")
        k = normalising_k(close, target_median_bps, horizon, span)
        width = horizon_barrier_bps(close, horizon=horizon, k=k, span=span)
        logger.info(
            "k=%.4f -> korlat mediana %.1f bps (p10 %.1f / p90 %.1f), "
            "also korlaton %.1f%%, felso korlaton %.1f%%",
            k, float(np.nanmedian(width)),
            float(np.nanpercentile(width, 10)), float(np.nanpercentile(width, 90)),
            float(np.nanmean(width <= 60.0) * 100),
            float(np.nanmean(width >= 400.0) * 100),
        )

        labels = triple_barrier_labels(
            close,
            bars["high"].to_numpy().astype("float64"),
            bars["low"].to_numpy().astype("float64"),
            horizon=horizon,
            up_bps=width,
            dn_bps=width,
        )
        dist = class_distribution(labels)
        logger.info("cimke eloszlas: %s", dist)

        # NaN marks "no usable barrier here" -- the EWM warm-up at the start and
        # the incomplete window at the end. It has to reach DuckDB as NULL, not
        # as a float NaN, or the TINYINT cast fails on it.
        frame = pl.DataFrame(
            {"open_time": bars["open_time"], col: labels, bps_col: width}
        ).with_columns(pl.col(col).fill_nan(None), pl.col(bps_col).fill_nan(None))
        conn.register("tb_incoming", frame.to_arrow())
        with contextlib.suppress(duckdb.CatalogException):
            conn.execute(f"ALTER TABLE target ADD COLUMN {col} TINYINT")
        with contextlib.suppress(duckdb.CatalogException):
            conn.execute(f"ALTER TABLE target ADD COLUMN {bps_col} REAL")
        conn.execute(
            f'UPDATE target SET {col} = CAST(i."{col}" AS TINYINT), '
            f'{bps_col} = CAST(i."{bps_col}" AS REAL) '
            f"FROM tb_incoming i WHERE target.open_time = i.open_time"
        )
        conn.unregister("tb_incoming")
        return dist
    finally:
        conn.close()


def report(
    asset_id: str | None = None,
    horizon: int = DEFAULT_HORIZON,
    barrier_bps: float = DEFAULT_BARRIER_BPS,
) -> None:
    """Print the class split overall and per year, plus the NULL tail check."""
    col = label_column_name(horizon, barrier_bps)
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = _connect_with_retry(db_path)
    try:
        rows = conn.execute(
            f"""
            SELECT strftime(open_time, '%Y') AS ev, count(*) AS n,
                   round(100.0 * avg(CASE WHEN {col} =  1 THEN 1.0 ELSE 0 END), 1) AS up,
                   round(100.0 * avg(CASE WHEN {col} = -1 THEN 1.0 ELSE 0 END), 1) AS dn,
                   round(100.0 * avg(CASE WHEN {col} =  0 THEN 1.0 ELSE 0 END), 1) AS flat
            FROM target WHERE {col} IS NOT NULL GROUP BY 1 ORDER BY 1
            """
        ).fetchall()
        print(f"{'ev':>6}{'n':>10}{'+1 (up)':>10}{'-1 (dn)':>10}{'0 (ido)':>10}")
        for r in rows:
            print(f"{r[0]:>6}{r[1]:>10,}{r[2]:>9.1f}%{r[3]:>9.1f}%{r[4]:>9.1f}%")

        nulls = conn.execute(
            f"SELECT count(*) FROM target WHERE {col} IS NULL"
        ).fetchone()
        print(f"\n  NULL sorok: {nulls[0] if nulls else 0}  (elvart: {horizon} a tabla vegen)")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Triple-barrier label sync")
    parser.add_argument("--asset-id", default="solusdt")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON)
    parser.add_argument("--barrier-bps", type=float, default=DEFAULT_BARRIER_BPS)
    parser.add_argument("--report", action="store_true", help="Csak eloszlas riport")
    parser.add_argument("--vol-scaled", action="store_true",
                        help="Volatilitashoz skalazott korlat (epic_060). A k ugy "
                             "van valasztva, hogy a korlat medianja a --barrier-bps "
                             "erteket adja -- igy a fix valtozattal osszevetheto.")
    parser.add_argument("--vol-span", type=int, default=DEFAULT_VOL_SPAN)
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.report:
        report(args.asset_id, args.horizon, args.barrier_bps)
        return

    if args.vol_scaled:
        dist = sync_vol_scaled(
            args.asset_id, args.horizon, args.barrier_bps, args.vol_span
        )
        col = f"tb_label_{args.horizon}_vol"
    else:
        dist = sync_triple_barrier(args.asset_id, args.horizon, args.barrier_bps)
        col = label_column_name(args.horizon, args.barrier_bps)
    print(
        f"\n{col}: "
        f"+1={dist['+1']}%  -1={dist['-1']}%  0={dist['0']}%  (n={dist['n']:,})"
    )


if __name__ == "__main__":
    main()
