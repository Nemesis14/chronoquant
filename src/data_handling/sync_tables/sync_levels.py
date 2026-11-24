#!/usr/bin/env python3
"""Sync the feat_levels table: S/R level + trendline features (epic_071 / t912).

Reads the full 1m ``ohlcv`` history, runs the S/R + trendline detector
(``_levels_polars``, frozen ``levels_detector`` config v1) and writes one row
per 1m bar into ``feat_levels`` (live main schema, PK open_time). The feature
definitions and the look-ahead / t-1 rule live in ``_levels_features``.

Idempotent upsert: rows of the requested range are written with
``INSERT OR REPLACE`` keyed on open_time, so a re-run with the same OHLCV
yields the same table. The detector state is path-dependent, so it always runs
on the full history up to the range end; the range only bounds the rows
written. Default range: from the last stored row to the last OHLCV row
(incremental); ``--full`` rewrites the whole history.

Not part of the champion feature set: ``feat_ohlcv_quant`` and the champion
``quant_train`` never read this table; only challenger profiles with
``extra_feature_tables: ["feat_levels"]`` join it (``03_build_quant_train.py
--features-profile solusdt_fw60_sr``).

Usage:
    uv run python -m data_handling.sync_tables.sync_levels --asset-id solusdt
    uv run python -m data_handling.sync_tables.sync_levels --asset-id solusdt --full
    (or: 02_sync_pipeline.py --skip-ohlcv --tables levels)
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta

import duckdb
import polars as pl

import utils
from data_handling.store.duckdb_query import latest_open_time, query_range_pl
from data_handling.store.duckdb_store import ensure_tables, get_connection
from data_handling.sync_tables._levels_features import compute_level_features

logger = logging.getLogger(__name__)

TABLE = "feat_levels"
_FMT = "%Y-%m-%d %H:%M:%S"


# %% Helpers


def _connect_with_retry(db_path: str, attempts: int = 24, wait_s: float = 5.0) -> duckdb.DuckDBPyConnection:
    """Open the live DB read-write, waiting out a transient lock (worker sync)."""
    for _ in range(attempts):
        try:
            return get_connection(db_path)
        except duckdb.IOException:
            time.sleep(wait_s)
    raise RuntimeError(
        "A live DuckDB tartosan zarolt -- fut a worker sync vagy egy masik iro folyamat?"
    )


def upsert_feat_levels(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """INSERT OR REPLACE feature rows into feat_levels (by column name).

    Args:
        conn : Open read-write DuckDB connection (ensure_tables already run).
        df   : Output of ``compute_level_features`` (open_time, available_ts, feat_*).

    Returns:
        Number of rows written.
    """
    if df.is_empty():
        return 0
    conn.register("_lv_batch", df)
    try:
        conn.execute(f"INSERT OR REPLACE INTO {TABLE} BY NAME SELECT * FROM _lv_batch")
    finally:
        conn.unregister("_lv_batch")
    return df.height


# %% Main


def sync_levels(
    asset_id   : str | None = None,
    start_time : str | None = None,
    end_time   : str | None = None,
    full       : bool = False,
    params     : dict | None = None,
) -> int:
    """Compute S/R level + trendline features and upsert them into feat_levels.

    Args:
        asset_id   : Asset key from config/assets.json; uses default if None.
        start_time : Inclusive lower bound of the rows written (UTC string). Default:
                     the row after the last stored one (everything if empty / ``full``).
        end_time   : Inclusive upper bound of the rows written / OHLCV read. Default: last OHLCV row.
        full       : Rewrite the whole history (ignores start_time).
        params     : Detector params override (tests only; default = frozen config v1).

    Returns:
        Number of feat_levels rows written.
    """
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]

    # --- schema (v6 migration creates feat_levels) ---
    conn = _connect_with_retry(db_path)
    try:
        ensure_tables(conn)
    finally:
        conn.close()

    # --- range of rows to write ---
    if full:
        start_time = None
    elif start_time is None:
        last = latest_open_time(db_path, TABLE)
        if last is not None:
            start_time = (last.to_pydatetime() + timedelta(minutes=1)).strftime(_FMT)

    # --- full OHLCV history up to end (detector state is path-dependent) ---
    t0 = time.monotonic()
    df = query_range_pl(db_path, "ohlcv", end=end_time,
                        columns=["open_time", "open", "high", "low", "close", "volume"])
    if df.is_empty():
        logger.warning("sync_levels: nincs OHLCV adat")
        return 0
    df = df.with_columns(pl.col(c).cast(pl.Float64)
                         for c in ("open", "high", "low", "close", "volume"))
    last_ohlcv = df["open_time"].max()
    if start_time is not None and isinstance(last_ohlcv, datetime) \
            and datetime.strptime(start_time, _FMT) > last_ohlcv:
        logger.info("sync_levels: nincs uj sor (start=%s > utolso ohlcv=%s)", start_time, last_ohlcv)
        return 0

    feats = compute_level_features(df, params=params, start=start_time, end=end_time)
    logger.info("sync_levels: %d sor szamolva (%.1fs), start=%s end=%s",
                feats.height, time.monotonic() - t0, start_time, end_time)

    # --- write (short read-write window) ---
    conn = _connect_with_retry(db_path)
    try:
        n = upsert_feat_levels(conn, feats)
    finally:
        conn.close()
    logger.info("sync_levels: %d sor irva a %s tablaba", n, TABLE)
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="feat_levels sync (S/R + trendline features)")
    parser.add_argument("--asset-id", default=None,
                        help="Asset key from config/assets.json (default: default_asset_id)")
    parser.add_argument("--start", default=None, metavar="YYYY-MM-DD HH:MM:SS",
                        help="Inclusive lower bound of the rows written (default: incremental)")
    parser.add_argument("--end", default=None, metavar="YYYY-MM-DD HH:MM:SS",
                        help="Inclusive upper bound (default: last OHLCV row)")
    parser.add_argument("--full", action="store_true", help="Rewrite the whole history")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    t0 = time.monotonic()
    n = sync_levels(args.asset_id, args.start, args.end, full=args.full)
    print(f"OK: feat_levels {n:,} sor irva ({time.monotonic() - t0:.1f}s)")


if __name__ == "__main__":
    main()
