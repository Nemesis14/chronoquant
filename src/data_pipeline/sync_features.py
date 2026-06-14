"""Compute technical indicators (feat_ohlcv_quant) for feature engineering.

Reads raw OHLCV data from DuckDB as a Polars DataFrame, computes all indicators
natively in Polars, then writes feature rows back into DuckDB (feat_ohlcv_quant).
Idempotent by open_time — safe to re-run.

Target computation is handled separately by sync_targets.py.

Data flow
---------
DuckDB (ohlcv)  →  query_range_pl()  →  Polars DataFrame
                                      →  compute_features_polars() (LazyFrame pipeline)
                                      →  insert_feat_ohlcv_quant() (native Polars insert)
                                      →  DuckDB (feat_ohlcv_quant table)

t-1 live consistency guarantee
-------------------------------
All OHLCV-based features are shifted by one bar inside compute_features_polars so
that the feature value stored in row t uses only market data from bars <= t-1.
Deterministic time-index features (P2) are exempt.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import polars as pl

import utils
from data_pipeline._features_polars import compute_features_polars
from store.duckdb_query import (
    asof_join_predictions_features,
    dataset_columns,
    dataset_exists,
    query_range_pl,
)
from store.duckdb_store import ensure_tables, get_connection, insert_feat_ohlcv_quant

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


# %% Main


def sync_features(
    start_time    : str,
    lookback_bars : int = 2880,
    end_time      : str | None = None,
    asset_id      : str | None = None,
) -> None:
    """Fetch OHLCV, compute all configured indicators, and write to feat_ohlcv_quant.

    All computation runs in Polars.  DuckDB is used only for storage and retrieval.
    Target variables are NOT computed here — run sync_targets separately.

    Args:
        start_time    : Lower bound for written rows, UTC YYYY-MM-DD HH:MM:SS.
        lookback_bars : Extra minutes fetched before start_time for indicator warmup.
        end_time      : Optional upper bound for controlled rebuilds.
        asset_id      : Asset key from config/assets.json; uses default if None.
    """
    # --- load configuration ---
    db_cfg   = utils.load_asset_config(asset_id)
    feat_cfg = utils.load_features_config(asset_id=asset_id)
    db_path     = db_cfg["database"]["db_path"]
    cfg_feat    = feat_cfg["database"]["features"]

    # --- detect available activity columns in the OHLCV dataset ---
    activity_source_cols = ["quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]
    if dataset_exists(db_path, "ohlcv"):
        ohlcv_cols         = dataset_columns(db_path, "ohlcv")
        available_activity = [c for c in activity_source_cols if c in ohlcv_cols]
    else:
        available_activity = []

    base_cols   = ["open_time", "open", "high", "low", "close", "volume"]
    select_cols = base_cols + available_activity

    # --- fetch raw OHLCV as Polars DataFrame ---
    fetch_start = (
        datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=lookback_bars)
    ).strftime("%Y-%m-%d %H:%M:%S")

    df = query_range_pl(db_path, "ohlcv", start=fetch_start, end=end_time, columns=select_cols)

    if df.is_empty():
        logger.warning("Nincs OHLCV adat: fetch_start=%s", fetch_start)
        return

    # Cast numeric columns to Float64 (DuckDB may return mixed types for sparse data)
    numeric_cols = ["open", "high", "low", "close", "volume"] + available_activity
    df = df.with_columns([
        pl.col(c).cast(pl.Float64) for c in numeric_cols if c in df.columns
    ])

    # --- generate technical indicators (Polars LazyFrame pipeline) ---
    indicators  = cfg_feat["indicators"]
    feat_prefix = "feat_"

    df_with_feats = compute_features_polars(
        df_ohlcv           = df,
        indicators         = indicators,
        feat_prefix        = feat_prefix,
        available_activity = available_activity,
        targets_cfg        = [],
    )

    # --- filter to requested range ---
    start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    df_final = df_with_feats.filter(pl.col("open_time") >= start_dt)
    if end_time is not None:
        end_dt   = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        df_final = df_final.filter(pl.col("open_time") <= end_dt)

    if df_final.is_empty():
        logger.warning("Nincs uj feature sor az irashoz")
        return

    # --- add metadata columns, keep only feat_* and meta ---
    all_feat_cols = [c for c in df_final.columns if c.startswith(feat_prefix)]

    df_final = df_final.with_columns([
        pl.col("open_time").alias("available_ts"),
        pl.col("open_time").alias("lookback_end_ts"),
    ])

    cols_to_keep = (
        ["open_time", "close", "available_ts", "lookback_end_ts"]
        + all_feat_cols
    )
    df_final = df_final.select([c for c in cols_to_keep if c in df_final.columns])

    # --- write to DuckDB (native Polars insert, no pandas conversion) ---
    conn = get_connection(db_path)
    ensure_tables(conn)
    try:
        written = insert_feat_ohlcv_quant(conn, df_final)
    finally:
        conn.close()
    logger.info("OK: %d feature sor szamolva, %d uj sor irva", len(df_final), written)


def build_lag_snapshot(
    db_path      : str,
    feature_cols : list[str] | None = None,
    start        : str | None = None,
    end          : str | None = None,
) -> pd.DataFrame:
    """Build a modeling-ready feature snapshot using ASOF JOIN.

    Joins each prediction timestamp to the most recently available feature
    row (available_ts <= prediction_ts), ensuring temporal correctness.
    Output contains prediction_ts and lookback_end_ts as explicit columns.

    Args:
        db_path      : Absolute path to the asset .duckdb file.
        feature_cols : Feature columns to include. None = all feat_* columns.
        start        : Optional lower bound on prediction_ts.
        end          : Optional upper bound on prediction_ts.

    Returns:
        DataFrame with prediction_ts, lookback_end_ts, and feature columns.
    """
    return asof_join_predictions_features(
        db_path,
        feature_cols = feature_cols,
        start        = start,
        end          = end,
    )
