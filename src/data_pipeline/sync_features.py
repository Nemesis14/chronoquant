"""Compute technical indicators and target variables for feature engineering.

Reads raw OHLCV data from DuckDB, computes configured indicators and targets
via the Polars bulk path, then writes feature rows into the DuckDB features table.
Idempotent by open_time — safe to re-run.

t-1 live consistency guarantee
-------------------------------
All OHLCV-based features are shifted by one bar before being written so that
the feature value stored in row t uses only market data from bars ≤ t-1.
This matches the live prediction setting where the freshest observable bar
is t-1.  Deterministic time-index features (P2) are exempt because they are
derived from the bar timestamp alone, not from OHLCV data.
"""

import logging
from datetime import timedelta

import numpy as np
import pandas as pd

import utils
from data_pipeline._features_polars import compute_features_polars
from store.duckdb_query import (
    asof_join_predictions_features,
    dataset_columns,
    dataset_exists,
    query_range,
)
from store.duckdb_store import ensure_tables, get_connection, insert_features

logger = logging.getLogger(__name__)


# %% Constants

# P2 deterministic time-index features: computed from the bar timestamp only,
# not from OHLCV data.  These must NOT be shifted by the global t-1 lag because
# they already represent the correct value for the prediction timestamp t.
_T_MINUS_1_SKIP: frozenset[str] = frozenset({
    "feat_bars_into_session_norm",
    "feat_hour_sin",
    "feat_hour_cos",
    "feat_dayofweek_sin",
    "feat_dayofweek_cos",
    "feat_weekend",
    "feat_session_asia",
    "feat_session_europe",
    "feat_session_us",
})


# %% Helpers


def _apply_t_minus_1_lag(df: pd.DataFrame, prefix: str) -> None:
    """Shift all OHLCV-based features by one bar for t-1 live consistency.

    P2 deterministic time-index features in _T_MINUS_1_SKIP are not shifted
    because they already represent the prediction timestamp, not OHLCV data.
    """
    lag_cols = [
        col for col in df.columns
        if col.startswith(prefix) and col not in _T_MINUS_1_SKIP
    ]
    df[lag_cols] = df[lag_cols].shift(1)


def _clean_feature_values(df: pd.DataFrame, prefix: str) -> None:
    """Replace infinite feature values with NaN in-place."""
    feat_cols = [col for col in df.columns if col.startswith(prefix)]
    df[feat_cols] = df[feat_cols].replace([np.inf, -np.inf], np.nan)


# %% Main


def sync_features(
    start_time    : str,
    lookback_bars : int = 2880,
    end_time      : str | None = None,
    asset_id      : str | None = None,
) -> None:
    """Fetch OHLCV, compute all configured features, and write to DuckDB.

    Args:
        start_time    : Lower bound for written rows, UTC YYYY-MM-DD HH:MM:SS.
        lookback_bars : Extra minutes fetched before start_time for indicator warmup.
        end_time      : Optional upper bound for controlled rebuilds.
        asset_id      : Asset key from config/assets.json; uses default if None.
    """
    # --- load configuration ---
    db_cfg   = utils.load_asset_config(asset_id)
    feat_cfg = utils.load_features_config(asset_id=asset_id)
    data_dir    = db_cfg["database"]["data_dir"]
    cfg_feat    = feat_cfg["database"]["features"]
    targets_cfg = cfg_feat.get("targets", [])

    # --- detect available activity columns in the OHLCV dataset ---
    activity_source_cols = ["quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]
    if dataset_exists(data_dir, "ohlcv"):
        ohlcv_cols         = dataset_columns(data_dir, "ohlcv")
        available_activity = [c for c in activity_source_cols if c in ohlcv_cols]
    else:
        available_activity = []

    base_cols   = ["open_time", "open", "high", "low", "close", "volume"]
    select_cols = base_cols + available_activity

    # --- fetch raw OHLCV via DuckDB ---
    fetch_start = (
        pd.to_datetime(start_time) - timedelta(minutes=lookback_bars)
    ).strftime("%Y-%m-%d %H:%M:%S")

    df = query_range(data_dir, "ohlcv", start=fetch_start, end=end_time, columns=select_cols)

    if df.empty:
        logger.warning("Nincs OHLCV adat: fetch_start=%s", fetch_start)
        return

    df["open_time"] = pd.to_datetime(df["open_time"])
    df = df.drop_duplicates(subset=["open_time"], keep="last")
    for col in ["open", "high", "low", "close", "volume"] + available_activity:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.set_index("open_time", inplace=True)

    # --- compute target variables ---
    for target_cfg in targets_cfg:
        direction   = target_cfg["direction"]
        rolling_win = target_cfg["rolling_window"]
        percentile  = target_cfg["percentile"]
        target_col  = utils.target_name_from_config(target_cfg)

        if direction == "long":
            rolling_max    = df["close"].iloc[::-1].rolling(rolling_win, min_periods=1).max().iloc[::-1]
            ratio_long     = rolling_max / df["close"].replace(0, np.nan)
            threshold      = ratio_long.quantile(percentile)
            df[target_col] = (ratio_long >= threshold).astype(int)

        elif direction == "short":
            rolling_min    = df["close"].iloc[::-1].rolling(rolling_win, min_periods=1).min().iloc[::-1]
            ratio_short    = rolling_min / df["close"].replace(0, np.nan)
            threshold      = ratio_short.quantile(percentile)
            df[target_col] = (ratio_short <= threshold).astype(int)

        else:
            raise ValueError(f"Unknown target direction: {direction}")

        if len(df) >= rolling_win:
            df.loc[df.index[-rolling_win:], target_col] = np.nan

    # --- generate technical indicators (polars bulk path) ---
    indicators  = cfg_feat["indicators"]
    feat_prefix = "feat_"

    df_with_feats = compute_features_polars(
        df_ohlcv           = df,
        indicators         = indicators,
        feat_prefix        = feat_prefix,
        available_activity = available_activity,
        targets_cfg        = targets_cfg,
    )

    # --- filter to requested range and write ---
    df_reset: pd.DataFrame = df_with_feats  # type: ignore[assignment]
    start_dt = pd.to_datetime(start_time)
    df_reset = df_reset[df_reset["open_time"] >= start_dt].copy()  # type: ignore[assignment]
    if end_time is not None:
        end_dt   = pd.to_datetime(end_time)
        df_reset = df_reset[df_reset["open_time"] <= end_dt].copy()  # type: ignore[assignment]

    feat_cols    = [col for col in df_reset.columns if col.startswith(feat_prefix)]
    target_cols  = utils.target_columns_from_config(feat_cfg)

    df_reset["available_ts"]    = df_reset["open_time"]
    df_reset["lookback_end_ts"] = df_reset["open_time"]
    df_reset["dataset_split"]   = None
    df_reset["fold_id"]         = None

    cols_to_keep = ["open_time", "close", "available_ts", "lookback_end_ts", "dataset_split", "fold_id"] + target_cols + feat_cols
    df_final: pd.DataFrame = df_reset[[c for c in cols_to_keep if c in df_reset.columns]].copy()  # type: ignore[assignment]

    if df_final.empty:
        logger.warning("Nincs uj feature sor az irashoz")
        return

    conn = get_connection(data_dir)
    ensure_tables(conn)
    try:
        written = insert_features(conn, df_final)
    finally:
        conn.close()
    logger.info("OK: %d feature sor szamolva, %d uj sor irva", len(df_final), written)


def build_lag_snapshot(
    data_dir     : str,
    feature_cols : list[str] | None = None,
    start        : str | None = None,
    end          : str | None = None,
) -> "pd.DataFrame":
    """Build a modeling-ready feature snapshot using ASOF JOIN.

    Joins each prediction timestamp to the most recently available feature
    row (available_ts <= prediction_ts), ensuring temporal correctness.
    Output contains prediction_ts and lookback_end_ts as explicit columns.

    Args:
        data_dir     : Root data directory for the asset.
        feature_cols : Feature columns to include. None = all feat_* columns.
        start        : Optional lower bound on prediction_ts.
        end          : Optional upper bound on prediction_ts.

    Returns:
        DataFrame with prediction_ts, lookback_end_ts, and feature columns.
    """
    return asof_join_predictions_features(
        data_dir,
        feature_cols = feature_cols,
        start        = start,
        end          = end,
    )
