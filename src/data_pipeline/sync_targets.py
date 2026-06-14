"""Compute and persist binary target labels for all configured targets.

Reads OHLCV from the DuckDB ohlcv table, computes forward return windows using
native DuckDB SQL window functions (t+1..t+k, bar t excluded), derives quantile
thresholds from the full history, and writes the target table.

After writing, updates the per-database metadata JSON with the computed thresholds
and computation range for audit purposes.

Pipeline position
-----------------
sync_ohlcv  →  sync_targets  →  sync_feat_ohlcv_quant  →  sync_predictions

Data flow
---------
DuckDB (ohlcv)  →  DuckDB SQL window functions (in-process)
                →  target DataFrame
                →  DuckDB (target table)
                →  database/solusdt/solusdt.json (threshold audit)
"""

from __future__ import annotations

import logging

import duckdb
import polars as pl

import utils
from store.duckdb_store import ensure_tables, get_connection, insert_target

logger = logging.getLogger(__name__)


# %% SQL helpers


_TARGET_SQL = """
WITH ohlcv_ordered AS (
    SELECT
        open_time,
        close
    FROM ohlcv
    ORDER BY open_time
),
forward_extrema AS (
    SELECT
        open_time,
        close,
        -- t+1..t+k: bar t is NOT included in the window
        MAX(close) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING
        ) AS future_max_close,
        MIN(close) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING
        ) AS future_min_close
    FROM ohlcv_ordered
),
returns AS (
    SELECT
        open_time,
        close,
        future_max_close / NULLIF(close, 0) - 1 AS future_max_return,
        future_min_close / NULLIF(close, 0) - 1 AS future_min_return
    FROM forward_extrema
)
SELECT
    open_time,
    close,
    future_max_return,
    future_min_return
FROM returns
ORDER BY open_time
"""


def _compute_target_df(
    conn    : duckdb.DuckDBPyConnection,
    horizon : int,
    targets : list[dict],
) -> pl.DataFrame:
    """Compute all target columns for the given horizon using DuckDB window functions.

    Args:
        conn    : Open read-write DuckDB connection (ohlcv table must exist).
        horizon : Forward window in bars (minutes for 1m data).
        targets : List of target config dicts with direction/quantile/name keys.

    Returns:
        Polars DataFrame with open_time, close, and one boolean column per target.
        Last `horizon` rows have NULL target values (insufficient future data).
    """
    sql = _TARGET_SQL.format(horizon=horizon)
    df  = conn.execute(sql).pl()

    if df.is_empty():
        return df

    # --- compute quantile thresholds on full non-null history ---
    long_returns  = df["future_max_return"].drop_nulls()
    short_returns = df["future_min_return"].drop_nulls()

    for target_cfg in targets:
        direction  = target_cfg["direction"]
        quantile   = target_cfg["quantile"]
        target_col = target_cfg["name"]

        if direction == "long":
            if long_returns.is_empty():
                logger.warning("No long return data available for threshold computation")
                df = df.with_columns(pl.lit(None).cast(pl.Boolean).alias(target_col))
                continue
            threshold = long_returns.quantile(quantile, interpolation="linear")
            label_col = pl.when(pl.col("future_max_return").is_not_null()).then(
                pl.col("future_max_return") >= threshold
            ).otherwise(None).alias(target_col)
        else:
            if short_returns.is_empty():
                logger.warning("No short return data available for threshold computation")
                df = df.with_columns(pl.lit(None).cast(pl.Boolean).alias(target_col))
                continue
            threshold = short_returns.quantile(quantile, interpolation="linear")
            label_col = pl.when(pl.col("future_min_return").is_not_null()).then(
                pl.col("future_min_return") <= threshold
            ).otherwise(None).alias(target_col)

        df = df.with_columns(label_col)
        target_cfg["_computed_threshold"] = float(threshold)  # type: ignore[assignment]
        logger.info(
            "target=%s direction=%s quantile=%.2f threshold=%.6f",
            target_col, direction, quantile, threshold,
        )

    return df


# %% Metadata update


def _update_metadata_thresholds(
    asset_id     : str | None,
    targets      : list[dict],
    computed_from: str | None,
    computed_to  : str | None,
) -> None:
    """Write computed thresholds and metadata audit fields to the per-DB JSON.

    Args:
        asset_id      : Asset key; uses default if None.
        targets       : Target config dicts with _computed_threshold injected.
        computed_from : Earliest OHLCV timestamp used in threshold computation.
        computed_to   : Latest OHLCV timestamp used in threshold computation.
    """
    computed_at = utils.now_utc_str()
    metadata    = utils.load_database_metadata(asset_id)

    if "targets" not in metadata:
        metadata["targets"] = {}

    for target_cfg in targets:
        name      = target_cfg["name"]
        threshold = target_cfg.get("_computed_threshold")
        if threshold is None:
            continue
        existing = metadata["targets"].get(name, {})
        existing.update({
            "direction":        target_cfg["direction"],
            "horizon":          target_cfg["horizon"],
            "quantile":         target_cfg["quantile"],
            "threshold":        threshold,
            "threshold_metric": "future_max_return" if target_cfg["direction"] == "long" else "future_min_return",
            "computed_from":    computed_from,
            "computed_to":      computed_to,
            "computed_at":      computed_at,
        })
        metadata["targets"][name] = existing

    utils.save_database_metadata(metadata, asset_id)
    logger.info("Metadata frissitve: %s", utils.load_asset_config(asset_id)["database"]["metadata_path"])


# %% Public API


def sync_targets(asset_id: str | None = None) -> None:
    """Rebuild the full target table from OHLCV using DuckDB window functions.

    Targets are defined in config/features.json under the active profile.
    The forward window uses t+1..t+k — bar t's close is NOT included.
    Quantile thresholds are computed from the full available OHLCV history.
    After writing, the computed thresholds are saved to database/<asset>/asset.json.

    Args:
        asset_id : Asset key from config/assets.json; uses default if None.
    """
    # --- load configuration ---
    db_cfg   = utils.load_asset_config(asset_id)
    feat_cfg = utils.load_features_config(asset_id=asset_id)
    db_path  = db_cfg["database"]["db_path"]

    raw_targets = feat_cfg["database"]["features"].get("targets", [])
    if not raw_targets:
        logger.warning("Nincs target konfig — sync_targets kihagyva")
        return

    # --- group targets by horizon (each horizon = one DuckDB window pass) ---
    horizons: dict[int, list[dict]] = {}
    for t in raw_targets:
        h = int(t["rolling_window"])
        name = utils.target_name_from_config(t)
        horizons.setdefault(h, []).append({
            "name":      name,
            "direction": t["direction"],
            "horizon":   h,
            "quantile":  float(t["percentile"]),
        })

    conn = get_connection(db_path)
    ensure_tables(conn)

    try:
        # --- check ohlcv exists ---
        ohlcv_count = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()
        if not ohlcv_count or ohlcv_count[0] == 0:
            logger.warning("Nincs OHLCV adat — sync_targets kihagyva")
            return

        # --- fetch time range for metadata ---
        stats_row = conn.execute(
            "SELECT MIN(open_time), MAX(open_time) FROM ohlcv"
        ).fetchone()
        computed_from = str(stats_row[0]) if stats_row and stats_row[0] else None
        computed_to   = str(stats_row[1]) if stats_row and stats_row[1] else None

        all_targets: list[dict] = []
        combined_df: pl.DataFrame | None = None

        # --- compute one DataFrame per horizon, join on open_time ---
        for horizon, horizon_targets in sorted(horizons.items()):
            logger.info("Targetek szamitasa: horizon=%d bars, %d target", horizon, len(horizon_targets))
            df = _compute_target_df(conn, horizon, horizon_targets)
            if df.is_empty():
                logger.warning("Ures eredmeny: horizon=%d", horizon)
                continue

            target_cols = [t["name"] for t in horizon_targets]
            df_subset   = df.select(["open_time", "close"] + target_cols)

            if combined_df is None:
                combined_df = df_subset
            else:
                combined_df = combined_df.join(
                    df_subset.drop("close"), on="open_time", how="outer"
                )

            all_targets.extend(horizon_targets)

        if combined_df is None or combined_df.is_empty():
            logger.warning("Nem szamolhato target adat")
            return

        # --- write target table (full replace within range) ---
        written = insert_target(conn, combined_df)
        logger.info(
            "OK: target tabla frissitve, sorok=%d, uj=%d",
            len(combined_df), written,
        )

        # --- persist thresholds to metadata JSON ---
        _update_metadata_thresholds(asset_id, all_targets, computed_from, computed_to)

    finally:
        conn.close()
