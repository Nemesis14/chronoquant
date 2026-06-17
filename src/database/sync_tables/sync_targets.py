"""Compute and persist fw60 forward outcome columns for the target table.

Reads OHLCV from the DuckDB ohlcv table, computes all forward window
outcomes for a 60-bar horizon using DuckDB SQL window functions (t+1..t+60,
bar t excluded), and writes the result to the target table.

After writing, updates the per-database metadata JSON with outcome column
definitions and the computation range.

Pipeline position
-----------------
sync_ohlcv  →  sync_targets  →  sync_feat_ohlcv_quant  →  sync_predictions

Data flow
---------
DuckDB (ohlcv)  →  DuckDB SQL window functions (in-process)
                →  target DataFrame (10 fw60 outcome columns)
                →  DuckDB (target table)
                →  database/solusdt/solusdt.json (outcome audit)
"""

from __future__ import annotations

import logging

import duckdb
import polars as pl

import utils
from database.store.duckdb_store import ensure_tables, get_connection, insert_target

logger = logging.getLogger(__name__)

_HORIZON = 60


# %% SQL helpers


_TARGET_SQL = """
WITH ohlcv_ordered AS (
    SELECT open_time, close
    FROM ohlcv
    ORDER BY open_time
),
forward_window AS (
    SELECT
        open_time,
        close,
        LEAD(close, {horizon}) OVER (ORDER BY open_time) AS fw_close,
        MAX(close) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING
        ) AS fw_max,
        MIN(close) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING
        ) AS fw_min,
        COUNT(*) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING
        ) AS fw_bar_count
    FROM ohlcv_ordered
)
SELECT
    open_time,
    close,
    CASE WHEN fw_bar_count >= {horizon} THEN fw_close           ELSE NULL END AS fw60_close,
    CASE WHEN fw_bar_count >= {horizon} THEN fw_max             ELSE NULL END AS fw60_max,
    CASE WHEN fw_bar_count >= {horizon} THEN fw_min             ELSE NULL END AS fw60_min,
    CASE WHEN fw_bar_count >= {horizon} AND close > 0
         THEN fw_close / close - 1                              ELSE NULL END AS fw60_close_ret,
    CASE WHEN fw_bar_count >= {horizon} AND close > 0
         THEN LN(fw_close / close)                              ELSE NULL END AS fw60_close_logret,
    CASE WHEN fw_bar_count >= {horizon} AND close > 0
         THEN fw_max / close                                    ELSE NULL END AS fw60_max_ratio,
    CASE WHEN fw_bar_count >= {horizon} AND close > 0
         THEN fw_min / close                                    ELSE NULL END AS fw60_min_ratio,
    CASE WHEN fw_bar_count >= {horizon} AND close > 0
         THEN LN(fw_max / close)                               ELSE NULL END AS long_mfe_fw60,
    CASE WHEN fw_bar_count >= {horizon} AND close > 0
         THEN LN(fw_min / close)                               ELSE NULL END AS short_mfe_fw60
FROM forward_window
ORDER BY open_time
"""


def _compute_outcome_df(
    conn    : duckdb.DuckDBPyConnection,
    horizon : int = _HORIZON,
) -> pl.DataFrame:
    """Compute all fw60 outcome columns using DuckDB window functions.

    Args:
        conn    : Open read-write DuckDB connection (ohlcv table must exist).
        horizon : Forward window in bars (minutes for 1m data).

    Returns:
        Polars DataFrame with open_time, close, and all fw60 outcome columns.
        Last `horizon` rows have NULL outcome values (insufficient future data).
    """
    sql = _TARGET_SQL.format(horizon=horizon)
    return conn.execute(sql).pl()


# %% Metadata update


def _update_metadata_outcomes(
    asset_id     : str | None,
    computed_from: str | None,
    computed_to  : str | None,
    horizon      : int = _HORIZON,
) -> None:
    """Write fw60 outcome column definitions and range to the per-DB JSON.

    Args:
        asset_id      : Asset key; uses default if None.
        computed_from : Earliest OHLCV timestamp used in computation.
        computed_to   : Latest OHLCV timestamp used in computation.
        horizon       : Forward window in bars.
    """
    computed_at = utils.now_utc_str()
    metadata    = utils.load_database_metadata(asset_id)

    metadata.pop("targets", None)

    h = horizon
    metadata["target_outcomes"] = {
        f"fw{h}": {
            "horizon"     : h,
            "window"      : f"t+1..t+{h}",
            "columns"     : {
                "close"             : "close[t] — reference bar close",
                f"fw{h}_close"      : f"close[t+{h}] — raw forward close",
                f"fw{h}_max"        : f"max(close[t+1:t+{h}]) — raw max price",
                f"fw{h}_min"        : f"min(close[t+1:t+{h}]) — raw min price",
                f"fw{h}_close_ret"  : f"close[t+{h}] / close[t] - 1",
                f"fw{h}_close_logret": f"log(close[t+{h}] / close[t])",
                f"fw{h}_max_ratio"  : f"max(close[t+1:t+{h}]) / close[t]",
                f"fw{h}_min_ratio"  : f"min(close[t+1:t+{h}]) / close[t]",
                f"long_mfe_fw{h}"   : f"log(max(close[t+1:t+{h}]) / close[t]) — LONG TARGET",
                f"short_mfe_fw{h}"  : f"log(min(close[t+1:t+{h}]) / close[t]) — SHORT TARGET",
            },
            "null_tail_rows": h,
            "computed_from" : computed_from,
            "computed_to"   : computed_to,
            "computed_at"   : computed_at,
        }
    }

    utils.save_database_metadata(metadata, asset_id)
    logger.info("Metadata frissitve: %s", utils.load_asset_config(asset_id)["database"]["metadata_path"])


# %% Public API


def sync_targets(asset_id: str | None = None) -> None:
    """Rebuild the full target table from OHLCV using DuckDB window functions.

    Computes fw60 forward outcome columns for a 60-bar horizon (t+1..t+60,
    bar t excluded).  Last 60 rows have NULL values (insufficient future data).
    After writing, the computed time range is saved to the per-asset metadata JSON.

    Args:
        asset_id : Asset key from config/assets.json; uses default if None.
    """
    db_cfg  = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    conn = get_connection(db_path)
    ensure_tables(conn)

    try:
        ohlcv_count = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()
        if not ohlcv_count or ohlcv_count[0] == 0:
            logger.warning("Nincs OHLCV adat — sync_targets kihagyva")
            return

        stats_row = conn.execute(
            "SELECT MIN(open_time), MAX(open_time) FROM ohlcv"
        ).fetchone()
        computed_from = str(stats_row[0]) if stats_row and stats_row[0] else None
        computed_to   = str(stats_row[1]) if stats_row and stats_row[1] else None

        logger.info("fw60 outcome szamitasa: horizon=%d, ohlcv_rows=%d", _HORIZON, ohlcv_count[0])
        df = _compute_outcome_df(conn)
        if df.is_empty():
            logger.warning("Ures eredmeny: fw60 outcome szamitas")
            return

        written = insert_target(conn, df)
        logger.info(
            "OK: target tabla frissitve, sorok=%d, uj=%d",
            len(df), written,
        )

        _update_metadata_outcomes(asset_id, computed_from, computed_to)

    finally:
        conn.close()
