"""Compute fw360 (6-hour, 360-bar) forward MFE targets.

Reads OHLCV from the DuckDB ohlcv table, computes forward window outcomes
for a 360-bar horizon using DuckDB SQL window functions (t+1..t+360,
bar t excluded), and writes the results to a persistent table or parquet file.

Unlike fw60 (which uses close-to-close), fw360 uses high/low to capture
the idealized directional excursion over a 6-hour forward window:
  long_mfe_fw360  = log(max(high[t+1:t+360]) / close[t])
  short_mfe_fw360 = log(min(low[t+1:t+360]) / close[t])

Pipeline
--------
sync_ohlcv  →  sync_targets (fw60)  →  01_target_computation (fw360)

Data flow
---------
DuckDB (ohlcv)  →  DuckDB SQL window functions (in-process)
                →  target DataFrame (fw360 MFE outcome columns)
                →  DuckDB target_fw360 table or Parquet output
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import polars as pl

import utils
from data_handling.store.duckdb_store import get_connection, ensure_tables

logger = logging.getLogger(__name__)

_HORIZON = 360  # 360 bars = 6 hours at 1-minute candles


# %% SQL helpers


_TARGET_SQL = """
WITH ohlcv_ordered AS (
    SELECT open_time, open, high, low, close
    FROM ohlcv
    ORDER BY open_time
),
forward_window AS (
    SELECT
        open_time,
        close,
        MAX(high) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING
        ) AS fw_max_high,
        MIN(low) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING
        ) AS fw_min_low,
        COUNT(*) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING
        ) AS fw_bar_count
    FROM ohlcv_ordered
)
SELECT
    open_time,
    close,
    CASE WHEN fw_bar_count >= {horizon} THEN fw_max_high    ELSE NULL END AS fw360_max_high,
    CASE WHEN fw_bar_count >= {horizon} THEN fw_min_low     ELSE NULL END AS fw360_min_low,
    CASE WHEN fw_bar_count >= {horizon} AND close > 0
         THEN fw_max_high / close - 1                        ELSE NULL END AS fw360_max_ratio_ret,
    CASE WHEN fw_bar_count >= {horizon} AND close > 0
         THEN LN(fw_max_high / close)                        ELSE NULL END AS long_mfe_fw360,
    CASE WHEN fw_bar_count >= {horizon} AND close > 0
         THEN LN(fw_min_low / close)                         ELSE NULL END AS short_mfe_fw360
FROM forward_window
ORDER BY open_time
"""


def _compute_outcome_df(
    conn    : duckdb.DuckDBPyConnection,
    horizon : int = _HORIZON,
) -> pl.DataFrame:
    """Compute all fw360 outcome columns using DuckDB window functions.

    Args:
        conn    : Open read-write DuckDB connection (ohlcv table must exist).
        horizon : Forward window in bars (minutes for 1m data).

    Returns:
        Polars DataFrame with open_time, close, and all fw360 outcome columns.
        Last `horizon` rows have NULL outcome values (insufficient future data).
    """
    sql = _TARGET_SQL.format(horizon=horizon)
    return conn.execute(sql).pl()


# %% Public API


def compute_fw360_targets(
    asset_id: str | None = None,
    output_format: str = "duckdb"
) -> str | pl.DataFrame:
    """Compute fw360 (360-bar forward MFE) targets from OHLCV.

    Computes fw360 forward outcome columns for a 360-bar horizon (t+1..t+360,
    bar t excluded). Last 360 rows have NULL values (insufficient future data).

    Args:
        asset_id      : Asset key from config/assets.json; uses default if None.
        output_format : "duckdb" (writes to table, returns path),
                       "parquet" (writes to file, returns path),
                       "dataframe" (returns Polars DataFrame).

    Returns:
        Path to output (DuckDB or Parquet) or DataFrame if output_format="dataframe".
    """
    db_cfg  = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    conn = get_connection(db_path)
    ensure_tables(conn)

    try:
        ohlcv_count = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()
        if not ohlcv_count or ohlcv_count[0] == 0:
            logger.warning("Nincs OHLCV adat — fw360 target szamitas kihagyva")
            return None

        stats_row = conn.execute(
            "SELECT MIN(open_time), MAX(open_time) FROM ohlcv"
        ).fetchone()
        computed_from = str(stats_row[0]) if stats_row and stats_row[0] else None
        computed_to   = str(stats_row[1]) if stats_row and stats_row[1] else None

        logger.info(
            "fw360 outcome szamitasa: horizon=%d, ohlcv_rows=%d",
            _HORIZON, ohlcv_count[0]
        )
        df = _compute_outcome_df(conn)
        if df.is_empty():
            logger.warning("Ures eredmeny: fw360 outcome szamitas")
            return None

        logger.info("fw360 target szamitva: rows=%d", len(df))

        # Output handling
        if output_format == "dataframe":
            return df

        elif output_format == "duckdb":
            # Write to DuckDB table: target_fw360
            table_name = "target_fw360"
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            logger.info("fw360 targets DuckDB tablaba irva: %s", table_name)
            return db_path

        elif output_format == "parquet":
            # Write to Parquet file in artifacts/
            artifacts_dir = Path("artifacts") / "fw360_targets"
            artifacts_dir.mkdir(parents=True, exist_ok=True)

            asset_name = asset_id or "solusdt"
            output_path = artifacts_dir / f"{asset_name}_fw360_targets.parquet"
            df.write_parquet(str(output_path))
            logger.info("fw360 targets Parquetbe irva: %s", output_path)
            return str(output_path)

        else:
            raise ValueError(f"Unknown output_format: {output_format}")

    finally:
        conn.close()


def validate_fw360_distribution(
    asset_id: str | None = None,
) -> dict:
    """Validate fw360 targets against fw60 for consistency and quality.

    Computes summary statistics for both fw60 and fw360 MFE targets:
      - NULL ratio (should be ~6x higher for fw360 due to longer horizon)
      - Mean, std, min, max, percentiles
      - Correlation between fw60 and fw360 (where both are non-NULL)

    Args:
        asset_id : Asset key; uses default if None.

    Returns:
        Dictionary with validation statistics.
    """
    db_cfg  = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    conn = get_connection(db_path)

    try:
        # First compute fw360 if not yet done
        df_fw360 = _compute_outcome_df(conn)

        # Read fw60 from existing target table
        query_fw60 = """
        SELECT open_time, long_mfe_fw60, short_mfe_fw60
        FROM target
        WHERE long_mfe_fw60 IS NOT NULL AND short_mfe_fw60 IS NOT NULL
        """
        df_fw60 = conn.execute(query_fw60).pl()

        # Join on open_time to compute correlation
        validation = {
            "fw360_rows": len(df_fw360),
            "fw360_long_null_ratio": 1.0 - (df_fw360["long_mfe_fw360"].null_count() / len(df_fw360)),
            "fw360_short_null_ratio": 1.0 - (df_fw360["short_mfe_fw360"].null_count() / len(df_fw360)),
            "fw60_rows": len(df_fw60),
            "fw60_long_null_ratio": 1.0 - (df_fw60["long_mfe_fw60"].null_count() / len(df_fw60)),
            "fw60_short_null_ratio": 1.0 - (df_fw60["short_mfe_fw60"].null_count() / len(df_fw60)),
        }

        # Summary stats (non-NULL rows)
        for col in ["long_mfe_fw360", "short_mfe_fw360"]:
            valid = df_fw360[col].drop_nulls()
            if len(valid) > 0:
                validation[f"{col}_mean"] = float(valid.mean())
                validation[f"{col}_std"] = float(valid.std())
                validation[f"{col}_min"] = float(valid.min())
                validation[f"{col}_max"] = float(valid.max())
                validation[f"{col}_p05"] = float(valid.quantile(0.05))
                validation[f"{col}_p25"] = float(valid.quantile(0.25))
                validation[f"{col}_p50"] = float(valid.quantile(0.50))
                validation[f"{col}_p75"] = float(valid.quantile(0.75))
                validation[f"{col}_p95"] = float(valid.quantile(0.95))

        logger.info("fw360 validacio: %s", validation)
        return validation

    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Example usage
    logger.info("fw360 target szamitas...")
    result = compute_fw360_targets(output_format="duckdb")
    logger.info("Eredmeny: %s", result)

    logger.info("Validacio...")
    validation = validate_fw360_distribution()
    logger.info("Validacio eredmenye: %s", validation)
