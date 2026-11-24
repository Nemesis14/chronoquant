"""Materialize quant_train from feat_ohlcv_quant JOIN target.

quant_train is the canonical model-ready table: one row per open_time,
all feat_* columns from feat_ohlcv_quant, and the two fw60 target columns
(long_mfe_fw60, short_mfe_fw60) from target.

Materialization is a pure in-database SQL JOIN — no Polars computation.
Rows with NULL targets are excluded via INNER JOIN + explicit NULL filter.

Data flow
---------
feat_ohlcv_quant + target  →  SQL INNER JOIN on open_time  →  quant_train

Rebuild semantics
-----------------
Full rebuild  (default):  CREATE OR REPLACE TABLE — deterministic, always correct.
Range rebuild (optional): DELETE range + INSERT — for incremental pipeline use.

Feature profiles (epic_071 / t912)
----------------------------------
Champion profile (default, the asset's ``features_profile``)  ->  ``quant_train``
(unchanged). A challenger profile with ``extra_feature_tables`` (e.g.
``solusdt_fw60_sr`` = solusdt_fw60 + ``feat_levels``) is built into its own
table ``quant_train__<profile_id>`` (``utils.quant_train_table``): the same
rows as the champion join, plus the extra tables' feat_* columns (LEFT JOIN).
"""

from __future__ import annotations

import logging
from pathlib import Path

import utils
from data_handling.store.duckdb_store import get_connection, rebuild_quant_train

logger = logging.getLogger(__name__)


def sync_quant_train(
    asset_id         : str | None = None,
    start_time       : str | None = None,
    end_time         : str | None = None,
    features_profile : str | None = None,
) -> int:
    """Rebuild quant_train (or a challenger profile's table) in DuckDB.

    With no bounds: full rebuild (CREATE OR REPLACE).
    With bounds: range rebuild (DELETE + INSERT for the given open_time window).

    Args:
        asset_id         : Asset key from config/assets.json; uses default if None.
        start_time       : Optional range lower bound, inclusive (YYYY-MM-DD HH:MM:SS).
        end_time         : Optional range upper bound, inclusive (YYYY-MM-DD HH:MM:SS).
        features_profile : Feature profile; None = the asset's profile (champion,
                           -> ``quant_train``). A profile with extra feature
                           tables -> ``quant_train__<profile_id>``.

    Returns:
        Total row count in the output table after rebuild.
    """
    db_cfg  = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]
    extras  = utils.feature_profile_extra_tables(features_profile)
    table   = utils.quant_train_table(features_profile)

    conn = get_connection(db_path)
    conn.execute("SET memory_limit='10GB'")
    conn.execute(f"SET temp_directory='{Path(db_path).parent / 'tmp'}'")
    try:
        n = rebuild_quant_train(
            conn,
            start_time           = start_time,
            end_time             = end_time,
            extra_feature_tables = extras,
            table_name           = table,
        )
    finally:
        conn.close()

    logger.info("sync_quant_train: %d sor az eredmény táblában (%s)", n, table)
    return n
