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
"""

from __future__ import annotations

import logging

from pathlib import Path

import utils
from database.store.duckdb_store import get_connection, rebuild_quant_train

logger = logging.getLogger(__name__)


def sync_quant_train(
    asset_id   : str | None = None,
    start_time : str | None = None,
    end_time   : str | None = None,
) -> int:
    """Rebuild quant_train from feat_ohlcv_quant + target in DuckDB.

    With no bounds: full rebuild (CREATE OR REPLACE).
    With bounds: range rebuild (DELETE + INSERT for the given open_time window).

    Args:
        asset_id   : Asset key from config/assets.json; uses default if None.
        start_time : Optional range lower bound, inclusive (YYYY-MM-DD HH:MM:SS).
        end_time   : Optional range upper bound, inclusive (YYYY-MM-DD HH:MM:SS).

    Returns:
        Total row count in quant_train after rebuild.
    """
    db_cfg  = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    conn = get_connection(db_path)
    conn.execute("SET memory_limit='10GB'")
    conn.execute(f"SET temp_directory='{Path(db_path).parent / 'tmp'}'")
    try:
        n = rebuild_quant_train(conn, start_time=start_time, end_time=end_time)
    finally:
        conn.close()

    logger.info("sync_quant_train: %d sor az eredmény táblában", n)
    return n
