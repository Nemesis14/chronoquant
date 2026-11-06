# =============================================================================
# sync_full handler: OHLCV -> features -> predictions
# =============================================================================
# Purpose:
#  - Port of src/ui/sync.py::_run_database_sync_locked (same steps, same order)
#  - Every step opens its own DuckDB connection inside data_handling (blueprint
#    5.4: no connection is held between steps); stdout is captured by the job log
# =============================================================================

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

import utils
from data_handling.store.duckdb_query import (
    latest_open_time,
    ohlcv_latest_open_time,
    ohlcv_row_count,
)
from data_handling.sync_tables.sync_features import sync_features
from data_handling.sync_tables.sync_ohlcv import sync_ohlcv
from data_handling.sync_tables.sync_predictions import sync_predictions


def sync_result_payload(
    start_time: str, end_time: str | None, rows_before: int, rows_after: int
) -> dict[str, Any]:
    return {
        "start_time": start_time,
        "end_time": end_time,
        "ohlcv_rows_before": rows_before,
        "ohlcv_rows_after": rows_after,
        "inserted_ohlcv_rows": max(0, rows_after - rows_before),
    }


def run_sync_full(params: dict[str, Any], logger: logging.Logger) -> dict[str, Any]:
    asset_id: str | None = params.get("asset_id")
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]

    logger.info("Database sync started")
    logger.info("DB path: %s", db_path)

    rows_before = ohlcv_row_count(db_path)
    last_open_time = ohlcv_latest_open_time(db_path)
    start_time = (
        utils.next_open_time(last_open_time) if last_open_time else utils.INITIAL_SYNC_START
    )
    start_ms = utils.utc_str_to_ms(start_time)

    logger.info("OHLCV sync from Binance started at %s", start_time)
    sync_ohlcv(start_ms, asset_id=asset_id)

    rows_after = ohlcv_row_count(db_path)
    ohlcv_latest_ts = ohlcv_latest_open_time(db_path)
    logger.info(
        "OHLCV rows before=%s after=%s inserted=%s",
        rows_before, rows_after, rows_after - rows_before,
    )

    if not ohlcv_latest_ts or pd.to_datetime(ohlcv_latest_ts) < pd.to_datetime(start_time):
        logger.info("No new OHLCV interval found for feature and prediction sync")
        logger.info("Database sync complete")
        return sync_result_payload(start_time, ohlcv_latest_ts, rows_before, rows_after)

    logger.info("Feature sync started: %s -> %s", start_time, ohlcv_latest_ts)
    sync_features(start_time, end_time=ohlcv_latest_ts, asset_id=asset_id)

    pred_latest_ts = latest_open_time(db_path, "predictions")
    pred_start_time = (
        utils.next_open_time(str(pred_latest_ts)) if pred_latest_ts is not None else start_time
    )
    logger.info("Prediction sync started: %s -> %s", pred_start_time, ohlcv_latest_ts)
    sync_predictions(pred_start_time, end_time=ohlcv_latest_ts, asset_id=asset_id)

    logger.info("Database sync complete")
    return sync_result_payload(start_time, ohlcv_latest_ts, rows_before, rows_after)
