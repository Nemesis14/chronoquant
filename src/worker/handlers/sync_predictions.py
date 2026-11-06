# =============================================================================
# sync_predictions handler: prediction back-fill only
# =============================================================================
# Purpose:
#  - Fill predictions from the latest stored prediction up to the latest OHLCV bar
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
from data_handling.sync_tables.sync_predictions import sync_predictions
from worker.handlers.sync_full import sync_result_payload


def run_sync_predictions(params: dict[str, Any], logger: logging.Logger) -> dict[str, Any]:
    asset_id: str | None = params.get("asset_id")
    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]

    rows = ohlcv_row_count(db_path)
    ohlcv_latest_ts = ohlcv_latest_open_time(db_path)
    pred_latest_ts = latest_open_time(db_path, "predictions")
    if not ohlcv_latest_ts:
        logger.info("No OHLCV data - nothing to predict")
        return sync_result_payload("", None, rows, rows)

    start_time = (
        utils.next_open_time(str(pred_latest_ts))
        if pred_latest_ts is not None
        else utils.INITIAL_SYNC_START
    )
    if pd.to_datetime(ohlcv_latest_ts) < pd.to_datetime(start_time):
        logger.info("Predictions are up to date (latest %s)", pred_latest_ts)
        return sync_result_payload(start_time, ohlcv_latest_ts, rows, rows)

    logger.info("Prediction sync started: %s -> %s", start_time, ohlcv_latest_ts)
    sync_predictions(start_time, end_time=ohlcv_latest_ts, asset_id=asset_id)
    logger.info("Prediction sync complete")
    return sync_result_payload(start_time, ohlcv_latest_ts, rows, rows)
