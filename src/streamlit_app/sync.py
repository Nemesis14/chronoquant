from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import dataclass
import io
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

import utils
from data_pipeline.sync_features import sync_features
from data_pipeline.sync_ohlcv import sync_ohlcv
from data_pipeline.sync_predictions import sync_predictions
from db.maintenance import update_prediction_signals
from db.table_ops import sqlite_connect, table_exists
from streamlit_app.dashboard_logging import get_dashboard_logger


INITIAL_SYNC_START = "2017-01-01 00:00:00"


@dataclass(frozen=True)
class SyncResult:
    start_time: str
    end_time: str | None
    ohlcv_rows_before: int
    ohlcv_rows_after: int

    @property
    def inserted_ohlcv_rows(self) -> int:
        return max(0, self.ohlcv_rows_after - self.ohlcv_rows_before)


class _LoggerWriter(io.StringIO):
    def __init__(self, logger: logging.Logger) -> None:
        super().__init__()
        self._logger = logger
        self._buffer = ""

    def write(self, value: str) -> int:
        self._buffer += value
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._log_line(line)
        return len(value)

    def flush(self) -> None:
        self._log_line(self._buffer)
        self._buffer = ""

    def _log_line(self, line: str) -> None:
        line = line.strip()
        if line:
            self._logger.info(line)


def run_database_sync(asset_id: str | None = None) -> SyncResult:
    logger = get_dashboard_logger()
    db_cfg = utils.load_asset_config(asset_id)["database"]
    db_path = db_cfg["db_path"]
    tables = db_cfg["tables"]
    table_ohlcv = tables["ohlcv"]
    table_pred = tables["predictions"]

    logger.info("Database sync started")
    logger.info("Active DB: %s", db_path)
    _prepare_database(db_path, logger)

    rows_before = _row_count(db_path, table_ohlcv)
    last_open_time = _latest_open_time(db_path, table_ohlcv)
    start_time = _next_open_time(last_open_time) if last_open_time else INITIAL_SYNC_START
    start_ms = _utc_str_to_ms(start_time)

    logger.info("OHLCV sync from Binance started at %s", start_time)
    _run_with_logged_stdout(sync_ohlcv, start_ms, asset_id=asset_id, logger=logger)

    rows_after = _row_count(db_path, table_ohlcv)
    latest_open_time = _latest_open_time(db_path, table_ohlcv)
    logger.info("OHLCV rows before=%s after=%s inserted=%s", rows_before, rows_after, rows_after - rows_before)

    if not latest_open_time or pd.to_datetime(latest_open_time) < pd.to_datetime(start_time):
        logger.info("No new OHLCV interval found for feature and prediction sync")
        logger.info("Database sync complete")
        return SyncResult(start_time, latest_open_time, rows_before, rows_after)

    logger.info("Feature sync started: %s -> %s", start_time, latest_open_time)
    _run_with_logged_stdout(
        sync_features,
        start_time,
        end_time=latest_open_time,
        asset_id=asset_id,
        logger=logger,
    )

    logger.info("Prediction sync started: %s -> %s", start_time, latest_open_time)
    _run_with_logged_stdout(
        sync_predictions,
        start_time,
        end_time=latest_open_time,
        asset_id=asset_id,
        logger=logger,
    )

    logger.info("Signal update started")
    _run_with_logged_stdout(update_prediction_signals, db_path, table_pred, asset_id=asset_id, logger=logger)

    logger.info("Database sync complete")
    return SyncResult(start_time, latest_open_time, rows_before, rows_after)


def _run_with_logged_stdout(func, *args, logger: logging.Logger, **kwargs) -> None:
    writer = _LoggerWriter(logger)
    with redirect_stdout(writer):
        func(*args, **kwargs)
    writer.flush()


def _latest_open_time(db_path: str, table_name: str) -> str | None:
    if not table_exists(db_path, table_name):
        return None

    with sqlite_connect(db_path) as conn:
        row = conn.execute(f"SELECT MAX(open_time) FROM {table_name}").fetchone()
    return row[0] if row and row[0] else None


def _row_count(db_path: str, table_name: str) -> int:
    if not table_exists(db_path, table_name):
        return 0

    with sqlite_connect(db_path) as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0] or 0) if row else 0


def _next_open_time(open_time: str) -> str:
    value = pd.to_datetime(open_time, errors="raise") + timedelta(minutes=1)
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _utc_str_to_ms(value: str) -> int:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def _prepare_database(db_path: str, logger: logging.Logger) -> None:
    with sqlite_connect(db_path) as conn:
        mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()
        conn.execute("PRAGMA synchronous=NORMAL")
    logger.info("SQLite journal mode: %s", mode[0] if mode else "unknown")
