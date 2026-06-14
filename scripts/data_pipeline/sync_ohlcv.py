#!/usr/bin/env python3
# =============================================================================
# Sync OHLCV klines from Binance into the asset database
# =============================================================================
# Purpose:
#  - Parse start date and asset identifier from CLI
#  - Set up console + file logging (database/<asset>/logs/)
#  - Convert start datetime to epoch milliseconds
#  - Delegate to the data pipeline sync_ohlcv
# =============================================================================

import argparse
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import utils
from data_pipeline.sync_ohlcv import sync_ohlcv

INIT_START_DATE = "2017-01-01 00:00:00"

logger = logging.getLogger(__name__)


# =============================================================================
# _setup_logging(asset_id) -> None
# =============================================================================
def _setup_logging(asset_id: str | None) -> None:
    """Configure console + rotating file handler.

    Log file is written to database/<asset_id>/logs/sync_ohlcv_<timestamp>.log.

    Args:
        asset_id: Asset key from config/assets.json; uses default if None.
    """
    fmt     = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler()]

    try:
        db_cfg  = utils.load_asset_config(asset_id)
        db_path = db_cfg["database"]["db_path"]
        log_dir = Path(db_path).parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        ts       = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"sync_ohlcv_{ts}.log"
        handlers.append(logging.FileHandler(str(log_file), encoding="utf-8"))
        print(f"INFO: Log file: {log_file}")
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: File logging not set up: {exc}")

    logging.basicConfig(
        level    = logging.INFO,
        format   = fmt,
        datefmt  = datefmt,
        handlers = handlers,
    )


# =============================================================================
# parse_args() -> argparse.Namespace
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync OHLCV klines from Binance into the asset database.",
    )
    parser.add_argument(
        "--start",
        default = INIT_START_DATE,
        help    = "Start time, format: YYYY-MM-DD HH:MM:SS (UTC)",
    )
    parser.add_argument(
        "--asset-id",
        default = None,
        help    = "Asset ID from config/assets.json (e.g. solusdt)",
    )
    return parser.parse_args()


# =============================================================================
# main() -> None
# =============================================================================
def main() -> None:
    args = parse_args()
    _setup_logging(args.asset_id)

    start_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)

    logger.info("sync_ohlcv indul: asset_id=%s start=%s", args.asset_id or "default", args.start)
    t0 = time.monotonic()

    sync_ohlcv(
        open_time_ms_from = start_ms,
        asset_id          = args.asset_id,
    )

    elapsed = time.monotonic() - t0
    logger.info("sync_ohlcv kesz: elapsed=%.1fs", elapsed)


if __name__ == "__main__":
    main()
