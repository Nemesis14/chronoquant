#!/usr/bin/env python3
# =============================================================================
# Sync OHLCV klines from Binance into the asset database
# =============================================================================
# Purpose:
#  - Parse start date and asset identifier from CLI
#  - Convert start datetime to epoch milliseconds
#  - Delegate to the data pipeline sync_ohlcv
# =============================================================================

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_pipeline.sync_ohlcv import sync_ohlcv

INIT_START_DATE = "2017-01-01 00:00:00"


# =============================================================================
# parse_args() -> argparse.Namespace
# =============================================================================
# Purpose:
#  - Parse CLI arguments for initial OHLCV sync
# Parameters:
#  - --start: start datetime "YYYY-MM-DD HH:MM:SS" (UTC)
#  - --asset-id: asset config key from config/assets.json
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
        help    = "Asset ID from config/assets.json (e.g. solusdt_fw60)",
    )
    return parser.parse_args()


# =============================================================================
# main() -> None
# =============================================================================
# Purpose:
#  - Convert start string to epoch milliseconds and run OHLCV sync
# =============================================================================
def main() -> None:
    args     = parse_args()
    start_ms = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    sync_ohlcv(
        open_time_ms_from = start_ms,
        asset_id          = args.asset_id,
    )


if __name__ == "__main__":
    main()
