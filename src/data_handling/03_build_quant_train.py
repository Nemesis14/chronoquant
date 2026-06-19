#!/usr/bin/env python3
"""Ad-hoc build script for the quant_train DuckDB table.

quant_train is NOT part of the live sync pipeline.  It is built on demand
before a training run to materialize the model-ready feature+target table
for a specific date range (or the full history).

Data flow
---------
feat_ohlcv_quant + target  →  SQL INNER JOIN on open_time  →  quant_train

Rebuild semantics
-----------------
No --start / --end : full rebuild (CREATE OR REPLACE TABLE).
With --start / --end: range rebuild (DELETE + INSERT for the window).

Rows with NULL long_mfe_fw60 or short_mfe_fw60 are excluded.

Usage examples
--------------
    # Full rebuild (all available data)
    uv run python src/database/03_build_quant_train.py

    # Range rebuild: one calendar year
    uv run python src/database/03_build_quant_train.py \\
        --start "2024-01-01 00:00:00" \\
        --end   "2024-12-31 23:59:00"

    # Explicit asset
    uv run python src/database/03_build_quant_train.py --asset-id solusdt
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from data_handling.sync_tables.sync_quant_train import sync_quant_train

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
        handlers = [logging.StreamHandler(sys.stdout)],
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description     = "Build quant_train table from feat_ohlcv_quant + target.",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--asset-id",
        default = None,
        metavar = "ASSET_ID",
        help    = "Asset key from config/assets.json. Uses default_asset_id if omitted.",
    )
    parser.add_argument(
        "--start",
        default = None,
        metavar = "YYYY-MM-DD HH:MM:SS",
        help    = "Range lower bound, inclusive. Omit for full rebuild.",
    )
    parser.add_argument(
        "--end",
        default = None,
        metavar = "YYYY-MM-DD HH:MM:SS",
        help    = "Range upper bound, inclusive. Omit for full rebuild.",
    )
    return parser.parse_args()


def main() -> None:
    _setup_logging()
    args = _parse_args()

    mode = "full rebuild" if (args.start is None and args.end is None) else "range rebuild"
    logger.info(
        "quant_train build indul: asset_id=%s mode=%s start=%s end=%s",
        args.asset_id or "default", mode, args.start, args.end,
    )

    t0 = time.monotonic()
    n = sync_quant_train(
        asset_id   = args.asset_id,
        start_time = args.start,
        end_time   = args.end,
    )
    logger.info("quant_train build kesz: rows=%d elapsed=%.1fs", n, time.monotonic() - t0)


if __name__ == "__main__":
    main()
