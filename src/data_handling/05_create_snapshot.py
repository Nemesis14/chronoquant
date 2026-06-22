#!/usr/bin/env python3
"""Freeze a range of live.quant_train into an immutable snapshot table.

A snapshot is a content-hashed, immutable copy of a date range of the live
``quant_train`` table, materialized as ``snap."<snapshot_id>"`` in the asset's
lab database and registered in ``reg.snapshots``.  The modeling pipeline reads
the snapshot (not the mutable live table), which is what makes a model
reproducible.

Snapshot id naming (plan 6): ``{asset}_fw{h}_{range}__{hash8}``,
e.g. ``solusdt_fw60_2101_2605__a1b2c3d4``.

Re-running on identical content does NOT overwrite — the existing snapshot is
reused (immutable), detected via the content sha256.

Usage
-----
    # Full-history snapshot of the default asset
    uv run python src/data_handling/05_create_snapshot.py

    # Range snapshot (inclusive bounds)
    uv run python src/data_handling/05_create_snapshot.py \
        --start "2021-01-01 00:00:00" --end "2026-05-31 23:59:00"

    # Explicit asset / horizon
    uv run python src/data_handling/05_create_snapshot.py \
        --asset-id solusdt --horizon 60
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils
from data_handling.store.snapshots import create_snapshot

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
        description     = "Freeze a range of live.quant_train into an immutable snapshot.",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--asset-id",
        default = None,
        metavar = "ASSET_ID",
        help    = "Asset key from config/assets.json. Uses default_asset_id if omitted.",
    )
    parser.add_argument(
        "--horizon",
        type    = int,
        default = 60,
        metavar = "BARS",
        help    = "Forward-window bar count for the snapshot id (fw{horizon}).",
    )
    parser.add_argument(
        "--start",
        default = None,
        metavar = "YYYY-MM-DD HH:MM:SS",
        help    = "Range lower bound, inclusive. Omit for full history.",
    )
    parser.add_argument(
        "--end",
        default = None,
        metavar = "YYYY-MM-DD HH:MM:SS",
        help    = "Range upper bound, inclusive. Omit for full history.",
    )
    return parser.parse_args()


def main() -> None:
    _setup_logging()
    args = _parse_args()

    asset_id = utils.resolve_asset_id(args.asset_id)
    logger.info(
        "create_snapshot indul: asset_id=%s horizon=fw%d start=%s end=%s",
        asset_id, args.horizon, args.start, args.end,
    )

    t0 = time.monotonic()
    conn = utils.open_lab_connection(asset_id)
    try:
        result = create_snapshot(
            conn,
            asset_id   = asset_id,
            horizon    = args.horizon,
            start_time = args.start,
            end_time   = args.end,
        )
    finally:
        conn.close()

    verb = "reused (immutable)" if result.reused else "created"
    logger.info(
        "snapshot %s: snapshot_id=%s rows=%d range=[%s..%s] elapsed=%.1fs",
        verb, result.snapshot_id, result.row_count,
        result.range_start, result.range_end, time.monotonic() - t0,
    )
    logger.info("content_sha256=%s feature_set_hash=%s",
                result.content_sha256, result.feature_set_hash)


if __name__ == "__main__":
    main()
