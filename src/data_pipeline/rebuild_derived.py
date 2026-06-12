"""Rebuild features and predictions datasets from stored OHLCV data.

Run this after training a new model or when the features config changes.
Reads OHLCV from Parquet, computes features, then runs champion model
inference and writes unified long/short predictions.

Processes in monthly chunks by default to avoid memory issues on long histories.

Usage examples:
    uv run python src/data_pipeline/rebuild_derived.py
    uv run python src/data_pipeline/rebuild_derived.py --start "2024-01-01 00:00:00"
    uv run python src/data_pipeline/rebuild_derived.py --features-only
    uv run python src/data_pipeline/rebuild_derived.py --predictions-only --start "2025-01-01 00:00:00"
    uv run python src/data_pipeline/rebuild_derived.py --asset-id solusdt_fw60 --chunk-months 3
"""

import argparse
import logging
import sys
from datetime import datetime

from dateutil.relativedelta import relativedelta

sys.path.insert(0, "src")

import utils
from data_pipeline.sync_features import sync_features
from data_pipeline.sync_predictions import sync_predictions
from store.parquet_store import list_partitions

# %% Constants

LOOKBACK_BARS        = 2880   # extra minutes loaded before each chunk for indicator warmup
DEFAULT_CHUNK_MONTHS = 3      # months per processing chunk (avoids OOM on large histories)

# %% Logging


def _configure_logging() -> None:
    logging.basicConfig(
        level    = logging.INFO,
        format   = "%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers = [logging.StreamHandler(sys.stdout)],
    )


# %% Helpers


def _earliest_ohlcv_date(data_dir: str) -> str:
    """Return the first OHLCV partition date as a UTC start string.

    Args:
        data_dir : Root data directory for the asset.

    Returns:
        UTC string 'YYYY-MM-DD 00:00:00' of the earliest OHLCV partition.

    Raises:
        SystemExit: If no OHLCV partitions exist.
    """
    dates = list_partitions(data_dir, "ohlcv")
    if not dates:
        print("ERROR: Nincs OHLCV particio a megadott data_dir-ben:", data_dir)
        sys.exit(1)
    return f"{dates[0]} 00:00:00"


def _latest_ohlcv_date(data_dir: str) -> str:
    """Return the last OHLCV partition date as a UTC end string.

    Args:
        data_dir : Root data directory for the asset.

    Returns:
        UTC string 'YYYY-MM-DD 23:59:59' of the latest OHLCV partition.

    Raises:
        SystemExit: If no OHLCV partitions exist.
    """
    dates = list_partitions(data_dir, "ohlcv")
    if not dates:
        print("ERROR: Nincs OHLCV particio a megadott data_dir-ben:", data_dir)
        sys.exit(1)
    return f"{dates[-1]} 23:59:59"


def _monthly_chunks(start: str, end: str, chunk_months: int) -> list[tuple[str, str]]:
    """Split [start, end] into sequential monthly chunks.

    Args:
        start        : UTC start string 'YYYY-MM-DD HH:MM:SS'.
        end          : UTC end string 'YYYY-MM-DD HH:MM:SS'.
        chunk_months : Number of months per chunk.

    Returns:
        List of (chunk_start, chunk_end) UTC string pairs.
    """
    fmt        = "%Y-%m-%d %H:%M:%S"
    dt_start   = datetime.strptime(start, fmt)
    dt_end     = datetime.strptime(end, fmt)
    chunks: list[tuple[str, str]] = []
    cursor = dt_start

    while cursor < dt_end:
        chunk_end = min(cursor + relativedelta(months=chunk_months), dt_end)
        chunks.append((cursor.strftime(fmt), chunk_end.strftime(fmt)))
        cursor = chunk_end

    return chunks


# %% CLI


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description     = "Rebuild features and/or predictions datasets from OHLCV.",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start",
        default = None,
        metavar = "YYYY-MM-DD HH:MM:SS",
        help    = "Rebuild start time (UTC). Defaults to earliest OHLCV partition.",
    )
    parser.add_argument(
        "--end",
        default = None,
        metavar = "YYYY-MM-DD HH:MM:SS",
        help    = "Rebuild end time (UTC). Defaults to latest OHLCV partition.",
    )
    parser.add_argument(
        "--asset-id",
        default = None,
        metavar = "ASSET_ID",
        help    = "Asset key from config/assets.json. Uses default_asset_id if omitted.",
    )
    parser.add_argument(
        "--features-only",
        action = "store_true",
        help   = "Run only sync_features, skip predictions.",
    )
    parser.add_argument(
        "--predictions-only",
        action = "store_true",
        help   = "Run only sync_predictions, skip features.",
    )
    parser.add_argument(
        "--chunk-months",
        type    = int,
        default = DEFAULT_CHUNK_MONTHS,
        metavar = "N",
        help    = "Months per processing chunk. Reduce if memory is tight.",
    )
    return parser.parse_args()


# %% Main


def main() -> None:
    """Resolve config and run chunked rebuild of features and/or predictions."""
    _configure_logging()
    args = _parse_args()

    if args.features_only and args.predictions_only:
        print("ERROR: --features-only es --predictions-only nem kombinalhato.")
        sys.exit(1)

    # --- resolve asset config ---
    asset_id = args.asset_id
    db_cfg   = utils.load_asset_config(asset_id)
    data_dir = db_cfg["database"]["data_dir"]
    resolved = db_cfg["database"]["asset_id"]

    start_time = args.start or _earliest_ohlcv_date(data_dir)
    end_time   = args.end   or _latest_ohlcv_date(data_dir)

    chunks = _monthly_chunks(start_time, end_time, args.chunk_months)

    print(f"INFO: asset_id={resolved}")
    print(f"INFO: data_dir={data_dir}")
    print(f"INFO: start={start_time}  end={end_time}")
    print(f"INFO: chunk_months={args.chunk_months}  chunks={len(chunks)}")

    # --- features ---
    if not args.predictions_only:
        print("INFO: sync_features indul...")
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            print(f"INFO: features chunk {i}/{len(chunks)}: {chunk_start} -> {chunk_end}")
            sync_features(
                start_time    = chunk_start,
                lookback_bars = LOOKBACK_BARS,
                end_time      = chunk_end,
                asset_id      = asset_id,
            )
        print("OK: sync_features kesz.")

    # --- predictions ---
    if not args.features_only:
        print("INFO: sync_predictions indul...")
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            print(f"INFO: predictions chunk {i}/{len(chunks)}: {chunk_start} -> {chunk_end}")
            sync_predictions(
                start_time = chunk_start,
                end_time   = chunk_end,
                asset_id   = asset_id,
            )
        print("OK: sync_predictions kesz.")

    print("OK: Rebuild befejezve.")


if __name__ == "__main__":
    main()
