"""Rebuild targets, features, and predictions from stored OHLCV data.

Run this after training a new model or when the features/target config changes.
Reads OHLCV from DuckDB, computes targets, then features, then runs champion
model inference and writes unified long/short predictions.

Processes features and predictions in monthly chunks by default to avoid memory
issues on long histories.  Targets are always rebuilt from the full OHLCV range.

Usage examples:
    uv run python scripts/data_pipeline/rebuild_derived.py
    uv run python scripts/data_pipeline/rebuild_derived.py --start "2024-01-01 00:00:00"
    uv run python scripts/data_pipeline/rebuild_derived.py --features-only
    uv run python scripts/data_pipeline/rebuild_derived.py --targets-only
    uv run python scripts/data_pipeline/rebuild_derived.py --predictions-only --start "2025-01-01 00:00:00"
    uv run python scripts/data_pipeline/rebuild_derived.py --asset-id solusdt --chunk-months 3
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils
from database.data_pipeline.sync_features import sync_features
from database.data_pipeline.sync_predictions import sync_predictions
from database.data_pipeline.sync_targets import sync_targets
from database.store.duckdb_query import ohlcv_time_stats

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


def _earliest_ohlcv_date(db_path: str) -> str:
    """Return the earliest OHLCV open_time as a UTC start string.

    Args:
        db_path : Absolute path to the asset .duckdb file.

    Returns:
        UTC string 'YYYY-MM-DD HH:MM:SS' of the earliest OHLCV row.

    Raises:
        SystemExit: If no OHLCV rows exist in DuckDB.
    """
    _, min_ts, _ = ohlcv_time_stats(db_path)
    if min_ts is None:
        print("ERROR: Nincs OHLCV adat a DuckDB-ben:", db_path)
        sys.exit(1)
    return min_ts


def _latest_ohlcv_date(db_path: str) -> str:
    """Return the latest OHLCV open_time as a UTC end string.

    Args:
        db_path : Absolute path to the asset .duckdb file.

    Returns:
        UTC string 'YYYY-MM-DD HH:MM:SS' of the latest OHLCV row.

    Raises:
        SystemExit: If no OHLCV rows exist in DuckDB.
    """
    _, _, max_ts = ohlcv_time_stats(db_path)
    if max_ts is None:
        print("ERROR: Nincs OHLCV adat a DuckDB-ben:", db_path)
        sys.exit(1)
    return max_ts


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
        description     = "Rebuild targets, features, and/or predictions datasets from OHLCV.",
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
        "--targets-only",
        action = "store_true",
        help   = "Run only sync_targets (full rebuild, ignores --start/--end).",
    )
    parser.add_argument(
        "--features-only",
        action = "store_true",
        help   = "Run only sync_features, skip targets and predictions.",
    )
    parser.add_argument(
        "--predictions-only",
        action = "store_true",
        help   = "Run only sync_predictions, skip targets and features.",
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
    """Resolve config and run chunked rebuild of targets, features, and/or predictions."""
    _configure_logging()
    args = _parse_args()

    exclusive = [args.targets_only, args.features_only, args.predictions_only]
    if sum(exclusive) > 1:
        print("ERROR: --targets-only, --features-only, --predictions-only nem kombinalhatok.")
        sys.exit(1)

    # --- resolve asset config ---
    asset_id = args.asset_id
    db_cfg   = utils.load_asset_config(asset_id)
    db_path  = db_cfg["database"]["db_path"]
    resolved = db_cfg["database"]["asset_id"]

    start_time = args.start or _earliest_ohlcv_date(db_path)
    end_time   = args.end   or _latest_ohlcv_date(db_path)

    chunks = _monthly_chunks(start_time, end_time, args.chunk_months)

    print(f"INFO: asset_id={resolved}")
    print(f"INFO: db_path={db_path}")
    print(f"INFO: start={start_time}  end={end_time}")
    print(f"INFO: chunk_months={args.chunk_months}  chunks={len(chunks)}")

    # --- targets (always full rebuild — quantile threshold must cover full history) ---
    if not args.features_only and not args.predictions_only:
        print("INFO: sync_targets indul (full rebuild)...")
        sync_targets(asset_id=asset_id)
        print("OK: sync_targets kesz.")

    # --- features ---
    if not args.targets_only and not args.predictions_only:
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
    if not args.targets_only and not args.features_only:
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
