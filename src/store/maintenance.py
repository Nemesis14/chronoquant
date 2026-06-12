"""Maintenance workflows for rebuilding derived Parquet datasets.

Provides backfill and full-rebuild operations for features and predictions,
chunked by month to limit peak memory usage. Safe to re-run.
"""

import logging
from datetime import UTC, datetime, timedelta
from store.duckdb_query import dataset_exists, query_range
from store.parquet_store import drop_partitions

import utils
from data_pipeline.sync_features import sync_features
from data_pipeline.sync_predictions import sync_predictions

logger = logging.getLogger(__name__)


# =============================================================================
# backfill_predictions(start, end, asset_id, chunk_months) -> None
# =============================================================================
def backfill_predictions(
    start        : str,
    end          : str | None = None,
    asset_id     : str | None = None,
    chunk_months : int = 3,
) -> None:
    """Backfill predictions for all active models over a date range.

    Processes in monthly chunks to limit peak memory usage.
    Safe to re-run: existing rows are overwritten via upsert.

    Args:
        start        : Start time, YYYY-MM-DD HH:MM:SS.
        end          : Optional end time; uses latest if None.
        asset_id     : Asset key from config/assets.json.
        chunk_months : Months per processing chunk.
    """
    db_cfg   = utils.load_asset_config(asset_id)
    data_dir = db_cfg["database"]["data_dir"]

    logger.info("BACKFILL PREDICTIONS | data_dir=%s start=%s end=%s", data_dir, start, end or "(latest)")

    chunks = _chunk_date_ranges(start, end, chunk_months)
    logger.info("Backfill: %d chunk, chunk_months=%d", len(chunks), chunk_months)
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        logger.info("Chunk %d/%d: %s -> %s", i, len(chunks), chunk_start, chunk_end or "(latest)")
        sync_predictions(chunk_start, end_time=chunk_end, asset_id=asset_id)

    log_dataset_check(data_dir, "predictions")
    logger.info("Backfill kesz")


# =============================================================================
# log_dataset_check(data_dir, dataset) -> None
# =============================================================================
def log_dataset_check(data_dir: str, dataset: str) -> None:
    """Log row count and open_time range for a Parquet dataset.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Dataset name: 'ohlcv', 'features', or 'predictions'.
    """
    if not dataset_exists(data_dir, dataset):
        logger.warning("%s: nincsenek particiok", dataset)
        return

    df = query_range(data_dir, dataset, columns=["open_time"])
    n  = len(df)
    mn = df["open_time"].min() if n else None
    mx = df["open_time"].max() if n else None
    logger.info("%s: sorok=%d, tartomany=%s -> %s", dataset, n, mn, mx)


# =============================================================================
# rebuild_derived_tables(...) -> None
# =============================================================================
def rebuild_derived_tables(
    start            : str,
    end              : str | None = None,
    drop             : bool = False,
    features_only    : bool = False,
    predictions_only : bool = False,
    asset_id         : str | None = None,
    chunk_months     : int = 6,
) -> None:
    """Rebuild features and predictions from existing OHLCV Parquet data.

    Args:
        start            : Start time, YYYY-MM-DD HH:MM:SS.
        end              : Optional end time.
        drop             : Delete existing derived partitions before rebuild.
        features_only    : Rebuild only features dataset.
        predictions_only : Rebuild only predictions dataset.
        asset_id         : Asset key from config/assets.json.
        chunk_months     : Months per processing chunk.
    """
    db_cfg   = utils.load_asset_config(asset_id)
    data_dir = db_cfg["database"]["data_dir"]

    logger.info(
        "REBUILD DERIVED DATASETS | data_dir=%s start=%s end=%s drop=%s",
        data_dir, start, end or "(latest)", drop,
    )

    if drop:
        if not predictions_only:
            deleted = drop_partitions(data_dir, "features")
            logger.info("Torolt feature particiok: %d", deleted)
        if not features_only:
            deleted = drop_partitions(data_dir, "predictions")
            logger.info("Torolt prediction particiok: %d", deleted)

    if not predictions_only:
        chunks = _chunk_date_ranges(start, end, chunk_months)
        logger.info("Features rebuild: %d chunk, chunk_months=%d", len(chunks), chunk_months)
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            logger.info("Chunk %d/%d: %s -> %s", i, len(chunks), chunk_start, chunk_end or "(latest)")
            sync_features(chunk_start, end_time=chunk_end, asset_id=asset_id)
        log_dataset_check(data_dir, "features")

    if not features_only:
        sync_predictions(start, end_time=end, asset_id=asset_id)
        log_dataset_check(data_dir, "predictions")

    logger.info("Rebuild kesz")


# =============================================================================
# _chunk_date_ranges(start, end, chunk_months) -> list[tuple[str, str | None]]
# =============================================================================
def _chunk_date_ranges(
    start        : str,
    end          : str | None,
    chunk_months : int,
) -> list[tuple[str, str | None]]:
    """Split a date range into sequential monthly chunks.

    Args:
        start        : Start time string.
        end          : End time string, or None for now.
        chunk_months : Number of months per chunk.

    Returns:
        List of (chunk_start, chunk_end) tuples.
    """
    end_dt    = datetime.fromisoformat(end) if end else datetime.now(UTC)
    start_dt  = datetime.fromisoformat(start)
    chunks    = []
    chunk_s   = start_dt

    while chunk_s < end_dt:
        month   = chunk_s.month - 1 + chunk_months
        year    = chunk_s.year + month // 12
        month   = month % 12 + 1
        chunk_e = chunk_s.replace(year=year, month=month) - timedelta(seconds=1)
        if chunk_e >= end_dt:
            chunks.append((chunk_s.strftime("%Y-%m-%d %H:%M:%S"), end))
            break
        chunks.append((chunk_s.strftime("%Y-%m-%d %H:%M:%S"), chunk_e.strftime("%Y-%m-%d %H:%M:%S")))
        chunk_s = chunk_e + timedelta(seconds=1)

    return chunks
