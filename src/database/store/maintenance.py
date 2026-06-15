"""Maintenance workflows for rebuilding derived DuckDB datasets.

Provides backfill and full-rebuild operations for features and predictions,
chunked by month to limit peak memory usage. Safe to re-run.
"""

import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

import utils
from database.data_pipeline.sync_features import sync_features
from database.data_pipeline.sync_predictions import sync_predictions
from database.store.duckdb_query import dataset_exists, ohlcv_dataset_exists, ohlcv_time_stats, query_range

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
    db_cfg  = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    logger.info("BACKFILL PREDICTIONS | db_path=%s start=%s end=%s", db_path, start, end or "(latest)")

    chunks = _chunk_date_ranges(start, end, chunk_months)
    logger.info("Backfill: %d chunk, chunk_months=%d", len(chunks), chunk_months)
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        logger.info("Chunk %d/%d: %s -> %s", i, len(chunks), chunk_start, chunk_end or "(latest)")
        sync_predictions(chunk_start, end_time=chunk_end, asset_id=asset_id)

    log_dataset_check(db_path, "predictions")
    logger.info("Backfill kesz")


# =============================================================================
# raw_manifest_audit(db_path, dataset) -> None
# =============================================================================
def raw_manifest_audit(db_path: str, dataset: str) -> None:
    """Run a DuckDB native integrity audit and log statistics.

    Checks row count, open_time range, null timestamps, and duplicate
    open_time values for the given native DuckDB table.

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Table name: 'ohlcv', 'target', 'feat_ohlcv_quant', or 'predictions'.
    """
    db_file = Path(db_path)
    if not db_file.exists():
        logger.warning("%s: raw_manifest_audit - DuckDB file not found: %s", dataset, db_file)
        return

    conn = duckdb.connect(str(db_file), read_only=True)
    try:
        tbl_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [dataset],
        ).fetchone()
        if not (tbl_exists and tbl_exists[0] > 0):
            logger.warning("%s: raw_manifest_audit - tabla nem talalhato", dataset)
            return

        result = conn.execute(f"""
            SELECT
                COUNT(*)                                                    AS row_count,
                MIN(open_time)                                              AS min_ts,
                MAX(open_time)                                              AS max_ts,
                SUM(CASE WHEN open_time IS NULL THEN 1 ELSE 0 END)         AS null_ts,
                COUNT(*) - COUNT(DISTINCT CAST(open_time AS VARCHAR))       AS dup_ts
            FROM {dataset}
        """).fetchone()
        if not result:
            logger.warning("%s: raw_manifest_audit - nincs adat", dataset)
            return

        row_count, min_ts, max_ts, null_ts, dup_ts = result
        logger.info(
            "%s | rows=%d min=%s max=%s null_ts=%d dup_ts=%d",
            dataset, row_count, min_ts, max_ts, null_ts, dup_ts,
        )
        if null_ts > 0 or dup_ts > 0:
            logger.warning("%s: GYANÚS - null_ts=%d dup_ts=%d", dataset, null_ts, dup_ts)
        else:
            logger.info("%s: raw_manifest_audit OK", dataset)
    except Exception:
        logger.exception("raw_manifest_audit DuckDB hiba: dataset=%s", dataset)
    finally:
        conn.close()


# =============================================================================
# log_dataset_check(db_path, dataset) -> None
# =============================================================================
def log_dataset_check(db_path: str, dataset: str) -> None:
    """Log row count, open_time range, and integrity audit for a DuckDB dataset.

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Dataset name: 'ohlcv', 'target', 'feat_ohlcv_quant', or 'predictions'.
    """
    if dataset == "ohlcv":
        if not ohlcv_dataset_exists(db_path):
            logger.warning("%s: nincsenek sorok", dataset)
            return
        n, mn, mx = ohlcv_time_stats(db_path)
    else:
        if not dataset_exists(db_path, dataset):
            logger.warning("%s: nincsenek sorok", dataset)
            return
        df = query_range(db_path, dataset, columns=["open_time"])
        n  = len(df)
        mn = df["open_time"].min() if n else None
        mx = df["open_time"].max() if n else None
    logger.info("%s: sorok=%d, tartomany=%s -> %s", dataset, n, mn, mx)

    raw_manifest_audit(db_path, dataset)


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
    """Rebuild features and predictions from existing OHLCV data in DuckDB.

    Args:
        start            : Start time, YYYY-MM-DD HH:MM:SS.
        end              : Optional end time.
        drop             : Delete existing derived rows before rebuild.
        features_only    : Rebuild only features dataset.
        predictions_only : Rebuild only predictions dataset.
        asset_id         : Asset key from config/assets.json.
        chunk_months     : Months per processing chunk.
    """
    db_cfg  = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]

    logger.info(
        "REBUILD DERIVED DATASETS | db_path=%s start=%s end=%s drop=%s",
        db_path, start, end or "(latest)", drop,
    )

    if drop:
        db_file = Path(db_path)
        if db_file.exists():
            conn = duckdb.connect(str(db_file))
            try:
                if not predictions_only:
                    conn.execute("DELETE FROM feat_ohlcv_quant WHERE 1=1")
                    logger.info("Torolt feat_ohlcv_quant sorok (DuckDB DELETE)")
                if not features_only:
                    conn.execute("DELETE FROM predictions WHERE 1=1")
                    logger.info("Torolt prediction sorok (DuckDB DELETE)")
            finally:
                conn.close()

    if not predictions_only:
        chunks = _chunk_date_ranges(start, end, chunk_months)
        logger.info("Features rebuild: %d chunk, chunk_months=%d", len(chunks), chunk_months)
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            logger.info("Chunk %d/%d: %s -> %s", i, len(chunks), chunk_start, chunk_end or "(latest)")
            sync_features(chunk_start, end_time=chunk_end, asset_id=asset_id)
        log_dataset_check(db_path, "feat_ohlcv_quant")

    if not features_only:
        sync_predictions(start, end_time=end, asset_id=asset_id)
        log_dataset_check(db_path, "predictions")

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
    end_dt   = datetime.fromisoformat(end) if end else datetime.now(UTC).replace(tzinfo=None)
    start_dt = datetime.fromisoformat(start)
    chunks   = []
    chunk_s  = start_dt

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


# =============================================================================
# CLI entry point: python -m src.store.maintenance check [asset_id]
# =============================================================================
if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s %(name)s %(message)s")

    cmd      = sys.argv[1] if len(sys.argv) > 1 else "check"
    asset_id = sys.argv[2] if len(sys.argv) > 2 else None

    if cmd == "check":
        db_cfg  = utils.load_asset_config(asset_id)
        db_path = db_cfg["database"]["db_path"]
        for ds in ("ohlcv", "target", "feat_ohlcv_quant", "predictions"):
            log_dataset_check(db_path, ds)
    else:
        print(f"Unknown command: {cmd!r}. Usage: python -m src.store.maintenance check [asset_id]")
        sys.exit(1)
