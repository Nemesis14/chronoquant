#!/usr/bin/env python3
"""Backfill predictions for any gap between feat_ohlcv_quant and predictions.

Detects feature rows that have no corresponding prediction row and fills them
in chronological monthly chunks using the current champion models.

Run this script when the trading service is stopped (DuckDB requires exclusive
write access).

Usage
-----
    # Auto-detect and fill all gaps
    uv run python src/data_handling/04_backfill_predictions.py

    # Limit to a specific date range
    uv run python src/data_handling/04_backfill_predictions.py \
        --start "2026-06-14 00:00:00" --end "2026-06-21 00:00:00"

    # Dry-run: show gaps without syncing
    uv run python src/data_handling/04_backfill_predictions.py --dry-run
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from dateutil.relativedelta import relativedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils
from data_handling.sync_tables.sync_predictions import sync_predictions

logger = logging.getLogger(__name__)

CHUNK_MONTHS = 1


def _setup_logging() -> None:
    logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _gap_ranges(db_path: str, start: str | None, end: str | None) -> list[tuple[str, str]]:
    """Return (gap_start, gap_end) pairs where feat_ohlcv_quant has no prediction.

    Computes contiguous missing stretches rather than individual missing rows.
    Each returned pair is the first and last open_time of a continuous gap block.
    The returned end is inclusive — add 1 minute before passing to sync_predictions.

    Args:
        db_path : Path to the asset DuckDB file.
        start   : Optional UTC lower bound for the search (inclusive).
        end     : Optional UTC upper bound for the search (inclusive).

    Returns:
        List of (gap_start, gap_end) UTC string tuples (both inclusive).
    """
    filters = ["(p.open_time IS NULL OR p.long_pred IS NULL)"]
    if start:
        filters.append(f"f.open_time >= '{start}'")
    if end:
        filters.append(f"f.open_time <= '{end}'")
    where = "WHERE " + " AND ".join(filters)

    query = f"""
        SELECT f.open_time
        FROM feat_ohlcv_quant f
        LEFT JOIN predictions p ON f.open_time = p.open_time
        {where}
        ORDER BY f.open_time
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        missing = conn.execute(query).fetchdf()
    finally:
        conn.close()

    if missing.empty:
        return []

    # Group consecutive timestamps into contiguous ranges (1-minute gaps are one block)
    fmt = "%Y-%m-%d %H:%M:%S"
    times = pd.to_datetime(missing["open_time"]).sort_values().reset_index(drop=True)
    gaps: list[tuple[str, str]] = []
    block_start = times.iloc[0]
    block_end   = times.iloc[0]

    for ts in times.iloc[1:]:
        if ts - block_end <= pd.Timedelta(minutes=1):
            block_end = ts
        else:
            gaps.append((block_start.strftime(fmt), block_end.strftime(fmt)))
            block_start = ts
            block_end   = ts
    gaps.append((block_start.strftime(fmt), block_end.strftime(fmt)))

    return gaps


def _monthly_chunks(start: str, end: str) -> list[tuple[str, str]]:
    fmt      = "%Y-%m-%d %H:%M:%S"
    dt_start = datetime.strptime(start, fmt)
    dt_end   = datetime.strptime(end, fmt)
    chunks: list[tuple[str, str]] = []
    cursor = dt_start
    while cursor <= dt_end:
        chunk_end = min(cursor + relativedelta(months=CHUNK_MONTHS), dt_end)
        chunks.append((cursor.strftime(fmt), chunk_end.strftime(fmt)))
        cursor = chunk_end + relativedelta(minutes=1)
    return chunks


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--asset-id", default=None, help="Asset ID from config/assets.json")
    p.add_argument("--start", default=None, metavar="YYYY-MM-DD HH:MM:SS",
                   help="Limit gap search to on or after this UTC time")
    p.add_argument("--end", default=None, metavar="YYYY-MM-DD HH:MM:SS",
                   help="Limit gap search to on or before this UTC time")
    p.add_argument("--dry-run", action="store_true",
                   help="Detect and report gaps without writing predictions")
    return p.parse_args()


def main() -> None:
    _setup_logging()
    args     = _parse_args()
    asset_id = args.asset_id

    db_cfg  = utils.load_asset_config(asset_id)
    db_path = db_cfg["database"]["db_path"]
    resolved = db_cfg["database"]["asset_id"]

    logger.info("Backfill predictions: asset_id=%s  start=%s  end=%s", resolved, args.start, args.end)

    # --- Detect gaps ---
    logger.info("Gap detekcio fut...")
    t0   = time.monotonic()
    gaps = _gap_ranges(db_path, args.start, args.end)
    logger.info("Gap detekcio kesz: %.1fs  gap blokkok=%d", time.monotonic() - t0, len(gaps))

    if not gaps:
        logger.info("Nincs hiany — predictions tábla naprakesz.")
        return

    total_missing = sum(
        int((pd.Timestamp(g[1]) - pd.Timestamp(g[0])).total_seconds() / 60) + 1
        for g in gaps
    )
    logger.info("Talalt hianyzasok: %d blokk, becsult ~%d sor", len(gaps), total_missing)
    for i, (gs, ge) in enumerate(gaps, 1):
        logger.info("  Gap %d: %s -> %s", i, gs, ge)

    if args.dry_run:
        logger.info("--dry-run: nincs iras.")
        return

    # --- Fill each gap in monthly chunks ---
    overall_t0 = time.monotonic()
    for gap_idx, (gap_start, gap_end) in enumerate(gaps, 1):
        chunks = _monthly_chunks(gap_start, gap_end)
        logger.info(
            "Gap %d/%d kitoltese: %s -> %s  (%d chunk)",
            gap_idx, len(gaps), gap_start, gap_end, len(chunks),
        )
        for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
            logger.info(
                "  Chunk %d/%d: %s -> %s",
                chunk_idx, len(chunks), chunk_start, chunk_end,
            )
            ct0 = time.monotonic()
            try:
                sync_predictions(
                    start_time = chunk_start,
                    end_time   = chunk_end,
                    asset_id   = asset_id,
                    backfill   = True,
                )
                logger.info("  Chunk kesz: %.1fs", time.monotonic() - ct0)
            except Exception as exc:
                logger.error("  Chunk HIBA: %s", exc)
                raise

    logger.info("Backfill kesz: total elapsed=%.1fs", time.monotonic() - overall_t0)


if __name__ == "__main__":
    main()
