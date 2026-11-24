#!/usr/bin/env python3
"""Unified database synchronisation pipeline for ChronoQuant.

Single entry point covering the full data lifecycle from raw OHLCV through
derived tables (targets, features, predictions).

Data sources
------------
- ohlcv       : Fetched from Binance REST API (requires API keys in env.json)
- targets     : Computed internally from DuckDB OHLCV via SQL window functions
- features    : Computed internally from DuckDB OHLCV via Polars
- predictions : Computed internally using champion models loaded from disk
- levels      : S/R level + trendline features (feat_levels, epic_071) from DuckDB
                OHLCV; full-history detector, incremental upsert (challenger only)
- funding     : Settled futures funding rates (funding_rate) from Binance
                fapi/v1/fundingRate; incremental after the stored max funding_ts
- scout       : Impulse Scout forward-log (scout_signals, epic_074 / t941) from DuckDB
                OHLCV via the frozen src/scout runtime; OPT-IN (not part of the
                default table set) — e.g. --tables scout --asset-id btcusdt

Dependency order (enforced by the pipeline)
--------------------------------------------
ohlcv  →  targets (full range, not chunked)
       →  features (monthly chunks)
              →  predictions (monthly chunks)
       →  levels (full-history detector, rows >= --start or incremental)
funding (independent; empty table: from --start or the earliest OHLCV row)
ohlcv  →  scout (opt-in; --start = earliest t0, default and minimum 2026-11-01)

Usage examples
--------------
    # Full sync from earliest OHLCV — all tables
    uv run python src/database/sync_pipeline.py

    # Full sync but skip the Binance fetch (derived only)
    uv run python src/database/sync_pipeline.py --skip-ohlcv

    # OHLCV from a specific start date
    uv run python src/database/sync_pipeline.py --start "2024-01-01 00:00:00"

    # Derived tables only, explicit table list
    uv run python src/database/sync_pipeline.py --tables targets,features,predictions

    # Features and predictions rebuild for a date range with custom chunk size
    uv run python src/database/sync_pipeline.py \\
        --tables features,predictions \\
        --start "2025-01-01 00:00:00" \\
        --end   "2025-06-01 00:00:00" \\
        --chunk-months 1

    # Single asset override
    uv run python src/database/sync_pipeline.py --asset-id solusdt

    # Repair interior 1m gaps in feat_ohlcv_quant + predictions (epic_076 / t947);
    # standalone mode (no other step runs), safe while the worker is live
    uv run python src/data_handling/02_sync_pipeline.py --repair-gaps --asset-id bchusdt --dry-run
    uv run python src/data_handling/02_sync_pipeline.py --repair-gaps --asset-id bchusdt
"""

import argparse
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils
from data_handling.store.duckdb_query import ohlcv_time_stats
from data_handling.sync_tables.sync_features import sync_features
from data_handling.sync_tables.sync_funding import sync_funding
from data_handling.sync_tables.sync_levels import sync_levels
from data_handling.sync_tables.sync_ohlcv import sync_ohlcv
from data_handling.sync_tables.sync_predictions import sync_predictions
from data_handling.sync_tables.sync_targets import sync_targets

# ── Constants ────────────────────────────────────────────────────────────────

INIT_START_DATE  = "2017-01-01 00:00:00"
LOOKBACK_BARS    = 2880          # extra minutes loaded before each chunk for indicator warmup
DEFAULT_CHUNK_MONTHS = 3         # months per chunk (avoids OOM on large histories)

ALL_TABLES = ("ohlcv", "targets", "features", "predictions", "levels", "funding", "scout")
#: tables run when --tables is omitted (scout is opt-in: experimental, per-asset decision)
DEFAULT_TABLES = ("ohlcv", "targets", "features", "predictions", "levels", "funding")

logger = logging.getLogger(__name__)


# ── Logging setup ─────────────────────────────────────────────────────────────


def _setup_logging(asset_id: str | None) -> None:
    """Configure console + rotating file handler.

    Log file is written to database/<asset_id>/logs/sync_pipeline_<timestamp>.log.

    Args:
        asset_id: Asset key from config/assets.json; uses default if None.
    """
    fmt     = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    try:
        db_cfg  = utils.load_asset_config(asset_id)
        db_path = db_cfg["database"]["db_path"]
        log_dir = Path(db_path).parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        ts       = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"sync_pipeline_{ts}.log"
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


# ── Helpers ───────────────────────────────────────────────────────────────────


def _earliest_ohlcv_date(db_path: str) -> str:
    """Return the earliest OHLCV open_time as a UTC string.

    Args:
        db_path: Absolute path to the asset .duckdb file.

    Returns:
        UTC string 'YYYY-MM-DD HH:MM:SS' of the earliest OHLCV row.

    Raises:
        SystemExit: If no OHLCV rows exist in DuckDB.
    """
    _, min_ts, _ = ohlcv_time_stats(db_path)
    if min_ts is None:
        logger.error("Nincs OHLCV adat a DuckDB-ben: %s", db_path)
        sys.exit(1)
    return min_ts


def _latest_ohlcv_date(db_path: str) -> str:
    """Return the latest OHLCV open_time as a UTC string.

    Args:
        db_path: Absolute path to the asset .duckdb file.

    Returns:
        UTC string 'YYYY-MM-DD HH:MM:SS' of the latest OHLCV row.

    Raises:
        SystemExit: If no OHLCV rows exist in DuckDB.
    """
    _, _, max_ts = ohlcv_time_stats(db_path)
    if max_ts is None:
        logger.error("Nincs OHLCV adat a DuckDB-ben: %s", db_path)
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
    fmt      = "%Y-%m-%d %H:%M:%S"
    dt_start = datetime.strptime(start, fmt)
    dt_end   = datetime.strptime(end, fmt)
    chunks: list[tuple[str, str]] = []
    cursor = dt_start

    while cursor < dt_end:
        chunk_end = min(cursor + relativedelta(months=chunk_months), dt_end)
        chunks.append((cursor.strftime(fmt), chunk_end.strftime(fmt)))
        cursor = chunk_end

    return chunks


# ── CLI ───────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description     = "Unified sync pipeline: OHLCV + derived tables (targets, features, predictions).",
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
        help    = (
            "UTC start time for derived table rebuild. "
            "Defaults to earliest OHLCV row. "
            "For --tables ohlcv this sets the Binance fetch start."
        ),
    )
    parser.add_argument(
        "--end",
        default = None,
        metavar = "YYYY-MM-DD HH:MM:SS",
        help    = "UTC end time for derived rebuild. Defaults to latest OHLCV row.",
    )
    parser.add_argument(
        "--chunk-months",
        type    = int,
        default = DEFAULT_CHUNK_MONTHS,
        metavar = "N",
        help    = "Months per processing chunk (features, predictions). Reduce if memory is tight.",
    )
    parser.add_argument(
        "--skip-ohlcv",
        action = "store_true",
        help   = "Skip Binance fetch; run only derived table re-computation.",
    )
    parser.add_argument(
        "--tables",
        default = None,
        metavar = "TABLE[,TABLE...]",
        help    = (
            "Comma-separated subset of tables to sync: ohlcv,targets,features,predictions,levels,funding,scout. "
            "Default: all tables except the opt-in scout. "
            "Dependency order is always enforced: ohlcv→targets, ohlcv→features→predictions."
        ),
    )
    parser.add_argument(
        "--repair-gaps",
        action = "store_true",
        help   = (
            "Standalone mode: backfill interior 1m gaps of feat_ohlcv_quant (vs ohlcv) and "
            "predictions (vs feat_ohlcv_quant); no other step runs (epic_076 / t947)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action = "store_true",
        help   = "With --repair-gaps: only report the gaps, write nothing.",
    )
    return parser.parse_args()


def _resolve_tables(args: argparse.Namespace) -> set[str]:
    """Resolve and validate the requested table set from CLI args.

    Args:
        args: Parsed argparse namespace.

    Returns:
        Set of table name strings to process.

    Raises:
        SystemExit: On unknown table names.
    """
    tables: set[str]
    if args.tables is None:
        tables = set(DEFAULT_TABLES)
    else:
        tables = {t.strip().lower() for t in args.tables.split(",")}
        unknown = tables - set(ALL_TABLES)
        if unknown:
            logger.error("Ismeretlen --tables ertekek: %s. Valid ertekek: %s", unknown, ALL_TABLES)
            sys.exit(1)

    if args.skip_ohlcv:
        tables.discard("ohlcv")

    return tables


# ── Pipeline steps ────────────────────────────────────────────────────────────


def _run_ohlcv(asset_id: str | None, start: str | None) -> None:
    """Fetch klines from Binance and append into DuckDB ohlcv table.

    Args:
        asset_id: Asset key; uses default if None.
        start   : UTC start string for Binance fetch. Defaults to INIT_START_DATE.
    """
    start_str = start or INIT_START_DATE
    ts        = pd.Timestamp(start_str, tz="UTC")
    start_ms  = int(ts.value // 1_000_000)  # nanoseconds → milliseconds

    logger.info("sync_ohlcv indul: asset_id=%s start=%s", asset_id or "default", start_str)
    t0 = time.monotonic()

    sync_ohlcv(
        open_time_ms_from = start_ms,
        asset_id          = asset_id,
    )

    logger.info("sync_ohlcv kesz: elapsed=%.1fs", time.monotonic() - t0)


def _run_targets(asset_id: str | None) -> None:
    """Rebuild the full target table from OHLCV (not chunked).

    Args:
        asset_id: Asset key; uses default if None.
    """
    logger.info("sync_targets indul (full rebuild): asset_id=%s", asset_id or "default")
    t0 = time.monotonic()

    sync_targets(asset_id=asset_id)

    logger.info("sync_targets kesz: elapsed=%.1fs", time.monotonic() - t0)


def _run_features(
    asset_id     : str | None,
    chunks       : list[tuple[str, str]],
) -> None:
    """Compute features for each monthly chunk.

    Args:
        asset_id : Asset key; uses default if None.
        chunks   : List of (chunk_start, chunk_end) UTC string pairs.
    """
    logger.info("sync_features indul: asset_id=%s chunks=%d", asset_id or "default", len(chunks))
    t0 = time.monotonic()

    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        logger.info(
            "features chunk %d/%d: %s -> %s",
            i, len(chunks), chunk_start, chunk_end,
        )
        sync_features(
            start_time    = chunk_start,
            lookback_bars = LOOKBACK_BARS,
            end_time      = chunk_end,
            asset_id      = asset_id,
        )

    logger.info("sync_features kesz: elapsed=%.1fs", time.monotonic() - t0)


def _run_predictions(
    asset_id : str | None,
    chunks   : list[tuple[str, str]],
) -> None:
    """Compute predictions for each monthly chunk.

    Args:
        asset_id : Asset key; uses default if None.
        chunks   : List of (chunk_start, chunk_end) UTC string pairs.
    """
    logger.info("sync_predictions indul: asset_id=%s chunks=%d", asset_id or "default", len(chunks))
    t0 = time.monotonic()

    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        logger.info(
            "predictions chunk %d/%d: %s -> %s",
            i, len(chunks), chunk_start, chunk_end,
        )
        sync_predictions(
            start_time = chunk_start,
            end_time   = chunk_end,
            asset_id   = asset_id,
        )

    logger.info("sync_predictions kesz: elapsed=%.1fs", time.monotonic() - t0)


def _run_levels(asset_id: str | None, start: str | None, end: str | None) -> None:
    """Upsert feat_levels (S/R + trendline features, epic_071 / t912).

    The detector always runs on the full OHLCV history; ``start`` / ``end`` only
    bound the rows written (no ``start`` = incremental from the last stored row).

    Args:
        asset_id : Asset key; uses default if None.
        start    : Optional inclusive lower bound of the rows written.
        end      : Optional inclusive upper bound.
    """
    logger.info("sync_levels indul: asset_id=%s start=%s end=%s",
                asset_id or "default", start or "incremental", end)
    t0 = time.monotonic()

    n = sync_levels(asset_id=asset_id, start_time=start, end_time=end)

    logger.info("sync_levels kesz: rows=%d elapsed=%.1fs", n, time.monotonic() - t0)


def _run_funding(asset_id: str | None, start: str | None) -> None:
    """Append settled funding rates (funding_rate) from Binance (epic_074 / t939).

    Args:
        asset_id : Asset key; uses default if None.
        start    : Start used only when the table is empty (default: earliest OHLCV row).
    """
    logger.info("sync_funding indul: asset_id=%s", asset_id or "default")
    t0 = time.monotonic()

    n = sync_funding(asset_id=asset_id, start_time=start)

    logger.info("sync_funding kesz: rows=%d elapsed=%.1fs", n, time.monotonic() - t0)


def _run_scout(asset_id: str | None, start: str | None) -> None:
    """Upsert the Impulse Scout forward-log (scout_signals, epic_074 / t941).

    Args:
        asset_id : Asset key; uses default if None.
        start    : Optional earliest event t0 (must be >= the frozen live_start 2026-11-01).
    """
    from data_handling.sync_tables.sync_scout import (
        sync_scout,  # noqa: PLC0415  (lazy: scout runtime import)
    )

    since = datetime.strptime(start, "%Y-%m-%d %H:%M:%S") if start else None
    logger.info("sync_scout indul: asset_id=%s since=%s", asset_id or "default", start or "live_start")
    res = sync_scout(asset_id=asset_id, since=since)
    logger.info("sync_scout eredmeny: %s", res.to_dict())


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    """Resolve config, validate CLI args, and run the requested pipeline steps."""
    args = _parse_args()
    _setup_logging(args.asset_id)

    if args.repair_gaps:
        from data_handling.sync_tables.repair_gaps import repair_gaps  # noqa: PLC0415

        res = repair_gaps(asset_id=args.asset_id, dry_run=args.dry_run)
        logger.info("repair_gaps eredmeny: %s", res.to_dict())
        return

    tables   = _resolve_tables(args)
    asset_id = args.asset_id

    # --- resolve asset config ---
    db_cfg   = utils.load_asset_config(asset_id)
    db_path  = db_cfg["database"]["db_path"]
    resolved = db_cfg["database"]["asset_id"]

    logger.info("sync_pipeline indul: asset_id=%s tables=%s", resolved, sorted(tables))

    pipeline_t0 = time.monotonic()

    # ── Step 1: OHLCV (Binance fetch) ────────────────────────────────────────
    if "ohlcv" in tables:
        _run_ohlcv(asset_id=asset_id, start=args.start)

    # ── Resolve derived range after ohlcv (may have grown) ───────────────────
    need_derived = tables & {"targets", "features", "predictions"}
    chunks: list[tuple[str, str]] = []
    if need_derived:
        start_time = args.start or _earliest_ohlcv_date(db_path)
        end_time   = args.end   or _latest_ohlcv_date(db_path)

        chunks = _monthly_chunks(start_time, end_time, args.chunk_months)

        logger.info(
            "derived range: start=%s  end=%s  chunk_months=%d  chunks=%d",
            start_time, end_time, args.chunk_months, len(chunks),
        )

    # ── Step 2: Targets (full rebuild, not chunked) ───────────────────────────
    if "targets" in tables:
        _run_targets(asset_id=asset_id)

    # ── Step 3: Features (chunked) ────────────────────────────────────────────
    if "features" in tables:
        _run_features(asset_id=asset_id, chunks=chunks)

    # ── Step 4: Predictions (chunked) ─────────────────────────────────────────
    if "predictions" in tables:
        _run_predictions(asset_id=asset_id, chunks=chunks)

    # ── Step 5: Levels (feat_levels, not chunked: full-history detector) ─────
    if "levels" in tables:
        _run_levels(asset_id=asset_id, start=args.start, end=args.end)

    # ── Step 6: Funding (Binance fundingRate, incremental) ───────────────────
    if "funding" in tables:
        _run_funding(asset_id=asset_id, start=args.start)

    # ── Step 7: Scout forward-log (opt-in) ───────────────────────────────────
    if "scout" in tables:
        _run_scout(asset_id=asset_id, start=args.start)

    logger.info(
        "sync_pipeline kesz: elapsed=%.1fs  tables=%s",
        time.monotonic() - pipeline_t0,
        sorted(tables),
    )


if __name__ == "__main__":
    main()
