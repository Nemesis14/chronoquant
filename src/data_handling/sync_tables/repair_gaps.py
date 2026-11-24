"""Detect and repair interior 1m gaps in feat_ohlcv_quant and predictions (epic_076 / t947).

Background
----------
Both feat_ohlcv_quant and predictions are written append-only by the live sync
(only open_time > MAX(open_time) is inserted).  A hole inside the stored range can
therefore never be filled by the live path.  Before t947 such holes appeared when a
worker sync_full job inserted an OHLCV bar and then failed before the feature write
(DuckDB file lock, crash, restart): the next job started features after the new
OHLCV tail, so that minute had no feature row and hence no prediction.
sync_features now has a catch-up start (no new holes); this module repairs the
existing ones.

Repair order
------------
1. Feature gaps  = ohlcv open_times missing from feat_ohlcv_quant, inside
   [MIN(feat.open_time), MAX(feat.open_time)]  →  sync_features(backfill=True).
2. Prediction gaps = feat_ohlcv_quant open_times missing from predictions, inside
   [MIN(pred.open_time), MAX(pred.open_time)]  →  sync_predictions(backfill=True).

The live tail (rows after the table's MAX) is NOT a gap — the next live sync
writes it.  Every step is idempotent (existing rows are never touched).

CLI: ``uv run python src/data_handling/02_sync_pipeline.py --repair-gaps --asset-id <id> [--dry-run]``
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

import duckdb

import utils
from data_handling.store.duckdb_query import _connect, _tbl_exists
from data_handling.sync_tables.sync_features import sync_features
from data_handling.sync_tables.sync_predictions import sync_predictions

logger = logging.getLogger(__name__)

_TS_FMT = "%Y-%m-%d %H:%M:%S"

LOCK_ATTEMPTS = 30
LOCK_WAIT_S   = 2.0


def _with_lock_retry(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run one idempotent step, retrying while another process (live worker) holds the DB file."""
    for i in range(LOCK_ATTEMPTS):
        try:
            return fn(*args, **kwargs)
        except duckdb.IOException as exc:
            if i + 1 >= LOCK_ATTEMPTS:
                raise
            logger.info("repair_gaps: DB zarolt, ujraprobalas %d/%d (%s)", i + 1, LOCK_ATTEMPTS, exc.__class__.__name__)
            time.sleep(LOCK_WAIT_S)
    raise AssertionError("unreachable")


@dataclass
class GapRepairResult:
    """Counters of a repair run (missing_* = before, remaining_* = after)."""

    asset_id                  : str
    dry_run                   : bool
    feature_ranges            : list[tuple[str, str]] = field(default_factory=list)
    prediction_ranges         : list[tuple[str, str]] = field(default_factory=list)
    missing_feature_rows      : int = 0
    missing_prediction_rows   : int = 0
    remaining_feature_rows    : int | None = None
    remaining_prediction_rows : int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


# %% Gap detection


def find_missing_ranges(
    db_path   : str,
    table     : str,
    reference : str,
) -> list[tuple[datetime, datetime]]:
    """Return contiguous [start, end] ranges of reference open_times missing from table.

    Only the interior of ``table`` is inspected: [MIN(table.open_time), MAX(table.open_time)].

    Args:
        db_path   : Asset DuckDB path.
        table     : Table checked for holes (e.g. 'predictions').
        reference : Table whose open_times define the expected minutes (e.g. 'feat_ohlcv_quant').

    Returns:
        List of (first_missing, last_missing) datetimes, ascending; empty when no gap.
    """
    conn = _connect(db_path)
    if conn is None:
        return []
    try:
        if not (_tbl_exists(conn, table) and _tbl_exists(conn, reference)):
            return []
        rows = conn.execute(
            f"""
            WITH bounds AS (SELECT MIN(open_time) AS lo, MAX(open_time) AS hi FROM {table}),
            missing AS (
                SELECT r.open_time
                FROM {reference} r, bounds
                WHERE r.open_time > bounds.lo AND r.open_time < bounds.hi
                  AND NOT EXISTS (SELECT 1 FROM {table} t WHERE t.open_time = r.open_time)
            ),
            grouped AS (
                SELECT open_time,
                       open_time - to_minutes(CAST(ROW_NUMBER() OVER (ORDER BY open_time) AS BIGINT)) AS grp
                FROM missing
            )
            SELECT MIN(open_time), MAX(open_time)
            FROM grouped
            GROUP BY grp
            ORDER BY 1
            """
        ).fetchall()
        return [(r[0], r[1]) for r in rows]
    finally:
        conn.close()


def _count(ranges: list[tuple[datetime, datetime]]) -> int:
    return sum(int((b - a).total_seconds() // 60) + 1 for a, b in ranges)


def _fmt(ranges: list[tuple[datetime, datetime]]) -> list[tuple[str, str]]:
    return [(a.strftime(_TS_FMT), b.strftime(_TS_FMT)) for a, b in ranges]


# %% Repair


def repair_gaps(asset_id: str | None = None, dry_run: bool = False) -> GapRepairResult:
    """Backfill interior feat_ohlcv_quant gaps, then interior predictions gaps.

    Args:
        asset_id : Asset key from config/assets.json; uses default if None.
        dry_run  : If True, only detect and report (no writes).

    Returns:
        GapRepairResult with the detected ranges and before/after counters.
    """
    resolved = utils.resolve_asset_id(asset_id)
    db_path  = utils.load_asset_config(resolved)["database"]["db_path"]

    feat_ranges = _with_lock_retry(find_missing_ranges, db_path, "feat_ohlcv_quant", "ohlcv")
    res = GapRepairResult(
        asset_id             = resolved,
        dry_run              = dry_run,
        feature_ranges       = _fmt(feat_ranges),
        missing_feature_rows = _count(feat_ranges),
    )
    logger.info(
        "repair_gaps %s: feature gaps=%d rows=%d", resolved, len(feat_ranges), res.missing_feature_rows
    )

    if not dry_run:
        # One short write transaction per range (live worker keeps running).
        for start, end in feat_ranges:
            _with_lock_retry(sync_features, start.strftime(_TS_FMT), end_time=end.strftime(_TS_FMT),
                             asset_id=resolved, backfill=True)

    # Dry run: feature holes are not filled yet, so measure against ohlcv (superset of
    # feat_ohlcv_quant) — a feature hole is also a prediction hole.
    pred_reference = "ohlcv" if dry_run else "feat_ohlcv_quant"
    pred_ranges = _with_lock_retry(find_missing_ranges, db_path, "predictions", pred_reference)
    res.prediction_ranges       = _fmt(pred_ranges)
    res.missing_prediction_rows = _count(pred_ranges)
    logger.info(
        "repair_gaps %s: prediction gaps=%d rows=%d", resolved, len(pred_ranges), res.missing_prediction_rows
    )

    if dry_run:
        return res

    for start, end in pred_ranges:
        _with_lock_retry(sync_predictions, start.strftime(_TS_FMT), end_time=end.strftime(_TS_FMT),
                         asset_id=resolved, backfill=True)

    res.remaining_feature_rows    = _count(_with_lock_retry(find_missing_ranges, db_path, "feat_ohlcv_quant", "ohlcv"))
    res.remaining_prediction_rows = _count(
        _with_lock_retry(find_missing_ranges, db_path, "predictions", "feat_ohlcv_quant")
    )
    logger.info("repair_gaps %s: %s", resolved, res.to_dict())
    return res

