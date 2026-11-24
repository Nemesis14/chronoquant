# =============================================================================
# Auto-sync scheduler
# =============================================================================
# Purpose:
#  - Port of ui/sync_runner.py timing: enqueue one sync_full per closed minute,
#    `ohlcv_sync_delay_seconds` after the minute boundary
#  - State lives in the jobs.sqlite settings table so the API can report it
# =============================================================================

from __future__ import annotations

import math
import time

import utils
from worker import assets
from worker.queue import JobQueue

DEFAULT_SYNC_DELAY_SECONDS = 3
LAST_MINUTE_KEY = "auto_sync_last_minute"


def load_sync_delay() -> int:
    """Return ohlcv_sync_delay_seconds from trading.json (default 3)."""
    try:
        cfg = utils.load_trading_config()
        return int(cfg.get("ohlcv_sync_delay_seconds", DEFAULT_SYNC_DELAY_SECONDS))
    except Exception:
        return DEFAULT_SYNC_DELAY_SECONDS


def closed_minute(epoch: float) -> int:
    """Unix seconds of the last completed minute boundary."""
    return int(epoch // 60) * 60


def next_due_seconds(
    enabled: bool, last_synced_minute: int | None, delay: int, now: float
) -> int | None:
    """Seconds until the next auto-sync is due (None if auto-sync is off)."""
    if not enabled:
        return None
    closed = closed_minute(now)
    if last_synced_minute is not None and last_synced_minute >= closed:
        return max(0, int(math.ceil(closed + 60 + delay - now)))
    return max(0, int(math.ceil(closed + delay - now)))


def _minute_key(asset_id: str) -> str:
    """Settings key of an asset's last auto-synced minute (the default asset keeps the legacy key)."""
    if asset_id == utils.default_asset_id():
        return LAST_MINUTE_KEY
    return f"{LAST_MINUTE_KEY}:{asset_id}"


def last_synced_minute(queue: JobQueue, asset_id: str | None = None) -> int | None:
    raw = queue.get_setting(_minute_key(utils.resolve_asset_id(asset_id)))
    return int(raw) if raw else None


def tick(queue: JobQueue, now: float | None = None, delay: int | None = None) -> bool:
    """Enqueue a sync_full for every sync-active asset that is due; True if any new job was created.

    Assets are served in order (default asset first); the worker runs one job at a time, so the
    per-asset syncs execute one after another.  An asset with a sync already queued/running is
    retried on the next tick (old behaviour).
    """
    if not queue.auto_sync_enabled():
        return False
    now = time.time() if now is None else now
    delay = load_sync_delay() if delay is None else delay
    created = False
    for asset_id in assets.sync_asset_ids():
        if next_due_seconds(True, last_synced_minute(queue, asset_id), delay, now) != 0:
            continue
        job, deduplicated = queue.enqueue("sync_full", {"trigger": "auto", "asset_id": asset_id})
        if deduplicated:
            continue
        queue.set_setting(_minute_key(asset_id), str(closed_minute(now)))
        created = created or bool(job)
    return created
