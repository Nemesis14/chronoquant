"""Worker job tools. `enqueue_sync` is the ONLY writing tool of the server."""

from __future__ import annotations

from cq_mcp.common import clamp, dump, resolve_asset
from worker.queue import JobQueue


def _queue() -> JobQueue:
    return JobQueue()


def list_jobs(status: str | None = None, limit: int = 20) -> str:
    """Recent worker jobs (newest first), optionally by status (queued/running/succeeded/failed/canceled)."""
    return dump({"items": _queue().list_jobs(status=status, kind=None, limit=clamp(limit, 1, 200))})


def enqueue_sync(asset: str | None = None) -> str:
    """Enqueue a full data sync (OHLCV -> features -> predictions) of an asset (`asset`: asset id, default solusdt). Deduplicated per asset: an already queued/running sync of the same asset is returned instead."""
    asset_id = resolve_asset(asset)
    queue = _queue()
    if not queue.worker_info()["alive"]:
        raise RuntimeError("worker is not running (worker_down); the sync cannot be enqueued")
    job, deduplicated = queue.enqueue("sync_full", {"asset_id": asset_id})
    return dump({"job": job, "deduplicated": deduplicated})
