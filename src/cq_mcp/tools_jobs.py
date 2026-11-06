"""Worker job tools. `enqueue_sync` is the ONLY writing tool of the server."""

from __future__ import annotations

from cq_mcp.common import clamp, dump
from worker.queue import JobQueue


def _queue() -> JobQueue:
    return JobQueue()


def list_jobs(status: str | None = None, limit: int = 20) -> str:
    """Recent worker jobs (newest first), optionally by status (queued/running/succeeded/failed/canceled)."""
    return dump({"items": _queue().list_jobs(status=status, kind=None, limit=clamp(limit, 1, 200))})


def enqueue_sync() -> str:
    """Enqueue a full data sync (OHLCV -> features -> predictions). Deduplicated: an already queued/running sync is returned instead."""
    queue = _queue()
    if not queue.worker_info()["alive"]:
        raise RuntimeError("worker is not running (worker_down); the sync cannot be enqueued")
    job, deduplicated = queue.enqueue("sync_full", {})
    return dump({"job": job, "deduplicated": deduplicated})
