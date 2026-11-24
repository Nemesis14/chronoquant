# =============================================================================
# /api/jobs router (blueprint 2.8)
# =============================================================================
# Purpose:
#  - Enqueue / list / inspect / cancel worker jobs, worker status, auto-sync
#  - Job log (pollable) and per-job SSE stream (status + log + done)
#  - The API never runs a sync itself and never opens DuckDB; it only talks to
#    the SQLite job store (worker.queue) and reads job log files
# =============================================================================

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from functools import lru_cache
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from worker import assets, scheduler
from worker.logs import read_log, read_log_entries
from worker.queue import ASSET_KINDS, TERMINAL_STATUSES, JobQueue

router = APIRouter(prefix="/api/jobs", tags=["jobs"])

SSE_POLL_SECONDS = 0.5
SSE_PING_SECONDS = 15.0
SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


@lru_cache(maxsize=1)
def _default_queue() -> JobQueue:
    return JobQueue()


def get_queue() -> JobQueue:
    """FastAPI dependency (override in tests via app.dependency_overrides)."""
    return _default_queue()


QueueDep = Annotated[JobQueue, Depends(get_queue)]


class JobCreate(BaseModel):
    kind: Literal["sync_full", "sync_predictions", "sentiment_refresh", "sync_scout"]
    params: dict[str, Any] = Field(default_factory=dict)


class AutoSyncUpdate(BaseModel):
    enabled: bool


def _error(status: int, detail: str, code: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"detail": detail, "code": code})


def _not_found(job_id: str) -> JSONResponse:
    return _error(404, f"job not found: {job_id}", "not_found")


def _auto_sync_payload(queue: JobQueue) -> dict[str, Any]:
    enabled = queue.auto_sync_enabled()
    delay = scheduler.load_sync_delay()
    return {
        "enabled": enabled,
        "delay_seconds": delay,
        "next_due_in_s": scheduler.next_due_seconds(
            enabled, scheduler.last_synced_minute(queue), delay, time.time()
        ),
    }


# --------------------------------------------------------------------- jobs
@router.post("", status_code=202)
def create_job(body: JobCreate, queue: QueueDep) -> Any:
    if not queue.worker_info()["alive"]:
        return _error(503, "worker is not running", "worker_down")
    if body.kind in ASSET_KINDS:
        try:
            assets.require_asset(body.params.get("asset_id"))
        except assets.UnknownAssetError as exc:
            return _error(404, str(exc), "not_found")
    if body.kind == "sync_scout":
        asset_id = assets.require_asset(body.params.get("asset_id"))
        if asset_id not in assets.scout_asset_ids():
            return _error(400, f"not a Scout asset: {asset_id}", "bad_request")
    job, deduplicated = queue.enqueue(body.kind, body.params)
    return {"job": job, "deduplicated": deduplicated}


@router.get("")
def list_jobs(
    queue: QueueDep,
    status: str | None = None,
    kind: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> dict[str, Any]:
    return {"items": queue.list_jobs(status=status, kind=kind, limit=limit)}


# Static paths must be registered before "/{job_id}".
@router.get("/worker")
def worker_status(queue: QueueDep) -> dict[str, Any]:
    return queue.worker_info()


@router.get("/auto-sync")
def get_auto_sync(queue: QueueDep) -> dict[str, Any]:
    return _auto_sync_payload(queue)


@router.put("/auto-sync")
def put_auto_sync(body: AutoSyncUpdate, queue: QueueDep) -> dict[str, Any]:
    queue.set_auto_sync(body.enabled)
    return _auto_sync_payload(queue)


@router.get("/{job_id}")
def get_job(job_id: str, queue: QueueDep) -> Any:
    job = queue.get(job_id)
    return job if job is not None else _not_found(job_id)


@router.delete("/{job_id}")
def cancel_job(job_id: str, queue: QueueDep) -> Any:
    outcome = queue.cancel(job_id)
    if outcome == "not_found":
        return _not_found(job_id)
    if outcome == "running":
        return _error(409, "running job cannot be canceled", "conflict")
    if outcome == "finished":
        return _error(409, "job already finished", "conflict")
    return queue.get(job_id)


@router.get("/{job_id}/log")
def get_job_log(
    job_id: str,
    queue: QueueDep,
    from_: Annotated[int, Query(alias="from", ge=0)] = 0,
) -> Any:
    job = queue.get(job_id)
    path = queue.log_path(job_id)
    if job is None or path is None:
        return _not_found(job_id)
    done = job["status"] in TERMINAL_STATUSES
    lines, offset = read_log(path, from_, final=done)
    return {"offset": offset, "lines": lines, "done": done}


# --------------------------------------------------------------------- SSE
def _sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


async def _job_events(
    queue: JobQueue, job_id: str, offset: int, poll: float = SSE_POLL_SECONDS
) -> AsyncIterator[str]:
    last_status: str | None = None
    last_ping = time.monotonic()
    path = queue.log_path(job_id)
    while True:
        job = queue.get(job_id)
        if job is None or path is None:
            yield _sse("done", {"status": "failed", "result": None, "error": "job not found"})
            return
        if job["status"] != last_status:
            last_status = job["status"]
            yield _sse("status", {"status": last_status})
        terminal = last_status in TERMINAL_STATUSES
        for line, end_offset in read_log_entries(path, offset, final=terminal):
            offset = end_offset
            yield _sse("log", {"offset": end_offset, "line": line})
        if terminal:
            yield _sse(
                "done",
                {"status": last_status, "result": job["result"], "error": job["error"]},
            )
            return
        if time.monotonic() - last_ping >= SSE_PING_SECONDS:
            last_ping = time.monotonic()
            yield ": ping\n\n"
        await asyncio.sleep(poll)


@router.get("/{job_id}/events")
async def job_events(
    job_id: str,
    queue: QueueDep,
    from_: Annotated[int, Query(alias="from", ge=0)] = 0,
) -> Any:
    if queue.get(job_id) is None:
        return _not_found(job_id)
    return StreamingResponse(
        _job_events(queue, job_id, from_),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )
