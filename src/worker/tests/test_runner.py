from __future__ import annotations

import logging
import threading
import time
from typing import Any

from worker.queue import JobQueue
from worker.runner import WorkerRunner


def _ok(params: dict[str, Any], log: logging.Logger) -> dict[str, Any]:
    log.info("hello from fake job")
    print("printed line")
    return {"echo": params}


def _boom(params: dict[str, Any], log: logging.Logger) -> dict[str, Any]:
    raise RuntimeError("kaboom")


def test_run_one_success_writes_log_and_result(queue: JobQueue) -> None:
    runner = WorkerRunner(queue, {"sync_full": _ok})
    job, _ = queue.enqueue("sync_full", {"x": 1})
    assert runner.run_one() is True
    done = queue.get(job["id"])
    assert done is not None
    assert done["status"] == "succeeded"
    assert done["result"] == {"echo": {"x": 1, "asset_id": "solusdt"}}
    assert done["started_at"] and done["finished_at"]
    path = queue.log_path(job["id"])
    assert path is not None
    text = path.read_text(encoding="utf-8")
    assert "hello from fake job" in text and "printed line" in text
    assert done["log_size"] == path.stat().st_size > 0
    assert runner.run_one() is False


def test_run_one_failure_recorded(queue: JobQueue) -> None:
    runner = WorkerRunner(queue, {"sync_full": _boom})
    job, _ = queue.enqueue("sync_full")
    runner.run_one()
    done = queue.get(job["id"])
    assert done is not None
    assert done["status"] == "failed"
    assert done["error"] == "RuntimeError: kaboom"
    path = queue.log_path(job["id"])
    assert path is not None and "kaboom" in path.read_text(encoding="utf-8")


def test_missing_handler_fails_job(queue: JobQueue) -> None:
    runner = WorkerRunner(queue, {})
    job, _ = queue.enqueue("sync_full")
    runner.run_one()
    done = queue.get(job["id"])
    assert done is not None and done["status"] == "failed"


def test_canceled_job_is_skipped(queue: JobQueue) -> None:
    runner = WorkerRunner(queue, {"sync_full": _ok, "sync_predictions": _ok})
    a, _ = queue.enqueue("sync_full")
    b, _ = queue.enqueue("sync_predictions")
    queue.cancel(a["id"])
    runner.run_one()
    ja, jb = queue.get(a["id"]), queue.get(b["id"])
    assert ja is not None and ja["status"] == "canceled"
    assert jb is not None and jb["status"] == "succeeded"


def test_startup_recovers_orphans_and_registers(queue: JobQueue) -> None:
    job, _ = queue.enqueue("sync_full")
    queue.claim_next()
    WorkerRunner(queue, {}).startup()
    done = queue.get(job["id"])
    assert done is not None and done["error"] == "worker_restart"
    assert queue.worker_info()["alive"] is True


def test_run_forever_processes_and_stops(queue: JobQueue) -> None:
    runner = WorkerRunner(queue, {"sync_full": _ok}, poll_seconds=0.05, heartbeat_seconds=0.1)
    t = threading.Thread(target=runner.run_forever, daemon=True)
    t.start()
    job, _ = queue.enqueue("sync_full")
    deadline = time.time() + 10
    while time.time() < deadline:
        j = queue.get(job["id"])
        if j and j["status"] == "succeeded":
            break
        time.sleep(0.05)
    runner.stop_event.set()
    t.join(timeout=5)
    final = queue.get(job["id"])
    assert final is not None and final["status"] == "succeeded"
    assert not t.is_alive()
