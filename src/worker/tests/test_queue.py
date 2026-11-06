from __future__ import annotations

import threading
from datetime import timedelta
from pathlib import Path

import pytest

from worker import scheduler
from worker.logs import read_log
from worker.queue import JobQueue, utc_now


def test_enqueue_creates_queued_job(queue: JobQueue) -> None:
    job, dedup = queue.enqueue("sync_full", {"a": 1})
    assert dedup is False
    assert job["status"] == "queued"
    assert job["params"] == {"a": 1}
    assert queue.get(job["id"]) is not None


def test_enqueue_unknown_kind_rejected(queue: JobQueue) -> None:
    with pytest.raises(ValueError):
        queue.enqueue("rm_rf")


def test_dedup_same_kind_queued_and_running(queue: JobQueue) -> None:
    first, _ = queue.enqueue("sync_full")
    second, dedup = queue.enqueue("sync_full")
    assert dedup is True and second["id"] == first["id"]
    queue.claim_next()  # now running
    third, dedup = queue.enqueue("sync_full")
    assert dedup is True and third["id"] == first["id"]
    other, dedup = queue.enqueue("sync_predictions")
    assert dedup is False and other["id"] != first["id"]


def test_enqueue_after_finish_creates_new(queue: JobQueue) -> None:
    first, _ = queue.enqueue("sync_full")
    queue.claim_next()
    queue.finish(first["id"], {"ok": True})
    second, dedup = queue.enqueue("sync_full")
    assert dedup is False and second["id"] != first["id"]


def test_concurrent_enqueue_dedups(queue: JobQueue) -> None:
    results: list[bool] = []

    def worker() -> None:
        results.append(queue.enqueue("sync_full")[1])

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(False) == 1
    assert len(queue.list_jobs()) == 1


def test_claim_is_fifo_and_single(queue: JobQueue) -> None:
    a, _ = queue.enqueue("sync_full")
    b, _ = queue.enqueue("sync_predictions")
    first = queue.claim_next()
    assert first is not None and first["id"] == a["id"] and first["status"] == "running"
    assert queue.claim_next() is None  # one job at a time
    queue.finish(a["id"], None)
    second = queue.claim_next()
    assert second is not None and second["id"] == b["id"]


def test_cancel_queued_only(queue: JobQueue) -> None:
    a, _ = queue.enqueue("sync_full")
    assert queue.cancel(a["id"]) == "canceled"
    job = queue.get(a["id"])
    assert job is not None and job["status"] == "canceled" and job["finished_at"]
    assert queue.claim_next() is None
    assert queue.cancel(a["id"]) == "finished"
    assert queue.cancel("nope") == "not_found"

    b, _ = queue.enqueue("sync_full")
    queue.claim_next()
    assert queue.cancel(b["id"]) == "running"


def test_recover_orphans(queue: JobQueue) -> None:
    a, _ = queue.enqueue("sync_full")
    queue.claim_next()
    assert queue.recover_orphans() == 1
    job = queue.get(a["id"])
    assert job is not None
    assert job["status"] == "failed" and job["error"] == "worker_restart"


def test_list_filters_newest_first(queue: JobQueue) -> None:
    a, _ = queue.enqueue("sync_full")
    queue.claim_next()
    queue.finish(a["id"], None)
    b, _ = queue.enqueue("sync_full")
    ids = [j["id"] for j in queue.list_jobs()]
    assert ids[0] == b["id"]
    assert [j["id"] for j in queue.list_jobs(status="succeeded")] == [a["id"]]
    assert queue.list_jobs(kind="sync_predictions") == []


def test_heartbeat_alive_window(queue: JobQueue) -> None:
    assert queue.worker_info()["alive"] is False
    queue.register_worker(pid=123)
    info = queue.worker_info()
    assert info["alive"] is True and info["pid"] == 123
    assert queue.worker_info(now=utc_now() + timedelta(seconds=60))["alive"] is False
    queue.beat("jobx")
    assert queue.worker_info()["current_job_id"] == "jobx"


def test_cleanup_keeps_recent(queue: JobQueue) -> None:
    a, _ = queue.enqueue("sync_full")
    queue.claim_next()
    queue.finish(a["id"], None)
    assert queue.cleanup() == []
    assert queue.get(a["id"]) is not None


def test_auto_sync_setting_and_scheduler(queue: JobQueue) -> None:
    assert queue.auto_sync_enabled() is False
    assert scheduler.tick(queue, now=1_000_000.0, delay=3) is False  # disabled
    queue.set_auto_sync(True)
    now = 1_000_020.0  # 1_000_020 % 60 == 0 -> exactly on a boundary + 0 s... use +10
    now += 10.0  # 10 s into the minute: delay passed
    assert scheduler.tick(queue, now=now, delay=3) is True
    job = queue.list_jobs()[0]
    assert job["kind"] == "sync_full"
    queue.claim_next()
    queue.finish(job["id"], None)
    # same closed minute: not again
    assert scheduler.tick(queue, now=now + 5, delay=3) is False
    # next minute, before delay
    next_minute = scheduler.closed_minute(now) + 60
    assert scheduler.tick(queue, now=next_minute + 1, delay=3) is False
    # next minute, after delay
    assert scheduler.tick(queue, now=next_minute + 3, delay=3) is True


def test_scheduler_dedup_does_not_mark_minute(queue: JobQueue) -> None:
    queue.set_auto_sync(True)
    queue.enqueue("sync_full")  # already queued
    assert scheduler.tick(queue, now=1_000_030.0, delay=3) is False
    assert scheduler.last_synced_minute(queue) is None


def test_next_due_seconds() -> None:
    assert scheduler.next_due_seconds(False, None, 3, 100.0) is None
    assert scheduler.next_due_seconds(True, None, 3, 125.0) == 0
    assert scheduler.next_due_seconds(True, 120, 3, 125.0) == 58  # 120+60+3-125


def test_read_log_partial_line(tmp_path: Path) -> None:
    p = tmp_path / "x.log"
    p.write_bytes(b"one\ntwo\npart")
    lines, off = read_log(p, 0)
    assert lines == ["one", "two"] and off == 8
    lines, off2 = read_log(p, off)
    assert lines == [] and off2 == off
    lines, off3 = read_log(p, off, final=True)
    assert lines == ["part"] and off3 == 12
