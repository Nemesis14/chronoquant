from __future__ import annotations

import json
import logging
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers import jobs as jobs_router
from worker.queue import JobQueue
from worker.runner import WorkerRunner


@pytest.fixture
def client(queue: JobQueue) -> TestClient:
    app = FastAPI()
    app.include_router(jobs_router.router)
    app.dependency_overrides[jobs_router.get_queue] = lambda: queue
    return TestClient(app)


def test_post_requires_worker(client: TestClient) -> None:
    r = client.post("/api/jobs", json={"kind": "sync_full"})
    assert r.status_code == 503 and r.json()["code"] == "worker_down"


def test_post_dedup_and_get(client: TestClient, queue: JobQueue) -> None:
    queue.register_worker()
    r = client.post("/api/jobs", json={"kind": "sync_full"})
    assert r.status_code == 202
    body = r.json()
    assert body["deduplicated"] is False and body["job"]["status"] == "queued"
    r2 = client.post("/api/jobs", json={"kind": "sync_full"})
    assert r2.json()["deduplicated"] is True
    assert r2.json()["job"]["id"] == body["job"]["id"]
    jid = body["job"]["id"]
    assert client.get(f"/api/jobs/{jid}").json()["id"] == jid
    assert len(client.get("/api/jobs").json()["items"]) == 1
    assert client.post("/api/jobs", json={"kind": "bogus"}).status_code == 422


def test_cancel_queued_and_running(client: TestClient, queue: JobQueue) -> None:
    queue.register_worker()
    jid = client.post("/api/jobs", json={"kind": "sync_full"}).json()["job"]["id"]
    r = client.delete(f"/api/jobs/{jid}")
    assert r.status_code == 200 and r.json()["status"] == "canceled"
    jid2 = client.post("/api/jobs", json={"kind": "sync_full"}).json()["job"]["id"]
    queue.claim_next()
    r = client.delete(f"/api/jobs/{jid2}")
    assert r.status_code == 409 and r.json()["code"] == "conflict"
    assert client.delete("/api/jobs/missing").status_code == 404


def test_worker_and_auto_sync(client: TestClient, queue: JobQueue) -> None:
    assert client.get("/api/jobs/worker").json()["alive"] is False
    queue.register_worker(pid=42)
    assert client.get("/api/jobs/worker").json()["pid"] == 42
    a = client.get("/api/jobs/auto-sync").json()
    assert a["enabled"] is False and a["next_due_in_s"] is None
    a = client.put("/api/jobs/auto-sync", json={"enabled": True}).json()
    assert a["enabled"] is True and isinstance(a["next_due_in_s"], int)


def _handler(params: dict[str, Any], log: logging.Logger) -> dict[str, Any]:
    log.info("line one")
    log.info("line two")
    return {"rows": 3}


def _data(block: str) -> dict[str, Any]:
    return json.loads(block.split("data: ", 1)[1])


def test_log_and_sse(client: TestClient, queue: JobQueue) -> None:
    queue.register_worker()
    jid = client.post("/api/jobs", json={"kind": "sync_full"}).json()["job"]["id"]
    WorkerRunner(queue, {"sync_full": _handler}).run_one()

    log = client.get(f"/api/jobs/{jid}/log").json()
    assert log["done"] is True and any("line two" in ln for ln in log["lines"])
    again = client.get(f"/api/jobs/{jid}/log", params={"from": log["offset"]}).json()
    assert again["lines"] == []

    with client.stream("GET", f"/api/jobs/{jid}/events") as resp:
        assert resp.headers["content-type"].startswith("text/event-stream")
        text = "".join(resp.iter_text())
    events = [blk for blk in text.split("\n\n") if blk.strip()]
    names = [blk.split("\n")[0] for blk in events]
    assert names[0] == "event: status"
    assert names[-1] == "event: done"
    assert names.count("event: log") >= 2
    done = _data(events[-1])
    assert done["status"] == "succeeded" and done["result"] == {"rows": 3}
    last_log = _data([e for e in events if e.startswith("event: log")][-1])
    assert last_log["offset"] == log["offset"]

    # resuming from the last offset yields no further log events
    with client.stream("GET", f"/api/jobs/{jid}/events", params={"from": log["offset"]}) as r2:
        text2 = "".join(r2.iter_text())
    assert "event: log" not in text2 and "event: done" in text2
    assert client.get("/api/jobs/missing/events").status_code == 404
