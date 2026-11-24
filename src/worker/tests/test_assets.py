"""Asset-aware worker: dedup key, job params, auto-sync over the sync-active assets, job API."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import utils
from api.routers import jobs as jobs_router
from worker import assets, scheduler
from worker.queue import JobQueue

NOW = 1_700_000_000.0  # any epoch; delay 0 => due immediately in the minute


def test_dedup_key_includes_asset(queue: JobQueue) -> None:
    sol, d1 = queue.enqueue("sync_full", {"asset_id": "solusdt"})
    bch, d2 = queue.enqueue("sync_full", {"asset_id": "bchusdt"})
    assert (d1, d2) == (False, False) and sol["id"] != bch["id"]
    again, d3 = queue.enqueue("sync_full", {"asset_id": "bchusdt"})
    assert d3 is True and again["id"] == bch["id"]
    # omitted asset_id == default asset
    implicit, d4 = queue.enqueue("sync_full", {})
    assert d4 is True and implicit["id"] == sol["id"]
    assert bch["params"]["asset_id"] == "bchusdt" and implicit["params"]["asset_id"] == "solusdt"
    # other kinds are independent
    _, d5 = queue.enqueue("sync_predictions", {"asset_id": "bchusdt"})
    assert d5 is False


@pytest.fixture
def two_assets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """solusdt + bchusdt configured, both with a live DB file; trading.json assigns only solusdt."""
    for name in ("solusdt.duckdb", "bchusdt.duckdb"):
        (tmp_path / name).write_bytes(b"")
    assets_cfg = {
        "default_asset_id": "solusdt",
        "assets": {"solusdt": {"symbol": "SOLUSDT"}, "bchusdt": {"symbol": "BCHUSDT"}},
    }
    monkeypatch.setattr(utils, "load_assets_config", lambda: assets_cfg)
    monkeypatch.setattr(
        utils, "load_asset_config",
        lambda a=None: {"database": {"db_path": str(tmp_path / f"{a or 'solusdt'}.duckdb")}},
    )
    monkeypatch.setattr(utils, "load_trading_config", lambda: {"assets": {"solusdt": {}}})
    return tmp_path


def test_sync_assets_require_trading_assignment_and_db(
    two_assets: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert assets.sync_asset_ids() == ["solusdt"]  # bch: DB file exists but no session yet
    monkeypatch.setattr(utils, "load_trading_config", lambda: {"assets": {"solusdt": {}, "bchusdt": {}}})
    assert assets.sync_asset_ids() == ["solusdt", "bchusdt"]
    (two_assets / "bchusdt.duckdb").unlink()
    assert assets.sync_asset_ids() == ["solusdt"]  # assigned but no DB file


def test_tick_enqueues_per_active_asset(
    queue: JobQueue, two_assets: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue.set_auto_sync(True)
    assert scheduler.tick(queue, now=NOW, delay=0) is True
    jobs = queue.list_jobs(kind="sync_full")
    assert [j["params"]["asset_id"] for j in jobs] == ["solusdt"]  # bch not active yet
    assert scheduler.tick(queue, now=NOW, delay=0) is False  # same minute: already done

    monkeypatch.setattr(utils, "load_trading_config", lambda: {"assets": {"solusdt": {}, "bchusdt": {}}})
    nxt = NOW + 60
    queue.claim_next()  # the solusdt job is running -> its next sync is deduplicated
    assert scheduler.tick(queue, now=nxt, delay=0) is True  # bch is new
    queued = sorted(j["params"]["asset_id"] for j in queue.list_jobs(kind="sync_full"))
    assert queued == ["bchusdt", "solusdt"]
    assert scheduler.last_synced_minute(queue, "bchusdt") == scheduler.closed_minute(nxt)
    assert scheduler.last_synced_minute(queue) == scheduler.closed_minute(NOW)  # sol retried later


def test_jobs_api_asset_validation(queue: JobQueue, two_assets: Path) -> None:
    app = FastAPI()
    app.include_router(jobs_router.router)
    app.dependency_overrides[jobs_router.get_queue] = lambda: queue
    client = TestClient(app)
    queue.register_worker()
    bad = client.post("/api/jobs", json={"kind": "sync_full", "params": {"asset_id": "dogeusdt"}})
    assert bad.status_code == 404 and bad.json()["code"] == "not_found"
    ok = client.post("/api/jobs", json={"kind": "sync_full", "params": {"asset_id": "bchusdt"}})
    assert ok.status_code == 202 and ok.json()["job"]["params"]["asset_id"] == "bchusdt"
    other = client.post("/api/jobs", json={"kind": "sync_full"})
    assert other.json()["deduplicated"] is False  # default asset: separate dedup key
