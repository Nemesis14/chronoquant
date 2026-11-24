"""Impulse Scout worker step (epic_074 / t942): asset list, throttle, non-fatal failure, alert, job kind."""

from __future__ import annotations

import logging
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import worker.handlers.sync_full as sync_full_mod
from api.routers import jobs as jobs_router
from worker import assets, scout_runner
from worker.handlers import default_handlers
from worker.handlers.sync_scout import run_sync_scout
from worker.queue import JobQueue

LOG = logging.getLogger("t")
SCOUT_ASSETS = ["solusdt", "btcusdt", "ethusdt"]


def _counters(**kw: Any) -> dict[str, Any]:
    base = {"asset_id": "solusdt", "model_id": "scout_s0c_r1", "events": 8, "inserted": 0,
            "inserted_backfilled": 0, "resolved": 0, "filled": 0, "refreshed": 1, "immutable_mismatch": 0,
            "missing_in_run": 0, "open_events": 1, "total_rows": 8, "as_of": None, "first_live_sync_at": None}
    return {**base, **kw}


class _Calls(list):
    """Recorded sync_scout calls + the counters the stub returns (``result``)."""

    def __init__(self) -> None:
        super().__init__()
        self.result: dict[str, Any] = _counters()


class _Res:
    def __init__(self, d: dict[str, Any]) -> None:
        self.d = d

    def to_dict(self) -> dict[str, Any]:
        return dict(self.d)


@pytest.fixture(autouse=True)
def _reset() -> Any:
    scout_runner.reset_state()
    yield
    scout_runner.reset_state()


@pytest.fixture
def fake_sync(monkeypatch: pytest.MonkeyPatch) -> _Calls:
    """Stub sync_scout + the artifact; record the calls."""
    import data_handling.sync_tables.sync_scout as ss  # noqa: PLC0415

    calls = _Calls()

    def _sync(asset_id: str, **kw: Any) -> _Res:
        calls.append((asset_id, kw))
        return _Res(calls.result)

    monkeypatch.setattr(ss, "sync_scout", _sync)
    monkeypatch.setattr(scout_runner, "get_artifact", lambda: "ART")
    monkeypatch.setattr(assets, "scout_asset_ids", lambda: SCOUT_ASSETS)
    return calls


# %% asset list


def test_scout_assets_from_manifest_excludes_bch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(assets, "scout_config", lambda: {})
    monkeypatch.setattr(assets, "scout_manifest", lambda _m=None: {"train": {"assets": ["btcusdt", "ethusdt", "solusdt"]}})
    monkeypatch.setattr(assets, "configured_asset_ids", lambda: ["solusdt", "bchusdt", "btcusdt", "ethusdt"])
    assert assets.scout_asset_ids() == ["solusdt", "btcusdt", "ethusdt"]


def test_scout_assets_config_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(assets, "scout_config", lambda: {"assets": ["solusdt", "unknown"]})
    monkeypatch.setattr(assets, "configured_asset_ids", lambda: ["solusdt", "bchusdt", "btcusdt"])
    assert assets.scout_asset_ids() == ["solusdt"]


def test_real_artifact_assets() -> None:
    """The frozen scout_s0c_r1 manifest: SOL / BTC / ETH, no BCH."""
    ids = assets.scout_asset_ids()
    assert set(ids) == {"solusdt", "btcusdt", "ethusdt"}


# %% sync_full step


def test_step_skips_non_scout_and_no_new_bars(fake_sync: _Calls) -> None:
    assert scout_runner.maybe_run_scout("bchusdt", 5, LOG) == {"status": "skipped", "reason": "not_scout_asset"}
    assert scout_runner.maybe_run_scout("solusdt", 0, LOG) == {"status": "skipped", "reason": "no_new_bars"}
    assert fake_sync == []


def test_step_runs_then_throttles(fake_sync: _Calls, monkeypatch: pytest.MonkeyPatch) -> None:
    out = scout_runner.maybe_run_scout("solusdt", 1, LOG)
    assert out["status"] == "ok" and out["events"] == 8 and "elapsed_s" in out
    assert fake_sync[0][0] == "solusdt" and fake_sync[0][1]["art"] == "ART"
    assert fake_sync[0][1]["attempts"] == scout_runner.CONNECT_ATTEMPTS
    assert scout_runner.maybe_run_scout("solusdt", 1, LOG) == {"status": "skipped", "reason": "throttled"}
    assert scout_runner.maybe_run_scout("btcusdt", 1, LOG)["status"] == "ok"  # per-asset throttle
    monkeypatch.setattr(assets, "scout_config", lambda: {"min_interval_s": 0})
    assert scout_runner.maybe_run_scout("solusdt", 1, LOG)["status"] == "ok"
    assert len(fake_sync) == 3


def test_step_failure_is_non_fatal(fake_sync: _Calls, monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom() -> Any:
        raise RuntimeError("artifact hash mismatch")

    monkeypatch.setattr(scout_runner, "get_artifact", _boom)
    out = scout_runner.maybe_run_scout("ethusdt", 3, LOG)
    assert out["status"] == "failed" and "artifact hash mismatch" in out["error"]


def test_alert_on_integrity_counters(fake_sync: _Calls, caplog: pytest.LogCaptureFixture) -> None:
    fake_sync.result = _counters(immutable_mismatch=1)
    with caplog.at_level(logging.ERROR):
        out = scout_runner.run_scout("solusdt", LOG)
    assert out["alert"] is True
    assert any("SCOUT ALERT solusdt" in r.message for r in caplog.records)


def test_sync_full_calls_step_and_survives_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = iter([10, 12])
    monkeypatch.setattr(sync_full_mod.utils, "load_asset_config", lambda _a=None: {"database": {"db_path": "x.duckdb"}})
    monkeypatch.setattr(sync_full_mod, "ohlcv_row_count", lambda _p: next(rows))
    monkeypatch.setattr(sync_full_mod, "ohlcv_latest_open_time", lambda _p: "2026-11-23 06:43:00")
    monkeypatch.setattr(sync_full_mod, "latest_open_time", lambda _p, _t: None)
    for name in ("sync_ohlcv", "sync_funding", "sync_features", "sync_predictions"):
        monkeypatch.setattr(sync_full_mod, name, lambda *_a, **_k: None)
    seen: list[tuple[str, int]] = []

    def _step(asset_id: str, new_bars: int, _log: Any) -> dict[str, Any]:
        seen.append((asset_id, new_bars))
        return {"status": "failed", "error": "boom"}

    monkeypatch.setattr(sync_full_mod.scout_runner, "maybe_run_scout", _step)
    out = sync_full_mod.run_sync_full({"asset_id": "btcusdt"}, LOG)
    assert seen == [("btcusdt", 2)]
    assert out["scout"] == {"status": "failed", "error": "boom"} and out["inserted_ohlcv_rows"] == 2


# %% standalone job


def test_handler_registered_and_rejects_bch(fake_sync: _Calls) -> None:
    assert default_handlers()["sync_scout"] is run_sync_scout
    with pytest.raises(scout_runner.NotScoutAssetError):
        run_sync_scout({"asset_id": "bchusdt"}, LOG)
    out = run_sync_scout({"asset_id": "ethusdt"}, LOG)
    assert out["events"] == 8 and fake_sync[-1][0] == "ethusdt"


def test_handler_ignores_throttle(fake_sync: _Calls) -> None:
    run_sync_scout({"asset_id": "solusdt"}, LOG)
    run_sync_scout({"asset_id": "solusdt"}, LOG)
    assert len(fake_sync) == 2


def test_queue_dedup_per_asset(queue: JobQueue) -> None:
    a, dup_a = queue.enqueue("sync_scout", {"asset_id": "solusdt"})
    b, dup_b = queue.enqueue("sync_scout", {"asset_id": "btcusdt"})
    a2, dup_a2 = queue.enqueue("sync_scout", {"asset_id": "solusdt"})
    assert (dup_a, dup_b, dup_a2) == (False, False, True) and a2["id"] == a["id"] != b["id"]


def test_jobs_api_sync_scout(queue: JobQueue, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(assets, "scout_asset_ids", lambda: SCOUT_ASSETS)
    app = FastAPI()
    app.include_router(jobs_router.router)
    app.dependency_overrides[jobs_router.get_queue] = lambda: queue
    client = TestClient(app)
    queue.register_worker()
    r = client.post("/api/jobs", json={"kind": "sync_scout", "params": {"asset_id": "btcusdt"}})
    assert r.status_code == 202 and r.json()["job"]["params"]["asset_id"] == "btcusdt"
    r = client.post("/api/jobs", json={"kind": "sync_scout", "params": {"asset_id": "bchusdt"}})
    assert r.status_code == 400 and r.json()["code"] == "bad_request"
    r = client.post("/api/jobs", json={"kind": "sync_scout", "params": {"asset_id": "nope"}})
    assert r.status_code == 404
