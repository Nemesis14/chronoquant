"""/api/scout/* (epic_074 / t942): signals + latest on a temp live DB with the real v8 scout_signals DDL."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from data_handling.store.duckdb_store import _m008_create_scout_signals

MODEL_ID = "scout_s0c_r1"


def _write_manifest(root: Path) -> None:
    d = root / "artifacts" / MODEL_ID
    d.mkdir(parents=True, exist_ok=True)
    (d / "manifest.json").write_text(json.dumps({
        "model_id": MODEL_ID, "display_name": "Impulse Scout S0+C", "status": "experimental_no_go",
        "thresholds": {"top20": 0.1879, "top10": 0.2106},
        "runtime": {"detect_anchor": "2026-07-24T00:00:00", "live_start": "2026-11-01T00:00:00",
                    "holdout2_start": "2026-08-01"},
        "event_definition": {"cell_id": "W4h_H8h", "h_min": 480},
        "train": {"assets": ["btcusdt", "ethusdt", "solusdt"], "oof_auc": 0.636},
        "rules": [{"key": "default", "rule": {"name": "default", "description": "P* / low / H"}},
                  {"key": "r2_03", "rule": {"name": "R2_03", "description": "TP 3s / SL 1.5s"}}],
        "freeze_hash": "abc",
    }), encoding="utf-8")


def _insert(c: duckdb.DuckDBPyConnection, t0: str, status: str, p: float, *, top20: bool = False) -> None:
    outcome = None if status == "open" else "tp"
    c.execute(
        "INSERT INTO scout_signals (event_id, asset_id, model_id, cell_id, t0, t_c, label_end, low_t0, p_star,"
        " stop, scorable, p_impulse, oof_percentile, top20, top10, entry, default_tp, default_sl,"
        " default_outcome, default_net_bps, r2_03_tp, r2_03_sl, status, label_class, as_of, scored_at, backfilled)"
        " VALUES (?, 'solusdt', ?, 'W4h_H8h', ?::TIMESTAMP, ?::TIMESTAMP + INTERVAL 4 HOUR,"
        " ?::TIMESTAMP + INTERVAL 12 HOUR, 100, 103, 100, true, ?, 50, ?, false, 101, 103, 100, ?, ?, 104, 99,"
        " ?, ?, TIMESTAMP '2026-11-23 07:34:00', ?::TIMESTAMP + INTERVAL 4 HOUR + INTERVAL 30 SECOND, false)",
        [f"solusdt|W4h_H8h|{t0}", MODEL_ID, t0, t0, t0, p, top20, outcome,
         None if outcome is None else 120.0, status, None if status == "open" else "impulse", t0],
    )


@pytest.fixture
def scout_env(env: Path) -> Path:
    _write_manifest(env)
    with duckdb.connect(str(env / "live.duckdb")) as c:
        _m008_create_scout_signals(c)
        _insert(c, "2026-11-05 10:00:00", "resolved", 0.12)
        _insert(c, "2026-11-12 10:00:00", "resolved", 0.20, top20=True)
        _insert(c, "2026-11-23 02:42:00", "open", 0.16)
    return env


def test_signals_ascending_with_rules(scout_env: Path, client: TestClient) -> None:
    r = client.get("/api/scout/signals", params={"asset": "solusdt"})
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is True and body["asset_id"] == "solusdt"
    items = body["items"]
    assert [i["t0"] for i in items] == ["2026-11-05T10:00:00Z", "2026-11-12T10:00:00Z", "2026-11-23T02:42:00Z"]
    first = items[0]
    assert set(first["rules"]) == {"default", "r2_03"}
    assert first["rules"]["default"] == {"tp": 103.0, "sl": 100.0, "outcome": "tp", "outcome_at": None,
                                         "exit_price": None, "net_bps": 120.0}
    assert "default_tp" not in first  # grouped under rules
    assert first["t_c"] == "2026-11-05T14:00:00Z" and first["write_lag_s"] == 30
    assert items[-1]["status"] == "open" and items[-1]["rules"]["default"]["outcome"] is None


def test_signals_since_and_limit(scout_env: Path, client: TestClient) -> None:
    r = client.get("/api/scout/signals", params={"since": "2026-11-10T00:00:00Z"})
    assert [i["t0"] for i in r.json()["items"]] == ["2026-11-12T10:00:00Z", "2026-11-23T02:42:00Z"]
    r = client.get("/api/scout/signals", params={"limit": 1})
    assert [i["t0"] for i in r.json()["items"]] == ["2026-11-23T02:42:00Z"]  # newest kept
    assert client.get("/api/scout/signals", params={"limit": 0}).status_code == 422


def test_latest(scout_env: Path, client: TestClient) -> None:
    r = client.get("/api/scout/latest", params={"asset": "solusdt", "recent": 2})
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is True
    assert body["counts"]["total"] == 3 and body["counts"]["open"] == 1 and body["counts"]["top20"] == 1
    assert body["counts"]["as_of"] == "2026-11-23T07:34:00Z"
    assert [e["event_id"] for e in body["open_events"]] == ["solusdt|W4h_H8h|2026-11-23 02:42:00"]
    assert [e["t0"] for e in body["recent"]] == ["2026-11-23T02:42:00Z", "2026-11-12T10:00:00Z"]
    meta = body["meta"]
    assert meta["model_id"] == MODEL_ID and meta["experimental"] is True and meta["scout_asset"] is True
    assert meta["thresholds"] == {"top20": 0.1879, "top10": 0.2106}
    assert meta["live_start"] == "2026-11-01T00:00:00"
    assert [x["key"] for x in meta["rules"]] == ["default", "r2_03"]


def test_no_table_answers_empty(env: Path, client: TestClient) -> None:
    """An asset without scout_signals (bchusdt in production): 200, available = false, no rows."""
    r = client.get("/api/scout/signals", params={"asset": "bchusdt"})
    assert r.status_code == 200
    assert r.json() == {"asset_id": "bchusdt", "available": False, "items": []}
    r = client.get("/api/scout/latest", params={"asset": "bchusdt"})
    assert r.status_code == 200
    body = r.json()
    assert body["available"] is False and body["counts"] is None and body["open_events"] == []
    assert body["meta"]["scout_asset"] is False and body["meta"]["experimental"] is True


def test_unknown_asset_404(env: Path, client: TestClient) -> None:
    r = client.get("/api/scout/latest", params={"asset": "nope"})
    assert r.status_code == 404 and r.json()["code"] == "not_found"
