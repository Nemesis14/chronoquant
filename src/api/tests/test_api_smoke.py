"""Smoke tests for the read-only BFF (TestClient + temporary DuckDB environment)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from api import deps
from api.main import create_app
from api.services import logs
from api.tests.conftest import LONG_SESSION, MODEL_L, MODEL_S, N_BARS, SHORT_SESSION

pytestmark = pytest.mark.smoke


def test_health(client: TestClient) -> None:
    body = client.get("/api/health").json()
    assert body["status"] == "ok"
    assert body["time"].endswith("Z")


def test_status(client: TestClient) -> None:
    body = client.get("/api/status").json()
    assert body["symbol"] == "SOLUSDT"
    assert body["live"]["ohlcv"]["rows"] == N_BARS
    assert body["live"]["predictions"]["latest_open_time"] == "2026-01-01T09:59:00Z"
    assert body["models"]["long"]["model_id"] == MODEL_L
    assert body["models"]["short"]["model_id"] == MODEL_S
    assert body["worker"]["alive"] is False
    assert body["db"]["state"] == "ok"


def test_market_config_and_tables(client: TestClient) -> None:
    cfg = client.get("/api/market/config").json()
    assert cfg["windows"] == ["24h", "8h", "4h"]
    tables = {t["table"]: t for t in client.get("/api/market/tables").json()}
    assert tables["ohlcv"]["rows"] == N_BARS
    assert tables["trading_errors"]["exists"] is False


def test_candles_resample(client: TestClient) -> None:
    body = client.get("/api/market/candles", params={"hours": 2, "tf": "15m"}).json()
    assert body["tf"] == "15m"
    assert 8 <= len(body["candles"]) <= 9
    first = body["candles"][0]
    assert {"t", "o", "h", "l", "c", "v"} <= first.keys()
    assert first["h"] >= first["l"]
    assert client.get("/api/market/candles", params={"tf": "7m"}).status_code == 422


def test_chart_payload(client: TestClient) -> None:
    body = client.get("/api/market/chart", params={"window": "4h"}).json()
    assert body["focus_hours"] == 4
    assert len(body["bars"]) == 241
    assert body["thresholds"] == {"long_entry": 0.9, "short_entry": 0.8}
    last = body["bars"][-1]
    assert last["long_pct"] == pytest.approx(1.0)
    assert 0.0 <= last["short_pct"] <= 1.0
    assert body["active_trade"]["side"] == "LONG"


def test_predictions(client: TestClient) -> None:
    latest = client.get("/api/predictions/latest").json()
    assert latest["long_model_id"] == MODEL_L
    hist = client.get("/api/predictions/history", params={"hours": 1}).json()["items"]
    assert len(hist) == 61
    sig = client.get("/api/predictions/signals", params={"hours": 24}).json()["items"]
    assert [s["side"] for s in sig] == ["short", "long"]  # crossings: short @0.8, long @0.9
    assert sig[0]["t"] < sig[1]["t"]


def test_trades_closed_and_journal(client: TestClient) -> None:
    body = client.get("/api/trades/closed", params={"limit": 3}).json()
    assert body["total"] == 10
    assert len(body["items"]) == 3
    assert {i["session_id"] for i in client.get("/api/trades/closed").json()["items"]} == {
        LONG_SESSION, SHORT_SESSION,
    }
    active = client.get("/api/trades/active").json()
    assert active["source"] == "journal" and active["side"] == "LONG"
    closed_pos = client.get("/api/trades/journal/positions", params={"status": "closed"}).json()
    assert closed_pos["total"] == 1
    orders = client.get("/api/trades/journal/orders").json()["items"]
    assert "request_json" not in orders[0]
    assert "request_json" in client.get("/api/trades/journal/orders", params={"full": True}).json()["items"][0]
    assert client.get("/api/trades/journal/errors").json() == {"items": [], "total": 0}


def test_binance_unavailable(client: TestClient) -> None:
    resp = client.get("/api/trades/binance")
    assert resp.status_code == 503
    assert resp.json()["code"] == "binance_unavailable"


def test_equity(client: TestClient) -> None:
    both = client.get("/api/equity").json()
    assert both["session_ids"]["long"] == LONG_SESSION
    assert len(both["points"]) == 10
    assert set(both["series"]) == {"long", "short"}
    long_only = client.get("/api/equity", params={"direction": "long"}).json()
    assert {p["direction"] for p in long_only["points"]} == {"long"}
    assert client.get("/api/equity/summary").json()["long"]["n_trades"] == 5


def test_strat(client: TestClient) -> None:
    active = client.get("/api/strat/active").json()
    assert active["long"]["entry_cutoff"] == 0.9
    assert active["short"]["metrics"]["win_rate"] == 0.5
    sessions = client.get("/api/strat/sessions").json()
    assert {s["session_id"] for s in sessions} == {LONG_SESSION, SHORT_SESSION}
    assert client.get(f"/api/strat/{LONG_SESSION}/artifact").json()["session_id"] == LONG_SESSION
    trades = client.get(f"/api/strat/{LONG_SESSION}/trades", params={"limit": 2}).json()
    assert trades["total"] == 5 and len(trades["items"]) == 2
    grid = client.get(f"/api/strat/{LONG_SESSION}/grid", params={"top": 2}).json()
    assert grid["items"][0]["total_fact_log_return"] == pytest.approx(0.4)
    assert client.get(f"/api/strat/{LONG_SESSION}/grid", params={"sort": "x; DROP"}).status_code == 400
    assert client.get("/api/strat/missing_session/trades").json()["code"] == "not_found"
    assert client.get('/api/strat/bad"id/trades').status_code in (400, 404)


def test_registry(client: TestClient) -> None:
    active = client.get("/api/registry/active").json()
    assert active["live_models"] == {"long": MODEL_L, "short": MODEL_S}
    assert len(active["pending_deployments"]) == 1
    models = client.get("/api/registry/models").json()
    assert models["total"] == 2
    assert client.get("/api/registry/models", params={"filter": "test_s"}).json()["total"] == 1
    bad = client.get("/api/registry/sqlite_master")
    assert bad.status_code == 400 and bad.json()["code"] == "bad_request"
    detail = client.get(f"/api/registry/models/{MODEL_L}").json()
    assert detail["feature_count"] == 2
    assert {a["kind"]: a["exists"] for a in detail["artifacts"]}["manifest"] is True
    assert client.get(f"/api/registry/models/{MODEL_L}/features").json()["features"] == ["feat_a", "feat_b"]
    assert client.get("/api/registry/models/nope").json()["code"] == "not_found"
    assert client.get("/api/registry/models/..%2Fetc").status_code in (400, 404)


def test_db_busy(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    def _locked(*_a: object, **_k: object) -> None:
        raise duckdb.IOException("file is locked by another process")

    monkeypatch.setattr(deps, "open_readonly_connection", _locked)
    resp = client.get("/api/market/chart")
    assert resp.status_code == 503
    assert resp.json()["code"] == "db_busy"
    status = client.get("/api/status").json()  # degrades instead of failing
    assert status["db"]["state"] == "busy"
    assert status["live"] is None


def test_unknown_api_path_is_json_404(client: TestClient) -> None:
    resp = client.get("/api/nope")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Not Found", "code": "not_found"}


def test_spa_fallback(env: Path) -> None:
    dist = env / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<html>spa</html>", encoding="utf-8")
    (dist / "assets" / "app.js").write_text("//js", encoding="utf-8")
    with TestClient(create_app(frontend_dist=dist)) as c:
        assert c.get("/").text == "<html>spa</html>"
        assert c.get("/trades").text == "<html>spa</html>"
        assert c.get("/assets/app.js").text == "//js"
        assert c.get("/api/health").json()["status"] == "ok"
        nf = c.get("/api/unknown")
        assert nf.status_code == 404 and nf.json()["code"] == "not_found"
        assert c.get("/..%2F..%2Fsecret").text == "<html>spa</html>"


def test_logs(client: TestClient, env: Path) -> None:
    (env / "logs" / "api.err.log").write_text(
        "2026-01-01 10:00:00,123 | INFO | api.main | started\nINFO:     Uvicorn running\nplain\n",
        encoding="utf-8",
    )
    lines = client.get("/api/logs/api", params={"lines": 10}).json()["lines"]
    assert lines[0]["level"] == "INFO" and lines[0]["logger"] == "api.main"
    assert lines[1]["msg"] == "Uvicorn running"
    assert lines[2]["level"] is None
    assert client.get("/api/logs/worker").json() == {"lines": []}
    assert client.get("/api/logs/other").status_code == 422
    assert logs.parse_line("x")["msg"] == "x"
