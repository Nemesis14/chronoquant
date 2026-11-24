"""chronoquant MCP tools: direct calls against the temp environment + protocol round-trip."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from cq_mcp import tools_data, tools_jobs, tools_models, tools_reports, tools_scout, tools_sentiment
from cq_mcp.common import dump
from cq_mcp.server import MOUNT_PATH, TOOLS
from worker.queue import JobQueue

EXPECTED_TOOLS = {
    "get_status", "list_tables", "query_sql", "get_candles", "get_predictions", "get_latest_signal",
    "get_active_strategy", "get_trades", "get_equity", "list_models", "get_model", "get_registry",
    "list_reports", "read_report", "list_jobs", "enqueue_sync", "sentiment_latest", "sentiment_refresh", "list_assets",
    "scout_signals",
}


def j(text: str) -> Any:
    return json.loads(text)


def test_tool_list_matches_blueprint() -> None:
    assert {fn.__name__ for fn in TOOLS} == EXPECTED_TOOLS


def test_dump_truncates() -> None:
    out = dump({"a": "x" * 100}, max_chars=20)
    assert out.endswith("narrow the query / lower limit]") and "TRUNCATED" in out


def test_query_sql_select_and_rejects_writes(env: Path) -> None:
    res = j(tools_data.query_sql("SELECT count(*) AS n FROM live.ohlcv"))
    assert res["rows"] == [[600]]
    with pytest.raises(ValueError, match="rejected"):
        tools_data.query_sql("DROP TABLE live.ohlcv")
    with pytest.raises(ValueError, match="rejected"):
        tools_data.query_sql("SELECT 1; SELECT 2")


def test_query_sql_limit_clamped(env: Path) -> None:
    res = j(tools_data.query_sql("SELECT * FROM live.ohlcv", limit=5))
    assert res["row_count"] == 5 and res["truncated"] is True


def test_list_tables(env: Path) -> None:
    res = j(tools_data.list_tables(catalog="live"))
    assert any(t["table"] == "ohlcv" for t in res["tables"])


def test_candles_predictions_signal(env: Path) -> None:
    candles = j(tools_data.get_candles(hours=2, tf="5m"))
    assert candles["tf"] == "5m" and candles["candles"]
    preds = j(tools_data.get_predictions(hours=1))
    assert preds["items"]
    sig = j(tools_data.get_latest_signal())
    assert sig["latest"]["long_model_id"] == "lgbm_test_l"
    assert sig["cutoffs"] == {"long": 0.9, "short": 0.8}
    assert sig["state"]["long"] == "above_cutoff"  # last bar: long pct == 1.0


def test_status_strategy_equity_trades(env: Path) -> None:
    assert j(tools_models.get_status())["asset_id"] == "solusdt"
    assert j(tools_models.get_active_strategy())["long"]["entry_cutoff"] == 0.9
    eq = j(tools_models.get_equity())
    assert eq["equity"]["points"] and "summary" in eq
    assert j(tools_models.get_trades("strat", 3))["items"]
    jr = j(tools_models.get_trades("journal", 10))
    assert jr["total"] == 2
    with pytest.raises(RuntimeError, match="binance unavailable"):
        tools_models.get_trades("binance")


def test_models_and_registry(env: Path) -> None:
    models = j(tools_models.list_models())
    assert {m["model_id"] for m in models["items"]} == {"lgbm_test_l", "lgbm_test_s"}
    detail = j(tools_models.get_model("lgbm_test_l"))
    assert detail["feature_count"] == 2 and "reports" not in detail
    assert j(tools_models.get_registry("strategies"))["total"] == 1
    with pytest.raises(Exception, match="table must be one of"):
        tools_models.get_registry("not_a_table")


def _write_report(root: Path, slug: str = "01_sampling") -> None:
    d = root / slug
    (d / "charts").mkdir(parents=True)
    (d / "report.md").write_text("# Hello\n{{chart:c1}}", encoding="utf-8")
    (d / "charts" / "c1.json").write_text('{"data": [], "layout": {}}', encoding="utf-8")
    (d / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "id": slug, "title": "Sampling", "order": 10, "summary": "s",
        "content": {"markdown": "report.md", "charts": [
            {"id": "c1", "title": "C1", "file": "charts/c1.json"}], "tables": []},
    }), encoding="utf-8")


def test_reports(env: Path) -> None:
    _write_report(env / "artifacts" / "lgbm_test_l" / "reports")
    _write_report(env / "_doc_" / "reports", "overview")
    listing = j(tools_reports.list_reports())["items"]
    assert {(r["owner"], r["slug"]) for r in listing} == {("lgbm_test_l", "01_sampling"), ("general", "overview")}
    only = j(tools_reports.list_reports("general"))["items"]
    assert len(only) == 1
    rep = j(tools_reports.read_report("lgbm_test_l", "01_sampling"))
    assert rep["markdown"].startswith("# Hello") and rep["charts"] == [{"id": "c1", "title": "C1"}]
    full = j(tools_reports.read_report("lgbm_test_l", "01_sampling", include_charts=True))
    assert full["charts"][0]["figure"] == {"data": [], "layout": {}}


def test_reports_reject_traversal(env: Path) -> None:
    with pytest.raises(ValueError):
        tools_reports.read_report("general", "../x")
    with pytest.raises(ValueError):
        tools_reports.read_report("../etc", "slug")
    with pytest.raises(FileNotFoundError):
        tools_reports.read_report("general", "missing")


def test_jobs_tools(env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue = JobQueue(tmp_path / "jobs.sqlite", tmp_path / "jobs_logs")
    monkeypatch.setattr(tools_jobs, "_queue", lambda: queue)
    with pytest.raises(RuntimeError, match="worker_down"):
        tools_jobs.enqueue_sync()
    queue.register_worker()
    first = j(tools_jobs.enqueue_sync())
    second = j(tools_jobs.enqueue_sync())
    assert first["deduplicated"] is False and second["deduplicated"] is True
    assert first["job"]["kind"] == "sync_full"
    assert len(j(tools_jobs.list_jobs())["items"]) == 1


def test_sentiment_tools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from sentiment.tv_report import paths

    monkeypatch.setattr(paths, "output_dir", lambda: tmp_path / "tv")
    with pytest.raises(FileNotFoundError, match="sentiment_refresh"):
        tools_sentiment.sentiment_latest()

    (tmp_path / "tv").mkdir()
    ideas = [
        {"id": i, "title": f"t{i}", "author": "a", "direction_label": "long", "description": "x" * 50,
         "is_education": False, "is_script": False}
        for i in range(3)
    ] + [{"id": 99, "title": "edu", "is_education": True, "is_script": False, "description": "e"}]
    paths.latest_path().write_text(json.dumps({"run_ts_utc": "20260101T000000Z", "n_long": 3, "ideas": ideas}), encoding="utf-8")
    digest = j(tools_sentiment.sentiment_latest(max_ideas=2, desc_chars=10))
    assert digest["n_long"] == 3 and digest["ideas_shown"] == 2 and digest["ideas_total_trade"] == 3
    assert digest["ideas"][0]["description"] == "x" * 10 + "..."
    assert digest["report_age_hours"] > 0

    queue = JobQueue(tmp_path / "jobs.sqlite", tmp_path / "jobs_logs")
    monkeypatch.setattr(tools_sentiment, "_queue", lambda: queue)
    with pytest.raises(RuntimeError, match="worker_down"):
        tools_sentiment.sentiment_refresh()
    queue.register_worker()
    first = j(tools_sentiment.sentiment_refresh(hours=9999, pages=0))
    second = j(tools_sentiment.sentiment_refresh())
    assert first["job"]["kind"] == "sentiment_refresh" and first["deduplicated"] is False
    assert first["job"]["params"] == {"hours": 720, "pages": 1}
    assert second["deduplicated"] is True


# --- protocol round-trip through the real app mount ----------------------------------------------

HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


def _rpc_payload(resp: Any) -> dict[str, Any]:
    if resp.headers["content-type"].startswith("text/event-stream"):
        data = [ln[5:].strip() for ln in resp.text.splitlines() if ln.startswith("data:")]
        return json.loads(data[-1])
    return resp.json()


def test_scout_signals(env: Path) -> None:
    """No scout_signals table in the temp live DB -> available = false; with rows -> meta + items."""
    out = j(tools_scout.scout_signals(asset="solusdt"))
    assert out["available"] is False and out["items"] == [] and out["meta"]["experimental"] is True
    import duckdb  # noqa: PLC0415

    from api.tests.test_scout_api import _insert, _write_manifest  # noqa: PLC0415
    from data_handling.store.duckdb_store import _m008_create_scout_signals  # noqa: PLC0415

    _write_manifest(env)
    with duckdb.connect(str(env / "live.duckdb")) as c:
        _m008_create_scout_signals(c)
        _insert(c, "2026-11-05 10:00:00", "resolved", 0.12)
        _insert(c, "2026-11-23 02:42:00", "open", 0.16)
    out = j(tools_scout.scout_signals(since="2026-11-10T00:00:00Z"))
    assert out["available"] is True and out["counts"]["open"] == 1
    assert [e["t0"] for e in out["items"]] == ["2026-11-23T02:42:00Z"]
    assert out["meta"]["model_id"] == "scout_s0c_r1" and "recent" not in out


def test_scout_signals_invalid_since(env: Path) -> None:
    """An unparsable `since` -> friendly ValueError naming the value and the expected format (no raw parser error)."""
    with pytest.raises(ValueError, match=r"invalid since: 'yesterday'; expected ISO-8601"):
        tools_scout.scout_signals(since="yesterday")
    with pytest.raises(ValueError, match="invalid since"):
        tools_scout.scout_signals(since="2026-13-40")


def test_mcp_endpoint_lists_and_calls_tools(env: Path) -> None:
    with TestClient(create_app(frontend_dist=env / "no_dist"), base_url="http://127.0.0.1:8600") as client:
        init = client.post(MOUNT_PATH, headers=HEADERS, json={
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                       "clientInfo": {"name": "t", "version": "0"}}})
        assert init.status_code == 200, init.text
        assert _rpc_payload(init)["result"]["serverInfo"]["name"] == "chronoquant"
        sid = init.headers.get("mcp-session-id")
        h = {**HEADERS, **({"mcp-session-id": sid} if sid else {})}
        client.post(MOUNT_PATH, headers=h, json={"jsonrpc": "2.0", "method": "notifications/initialized"})
        tools = client.post(MOUNT_PATH, headers=h, json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        names = {t["name"] for t in _rpc_payload(tools)["result"]["tools"]}
        assert names == EXPECTED_TOOLS
        call = client.post(MOUNT_PATH, headers=h, json={
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": "query_sql", "arguments": {"sql": "SELECT count(*) AS n FROM live.ohlcv"}}})
        result = _rpc_payload(call)["result"]
        assert not result.get("isError")
        assert j(result["content"][0]["text"])["rows"] == [[600]]


def test_asset_parameter(env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    listing = j(tools_models.list_assets())
    assert listing["default_asset_id"] == "solusdt"
    assert {a["asset_id"] for a in listing["items"]} >= {"solusdt", "bchusdt"}
    assert j(tools_models.get_status(asset="solusdt"))["asset_id"] == "solusdt"
    assert j(tools_data.get_predictions(hours=1, asset="solusdt")) == j(tools_data.get_predictions(hours=1))
    for call in (
        lambda: tools_models.get_status(asset="dogeusdt"),
        lambda: tools_data.query_sql("SELECT 1", asset="dogeusdt"),
        lambda: tools_jobs.enqueue_sync(asset="dogeusdt"),
    ):
        with pytest.raises(ValueError, match="unknown asset"):
            call()
    queue = JobQueue(tmp_path / "jobs.sqlite", log_dir=tmp_path / "logs")
    queue.register_worker()
    monkeypatch.setattr(tools_jobs, "_queue", lambda: queue)
    sol = j(tools_jobs.enqueue_sync())
    bch = j(tools_jobs.enqueue_sync(asset="bchusdt"))
    assert sol["job"]["params"]["asset_id"] == "solusdt" and bch["job"]["params"]["asset_id"] == "bchusdt"
    assert bch["deduplicated"] is False and sol["job"]["id"] != bch["job"]["id"]
