"""Asset context: ``?asset=`` dependency, ``/api/assets``, per-asset Binance symbol / cache."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest
from fastapi.testclient import TestClient

import utils
from api.services import binance
from api.tests.conftest import N_BARS
from worker import assets as worker_assets

BCH_BARS = 7


@pytest.fixture
def bch(env: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A second asset (bchusdt) with its own small live DB and no strategy sessions."""
    live_sol = utils.load_asset_config("solusdt")
    live_bch = env / "bch.duckdb"
    with duckdb.connect(str(live_bch)) as c:
        c.execute(
            "CREATE TABLE ohlcv AS SELECT TIMESTAMP '2026-02-01 00:00:00' + to_minutes(i) AS open_time,"
            " 10.0 AS open, 11.0 AS high, 9.0 AS low, 10.5 AS close, 1.0 AS volume"
            f" FROM range({BCH_BARS}) t(i)"
        )
    bch_cfg = {"database": {"asset_id": "bchusdt", "db_path": str(live_bch), "symbol": "BCHUSDT",
                            "interval": "1m"}}

    def load_asset_config(asset_id: str | None = None) -> dict[str, Any]:
        return bch_cfg if asset_id == "bchusdt" else live_sol

    def lab_db_path(asset_id: str | None = None) -> str:
        return str(env / ("bchusdt_lab.duckdb" if asset_id == "bchusdt" else "live_lab.duckdb"))

    monkeypatch.setattr(utils, "load_asset_config", load_asset_config)
    monkeypatch.setattr(utils, "lab_db_path", lab_db_path)
    return live_bch


def test_unknown_asset_is_404(client: TestClient) -> None:
    for path in ("/api/status", "/api/market/config", "/api/market/candles", "/api/predictions/latest",
                 "/api/trades/closed", "/api/equity", "/api/strat/active", "/api/registry/active"):
        r = client.get(path, params={"asset": "dogeusdt"})
        assert r.status_code == 404, path
        assert r.json()["code"] == "not_found"


def test_default_asset_unchanged(client: TestClient) -> None:
    implicit = client.get("/api/status").json()
    explicit = client.get("/api/status", params={"asset": "solusdt"}).json()
    assert implicit["asset_id"] == explicit["asset_id"] == "solusdt"
    assert implicit["live"]["ohlcv"]["rows"] == explicit["live"]["ohlcv"]["rows"] == N_BARS


def test_assets_listing(client: TestClient) -> None:
    body = client.get("/api/assets").json()
    assert body["default_asset_id"] == "solusdt"
    by_id = {a["asset_id"]: a for a in body["items"]}
    assert {"solusdt", "bchusdt"} <= by_id.keys()
    sol = by_id["solusdt"]
    assert sol["is_default"] is True and sol["symbol"] == "SOLUSDT" and sol["has_data"] is True
    assert sol["active"] is True  # temp env has a flat (legacy) trading config => default asset
    assert by_id["bchusdt"]["is_default"] is False
    # UI order = config/assets.json display_order (t946), not "default first"
    assert [a["asset_id"] for a in body["items"]] == worker_assets.display_asset_ids()


def test_display_order(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg: dict[str, Any] = {
        "default_asset_id": "solusdt",
        "display_order": ["btcusdt", "dogeusdt", "bchusdt", "btcusdt", "solusdt"],
        "assets": {"solusdt": {}, "bchusdt": {}, "btcusdt": {}, "ethusdt": {}},
    }
    monkeypatch.setattr(utils, "load_assets_config", lambda: cfg)
    # unknown / duplicate ids skipped, unlisted ids appended in configured order
    assert worker_assets.display_asset_ids() == ["btcusdt", "bchusdt", "solusdt", "ethusdt"]
    assert worker_assets.configured_asset_ids()[0] == "solusdt"  # scheduler order unchanged
    del cfg["display_order"]
    assert worker_assets.display_asset_ids() == worker_assets.configured_asset_ids()


def test_asset_selects_database(client: TestClient, bch: Path) -> None:
    sol = client.get("/api/status", params={"asset": "solusdt"}).json()
    other = client.get("/api/status", params={"asset": "bchusdt"}).json()
    assert sol["live"]["ohlcv"]["rows"] == N_BARS
    assert other["symbol"] == "BCHUSDT" and other["asset_id"] == "bchusdt"
    assert other["live"]["ohlcv"]["rows"] == BCH_BARS
    candles = client.get("/api/market/candles", params={"asset": "bchusdt", "hours": 1}).json()
    assert len(candles["candles"]) == BCH_BARS
    # no strategy session for the second asset => no cards / cutoffs, SOL untouched
    assert client.get("/api/strat/active", params={"asset": "bchusdt"}).json() == {"long": None, "short": None}
    assert client.get("/api/strat/active").json()["long"] is not None
    chart = client.get("/api/market/chart", params={"asset": "bchusdt", "include_trade": False}).json()
    assert chart["thresholds"] == {"long_entry": None, "short_entry": None}


def test_binance_symbol_and_cache_per_asset(monkeypatch: pytest.MonkeyPatch, bch: Path) -> None:
    binance.clear_position_cache()
    seen: list[str] = []

    class _Client:
        def futures_position_information(self, symbol: str) -> list[dict[str, str]]:
            seen.append(symbol)
            return [{"positionAmt": "1.0", "entryPrice": "5.0"}] if symbol == "BCHUSDT" else []

    monkeypatch.setattr(binance, "_make_client", lambda: _Client())
    assert binance.current_position("solusdt") is None
    bch_pos = binance.current_position("bchusdt")
    assert bch_pos is not None and bch_pos["side"] == "LONG"
    binance.current_position("solusdt")
    binance.current_position("bchusdt")
    assert seen == ["SOLUSDT", "BCHUSDT"]  # one lookup per asset, then cached under its own key
    assert binance.current_position() is None  # default asset == solusdt, cache hit
    assert seen == ["SOLUSDT", "BCHUSDT"]
    binance.clear_position_cache()


def test_journal_is_filtered_by_asset(client: TestClient, bch: Path) -> None:
    """Journal is asset-agnostic: non-default assets only see rows of their own runs."""
    sol_total = client.get("/api/trades/journal/runs").json()["total"]
    for kind in ("positions", "orders", "signals", "errors", "runs"):
        r = client.get(f"/api/trades/journal/{kind}", params={"asset": "bchusdt"})
        assert r.status_code == 200, kind
        assert r.json() == {"items": [], "total": 0} or kind == "runs"
    assert client.get("/api/trades/journal/runs", params={"asset": "bchusdt"}).json()["total"] == 0
    assert client.get("/api/trades/journal/runs").json()["total"] == sol_total


def test_chart_active_trade_is_per_asset(
    client: TestClient, bch: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Chart active_trade uses the asset's own journal runs and Binance symbol (no cross-asset leak)."""
    binance.clear_position_cache()
    seen: list[str] = []

    class _Client:
        def futures_position_information(self, symbol: str) -> list[dict[str, str]]:
            seen.append(symbol)
            return [{"positionAmt": "2.0", "entryPrice": "90.5"}] if symbol == "SOLUSDT" else []

    monkeypatch.setattr(binance, "_make_client", lambda: _Client())
    bch_chart = client.get("/api/market/chart", params={"asset": "bchusdt"}).json()
    assert bch_chart["active_trade"] is None
    sol_chart = client.get("/api/market/chart").json()
    assert sol_chart["active_trade"] is not None  # SOL journal row (fixture) is untouched
    assert seen == ["BCHUSDT"]  # BCH looked up only its own symbol
    binance.clear_position_cache()


def test_report_list_has_asset_id(client: TestClient) -> None:
    items = client.get("/api/reports").json()["items"]
    for item in items:
        assert "asset_id" in item
        if item["owner"] == "general":
            assert item["asset_id"] is None
