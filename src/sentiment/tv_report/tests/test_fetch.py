"""TV report fetch + worker handler tests with a fake ideas client (no network)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from sentiment.tv_report import author_backtest, fetch, paths
from worker.handlers import default_handlers
from worker.handlers.sentiment_refresh import run_sentiment_refresh


def _idea(idea_id: int, *, hours_ago: float, symbol: str = "BTCUSD", direction: int | None = 1,
          education: bool = False) -> dict[str, Any]:
    created = (datetime.now(UTC) - timedelta(hours=hours_ago)).isoformat()
    return {
        "id": idea_id, "name": f"idea {idea_id}", "created_at": created, "chart_url": f"https://tv/{idea_id}",
        "user": {"username": f"u{idea_id}", "id": idea_id}, "description": "text",
        "is_education": education, "is_video": False, "is_script": False,
        "symbol": {"short_name": symbol, "direction": direction, "interval": "60"},
    }


class FakeClient:
    def __init__(self, raw_dir: str) -> None:
        self.raw_dir = raw_dir
        self.calls: list[int] = []

    def fetch_page(self, rel_path: str, *, symbol: str, page: int) -> dict[str, Any]:
        self.calls.append(page)
        return {"page_count": 2, "results": [
            _idea(page * 10 + 1, hours_ago=1, direction=1),
            _idea(page * 10 + 2, hours_ago=2, direction=2),
            _idea(page * 10 + 3, hours_ago=3, direction=None, education=True),
            _idea(page * 10 + 4, hours_ago=100, direction=1),          # too old for --hours 24
            _idea(page * 10 + 5, hours_ago=1, symbol="BTC.D"),          # not a BTC price series
        ]}


@pytest.fixture
def tv_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(paths, "output_dir", lambda: tmp_path / "tv_report")
    monkeypatch.setattr(fetch, "make_client", FakeClient)
    return tmp_path / "tv_report"


def test_run_fetch_counts_and_files(tv_dir: Path) -> None:
    report = fetch.run_fetch("BTCUSD", pages=2, hours=24)
    assert report["n_fetched_raw"] == 10
    assert report["n_after_symbol_filter"] == 6      # 2 pages x (1,2,3 kept; 4 too old, 5 BTC.D dropped)
    assert report["n_trade_ideas"] == 4              # education excluded
    assert (report["n_long"], report["n_short"], report["n_directed"]) == (2, 2, 4)
    assert report["n_new_since_last_run"] == 6
    stamps = [i["created_at_utc"] for i in report["ideas"]]
    assert stamps == sorted(stamps, reverse=True)
    latest = json.loads(paths.latest_path().read_text(encoding="utf-8"))
    assert latest["run_ts_utc"] == report["run_ts_utc"]
    assert (paths.runs_dir() / "report_latest_run.json").exists()
    assert len(json.loads(paths.state_path().read_text(encoding="utf-8"))["seen_ids"]) == 6


def test_second_run_marks_nothing_new(tv_dir: Path) -> None:
    fetch.run_fetch("BTCUSD", pages=1, hours=24)
    again = fetch.run_fetch("BTCUSD", pages=1, hours=24)
    assert again["n_new_since_last_run"] == 0


def test_handler_registered_and_header_only(tv_dir: Path) -> None:
    assert "sentiment_refresh" in default_handlers()
    result = run_sentiment_refresh({"hours": 24, "pages": 1}, logging.getLogger("test"))
    assert "ideas" not in result and result["n_trade_ideas"] == 2
    assert paths.latest_path().exists()


def test_handler_clamps_params(tv_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pages_seen: list[int] = []

    class Spy(FakeClient):
        def fetch_page(self, rel_path: str, *, symbol: str, page: int) -> dict[str, Any]:
            pages_seen.append(page)
            return {"page_count": 99, "results": [_idea(page, hours_ago=1)]}

    monkeypatch.setattr(fetch, "make_client", Spy)
    result = run_sentiment_refresh({"pages": 50, "hours": 0}, logging.getLogger("test"))
    assert pages_seen == [1, 2, 3, 4, 5] and result["requested_hours_window"] == 1.0


# --- author backtest pure functions ---------------------------------------------------------------

def test_extract_tp_sl() -> None:
    assert author_backtest.extract_tp_sl("Target 1: 70,000\nStop Loss: 65,000") == (70000.0, 65000.0)
    assert author_backtest.extract_tp_sl("no numbers here") is None
    assert author_backtest.extract_tp_sl("TP: 1000000 SL: 10") is None   # implausible ratio


def test_evaluate_sl_wins_tie_and_tp() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    ms = int(t0.timestamp() * 1000)
    candles = [[ms, 0, 102, 98, 100, 1], [ms + 3_600_000, 0, 130, 100, 120, 1]]
    assert author_backtest.evaluate(candles, t0.isoformat(), "long", 105, 95) == "TP"
    both = [[ms, 0, 110, 90, 100, 1]]
    assert author_backtest.evaluate(both, t0.isoformat(), "long", 105, 95) == "SL"  # same candle: SL wins
    assert author_backtest.evaluate([], t0.isoformat(), "long", 105, 95) == "no_data"
