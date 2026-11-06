"""Open-position TTL cache (Binance fallback) and the ``/market/chart?include_trade`` flag."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.services import binance

SHORT_POS = {"positionAmt": "-2.5", "entryPrice": "150.0"}


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture(autouse=True)
def clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Clock]:
    """Fresh cache per test and a controllable monotonic clock."""
    binance.clear_position_cache()
    fake = _Clock()
    monkeypatch.setattr(binance, "_monotonic", fake)
    yield fake
    binance.clear_position_cache()


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fake Binance futures client that counts lookups; set ``positions`` / ``error``."""
    state: dict[str, Any] = {"n": 0, "positions": [], "error": None}

    class _Client:
        def futures_position_information(self, symbol: str) -> list[dict[str, str]]:
            state["n"] += 1
            if state["error"] is not None:
                raise state["error"]
            return state["positions"]

    monkeypatch.setattr(binance, "_make_client", lambda: _Client())
    return state


def test_position_is_cached_within_ttl(calls: dict[str, Any], clock: _Clock) -> None:
    calls["positions"] = [SHORT_POS]
    first = binance.current_position()
    assert first == {
        "side": "SHORT", "entry_time": None, "entry_price": 150.0, "quantity": 2.5,
        "stop_loss": None, "take_profit": None, "source": "binance",
    }
    assert binance.current_position() == first
    clock.now += binance.POSITION_CACHE_TTL_S - 1
    assert binance.current_position() == first
    assert calls["n"] == 1


def test_cache_expires_after_ttl(calls: dict[str, Any], clock: _Clock) -> None:
    calls["positions"] = []
    assert binance.current_position() is None
    clock.now += binance.POSITION_CACHE_TTL_S
    calls["positions"] = [SHORT_POS]
    refreshed = binance.current_position()
    assert refreshed is not None and refreshed["side"] == "SHORT"
    assert calls["n"] == 2


def test_errors_are_cached_too(calls: dict[str, Any]) -> None:
    calls["error"] = RuntimeError("binance down")
    assert binance.current_position() is None
    assert binance.current_position() is None
    assert calls["n"] == 1


def test_returned_dict_is_a_copy(calls: dict[str, Any]) -> None:
    calls["positions"] = [SHORT_POS]
    first = binance.current_position()
    assert first is not None
    first["side"] = "MUTATED"
    second = binance.current_position()
    assert second is not None and second["side"] == "SHORT"
    assert calls["n"] == 1


def test_chart_include_trade_flag(client: TestClient, calls: dict[str, Any]) -> None:
    # The fixture journal holds an open LONG, so the journal path answers without Binance.
    assert client.get("/api/market/chart").json()["active_trade"]["side"] == "LONG"
    skipped = client.get("/api/market/chart", params={"include_trade": False}).json()
    assert skipped["active_trade"] is None
    assert skipped["bars"]  # payload shape unchanged
    assert calls["n"] == 0
