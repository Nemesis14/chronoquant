"""Smoke tests for sync_funding (epic_074 / t939).

The Binance client is mocked — no real API calls.  Covers: empty-table
bootstrap with pagination, idempotent re-run, the worker "only_if_due" skip
(no network), the non-8h interval estimate, and the column intersection with
an older ad-hoc funding_rate table (no mark_price).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import data_handling.sync_tables.sync_funding as sync_funding_module
from data_handling.store.duckdb_store import ensure_tables, get_connection

pytestmark = pytest.mark.smoke

_T0_MS = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC
_H8_MS = 8 * 3_600_000


def _rows(n: int, start_ms: int = _T0_MS, step_ms: int = _H8_MS) -> list[dict]:
    return [
        {"symbol": "BTCUSDT", "fundingTime": start_ms + i * step_ms,
         "fundingRate": f"{0.0001 * (i + 1):.8f}", "markPrice": "100.0"}
        for i in range(n)
    ]


def _install(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, rows: list[dict], market: str = "futures") -> dict:
    keys = tmp_path / "keys.json"
    keys.write_text(json.dumps({"api_key": "k", "api_secret": "s"}), encoding="utf-8")
    db_path = tmp_path / "live.duckdb"
    calls: dict = {"n": 0, "starts": []}

    class _Client:
        def __init__(self, *_a, **_k) -> None:
            pass

        def futures_funding_rate(self, symbol: str, startTime: int, limit: int) -> list[dict]:
            calls["n"] += 1
            calls["starts"].append(startTime)
            return [r for r in rows if r["fundingTime"] >= startTime][:limit]

    monkeypatch.setattr(sync_funding_module, "Client", _Client)
    monkeypatch.setattr(sync_funding_module.utils, "load_env_config", lambda: {"api": {"binance_keys_path": str(keys)}})
    monkeypatch.setattr(
        sync_funding_module.utils, "load_asset_config",
        lambda _a=None: {"database": {"db_path": str(db_path), "symbol": "BTCUSDT", "market": market, "asset_id": "btcusdt"}},
    )
    calls["db_path"] = str(db_path)
    return calls


def _count(db_path: str) -> int:
    conn = get_connection(db_path)
    try:
        row = conn.execute("SELECT COUNT(*) FROM funding_rate").fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def test_bootstrap_paginates_and_is_idempotent(monkeypatch, tmp_path):
    monkeypatch.setattr(sync_funding_module, "_PAGE_LIMIT", 2)
    calls = _install(monkeypatch, tmp_path, _rows(5))

    n = sync_funding_module.sync_funding(start_time="2024-01-01 00:00:00")
    assert n == 5
    assert _count(calls["db_path"]) == 5
    assert calls["n"] == 3  # 2 + 2 + 1

    assert sync_funding_module.sync_funding() == 0
    assert _count(calls["db_path"]) == 5


def test_only_if_due_skips_network(monkeypatch, tmp_path):
    calls = _install(monkeypatch, tmp_path, _rows(3))
    sync_funding_module.sync_funding(start_time="2024-01-01 00:00:00")
    n_calls = calls["n"]

    last = datetime(2024, 1, 1, 16, 0, 0)  # third settlement
    n = sync_funding_module.sync_funding(only_if_due=True, now=last + timedelta(hours=7))
    assert n == 0
    assert calls["n"] == n_calls  # no Binance call before the next settlement

    sync_funding_module.sync_funding(only_if_due=True, now=last + timedelta(hours=8, minutes=1))
    assert calls["n"] == n_calls + 1


def test_next_due_uses_observed_interval():
    t = datetime(2024, 1, 1, 8)
    assert sync_funding_module.next_due([]) is None
    assert sync_funding_module.next_due([t]) == t + timedelta(hours=8)
    assert sync_funding_module.next_due([t, t - timedelta(hours=2)]) == t + timedelta(hours=2)
    assert sync_funding_module.next_due([t, t]) == t + timedelta(hours=1)  # floor 1h


def test_legacy_table_without_mark_price(monkeypatch, tmp_path):
    calls = _install(monkeypatch, tmp_path, _rows(2))
    conn = get_connection(calls["db_path"])
    try:
        conn.execute("CREATE TABLE funding_rate (funding_ts TIMESTAMP, funding_rate DOUBLE)")
        ensure_tables(conn)  # v7 is IF NOT EXISTS: legacy table kept as is
    finally:
        conn.close()

    assert sync_funding_module.sync_funding(start_time="2024-01-01 00:00:00") == 2
    assert _count(calls["db_path"]) == 2


def test_spot_asset_is_skipped(monkeypatch, tmp_path):
    calls = _install(monkeypatch, tmp_path, _rows(2), market="spot")
    assert sync_funding_module.sync_funding() == 0
    assert calls["n"] == 0
