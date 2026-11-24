"""sync_full: the funding step (epic_074 / t939) is throttled and never fails the job."""

import logging

import worker.handlers.sync_full as h


def _patch(monkeypatch, funding):
    calls: list[str] = []
    monkeypatch.setattr(h.utils, "load_asset_config", lambda _a=None: {"database": {"db_path": "x.duckdb"}})
    monkeypatch.setattr(h, "ohlcv_row_count", lambda _p: 10)
    monkeypatch.setattr(h, "ohlcv_latest_open_time", lambda _p: "2026-11-23 06:43:00")
    monkeypatch.setattr(h, "latest_open_time", lambda _p, _t: None)
    monkeypatch.setattr(h, "sync_ohlcv", lambda *_a, **_k: calls.append("ohlcv"))
    monkeypatch.setattr(h, "sync_funding", funding)
    monkeypatch.setattr(h, "sync_features", lambda *_a, **_k: calls.append("features"))
    monkeypatch.setattr(h, "sync_predictions", lambda *_a, **_k: calls.append("predictions"))
    return calls


def test_funding_failure_is_non_fatal(monkeypatch):
    def _boom(**_k):
        raise RuntimeError("binance down")

    calls = _patch(monkeypatch, _boom)
    out = h.run_sync_full({"asset_id": "btcusdt"}, logging.getLogger("t"))
    assert calls == ["ohlcv"]  # up to date -> no feature/prediction step, but no exception
    assert out["end_time"] == "2026-11-23 06:43:00"


def test_funding_called_in_due_mode(monkeypatch):
    seen: dict = {}
    _patch(monkeypatch, lambda **k: seen.update(k) or 0)
    h.run_sync_full({"asset_id": "ethusdt"}, logging.getLogger("t"))
    assert seen == {"asset_id": "ethusdt", "only_if_due": True}
