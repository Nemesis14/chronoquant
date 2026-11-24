"""utils.strategy_session_ids: per-asset format + legacy flat compat."""

import utils


def _patch(monkeypatch, trading: dict, default: str = "solusdt") -> None:
    monkeypatch.setattr(utils, "load_trading_config", lambda: trading)
    monkeypatch.setattr(utils, "load_assets_config", lambda: {"default_asset_id": default})


def test_per_asset_format(monkeypatch):
    _patch(monkeypatch, {"assets": {
        "solusdt": {"strategy_session_long_id": "L/strategy", "strategy_session_short_id": "S/strategy"},
        "bchusdt": {"strategy_session_long_id": "BL/strategy"},
    }})
    assert utils.strategy_session_ids("solusdt") == {"long": "L/strategy", "short": "S/strategy"}
    assert utils.strategy_session_ids("bchusdt") == {"long": "BL/strategy", "short": None}
    assert utils.strategy_session_ids() == {"long": "L/strategy", "short": "S/strategy"}
    assert utils.trading_asset_ids() == ["solusdt", "bchusdt"]


def test_legacy_flat_format(monkeypatch):
    _patch(monkeypatch, {"strategy_session_long_id": "L", "strategy_session_short_id": "S"})
    assert utils.strategy_session_ids() == {"long": "L", "short": "S"}
    assert utils.strategy_session_ids("bchusdt") == {"long": None, "short": None}
    assert utils.trading_asset_ids() == ["solusdt"]
