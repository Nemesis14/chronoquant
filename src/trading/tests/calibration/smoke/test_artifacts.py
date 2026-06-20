"""Smoke tests for trading.calibration.artifacts."""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.smoke

from trading.calibration.artifacts import load_strategy_artifact, write_strategy_artifact


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_DUMMY_STRATEGY_CFG = {
    "side":             "long",
    "entry_threshold":  0.45,
    "max_hold_minutes": 120,
}

_DUMMY_SUMMARY = {
    "trade_count":   5,
    "win_rate":      0.6,
    "total_return":  0.05,
    "profit":        500.0,
    "profit_factor": 1.5,
    "max_drawdown":  -0.02,
    "avg_hold_minutes": 60.0,
    "exposure_pct":  0.1,
    "initial_equity": 10000.0,
    "final_equity":  10500.0,
    "exit_reasons":  {"max_hold": 3, "probability_exit": 2},
}

_DUMMY_OOS_PERIOD = {"start": "2022-01-01", "end": "2022-06-30"}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_write_strategy_artifact_creates_valid_json(tmp_path, monkeypatch) -> None:
    """write_strategy_artifact creates a file that contains valid JSON."""
    import utils

    # Redirect _resolve_path so artifact lands in tmp_path
    def _fake_resolve(path: str) -> str:
        return str(tmp_path / path)

    monkeypatch.setattr(utils, "_resolve_path", _fake_resolve)

    out_path = write_strategy_artifact(
        model_id="test_model",
        side="long",
        strategy_cfg=_DUMMY_STRATEGY_CFG,
        summary=_DUMMY_SUMMARY,
        oos_period=_DUMMY_OOS_PERIOD,
    )

    assert out_path.exists(), "Artifact file was not created"
    content = out_path.read_text(encoding="utf-8")
    parsed = json.loads(content)
    assert isinstance(parsed, dict), "Artifact is not a JSON object"


def test_load_strategy_artifact_roundtrip(tmp_path, monkeypatch) -> None:
    """write then load returns a dict with required top-level keys."""
    import utils

    def _fake_resolve(path: str) -> str:
        return str(tmp_path / path)

    monkeypatch.setattr(utils, "_resolve_path", _fake_resolve)

    write_strategy_artifact(
        model_id="test_model",
        side="long",
        strategy_cfg=_DUMMY_STRATEGY_CFG,
        summary=_DUMMY_SUMMARY,
        oos_period=_DUMMY_OOS_PERIOD,
    )

    artifact = load_strategy_artifact("test_model")

    for key in ("model_id", "side", "strategy", "metrics"):
        assert key in artifact, f"Missing key: {key}"
