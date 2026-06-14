# =============================================================================
# Asset configuration tests
# =============================================================================
# Purpose:
#  - Verify SOLUSDT asset config resolves correct paths, tables, and features
# =============================================================================

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import utils


def test_sol_asset_config_resolves_dev_database() -> None:
    asset_cfg = utils.load_asset_config("solusdt_fw60")["database"]

    assert asset_cfg["asset_id"] == "solusdt_fw60"
    assert asset_cfg["symbol"] == "SOLUSDT"
    assert asset_cfg["interval"] == "1m"
    assert asset_cfg["features_profile"] == "solusdt_fw60"
    assert "data_dir" in asset_cfg


def test_unknown_asset_id_raises_clear_error() -> None:
    with pytest.raises(ValueError, match="Asset not found"):
        utils.load_asset_config("unknown_asset")


def test_sol_feature_profile_has_correct_targets() -> None:
    sol_cfg = utils.load_features_config("solusdt_fw60")
    sol_features = sol_cfg["database"]["features"]

    assert sol_features["profile_id"] == "solusdt_fw60"
    assert utils.target_columns_from_config(sol_cfg) == [
        "trg_l_fw60_q90",
        "trg_s_fw60_q10",
    ]
    assert sol_features["targets"][0] == {
        "direction": "long",
        "name": "trg_l_fw60_q90",
        "rolling_window": 60,
        "percentile": 0.9,
    }
    assert sol_features["targets"][1] == {
        "direction": "short",
        "name": "trg_s_fw60_q10",
        "rolling_window": 60,
        "percentile": 0.1,
    }


def test_sol_feature_profile_has_expected_indicator_groups() -> None:
    sol_cfg = utils.load_features_config("solusdt_fw60")
    sol_keys = set(sol_cfg["database"]["features"]["indicators"].keys())

    expected_groups = {
        "activity", "return_distance", "regime_rank",
        "candle_shape", "trend_slope", "interaction", "time_session",
    }
    assert expected_groups.issubset(sol_keys), (
        f"SOL missing expected feature groups: {expected_groups - sol_keys}"
    )
