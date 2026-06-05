# =============================================================================
# Asset configuration tests
# =============================================================================
# Purpose:
#  - Verify asset-aware config helpers preserve the BCH default
#  - Verify explicit SOLUSDT asset resolution uses dedicated paths and tables
# =============================================================================

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import utils


def test_default_asset_config_matches_bch_db_config() -> None:
    db_cfg = utils.load_db_config()["database"]
    asset_cfg = utils.load_asset_config()["database"]

    assert asset_cfg["asset_id"] == "bchusdt_fw240"
    assert asset_cfg["db_path"] == db_cfg["db_path"]
    assert asset_cfg["symbol"] == db_cfg["symbol"]
    assert asset_cfg["tables"] == db_cfg["tables"]


def test_explicit_sol_asset_config_resolves_dev_database() -> None:
    asset_cfg = utils.load_asset_config("solusdt_fw60")["database"]

    assert asset_cfg["asset_id"] == "solusdt_fw60"
    assert asset_cfg["symbol"] == "SOLUSDT"
    assert asset_cfg["interval"] == "1m"
    assert asset_cfg["features_profile"] == "solusdt_fw60"
    assert asset_cfg["db_path"].endswith("database/solusdt_data_dev.db")
    assert asset_cfg["tables"] == {
        "ohlcv": "solusdt_1m",
        "features": "solusdt_1m_features",
        "predictions": "solusdt_1m_predictions",
    }


def test_unknown_asset_id_raises_clear_error() -> None:
    with pytest.raises(ValueError, match="Asset not found"):
        utils.load_asset_config("unknown_asset")


def test_feature_profiles_preserve_bch_and_add_sol_target() -> None:
    bch_cfg = utils.load_features_config("bchusdt_fw240")
    sol_cfg = utils.load_features_config("solusdt_fw60")

    bch_features = bch_cfg["database"]["features"]
    sol_features = sol_cfg["database"]["features"]

    assert bch_features["profile_id"] == "bchusdt_fw240"
    assert utils.target_columns_from_config(bch_cfg) == [
        "trg_l_fw240_q90",
        "trg_s_fw240_q10",
    ]

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

    assert sol_features["indicators"].keys() == bch_features["indicators"].keys()
