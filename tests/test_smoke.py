# =============================================================================
# Smoke tests for the repository layout
# =============================================================================
# Purpose:
#  - Verify core modules import after structural refactors
#  - Verify JSON config loading works
# =============================================================================

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))


def test_core_imports() -> None:
    from db.maintenance import rebuild_derived_tables

    from data_pipeline.sync_features import sync_features
    from data_pipeline.sync_predictions import sync_predictions
    from streamlit_app.data import latest_prediction

    assert sync_features is not None
    assert sync_predictions is not None
    assert rebuild_derived_tables is not None
    assert latest_prediction is not None


def test_config_loads() -> None:
    import utils

    db_cfg = utils.load_db_config()
    model_cfg = utils.load_models_config()
    predictions_cfg = utils.load_predictions_config()
    strategies_cfg = utils.load_strategies_config()

    assert "database" in db_cfg
    assert "models" in model_cfg
    assert "live_predictions" in predictions_cfg
    assert "strategies" in strategies_cfg
