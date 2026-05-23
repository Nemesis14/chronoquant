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
    from app.ui import App
    from data_pipeline.sync_features import sync_features
    from data_pipeline.sync_predictions import sync_predictions
    from db.maintenance import rebuild_derived_tables
    from plotting.prediction_view import fetch_predictions_df

    assert App is not None
    assert sync_features is not None
    assert sync_predictions is not None
    assert rebuild_derived_tables is not None
    assert fetch_predictions_df is not None


def test_config_loads() -> None:
    import utils

    db_cfg = utils.load_db_config()
    model_cfg = utils.load_models_config()

    assert "database" in db_cfg
    assert "models" in model_cfg
