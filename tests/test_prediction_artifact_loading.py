# =============================================================================
# Prediction artifact loading tests
# =============================================================================
# Purpose:
#  - Verify prediction feature resolution supports legacy and pipeline artifacts
# =============================================================================

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from data_pipeline.sync_predictions import _feature_list_for_prediction


class DummyPipelineModel:
    feature_names_in_ = np.array(["feat_a", "feat_b", "feat_c"])


def test_prediction_features_prefer_explicit_input_features() -> None:
    features_data = {
        "features": ["feat_a"],
        "input_features": ["feat_a", "feat_b"],
    }

    result = _feature_list_for_prediction(features_data, DummyPipelineModel())

    assert result == ["feat_a", "feat_b"]


def test_prediction_features_use_model_feature_names_for_pipeline_artifact() -> None:
    features_data = {
        "features": ["feat_a"],
    }

    result = _feature_list_for_prediction(features_data, DummyPipelineModel())

    assert result == ["feat_a", "feat_b", "feat_c"]


def test_prediction_features_keep_legacy_list_payload() -> None:
    result = _feature_list_for_prediction(["feat_a", "feat_b"], object())

    assert result == ["feat_a", "feat_b"]


def test_prediction_features_use_selected_features_for_statsmodels() -> None:
    features_data = {
        "features": ["feat_selected"],
        "input_features": ["feat_a", "feat_b", "feat_selected"],
    }

    result = _feature_list_for_prediction(
        features_data,
        DummyPipelineModel(),
        trainer="statsmodels_pvalue_logreg",
    )

    assert result == ["feat_selected"]
