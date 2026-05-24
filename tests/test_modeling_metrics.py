# =============================================================================
# Modeling metrics tests
# =============================================================================
# Purpose:
#  - Verify shared metric definitions on deterministic toy data
# =============================================================================

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from modeling.metrics import binary_classification_metrics, lift_at_percentiles


def test_binary_classification_metrics_for_good_predictions() -> None:
    y_true = [0, 0, 0, 1, 1, 1]
    y_pred = [0.05, 0.10, 0.20, 0.70, 0.85, 0.95]

    metrics = binary_classification_metrics(y_true, y_pred)

    assert metrics["n"] == 6
    assert metrics["positive_count"] == 3
    assert metrics["positive_rate"] == 0.5
    assert metrics["roc_auc"] == 1.0
    assert metrics["pr_auc"] == 1.0
    assert metrics["brier_score"] < 0.06
    assert metrics["lift"]["top_10pct"]["event_rate"] == 1.0
    assert len(metrics["calibration"]) > 0


def test_lift_at_percentiles_uses_top_predictions() -> None:
    y_true = [0, 1, 0, 1, 0, 1, 0, 1, 0, 1]
    y_pred = [0.1, 0.9, 0.2, 0.8, 0.3, 0.7, 0.4, 0.6, 0.05, 0.95]

    lift = lift_at_percentiles(y_true, y_pred, percentiles=(0.2,))

    assert lift["top_20pct"]["top_n"] == 2
    assert lift["top_20pct"]["event_rate"] == 1.0
    assert lift["top_20pct"]["lift"] == 2.0


def test_single_class_auc_metrics_return_none() -> None:
    y_true = [0, 0, 0, 0]
    y_pred = [0.1, 0.2, 0.3, 0.4]

    metrics = binary_classification_metrics(y_true, y_pred)

    assert metrics["roc_auc"] is None
    assert metrics["pr_auc"] == 0.0
    assert metrics["positive_rate"] == 0.0
    assert metrics["lift"]["top_10pct"]["lift"] is None
