# =============================================================================
# Modeling report tests
# =============================================================================
# Purpose:
#  - Verify shared report helper calculations on deterministic toy data
# =============================================================================

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from modeling.reports import validation_calibration_summary


def test_validation_calibration_summary_uses_equal_count_bins() -> None:
    predictions = pd.DataFrame(
        {
            "y_true": [0, 0, 1, 1, 0, 1, 0, 1],
            "y_pred": [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80],
        }
    )

    summary = validation_calibration_summary(predictions, n_bins=4)

    assert summary["n"].tolist() == [2, 2, 2, 2]
    assert summary["bin"].tolist() == [1, 2, 3, 4]
    assert summary["baseline_target_rate"].iloc[0] == 0.5
    assert summary["target_rate"].tolist() == [0.0, 1.0, 0.5, 0.5]
