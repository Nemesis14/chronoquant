# =============================================================================
# Statsmodels p-value logistic regression training tests
# =============================================================================
# Purpose:
#  - Verify the legacy statsmodels workflow uses shared training artifacts
#  - Verify p-value filtering rounds are exposed as the tuning parameter
# =============================================================================

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from modeling import datasets
from modeling.sampling import save_sample_definition
from modeling.statsmodels_logreg import train_statsmodels_pvalue_logreg


def test_train_statsmodels_pvalue_logreg_writes_standard_artifacts(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "training.db"
    table_name = "features"
    periods = 180
    index = np.arange(periods)
    signal = np.sin(index / 6)
    target = (signal > 0.2).astype(int)
    df = pd.DataFrame(
        {
            "open_time": pd.date_range("2024-01-01", periods=periods, freq="min").strftime("%Y-%m-%d %H:%M:%S"),
            "trg_l_fw240_q90": target,
            "feat_signal": signal,
            "feat_noise_a": np.cos(index / 11),
            "feat_noise_b": (index % 13) / 13,
        }
    )

    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, index=False, if_exists="replace")

    monkeypatch.setattr(
        datasets.utils,
        "load_db_config",
        lambda: {"database": {"db_path": str(db_path), "tables": {"features": table_name}}},
    )

    sample_dir = tmp_path / "sample"
    sample = {
        "sample_id": "unit_sample",
        "split_type": "expanding_window",
        "target_horizon_minutes": 1,
        "embargo_minutes": 1,
        "data": {
            "start": "2024-01-01 00:00:00",
            "end": "2024-01-01 02:59:00",
        },
        "parameters": {},
        "folds": [
            {
                "fold": 1,
                "train_start": "2024-01-01 00:00:00",
                "train_end": "2024-01-01 01:19:00",
                "valid_start": "2024-01-01 01:25:00",
                "valid_end": "2024-01-01 02:09:00",
            }
        ],
        "test": {
            "start": "2024-01-01 02:20:00",
            "end": "2024-01-01 02:59:00",
        },
    }
    save_sample_definition(sample, sample_dir)

    output_dir = tmp_path / "model"
    result = train_statsmodels_pvalue_logreg(
        model_id      = "unit_statsmodels",
        target_col    = "trg_l_fw240_q90",
        sample_dir    = sample_dir,
        output_dir    = output_dir,
        p_threshold   = 0.05,
        pvalue_rounds = [0, 1, 2],
        row_stride    = 1,
        max_fit_iter  = 100,
        min_features  = 1,
    )

    assert result["trainer"] == "statsmodels_pvalue_logreg"
    assert result["tuning_param"] == "pvalue_rounds"
    assert result["best_tuning_value"] in {0, 1, 2}
    assert result["n_features_selected"] >= 1
    assert (output_dir / "model.pkl").exists()
    assert (output_dir / "features.json").exists()
    assert (output_dir / "metrics.json").exists()
    assert (output_dir / "cv_results.csv").exists()
    assert (output_dir / "report.html").exists()
