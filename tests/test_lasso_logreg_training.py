# =============================================================================
# Lasso logistic regression training tests
# =============================================================================
# Purpose:
#  - Verify the sklearn Lasso trainer writes model artifacts and HTML report
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
from modeling.lasso_logreg import train_lasso_logreg
from modeling.sampling import save_sample_definition


def test_train_lasso_logreg_writes_artifacts(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "training.db"
    table_name = "features"
    periods = 120
    index = np.arange(periods)
    target = ((index % 5) == 0).astype(int)
    df = pd.DataFrame(
        {
            "open_time": pd.date_range("2024-01-01", periods=periods, freq="min").strftime("%Y-%m-%d %H:%M:%S"),
            "trg_l_rw_240_prc_09": target,
            "feat_signal": target + (index / 1000),
            "feat_noise": np.sin(index),
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
            "end": "2024-01-01 01:59:00",
        },
        "parameters": {},
        "folds": [
            {
                "fold": 1,
                "train_start": "2024-01-01 00:00:00",
                "train_end": "2024-01-01 00:49:00",
                "valid_start": "2024-01-01 00:55:00",
                "valid_end": "2024-01-01 01:19:00",
            }
        ],
        "test": {
            "start": "2024-01-01 01:30:00",
            "end": "2024-01-01 01:59:00",
        },
    }
    save_sample_definition(sample, sample_dir)

    output_dir = tmp_path / "model"
    result = train_lasso_logreg(
        model_id     = "unit_lasso",
        target_col   = "trg_l_rw_240_prc_09",
        sample_dir   = sample_dir,
        output_dir   = output_dir,
        alphas       = [0.1, 1.0],
        class_weight = None,
        max_iter     = 500,
        random_state = 42,
    )

    assert result["model_id"] == "unit_lasso"
    assert result["best_alpha"] in {0.1, 1.0}
    assert (output_dir / "model.pkl").exists()
    assert (output_dir / "features.json").exists()
    assert (output_dir / "metrics.json").exists()
    assert (output_dir / "cv_results.csv").exists()
    assert (output_dir / "report.html").exists()
