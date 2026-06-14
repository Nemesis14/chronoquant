# =============================================================================
# LightGBM training tests
# =============================================================================
# Purpose:
#  - Verify the LightGBM trainer uses shared splits, metrics, artifacts, and report
# =============================================================================

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from modeling import datasets
from modeling.lightgbm_model import train_lightgbm_binary
from modeling.sampling import save_sample_definition


def test_train_lightgbm_binary_writes_standard_artifacts(tmp_path, monkeypatch) -> None:
    import duckdb
    data_dir = tmp_path / "data"
    db_path  = Path(data_dir).with_suffix(".duckdb")
    periods = 160
    index = np.arange(periods)
    target = ((index % 7) < 2).astype(int)
    df = pd.DataFrame(
        {
            "open_time":       pd.date_range("2024-01-01", periods=periods, freq="min"),
            "trg_l_fw240_q90": target.astype(float),
            "feat_signal":     target + (index / 1000),
            "feat_noise":      np.sin(index),
            "feat_trend":      index / periods,
        }
    )
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE features AS SELECT * FROM df")
    con.close()

    monkeypatch.setattr(
        datasets.utils,
        "load_asset_config",
        lambda asset_id=None: {"database": {"data_dir": str(data_dir)}},
    )
    monkeypatch.setattr(
        "modeling.lightgbm_model.utils.load_model_params_config",
        lambda: {
            "unit_lightgbm": {
                "tuning": {
                    "primary_param": "num_leaves",
                    "values": [3, 7],
                },
                "params": {
                    "objective": "binary",
                    "boosting_type": "gbdt",
                    "n_estimators": 20,
                    "learning_rate": 0.1,
                    "max_depth": 3,
                    "min_child_samples": 2,
                    "subsample": 1.0,
                    "colsample_bytree": 1.0,
                    "reg_alpha": 0.0,
                    "reg_lambda": 0.0,
                    "n_jobs": 1,
                    "verbosity": -1,
                },
            }
        },
    )

    sample_dir = tmp_path / "sample"
    sample = {
        "sample_id": "unit_sample",
        "split_type": "expanding_window",
        "target_horizon_minutes": 1,
        "embargo_minutes": 1,
        "data": {
            "start": "2024-01-01 00:00:00",
            "end": "2024-01-01 02:39:00",
        },
        "parameters": {},
        "folds": [
            {
                "fold": 1,
                "train_start": "2024-01-01 00:00:00",
                "train_end": "2024-01-01 01:09:00",
                "valid_start": "2024-01-01 01:15:00",
                "valid_end": "2024-01-01 01:59:00",
            }
        ],
        "test": {
            "start": "2024-01-01 02:10:00",
            "end": "2024-01-01 02:39:00",
        },
    }
    save_sample_definition(sample, sample_dir)

    output_dir = tmp_path / "model"
    result = train_lightgbm_binary(
        model_id      = "unit_lgbm",
        target_col    = "trg_l_fw240_q90",
        sample_dir    = sample_dir,
        output_dir    = output_dir,
        param_profile = "unit_lightgbm",
        row_stride    = 1,
        random_state  = 42,
    )

    assert result["trainer"] == "lightgbm_binary"
    assert result["tuning_param"] == "num_leaves"
    assert result["best_tuning_value"] in {3, 7}
    assert result["n_features_input"] == 3
    assert (output_dir / "model.pkl").exists()
    assert (output_dir / "features.json").exists()
    assert (output_dir / "metrics.json").exists()
    assert (output_dir / "params.json").exists()
    assert (output_dir / "cv_results.csv").exists()
    assert (output_dir / "report.html").exists()
