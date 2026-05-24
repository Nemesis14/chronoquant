# =============================================================================
# Modeling dataset and sampling tests
# =============================================================================
# Purpose:
#  - Verify model dataset loading is independent from model family
#  - Verify shared time-based CV splits are deterministic and non-overlapping
# =============================================================================

import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from modeling.datasets import load_modeling_dataset
from modeling.sampling import create_expanding_window_splits, validate_sample_definition


def test_load_modeling_dataset_from_features_table(tmp_path) -> None:
    db_path = tmp_path / "dataset.db"
    table_name = "features"
    df = pd.DataFrame(
        {
            "open_time": pd.date_range("2024-01-01", periods=5, freq="min").strftime("%Y-%m-%d %H:%M:%S"),
            "close": [1, 2, 3, 4, 5],
            "trg_l_rw_240_prc_09": [0, 1, 0, 1, 0],
            "feat_a": [0.1, 0.2, 0.3, 0.4, 0.5],
            "feat_b": [1.0, 1.1, 1.2, 1.3, 1.4],
        }
    )

    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, index=False, if_exists="replace")

    dataset = load_modeling_dataset(
        target_col = "trg_l_rw_240_prc_09",
        db_path    = str(db_path),
        table_name = table_name,
    )

    assert dataset.target_col == "trg_l_rw_240_prc_09"
    assert dataset.feature_cols == ["feat_a", "feat_b"]
    assert list(dataset.X.columns) == ["feat_a", "feat_b"]
    assert len(dataset.y) == 5


def test_create_expanding_window_splits_are_non_overlapping() -> None:
    sample = create_expanding_window_splits(
        sample_id              = "test_sample",
        data_start             = "2020-01-01 00:00:00",
        data_end               = "2024-01-01 00:00:00",
        target_horizon_minutes = 240,
        min_train_days         = 365,
        valid_days             = 90,
        step_days              = 90,
        test_days              = 365,
    )

    validate_sample_definition(sample)

    assert sample["sample_id"] == "test_sample"
    assert len(sample["folds"]) > 0
    assert sample["folds"][0]["train_start"] == "2020-01-01 00:00:00"
    assert sample["test"]["start"] == "2023-01-01 00:00:00"
