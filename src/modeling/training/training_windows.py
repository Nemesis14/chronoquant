# =============================================================================
# Shared training window helpers
# =============================================================================
# Purpose:
#  - Slice modeling datasets by persisted fold definitions
#  - Keep sample-size and final train/test selection model-independent
# =============================================================================

from dataclasses import dataclass

import pandas as pd

from modeling.training.datasets import ModelingDataset


# =============================================================================
# DatasetSplit
# =============================================================================
# Purpose:
#  - Hold feature and target matrices for a train/evaluation pair
# =============================================================================
@dataclass(frozen=True)
class DatasetSplit:
    X_train: pd.DataFrame
    y_train: pd.Series
    X_eval: pd.DataFrame
    y_eval: pd.Series


# =============================================================================
# fold_split(dataset: ModelingDataset, fold: dict) -> DatasetSplit
# =============================================================================
# Purpose:
#  - Return train/validation matrices for one persisted CV fold
# =============================================================================
def fold_split(dataset: ModelingDataset, fold: dict) -> DatasetSplit:
    train_mask = between(dataset.open_time, fold["train_start"], fold["train_end"])
    valid_mask = between(dataset.open_time, fold["valid_start"], fold["valid_end"])
    return DatasetSplit(
        X_train=dataset.X.loc[train_mask],
        y_train=dataset.y.loc[train_mask],
        X_eval=dataset.X.loc[valid_mask],
        y_eval=dataset.y.loc[valid_mask],
    )


# =============================================================================
# final_train_test_split(dataset: ModelingDataset, sample: dict) -> DatasetSplit
# =============================================================================
# Purpose:
#  - Return pre-test training data and final holdout test data
# =============================================================================
def final_train_test_split(dataset: ModelingDataset, sample: dict) -> DatasetSplit:
    test_start = sample["test"]["start"]
    test_end = sample["test"]["end"]
    train_mask = dataset.open_time < pd.to_datetime(test_start)
    test_mask = between(dataset.open_time, test_start, test_end)
    return DatasetSplit(
        X_train=dataset.X.loc[train_mask],
        y_train=dataset.y.loc[train_mask],
        X_eval=dataset.X.loc[test_mask],
        y_eval=dataset.y.loc[test_mask],
    )


# =============================================================================
# fold_sample_size_row(fold: dict, split: DatasetSplit) -> dict
# =============================================================================
# Purpose:
#  - Return sample-size metadata for CV reporting
# =============================================================================
def fold_sample_size_row(fold: dict, split: DatasetSplit) -> dict:
    return {
        "fold": fold["fold"],
        "train_start": fold["train_start"],
        "train_end": fold["train_end"],
        "valid_start": fold["valid_start"],
        "valid_end": fold["valid_end"],
        "train_n": int(len(split.y_train)),
        "train_positive_rate": float(split.y_train.mean()),
        "valid_n": int(len(split.y_eval)),
        "valid_positive_rate": float(split.y_eval.mean()),
    }


# =============================================================================
# between(series: pd.Series, start: str, end: str) -> pd.Series
# =============================================================================
# Purpose:
#  - Build inclusive timestamp mask
# =============================================================================
def between(series: pd.Series, start: str, end: str) -> pd.Series:
    return (series >= pd.to_datetime(start)) & (series <= pd.to_datetime(end))
