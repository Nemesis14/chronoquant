"""Smoke and sanity tests for sampling.splits — build_expanding_window_splits."""

import pytest

from modeling.quantitative.sampling.splits import build_expanding_window_splits

pytestmark = pytest.mark.smoke

DATA_START = "2020-01-01 00:00:00"
DATA_END   = "2024-12-31 23:59:00"


def test_splits_returns_dict() -> None:
    result = build_expanding_window_splits(
        data_start=DATA_START,
        data_end=DATA_END,
        min_train_days=730,
        valid_days=180,
        step_days=180,
        test_days=365,
    )
    assert isinstance(result, dict)
    assert "folds" in result
    assert "test" in result


def test_splits_folds_are_1indexed() -> None:
    result = build_expanding_window_splits(
        data_start=DATA_START,
        data_end=DATA_END,
        min_train_days=730,
        valid_days=180,
        step_days=180,
        test_days=365,
    )
    folds = result["folds"]
    assert len(folds) >= 1
    assert folds[0]["fold"] == 1
    for i, fold in enumerate(folds):
        assert fold["fold"] == i + 1


def test_splits_fold_keys_present() -> None:
    result = build_expanding_window_splits(
        data_start=DATA_START,
        data_end=DATA_END,
        min_train_days=730,
        valid_days=180,
        step_days=180,
        test_days=365,
    )
    required_keys = {"fold", "train_start", "train_end", "valid_start", "valid_end"}
    for fold in result["folds"]:
        assert required_keys <= set(fold.keys())


def test_splits_test_keys_present() -> None:
    result = build_expanding_window_splits(
        data_start=DATA_START,
        data_end=DATA_END,
        min_train_days=730,
        valid_days=180,
        step_days=180,
        test_days=365,
    )
    assert "start" in result["test"]
    assert "end" in result["test"]


def test_splits_chronological_order() -> None:
    from datetime import datetime

    result = build_expanding_window_splits(
        data_start=DATA_START,
        data_end=DATA_END,
        min_train_days=730,
        valid_days=180,
        step_days=180,
        test_days=365,
    )
    test_start = datetime.fromisoformat(result["test"]["start"])
    for fold in result["folds"]:
        ts = datetime.fromisoformat(fold["train_start"])
        te = datetime.fromisoformat(fold["train_end"])
        vs = datetime.fromisoformat(fold["valid_start"])
        ve = datetime.fromisoformat(fold["valid_end"])
        assert ts < te < vs < ve < test_start


def test_splits_raises_on_inverted_range() -> None:
    with pytest.raises(ValueError, match="data_end must be after data_start"):
        build_expanding_window_splits(
            data_start="2024-01-01 00:00:00",
            data_end="2020-01-01 00:00:00",
            min_train_days=730,
            valid_days=180,
            step_days=180,
            test_days=365,
        )


def test_splits_raises_when_no_folds_fit() -> None:
    with pytest.raises(ValueError, match="No folds generated"):
        build_expanding_window_splits(
            data_start="2023-01-01 00:00:00",
            data_end="2023-12-31 23:59:00",
            min_train_days=730,
            valid_days=180,
            step_days=180,
            test_days=365,
        )
