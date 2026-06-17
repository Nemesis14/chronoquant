"""Smoke tests for sampling.artifacts — write, load, validate."""

import json
from pathlib import Path

import pytest

from modeling.quantitative.sampling.artifacts import (
    load_sample_definition,
    validate_sample_definition,
    write_sample_artifacts,
)

pytestmark = pytest.mark.smoke

_FOLDS = {
    "folds": [
        {
            "fold": 1,
            "train_start": "2020-01-01 00:00:00",
            "train_end": "2021-12-31 23:59:00",
            "valid_start": "2022-01-01 00:00:00",
            "valid_end": "2022-06-29 23:59:00",
        }
    ],
    "test": {
        "start": "2023-01-01 00:00:00",
        "end": "2023-12-31 23:59:00",
    },
}

_METADATA = {
    "sample_id": "test_s1",
    "asset_id": "solusdt",
    "source": {
        "db_relative_path": "database/solusdt/solusdt.duckdb",
        "feature_table": "feat_ohlcv_quant",
        "target_table": "target",
    },
}

_AUDIT = {"data_start_safe": "2020-01-01 00:00:00", "data_end_safe": "2023-12-31 23:59:00"}


def test_write_creates_three_files(tmp_path: Path) -> None:
    write_sample_artifacts(tmp_path, _METADATA, _FOLDS, _AUDIT)
    assert (tmp_path / "metadata.json").exists()
    assert (tmp_path / "folds.json").exists()
    assert (tmp_path / "audit.json").exists()


def test_write_creates_dir_if_missing(tmp_path: Path) -> None:
    target = tmp_path / "new_subdir"
    assert not target.exists()
    write_sample_artifacts(target, _METADATA, _FOLDS, _AUDIT)
    assert target.exists()


def test_write_injects_generated_at(tmp_path: Path) -> None:
    write_sample_artifacts(tmp_path, _METADATA, _FOLDS, _AUDIT)
    meta = json.loads((tmp_path / "metadata.json").read_text())
    assert "generated_at" in meta


def test_write_source_is_relative_path(tmp_path: Path) -> None:
    write_sample_artifacts(tmp_path, _METADATA, _FOLDS, _AUDIT)
    meta = json.loads((tmp_path / "metadata.json").read_text())
    db_path = meta["source"]["db_relative_path"]
    assert not Path(db_path).is_absolute()


def test_load_returns_merged_dict(tmp_path: Path) -> None:
    write_sample_artifacts(tmp_path, _METADATA, _FOLDS, _AUDIT)
    sample = load_sample_definition(tmp_path)
    assert "folds" in sample
    assert "test" in sample
    assert "sample_id" in sample
    assert isinstance(sample["folds"], list)


def test_load_raises_when_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_sample_definition(tmp_path / "nonexistent")


def test_validate_passes_on_valid_sample(tmp_path: Path) -> None:
    write_sample_artifacts(tmp_path, _METADATA, _FOLDS, _AUDIT)
    sample = load_sample_definition(tmp_path)
    validate_sample_definition(sample)


def test_validate_raises_on_inverted_test(tmp_path: Path) -> None:
    bad_folds = {**_FOLDS, "test": {"start": "2024-01-01 00:00:00", "end": "2023-01-01 00:00:00"}}
    write_sample_artifacts(tmp_path, _METADATA, bad_folds, _AUDIT)
    sample = load_sample_definition(tmp_path)
    with pytest.raises(ValueError):
        validate_sample_definition(sample)
