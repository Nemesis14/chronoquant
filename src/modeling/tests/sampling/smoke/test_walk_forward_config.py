"""Smoke tests for walk-forward sampling config metadata handling."""

import pytest

from modeling.sampling.config import WalkForwardSamplingConfig

pytestmark = pytest.mark.smoke


def test_walk_forward_config_n_folds_default() -> None:
    """WalkForwardSamplingConfig instantiates with default n_folds=4."""
    cfg = WalkForwardSamplingConfig(
        sample_id="test_fw_2021",
        asset_id="solusdt",
        year=2021,
        seed=63,
    )
    assert cfg.n_folds == 4


def test_walk_forward_config_n_folds_explicit() -> None:
    """WalkForwardSamplingConfig accepts explicit n_folds value."""
    cfg = WalkForwardSamplingConfig(
        sample_id="test_fw_2023",
        asset_id="solusdt",
        year=2023,
        seed=65,
        train_months=9,
        valid_months=8,
        shift_months=8,
        n_folds=4,
    )
    assert cfg.n_folds == 4
    assert cfg.train_months == 9
    assert cfg.valid_months == 8
    assert cfg.shift_months == 8


def test_walk_forward_config_frozen() -> None:
    """WalkForwardSamplingConfig is immutable (frozen dataclass)."""
    cfg = WalkForwardSamplingConfig(
        sample_id="test_fw",
        asset_id="solusdt",
        year=2021,
        seed=63,
    )
    with pytest.raises(Exception):
        cfg.n_folds = 99  # type: ignore[misc]


def test_year_read_from_sampling_meta_explicit() -> None:
    """When meta['sampling']['year'] is present, year is read directly."""
    sampling_meta = {
        "sample_id": "solusdt_fw60_2101_2605",
        "year": 2023,
        "train_months": 9,
        "valid_months": 8,
        "shift_months": 8,
        "n_folds": 4,
    }

    sample_id = sampling_meta["sample_id"]
    if "year" in sampling_meta:
        year = int(sampling_meta["year"])
    elif "_yearly_" in sample_id:
        year = int(sample_id.split("_yearly_")[-1])
    else:
        year = int(sample_id.split("_")[-1])

    assert year == 2023


def test_year_fallback_yearly_sample_id() -> None:
    """Backward compat: when no 'year' key, parse _yearly_ suffix."""
    sampling_meta = {
        "sample_id": "solusdt_yearly_2021",
    }

    sample_id = sampling_meta["sample_id"]
    if "year" in sampling_meta:
        year = int(sampling_meta["year"])
    elif "_yearly_" in sample_id:
        year = int(sample_id.split("_yearly_")[-1])
    else:
        year = int(sample_id.split("_")[-1])

    assert year == 2021


def test_year_fallback_generic_sample_id() -> None:
    """Backward compat: when no 'year' and no _yearly_, parse last _ segment."""
    sampling_meta = {
        "sample_id": "solusdt_fw60_2021",
    }

    sample_id = sampling_meta["sample_id"]
    if "year" in sampling_meta:
        year = int(sampling_meta["year"])
    elif "_yearly_" in sample_id:
        year = int(sample_id.split("_yearly_")[-1])
    else:
        year = int(sample_id.split("_")[-1])

    assert year == 2021


def test_n_folds_read_from_sampling_meta() -> None:
    """n_folds is read from sampling meta with default fallback of 4."""
    sampling_meta_with: dict = {
        "n_folds": 6,
        "train_months": 9,
        "valid_months": 8,
        "shift_months": 8,
    }
    sampling_meta_without: dict = {
        "train_months": 9,
        "valid_months": 8,
        "shift_months": 8,
    }

    n_folds_with = int(sampling_meta_with.get("n_folds", 4))
    n_folds_without = int(sampling_meta_without.get("n_folds", 4))

    assert n_folds_with == 6
    assert n_folds_without == 4


def test_champion_model_config_builds_correct_wf_config() -> None:
    """Champion model sampling meta builds WalkForwardSamplingConfig with correct params."""
    sampling_meta = {
        "sample_id": "solusdt_fw60_2101_2605",
        "year": 2023,
        "train_months": 9,
        "valid_months": 8,
        "shift_months": 8,
        "n_folds": 4,
    }

    sample_id = sampling_meta["sample_id"]
    if "year" in sampling_meta:
        year = int(sampling_meta["year"])
    elif "_yearly_" in sample_id:
        year = int(sample_id.split("_yearly_")[-1])
    else:
        year = int(sample_id.split("_")[-1])

    seed = 42 + year

    cfg = WalkForwardSamplingConfig(
        sample_id=sample_id,
        asset_id="solusdt",
        year=year,
        seed=seed,
        train_months=int(sampling_meta.get("train_months", 9)),
        valid_months=int(sampling_meta.get("valid_months", 3)),
        shift_months=int(sampling_meta.get("shift_months", 3)),
        n_folds=int(sampling_meta.get("n_folds", 4)),
        target_cols=("long_mfe_fw60",),
        feature_cols=(),
    )

    assert cfg.year == 2023
    assert cfg.seed == 2065
    assert cfg.train_months == 9
    assert cfg.valid_months == 8
    assert cfg.shift_months == 8
    assert cfg.n_folds == 4
    assert cfg.sample_id == "solusdt_fw60_2101_2605"
