"""Smoke tests for sampling.config — SamplingConfig dataclass."""

import pytest

from modeling.quantitative.sampling.config import SamplingConfig

pytestmark = pytest.mark.smoke


def test_sampling_config_instantiation() -> None:
    cfg = SamplingConfig(
        sample_id="test_s1",
        asset_id="solusdt",
        target_col="long_mfe_fw60",
        target_horizon_minutes=60,
    )
    assert cfg.sample_id == "test_s1"
    assert cfg.asset_id == "solusdt"
    assert cfg.target_col == "long_mfe_fw60"
    assert cfg.target_horizon_minutes == 60
    assert cfg.min_train_days == 730
    assert cfg.valid_days == 180
    assert cfg.step_days == 180
    assert cfg.test_days == 365
    assert cfg.embargo_minutes is None


def test_sampling_config_frozen() -> None:
    cfg = SamplingConfig(
        sample_id="s1",
        asset_id="solusdt",
        target_col="long_mfe_fw60",
        target_horizon_minutes=60,
    )
    with pytest.raises(AttributeError):
        cfg.sample_id = "changed"  # type: ignore[misc]


def test_sampling_config_custom_defaults() -> None:
    cfg = SamplingConfig(
        sample_id="s2",
        asset_id="solusdt",
        target_col="short_mfe_fw60",
        target_horizon_minutes=60,
        min_train_days=365,
        valid_days=90,
        step_days=90,
        test_days=180,
        embargo_minutes=120,
    )
    assert cfg.min_train_days == 365
    assert cfg.embargo_minutes == 120
