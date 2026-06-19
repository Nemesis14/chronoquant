"""Sampling package — public exports.

Yearly API (new):
    from modeling.sampling import YearlySamplingConfig, create_yearly_sample
    from modeling.sampling import load_yearly_sample

Legacy API (kept for lightgbm_model.py / lgbm_search.py):
    from modeling.sampling import load_sample_definition, validate_sample_definition
"""

from .artifacts import (
    load_sample_definition,
    load_yearly_sample,
    validate_sample_definition,
    write_yearly_artifacts,
)
from .config import YearlySamplingConfig
from .create_sample import create_yearly_sample
from .yearly_sampler import (
    assign_segments,
    select_hourly_observations,
    select_monthly_validation_weeks,
)

__all__ = [
    # yearly
    "YearlySamplingConfig",
    "create_yearly_sample",
    "load_yearly_sample",
    "write_yearly_artifacts",
    "select_hourly_observations",
    "select_monthly_validation_weeks",
    "assign_segments",
    # legacy
    "load_sample_definition",
    "validate_sample_definition",
]
