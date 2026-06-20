"""Sampling package — public exports.

Yearly API (new):
    from modeling.sampling import YearlySamplingConfig, create_yearly_sample
    from modeling.sampling import load_yearly_sample

Walk-forward API (new):
    from modeling.sampling import WalkForwardSamplingConfig
    from modeling.sampling import create_walk_forward_sample, create_model_walk_forward_sample
    from modeling.sampling import generate_walk_forward_folds, assign_walk_forward_fold_ids

Legacy API (kept for backward compatibility):
    from modeling.sampling import load_sample_definition, validate_sample_definition
"""

from .artifacts import (
    load_sample_definition,
    load_yearly_sample,
    validate_sample_definition,
    write_yearly_artifacts,
)
from .config import WalkForwardSamplingConfig, YearlySamplingConfig
from .create_sample import (
    create_model_sample,
    create_model_walk_forward_sample,
    create_walk_forward_sample,
    create_yearly_sample,
)
from .yearly_sampler import (
    assign_fold_ids,
    assign_walk_forward_fold_ids,
    generate_walk_forward_folds,
    select_hourly_observations,
)

__all__ = [
    # yearly
    "YearlySamplingConfig",
    "create_model_sample",
    "create_yearly_sample",
    "load_yearly_sample",
    "write_yearly_artifacts",
    "select_hourly_observations",
    "assign_fold_ids",
    # walk-forward
    "WalkForwardSamplingConfig",
    "create_walk_forward_sample",
    "create_model_walk_forward_sample",
    "generate_walk_forward_folds",
    "assign_walk_forward_fold_ids",
    # legacy
    "load_sample_definition",
    "validate_sample_definition",
]
