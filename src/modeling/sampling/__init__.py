"""Sampling package — public exports.

Snapshot-native API (DuckDB-native path, plan section 5):
    from modeling.sampling import create_model_sample, create_snapshot_sample
    from modeling.sampling import WalkForwardSamplingConfig
    from modeling.sampling import generate_walk_forward_folds, assign_walk_forward_fold_ids

SQL builders (IO-free, over an immutable snapshot):
    from modeling.sampling import build_sample_select_sql, build_sample_ctas_sql
    from modeling.sampling import build_fold_case_sql, build_feature_set_id
    from modeling.sampling import sample_table_fqn, snapshot_table_fqn

Pure helpers (no IO):
    from modeling.sampling import select_hourly_observations, assign_fold_ids

Legacy artifact IO (kept for backward compatibility — expanding-window format):
    from modeling.sampling import load_sample_definition, validate_sample_definition
"""

from .artifacts import (
    load_sample_definition,
    load_yearly_sample,
    validate_sample_definition,
    write_yearly_artifacts,
)
from .config import WalkForwardSamplingConfig, YearlySamplingConfig
from .create_sample import create_model_sample, create_snapshot_sample
from .snapshot_sampler import (
    build_feature_set_id,
    build_fold_case_sql,
    build_sample_ctas_sql,
    build_sample_select_sql,
    pred_table_fqn,
    sample_table_fqn,
    snapshot_table_fqn,
)
from .yearly_sampler import (
    assign_fold_ids,
    assign_walk_forward_fold_ids,
    generate_walk_forward_folds,
    select_hourly_observations,
)

__all__ = [
    # config
    "WalkForwardSamplingConfig",
    "YearlySamplingConfig",
    # snapshot-native orchestration
    "create_model_sample",
    "create_snapshot_sample",
    # SQL builders
    "build_feature_set_id",
    "build_fold_case_sql",
    "build_sample_ctas_sql",
    "build_sample_select_sql",
    "pred_table_fqn",
    "sample_table_fqn",
    "snapshot_table_fqn",
    # pure helpers
    "select_hourly_observations",
    "assign_fold_ids",
    "generate_walk_forward_folds",
    "assign_walk_forward_fold_ids",
    # legacy artifact IO
    "load_yearly_sample",
    "write_yearly_artifacts",
    "load_sample_definition",
    "validate_sample_definition",
]
