"""Sampling package — public exports.

Snapshot-native API (DuckDB-native path, plan section 5):
    from modeling.sampling import create_model_sample, create_snapshot_sample_train_valid_split

SQL builders and naming (IO-free, over an immutable snapshot):
    from modeling.sampling import build_train_valid_split_select_sql, build_train_valid_split_ctas_sql
    from modeling.sampling import build_feature_set_id
    from modeling.sampling import sample_table_fqn, snapshot_table_fqn, pred_table_fqn
"""

from .create_sample import create_model_sample, create_snapshot_sample_train_valid_split
from .snapshot_sampler import (
    build_feature_set_id,
    build_train_valid_split_ctas_sql,
    build_train_valid_split_select_sql,
    pred_table_fqn,
    sample_table_fqn,
    snapshot_table_fqn,
)

__all__ = [
    "create_model_sample",
    "create_snapshot_sample_train_valid_split",
    "build_feature_set_id",
    "build_train_valid_split_ctas_sql",
    "build_train_valid_split_select_sql",
    "pred_table_fqn",
    "sample_table_fqn",
    "snapshot_table_fqn",
]
