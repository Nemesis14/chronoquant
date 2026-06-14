# =============================================================================
# src/schemas/modeling.py — Model training and search schemas
# =============================================================================
# Purpose:
#  - Define typed contracts for training config, model metadata, and search results
#  - Used as cross-module boundary between modeling and evaluation layers
# =============================================================================

from typing import Any

from .base import ChronoBase


class TrainingConfig(ChronoBase):
    model_id    : str
    asset_id    : str
    target_name : str
    sample_id   : str
    row_stride  : int
    params      : dict[str, Any]


class ModelMetrics(ChronoBase):
    model_id         : str
    mean_valid_prauc : float | None = None
    mean_valid_ll    : float | None = None
    mean_gap         : float | None = None
    trial_no         : int | None   = None


class ModelMetadata(ChronoBase):
    model_id    : str
    asset_id    : str
    target_name : str
    family      : str
    variant     : str
    active      : bool
    metrics     : ModelMetrics | None = None


class SearchResult(ChronoBase):
    model_id         : str
    trial_no         : int
    params           : dict[str, Any]
    mean_valid_prauc : float | None = None
    mean_valid_ll    : float | None = None
    mean_gap         : float | None = None
