# =============================================================================
# src/schemas/data.py — Market data schemas
# =============================================================================
# Purpose:
#  - Define typed contracts for OHLCV bars, feature rows, and prediction rows
#  - Used as cross-module boundaries between data pipeline and modeling layers
# =============================================================================

from .base import ChronoBase


class OHLCVBar(ChronoBase):
    open_time : str    # UTC "YYYY-MM-DD HH:MM:SS"
    open      : float
    high      : float
    low       : float
    close     : float
    volume    : float
    asset_id  : str


class FeatureRow(ChronoBase):
    open_time : str
    asset_id  : str
    # Feature columns are dynamic; validated downstream per asset profile.
    # This schema covers the minimal required envelope.


class PredictionRow(ChronoBase):
    open_time   : str
    asset_id    : str
    model_id    : str
    probability : float
    side        : str  # "long" | "short"
