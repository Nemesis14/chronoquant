"""Feature engineering analysis layer for ChronoQuant.

Analyses feat_* columns in the quant_train DuckDB table across four
independent dimensions and produces a feature_set.json that downstream
sampling and training can consume without manual editing.

Typical usage:
    from modeling.feature_engineering import (
        FeatureEngineeringConfig,
        analyze_quality,
        analyze_target_relation,
        analyze_redundancy,
        analyze_stability,
        generate_outputs,
    )

Analysis steps (implemented by analyst_agent in t106–t110):
    quality        — univariate null / variance / outlier checks
    target_relation— Pearson and Spearman signal strength vs. fw60 targets
    redundancy     — high-correlation cluster detection and rep selection
    stability      — rolling-bucket drift and decay flags
    reporting      — merge results → analyst_report.md + feature_set.json
"""

from .config import FeatureEngineeringConfig
from .quality import analyze_quality
from .redundancy import analyze_redundancy
from .reporting import generate_outputs
from .stability import analyze_stability
from .target_relation import analyze_target_relation

__all__ = [
    "FeatureEngineeringConfig",
    "analyze_quality",
    "analyze_target_relation",
    "analyze_redundancy",
    "analyze_stability",
    "generate_outputs",
]
