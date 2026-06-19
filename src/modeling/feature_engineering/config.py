"""FeatureEngineeringConfig — thresholds and runtime parameters for all analysis steps."""

from dataclasses import dataclass


@dataclass(frozen=True)
class FeatureEngineeringConfig:
    """Immutable configuration for the feature engineering analysis layer.

    Thresholds are grouped by analysis step: quality, target relation, redundancy,
    and stability.  All analyses read from the `quant_train` DuckDB table.

    Args:
        asset_id              : Asset key from config/assets.json (e.g. 'solusdt').
        run_id                : Unique identifier for this analysis run; becomes the
                                output subdirectory name under
                                database/<asset_id>/feature_engineering/<run_id>/.
        target_cols           : Target columns in quant_train to analyse against.
        max_null_rate         : Drop feature if null fraction exceeds this value.
        max_inf_rate          : Drop feature if infinite-value fraction exceeds this.
        min_variance          : Drop feature if variance is below this threshold.
        max_outlier_ratio     : Flag for review if outlier fraction exceeds this.
        min_spearman_abs      : Flag as weak signal if |Spearman ρ| with either target
                                is below this across the full dataset.
        max_spearman_leakage  : Flag leakage suspicion if |Spearman ρ| with a target
                                exceeds this value.
        pearson_cluster_thr   : Pearson |r| threshold for grouping redundant features.
        redundancy_max_rows   : Row cap for loading feat_* into numpy for correlation
                                matrix computation.  Sampling keeps RAM under control
                                without affecting accuracy at the 0.95 threshold.
        stability_bucket_days : Number of days per rolling time bucket for stability checks.
        max_drift_threshold   : Flag as unstable if mean-normalised drift across buckets
                                exceeds this value for either target correlation.
    """

    asset_id              : str
    run_id                : str
    target_cols           : tuple[str, ...] = ("long_mfe_fw60", "short_mfe_fw60")

    # --- quality ---
    max_null_rate         : float = 0.01
    max_inf_rate          : float = 0.001
    min_variance          : float = 1e-8
    max_outlier_ratio     : float = 0.05

    # --- target relation ---
    min_spearman_abs      : float = 0.01
    max_spearman_leakage  : float = 0.95

    # --- redundancy ---
    pearson_cluster_thr   : float = 0.95
    redundancy_max_rows   : int   = 50_000

    # --- stability ---
    stability_bucket_days : int   = 90
    max_drift_threshold   : float = 0.30
