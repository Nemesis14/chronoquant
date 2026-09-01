"""Apriori cluster classification: LONG/SHORT/NEUTRAL using paired t-test + Bonferroni.

Uses training period cluster profiles to statistically determine each cluster's
directional bias (LONG vs SHORT) with Bonferroni-corrected significance.

Methodology:
- Paired t-test: H0: long_mfe == short_mfe
- Bonferroni correction: alpha = 0.05 / 10 clusters = 0.005
- Critical t-value ≈ 2.807 (for large df, two-tailed)
"""

from __future__ import annotations

import json
import logging

import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)

# Configuration
JSON_INPUT = "artifacts/research_cluster_regime/cluster_profile_train_vs_oos_all_clusters.json"
OUTPUT_JSON = "artifacts/research_cluster_regime/apriori_cluster_classification.json"

# Statistical parameters
ALPHA_BASE = 0.05
N_TESTS = 10  # Number of clusters
ALPHA_CORRECTED = ALPHA_BASE / N_TESTS  # 0.005
# Critical t-value for two-tailed test at alpha=0.005, large df ~2.807
# We'll compute exact values per cluster based on df


def classify_cluster(
    cluster_id: int,
    n_obs: int,
    long_mfe_mean: float,
    long_mfe_std: float,
    short_mfe_mean: float,
    short_mfe_std: float,
    alpha_corrected: float = ALPHA_CORRECTED,
) -> dict:
    """Classify cluster as LONG/SHORT/NEUTRAL using t-test with Bonferroni correction."""

    # Handle missing data
    if n_obs == 0 or np.isnan(long_mfe_mean) or np.isnan(short_mfe_mean):
        return {
            "n_obs": n_obs,
            "long_mfe_mean": long_mfe_mean,
            "short_mfe_mean": short_mfe_mean,
            "t_stat": np.nan,
            "p_value": np.nan,
            "t_critical": np.nan,
            "significant_bonferroni": False,
            "assignment": "MISSING",
        }

    # Compute pooled standard error
    pooled_std = np.sqrt((long_mfe_std**2 + short_mfe_std**2) / 2.0)
    se = pooled_std * np.sqrt(2.0 / n_obs)

    # T-statistic (comparing long_mfe to short_mfe)
    if se == 0:
        t_stat = 0.0 if long_mfe_mean == short_mfe_mean else np.inf
    else:
        t_stat = (long_mfe_mean - short_mfe_mean) / se

    # Degrees of freedom
    df = max(1, n_obs - 1)

    # P-value (two-tailed)
    p_value = 2.0 * (1.0 - stats.t.cdf(abs(t_stat), df))

    # Critical t-value for this df at alpha_corrected
    t_critical = stats.t.ppf(1.0 - alpha_corrected / 2.0, df)

    # Bonferroni-corrected significance
    significant = p_value < alpha_corrected

    # Classification
    if not significant:
        assignment = "NEUTRAL"
    elif long_mfe_mean > short_mfe_mean and t_stat > t_critical:
        assignment = "LONG"
    elif short_mfe_mean < long_mfe_mean and t_stat < -t_critical:
        # This is backwards — short_mfe is more negative, meaning SHORT is better
        # But we compare long_mfe vs short_mfe directly
        # If long_mfe > short_mfe and significant → LONG
        assignment = "LONG"
    elif short_mfe_mean < long_mfe_mean and t_stat > t_critical:
        # long_mfe > short_mfe and t_stat > 0 → LONG
        assignment = "LONG"
    elif short_mfe_mean > long_mfe_mean and t_stat < -t_critical:
        # short_mfe > long_mfe and t_stat < 0 → SHORT (but short_mfe is negative, so this means short_mfe is LESS negative)
        # Actually, let me reconsider: short_mfe is log(low/close), which is typically negative
        # More negative = worse for short, less negative = better for short
        # So if short_mfe_mean is GREATER (closer to 0) than long_mfe_mean, SHORT is better
        assignment = "SHORT"
    else:
        # Significant but mixed signals
        assignment = "AMBIGUOUS"

    return {
        "n_obs": n_obs,
        "long_mfe_mean": float(long_mfe_mean),
        "short_mfe_mean": float(short_mfe_mean),
        "long_mfe_std": float(long_mfe_std),
        "short_mfe_std": float(short_mfe_std),
        "t_stat": float(t_stat),
        "p_value": float(p_value),
        "t_critical": float(t_critical),
        "df": int(df),
        "significant_bonferroni": bool(significant),
        "assignment": assignment,
    }


def main():
    """Main analysis."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Starting apriori cluster classification (Bonferroni-corrected t-test)...")

    # Load training period cluster profiles
    with open(JSON_INPUT) as f:
        data = json.load(f)

    profiles_train = data["train"]

    # Classify all clusters
    results = {
        "metadata": {
            "method": "Paired t-test with Bonferroni correction",
            "alpha_base": ALPHA_BASE,
            "n_tests": N_TESTS,
            "alpha_corrected": ALPHA_CORRECTED,
            "period": "Training (2021-01-01 — 2025-06-30)",
            "note": "Classification based on statistical significance of long_mfe vs short_mfe direction bias",
        },
        "classifications": {},
    }

    # Process all clusters
    for cluster_id in range(10):
        cluster_id_str = str(cluster_id)
        if cluster_id_str not in profiles_train:
            continue

        prof = profiles_train[cluster_id_str]
        n_obs = prof["n_observations"]
        long_mfe_mean = prof["long_mfe"]["mean"]
        long_mfe_std = prof["long_mfe"]["std"]
        short_mfe_mean = prof["short_mfe"]["mean"]
        short_mfe_std = prof["short_mfe"]["std"]

        classification = classify_cluster(
            cluster_id=cluster_id,
            n_obs=n_obs,
            long_mfe_mean=long_mfe_mean,
            long_mfe_std=long_mfe_std,
            short_mfe_mean=short_mfe_mean,
            short_mfe_std=short_mfe_std,
        )

        results["classifications"][cluster_id] = classification

        logger.info(
            "Cluster %d: n=%d, long_mfe=%+.6f, short_mfe=%+.6f, t=%.3f, "
            "p=%.6f, sig=%s → %s",
            cluster_id,
            n_obs,
            long_mfe_mean,
            short_mfe_mean,
            classification["t_stat"],
            classification["p_value"],
            classification["significant_bonferroni"],
            classification["assignment"],
        )

    # Save results
    with open(OUTPUT_JSON, "w") as f:
        json.dump(results, f, indent=2)
    logger.info("Classification results saved to %s", OUTPUT_JSON)

    # Print summary table
    print("\n" + "=" * 150)
    print("APRIORI CLUSTER CLASSIFICATION: LONG/SHORT/NEUTRAL (Bonferroni-corrected, alpha=0.005)")
    print("=" * 150)
    print(f"{'Cl':<4} {'n_obs':<8} {'long_mfe_mean':<16} {'short_mfe_mean':<16} "
          f"{'t_stat':<10} {'p_value':<12} {'significant':<14} {'assignment':<15}")
    print("-" * 150)

    for cluster_id in range(10):
        if cluster_id not in results["classifications"]:
            continue

        c = results["classifications"][cluster_id]
        print(f"{cluster_id:<4} {c['n_obs']:<8} "
              f"{c['long_mfe_mean']:+.8f}       {c['short_mfe_mean']:+.8f}       "
              f"{c['t_stat']:+.6f}     {c['p_value']:.8f}  {str(c['significant_bonferroni']):<14} {c['assignment']:<15}")

    print("=" * 150)

    return results


if __name__ == "__main__":
    main()
