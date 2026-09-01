"""Corrected apriori cluster classification using PAIRED t-test + Bonferroni.

Fixes a critical bug in the previous approach: since long_mfe (positive) and
short_mfe (negative) come from the SAME observation window, they are correlated
and must be tested with a PAIRED t-test, not treated as independent samples.

Correct methodology:
  diff_i = long_mfe_i + short_mfe_i   (per-observation; since short_mfe < 0,
                                        this equals |long_mfe_i| - |short_mfe_i|)
  H0: E[diff] = 0  (no directional bias)
  t_stat = mean(diff) / (std(diff) / sqrt(n))
  p_value = 2 * (1 - t.cdf(|t_stat|, df=n-1))

Bonferroni correction: alpha = 0.05 / 10 clusters = 0.005

Assignment:
  t_stat > t_critical AND mean(diff) > 0  -> LONG  (|long_mfe| significantly > |short_mfe|)
  t_stat < -t_critical AND mean(diff) < 0 -> SHORT (|short_mfe| significantly > |long_mfe|)
  else                                     -> NEUTRAL
"""

from __future__ import annotations

import json
import logging
import pickle

import numpy as np
import pandas as pd
import polars as pl
from scipy import stats

logger = logging.getLogger(__name__)

# Configuration
TRAIN_END = "2025-06-30"
ALL_CLUSTERS = list(range(10))

ALPHA_BASE = 0.05
N_TESTS = 10
ALPHA_CORRECTED = ALPHA_BASE / N_TESTS  # 0.005

MODEL_PATH = "artifacts/research_cluster_regime/kmeans_k10_trained_2021_2025_06.pkl"
WINDOW_REPR_PATH = "artifacts/research_cluster_regime/solusdt_window_repr.parquet"
OUTPUT_JSON = "artifacts/research_cluster_regime/paired_ttest_cluster_classification.json"


def load_model_and_data() -> tuple[object, pd.DataFrame, np.ndarray]:
    """Load trained K-means model and raw window representation data."""
    with open(MODEL_PATH, "rb") as f:
        kmeans = pickle.load(f)
    logger.info("Loaded trained model from %s", MODEL_PATH)

    df = pl.read_parquet(WINDOW_REPR_PATH).to_pandas()
    df["open_time"] = pd.to_datetime(df["open_time"])

    all_cols = df.columns.tolist()
    skip_prefixes = ("open_time", "long_mfe_fw", "short_mfe_fw")
    feature_cols = [c for c in all_cols if not any(c.startswith(p) for p in skip_prefixes)]

    X = df[feature_cols].values.astype(np.float64)

    for j in range(X.shape[1]):
        col_data = X[:, j]
        mask_nan = np.isnan(col_data)
        if np.any(mask_nan):
            col_median = np.nanmedian(col_data)
            X[mask_nan, j] = col_median

    logger.info("Loaded data: %d rows, %d features", len(df), len(feature_cols))
    return kmeans, df, X


def paired_ttest_classify(
    cluster_id: int,
    long_mfe: np.ndarray,
    short_mfe: np.ndarray,
    alpha_corrected: float = ALPHA_CORRECTED,
) -> dict:
    """Classify cluster direction using paired t-test on diff = long_mfe + short_mfe.

    Args:
        cluster_id      : Cluster ID (for logging).
        long_mfe        : Array of long_mfe values for this cluster (paired with short_mfe).
        short_mfe       : Array of short_mfe values for this cluster (same observations).
        alpha_corrected : Bonferroni-corrected significance threshold.

    Returns:
        Dict with classification results.
    """
    n = len(long_mfe)

    if n < 2:
        return {
            "n_obs": n,
            "assignment": "MISSING",
        }

    # Per-observation paired difference
    # Since short_mfe < 0 typically, diff = long_mfe + short_mfe = |long| - |short|
    diff = long_mfe + short_mfe

    diff_mean = float(np.mean(diff))
    diff_std = float(np.std(diff, ddof=1))

    df = n - 1

    if diff_std == 0:
        t_stat = 0.0 if diff_mean == 0 else np.inf
    else:
        se = diff_std / np.sqrt(n)
        t_stat = diff_mean / se

    p_value = 2.0 * (1.0 - stats.t.cdf(abs(t_stat), df))
    t_critical = stats.t.ppf(1.0 - alpha_corrected / 2.0, df)

    significant = p_value < alpha_corrected

    if not significant:
        assignment = "NEUTRAL"
    elif t_stat > t_critical and diff_mean > 0:
        assignment = "LONG"
    elif t_stat < -t_critical and diff_mean < 0:
        assignment = "SHORT"
    else:
        assignment = "NEUTRAL"

    return {
        "n_obs": n,
        "long_mfe_mean": float(np.mean(long_mfe)),
        "short_mfe_mean": float(np.mean(short_mfe)),
        "diff_mean": diff_mean,
        "diff_std": diff_std,
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

    logger.info("Starting PAIRED t-test cluster classification (corrected methodology)...")

    kmeans, df, X = load_model_and_data()

    # Training period only
    train_mask = df["open_time"] <= TRAIN_END
    df_train = df[train_mask].reset_index(drop=True)
    X_train = X[train_mask]

    logger.info("Train: %d rows", len(df_train))

    # Predict cluster assignments
    labels_train = kmeans.predict(X_train)
    df_train = df_train.copy()
    df_train["cluster"] = labels_train

    results = {
        "metadata": {
            "method": "PAIRED t-test with Bonferroni correction (CORRECTED)",
            "formula": "diff_i = long_mfe_i + short_mfe_i; t_stat = mean(diff) / (std(diff) / sqrt(n))",
            "alpha_base": ALPHA_BASE,
            "n_tests": N_TESTS,
            "alpha_corrected": ALPHA_CORRECTED,
            "period": "Training (2021-01-01 -- 2025-06-30)",
            "note": "Fixes bug in previous independent-sample t-test which always yielded positive t_stat",
        },
        "classifications": {},
    }

    for cluster_id in ALL_CLUSTERS:
        mask = df_train["cluster"] == cluster_id
        cluster_rows = df_train.loc[mask]

        # Extract PAIRED observations (drop rows where either is NaN)
        paired = cluster_rows[["long_mfe_fw360", "short_mfe_fw360"]].dropna()
        long_mfe = paired["long_mfe_fw360"].values
        short_mfe = paired["short_mfe_fw360"].values

        classification = paired_ttest_classify(cluster_id, long_mfe, short_mfe)
        results["classifications"][cluster_id] = classification

        if classification["assignment"] != "MISSING":
            logger.info(
                "Cluster %d: n=%d, long_mfe=%+.6f, short_mfe=%+.6f, diff_mean=%+.6f, "
                "t=%.3f, p=%.8f, sig=%s -> %s",
                cluster_id,
                classification["n_obs"],
                classification["long_mfe_mean"],
                classification["short_mfe_mean"],
                classification["diff_mean"],
                classification["t_stat"],
                classification["p_value"],
                classification["significant_bonferroni"],
                classification["assignment"],
            )

    with open(OUTPUT_JSON, "w") as f:
        json.dump(results, f, indent=2)
    logger.info("Results saved to %s", OUTPUT_JSON)

    # Print summary table
    print("\n" + "=" * 150)
    print("CORRECTED CLUSTER CLASSIFICATION: PAIRED t-test (diff = long_mfe + short_mfe), Bonferroni alpha=0.005")
    print("=" * 150)
    print(f"{'Cl':<4} {'n_obs':<8} {'long_mfe_mean':<16} {'short_mfe_mean':<16} {'diff_mean':<14} "
          f"{'t_stat':<12} {'p_value':<14} {'significant':<14} {'assignment':<12}")
    print("-" * 150)

    for cluster_id in ALL_CLUSTERS:
        c = results["classifications"][cluster_id]
        if c["assignment"] == "MISSING":
            print(f"{cluster_id:<4} {c['n_obs']:<8} {'--':<16} {'--':<16} {'--':<14} {'--':<12} {'--':<14} {'--':<14} MISSING")
            continue
        print(f"{cluster_id:<4} {c['n_obs']:<8} "
              f"{c['long_mfe_mean']:+.8f}       {c['short_mfe_mean']:+.8f}       "
              f"{c['diff_mean']:+.8f}    {c['t_stat']:+.6f}   {c['p_value']:.8f}    "
              f"{str(c['significant_bonferroni']):<14} {c['assignment']:<12}")

    print("=" * 150)

    return results


if __name__ == "__main__":
    main()
