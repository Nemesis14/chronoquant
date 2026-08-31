"""Out-of-sample validation of cluster regime strategy with multiple-testing correction.

Performs train/valid/OOS split validation on K-means cluster model:
1. Fit K-means on train period (2021)
2. Predict cluster assignments for valid (2022) and OOS (2023-2025) periods
3. Profile each cluster by MFE direction (long/short)
4. Apply Bonferroni multiple-testing correction
5. Report which clusters remain significant OOS under corrected alpha

Pipeline
--------
01_target_computation  →  02_window_representation  →  03_clustering  →  04_oos_validation

Input
-----
solusdt_window_repr.parquet : 104K rows, 70 features + targets (2020-09-14 to 2026-08-21)
cluster_labels.csv : Train cluster assignments (t866 output)

Output
------
oos_validation_results.json : Full validation results with year-by-year breakdown
oos_cluster_profiles.json : Per-cluster OOS statistics (long/short, Bonferroni corrected)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
from scipy import stats
from sklearn.cluster import KMeans

logger = logging.getLogger(__name__)

# Configuration
RANDOM_STATE = 42
N_CLUSTERS = 3
TRAIN_START = "2021-01-01"
TRAIN_END = "2021-12-31"
VALID_START = "2022-01-01"
VALID_END = "2022-12-31"
OOS_START = "2023-01-01"
OOS_END = "2025-12-31"

# Bonferroni correction: alpha_base / (n_clusters * 2_directions)
ALPHA_BASE = 0.05
ALPHA_CORRECTED = ALPHA_BASE / (N_CLUSTERS * 2)  # = 0.0083

LAG_MAX_AUTOCORR = 50  # Lag for autocorrelation-based effective N estimation


def _estimate_effective_n(values: np.ndarray, lag_max: int = LAG_MAX_AUTOCORR) -> float:
    """Estimate effective sample size for autocorrelated data.

    Formula: N_eff = N / (1 + 2 * sum(rho[k] for k in 1..lag_max))

    Args:
        values  : 1D array of time series values (may contain NaN).
        lag_max : Maximum lag to consider.

    Returns:
        Effective sample size (float).
    """
    values_clean = values[~np.isnan(values)]
    n = len(values_clean)

    if n < lag_max + 1:
        logger.warning("Not enough data for effective N estimation (n=%d)", n)
        return float(n)

    mean_val = np.mean(values_clean)
    c0 = np.var(values_clean, ddof=0)

    if c0 == 0:
        return float(n)

    acf = np.zeros(lag_max + 1)
    acf[0] = 1.0

    for k in range(1, lag_max + 1):
        ck = np.mean((values_clean[:-k] - mean_val) * (values_clean[k:] - mean_val)) / c0
        acf[k] = float(np.clip(ck, -1, 1))

    rho_sum = np.sum(acf[1:])
    n_eff = n / (1.0 + 2.0 * rho_sum)
    return max(1.0, n_eff)


def _load_data(parquet_path: str | None = None) -> tuple[pd.DataFrame, np.ndarray, list[str]]:
    """Load feature matrix and targets from parquet.

    Args:
        parquet_path : Path to solusdt_window_repr.parquet. If None, uses default.

    Returns:
        Tuple of (df, X_features, feature_cols)
    """
    if parquet_path is None:
        parquet_path = "artifacts/research_cluster_regime/solusdt_window_repr.parquet"

    logger.info("Loading data from %s", parquet_path)
    df = pl.read_parquet(parquet_path).to_pandas()
    df["open_time"] = pd.to_datetime(df["open_time"])

    # Identify feature columns (all except open_time and *_fw* targets)
    all_cols = df.columns.tolist()
    skip_prefixes = ("open_time", "long_mfe_fw", "short_mfe_fw")
    feature_cols = [c for c in all_cols if not any(c.startswith(p) for p in skip_prefixes)]

    logger.info("Loaded: %d rows, %d columns; %d features", len(df), len(all_cols), len(feature_cols))

    # Extract feature matrix
    X = df[feature_cols].values.astype(np.float64)

    # Fill NaNs with column median
    for j in range(X.shape[1]):
        col_data = X[:, j]
        mask_nan = np.isnan(col_data)
        if np.any(mask_nan):
            col_median = np.nanmedian(col_data)
            X[mask_nan, j] = col_median
            logger.debug("Filled %d NaNs in feature %d", np.sum(mask_nan), j)

    return df, X, feature_cols


def _split_by_date(
    df: pd.DataFrame,
    X: np.ndarray,
) -> dict[str, tuple[pd.DataFrame, np.ndarray]]:
    """Split data into train/valid/OOS by date ranges.

    Args:
        df : Full DataFrame with open_time.
        X  : Feature matrix (N, D).

    Returns:
        Dictionary with keys "train", "valid", "oos", each value is (df_subset, X_subset)
    """
    splits = {}

    # Train: 2021-01-01 to 2021-12-31
    train_mask = (df["open_time"] >= TRAIN_START) & (df["open_time"] <= TRAIN_END)
    splits["train"] = (df[train_mask].reset_index(drop=True), X[train_mask])

    # Valid: 2022-01-01 to 2022-12-31
    valid_mask = (df["open_time"] >= VALID_START) & (df["open_time"] <= VALID_END)
    splits["valid"] = (df[valid_mask].reset_index(drop=True), X[valid_mask])

    # OOS: 2023-01-01 to 2025-12-31
    oos_mask = (df["open_time"] >= OOS_START) & (df["open_time"] <= OOS_END)
    splits["oos"] = (df[oos_mask].reset_index(drop=True), X[oos_mask])

    for split_name, (df_split, _X_split) in splits.items():
        logger.info("%s: %d rows", split_name.upper(), len(df_split))

    return splits


def _fit_kmeans_on_train(X_train: np.ndarray) -> KMeans:
    """Fit K-means on train data.

    Args:
        X_train : (N, D) feature matrix for train period.

    Returns:
        Fitted KMeans object.
    """
    logger.info("Fitting K-means on train data: n_clusters=%d, random_state=%d",
                N_CLUSTERS, RANDOM_STATE)

    kmeans = KMeans(
        n_clusters=N_CLUSTERS,
        init="k-means++",
        max_iter=200,
        n_init=3,
        random_state=RANDOM_STATE,
        verbose=1,
    )
    kmeans.fit(X_train)
    logger.info("K-means fitted: inertia=%.2f", kmeans.inertia_)

    return kmeans


def _profile_period(
    df_period: pd.DataFrame,
    kmeans: KMeans,
    X_period: np.ndarray,
    period_name: str,
) -> dict[int, dict[str, Any]]:
    """Profile each cluster for a specific period (valid or OOS).

    Args:
        df_period : DataFrame with targets and open_time.
        kmeans    : Fitted KMeans model.
        X_period  : Feature matrix for this period.
        period_name : Name of period (e.g., "valid", "oos").

    Returns:
        Dictionary: {cluster_id: {profile_stats...}}
    """
    logger.info("Profiling clusters for %s period", period_name)

    # Predict cluster assignments
    cluster_labels = kmeans.predict(X_period)

    profiles = {}
    global_long_mean = df_period["long_mfe_fw360"].dropna().mean()
    global_short_mean = df_period["short_mfe_fw360"].dropna().mean()

    logger.info("Global means (period=%s): long=%.6f, short=%.6f",
                period_name, global_long_mean, global_short_mean)

    for cluster_id in range(N_CLUSTERS):
        cluster_mask = cluster_labels == cluster_id
        n_obs = np.sum(cluster_mask)

        logger.info("  Cluster %d: %d observations", cluster_id, n_obs)

        # Extract long and short MFE for this cluster
        long_mfe = df_period.loc[cluster_mask, "long_mfe_fw360"].dropna().values
        short_mfe = df_period.loc[cluster_mask, "short_mfe_fw360"].dropna().values

        # Summary statistics
        long_mean = np.mean(long_mfe) if len(long_mfe) > 0 else np.nan
        long_std = np.std(long_mfe, ddof=1) if len(long_mfe) > 1 else np.nan
        long_n = len(long_mfe)
        long_n_eff = _estimate_effective_n(long_mfe) if len(long_mfe) > 0 else np.nan

        short_mean = np.mean(short_mfe) if len(short_mfe) > 0 else np.nan
        short_std = np.std(short_mfe, ddof=1) if len(short_mfe) > 1 else np.nan
        short_n = len(short_mfe)
        short_n_eff = _estimate_effective_n(short_mfe) if len(short_mfe) > 0 else np.nan

        # T-tests vs global mean (two-tailed)
        long_t_stat = np.nan
        long_p_value = np.nan
        if len(long_mfe) > 1 and long_std > 0:
            se_long = long_std / np.sqrt(long_n_eff)
            long_t_stat = (long_mean - global_long_mean) / se_long
            long_p_value = 2.0 * (1.0 - stats.t.cdf(abs(long_t_stat), long_n_eff - 1))

        short_t_stat = np.nan
        short_p_value = np.nan
        if len(short_mfe) > 1 and short_std > 0:
            se_short = short_std / np.sqrt(short_n_eff)
            short_t_stat = (short_mean - global_short_mean) / se_short
            short_p_value = 2.0 * (1.0 - stats.t.cdf(abs(short_t_stat), short_n_eff - 1))

        # Significance under Bonferroni correction
        long_sig_bonferroni = long_p_value < ALPHA_CORRECTED if not np.isnan(long_p_value) else False
        short_sig_bonferroni = short_p_value < ALPHA_CORRECTED if not np.isnan(short_p_value) else False

        profiles[cluster_id] = {
            "n_obs": int(n_obs),
            "long_mfe": {
                "n": long_n,
                "n_eff": float(long_n_eff),
                "mean": float(long_mean),
                "std": float(long_std),
                "t_stat": float(long_t_stat),
                "p_value": float(long_p_value),
                "significant_bonferroni": bool(long_sig_bonferroni),
            },
            "short_mfe": {
                "n": short_n,
                "n_eff": float(short_n_eff),
                "mean": float(short_mean),
                "std": float(short_std),
                "t_stat": float(short_t_stat),
                "p_value": float(short_p_value),
                "significant_bonferroni": bool(short_sig_bonferroni),
            },
        }

        logger.info(
            "  Cluster %d: long_mean=%.6f (n_eff=%.0f, p=%.6f, sig=%s), "
            "short_mean=%.6f (n_eff=%.0f, p=%.6f, sig=%s)",
            cluster_id,
            long_mean, long_n_eff, long_p_value, long_sig_bonferroni,
            short_mean, short_n_eff, short_p_value, short_sig_bonferroni,
        )

    return profiles


def _profile_oos_by_year(
    df_oos: pd.DataFrame,
    kmeans: KMeans,
    X_oos: np.ndarray,
) -> dict[int, dict[str, Any]]:
    """Profile OOS period by year (2023, 2024, 2025) for detailed breakdown.

    Args:
        df_oos : OOS DataFrame.
        kmeans : Fitted KMeans model.
        X_oos  : OOS feature matrix.

    Returns:
        Dictionary: {year: {cluster_id: {profile_stats...}}}
    """
    logger.info("Profiling OOS by year")

    cluster_labels_oos = kmeans.predict(X_oos)
    df_oos_labeled = df_oos.copy()
    df_oos_labeled["cluster_id"] = cluster_labels_oos
    df_oos_labeled["year"] = df_oos_labeled["open_time"].dt.year

    year_profiles = {}

    for year in sorted(df_oos_labeled["year"].unique()):
        logger.info("  Year %d", year)
        df_year = df_oos_labeled[df_oos_labeled["year"] == year].reset_index(drop=True)
        X_year = X_oos[df_oos_labeled["year"] == year]

        year_profiles[year] = _profile_period(df_year, kmeans, X_year, f"OOS {year}")

    return year_profiles


def perform_oos_validation(
    parquet_path: str | None = None,
    output_dir: str | None = None,
) -> dict[str, Any]:
    """Perform OOS validation of cluster regime strategy.

    Pipeline:
    1. Load data and split into train/valid/OOS.
    2. Fit K-means on train period.
    3. Predict and profile valid and OOS periods.
    4. Apply Bonferroni multiple-testing correction.
    5. Report which clusters remain significant OOS.

    Args:
        parquet_path : Path to solusdt_window_repr.parquet. If None, uses default.
        output_dir   : Output directory. If None, uses artifacts/research_cluster_regime/.

    Returns:
        Dictionary with validation results.
    """
    if output_dir is None:
        output_dir = "artifacts/research_cluster_regime"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Load and split
    logger.info("Step 1: Loading and splitting data...")
    df, X, feature_cols = _load_data(parquet_path)
    splits = _split_by_date(df, X)
    df_train, X_train = splits["train"]
    df_valid, X_valid = splits["valid"]
    df_oos, X_oos = splits["oos"]

    # Step 2: Fit K-means on train
    logger.info("Step 2: Fitting K-means on train period...")
    kmeans = _fit_kmeans_on_train(X_train)

    # Step 3: Profile train, valid, OOS
    logger.info("Step 3: Profiling periods...")
    profiles_train = _profile_period(df_train, kmeans, X_train, "train")
    profiles_valid = _profile_period(df_valid, kmeans, X_valid, "valid")
    profiles_oos = _profile_period(df_oos, kmeans, X_oos, "oos")
    profiles_oos_by_year = _profile_oos_by_year(df_oos, kmeans, X_oos)

    # Step 4: Compile results
    logger.info("Step 4: Compiling results...")

    # Convert oos_by_year keys to strings for JSON serialization
    oos_by_year_dict = {
        year: len(df_oos[df_oos["open_time"].dt.year == year])
        for year in sorted(df_oos["open_time"].dt.year.unique())
    }
    oos_by_year_str = {str(k): int(v) for k, v in oos_by_year_dict.items()}
    profiles_oos_by_year_str = {str(k): v for k, v in profiles_oos_by_year.items()}

    results = {
        "metadata": {
            "description": "OOS validation of K-means cluster regime strategy (t867)",
            "date": str(pd.Timestamp.now()),
            "train_period": [TRAIN_START, TRAIN_END],
            "valid_period": [VALID_START, VALID_END],
            "oos_period": [OOS_START, OOS_END],
            "n_features": len(feature_cols),
            "n_clusters": N_CLUSTERS,
            "random_state": RANDOM_STATE,
            "alpha_base": ALPHA_BASE,
            "alpha_corrected_bonferroni": float(ALPHA_CORRECTED),
            "correction_rationale": f"Bonferroni: alpha / ({N_CLUSTERS} clusters × 2 directions) = {ALPHA_CORRECTED:.4f}",
        },
        "data_summary": {
            "train": {"n_rows": len(df_train)},
            "valid": {"n_rows": len(df_valid)},
            "oos": {
                "n_rows": len(df_oos),
                "by_year": oos_by_year_str
            }
        },
        "profiles": {
            "train": profiles_train,
            "valid": profiles_valid,
            "oos": profiles_oos,
            "oos_by_year": profiles_oos_by_year_str,
        },
        "summary": {},
    }

    # Step 5: Summary and significance flags
    logger.info("Step 5: Summary analysis...")

    summary = []

    for cluster_id in range(N_CLUSTERS):
        oos_long_sig = profiles_oos[cluster_id]["long_mfe"]["significant_bonferroni"]
        oos_short_sig = profiles_oos[cluster_id]["short_mfe"]["significant_bonferroni"]
        train_long_mean = profiles_train[cluster_id]["long_mfe"]["mean"]
        train_short_mean = profiles_train[cluster_id]["short_mfe"]["mean"]
        oos_long_mean = profiles_oos[cluster_id]["long_mfe"]["mean"]
        oos_short_mean = profiles_oos[cluster_id]["short_mfe"]["mean"]

        summary.append({
            "cluster_id": cluster_id,
            "train": {
                "long_mfe_mean": float(train_long_mean),
                "short_mfe_mean": float(train_short_mean),
            },
            "oos": {
                "long_mfe_mean": float(oos_long_mean),
                "short_mfe_mean": float(oos_short_mean),
                "long_significant_bonferroni": bool(oos_long_sig),
                "short_significant_bonferroni": bool(oos_short_sig),
            },
        })

        logger.info(
            "Cluster %d: long_bias train→oos (%.6f → %.6f, sig=%s), "
            "short_bias train→oos (%.6f → %.6f, sig=%s)",
            cluster_id,
            train_long_mean, oos_long_mean, oos_long_sig,
            train_short_mean, oos_short_mean, oos_short_sig,
        )

    results["summary"] = summary

    # Step 6: Save outputs
    logger.info("Step 6: Saving outputs...")

    validation_path = output_dir / "oos_validation_results.json"
    with open(validation_path, "w") as f:
        json.dump(results, f, indent=2)
    logger.info("Validation results saved to %s", validation_path)

    # Simplified profiles for easier consumption
    profiles_compact = {}
    for cluster_id in range(N_CLUSTERS):
        profiles_compact[cluster_id] = results["profiles"]["oos"][cluster_id]
    profiles_compact_path = output_dir / "oos_cluster_profiles.json"
    with open(profiles_compact_path, "w") as f:
        json.dump(profiles_compact, f, indent=2)
    logger.info("Cluster profiles saved to %s", profiles_compact_path)

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Starting OOS validation...")
    results = perform_oos_validation()
    logger.info("OOS validation complete.")

    # Print summary
    print("\n" + "=" * 80)
    print("OOS VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Alpha (base): {results['metadata']['alpha_base']}")
    print(f"Alpha (Bonferroni corrected): {results['metadata']['alpha_corrected_bonferroni']:.4f}")
    print()
    for item in results["summary"]:
        cluster_id = item["cluster_id"]
        oos_long_sig = item["oos"]["long_significant_bonferroni"]
        oos_short_sig = item["oos"]["short_significant_bonferroni"]
        print(f"Cluster {cluster_id}:")
        print(f"  Long MFE:  {item['oos']['long_mfe_mean']:>8.6f} (sig: {oos_long_sig})")
        print(f"  Short MFE: {item['oos']['short_mfe_mean']:>8.6f} (sig: {oos_short_sig})")
    print("=" * 80)
