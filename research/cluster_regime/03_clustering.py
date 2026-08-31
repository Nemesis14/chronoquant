"""K-means clustering on windowed feature representations.

Performs K-means clustering on the 70-dimensional window representation
from t865 (02_window_representation.py), profiles each cluster by MFE direction,
and evaluates statistical significance with effective N estimation.

Pipeline
--------
01_target_computation (fw360 targets)  →  02_window_representation  →  03_clustering

Data flow
---------
solusdt_window_repr.parquet (104K rows × 70 features + targets)
    →  K-means on 70-dimensional space (k=3..8 via elbow method)
    →  Cluster labels + centroid membership
    →  Per-cluster MFE profiling (long/short direction bias)
    →  Effective N calculation (autokorrelation-aware)
    →  cluster_labels.csv + cluster_profiles.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from scipy import stats
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

logger = logging.getLogger(__name__)

# K-means hyperparameters
RANDOM_STATE = 42
K_MIN = 3
K_MAX = 8
INIT_METHOD = "k-means++"
MAX_ITER = 200
N_INIT = 3  # Reduced from 10 for speed


def _load_window_representation(
    artifact_path: str | None = None,
) -> tuple[pl.DataFrame, np.ndarray, list[str]]:
    """Load windowed feature representation from Parquet.

    Args:
        artifact_path : Path to solusdt_window_repr.parquet. If None, uses default.

    Returns:
        Tuple of (df_full, X_features, feature_columns)
            - df_full : Full DataFrame with targets
            - X_features : (N, 70) numpy array of features for clustering
            - feature_columns : List of 70 feature column names
    """
    if artifact_path is None:
        artifact_path = "artifacts/research_cluster_regime/solusdt_window_repr.parquet"

    artifact_path = Path(artifact_path)
    if not artifact_path.exists():
        raise FileNotFoundError(f"Window representation not found: {artifact_path}")

    logger.info("Loading window representation from %s", artifact_path)
    df = pl.read_parquet(str(artifact_path))
    logger.info("Loaded: %d rows, %d columns", len(df), len(df.columns))

    # Identify feature columns (all except open_time and *_fw* targets)
    all_cols = df.columns
    skip_prefixes = ("open_time", "long_mfe_fw", "short_mfe_fw")
    feature_cols = [c for c in all_cols if not any(c.startswith(p) for p in skip_prefixes)]

    logger.info("Feature columns: %d (e.g., %s)", len(feature_cols), feature_cols[:5])

    # Extract feature matrix
    X = df.select(feature_cols).to_numpy(allow_copy=True, use_pyarrow=False)

    # Handle NaNs: fill with column median
    for j in range(X.shape[1]):
        col_data = X[:, j]
        mask_nan = np.isnan(col_data)
        if np.any(mask_nan):
            col_median = np.nanmedian(col_data)
            X[mask_nan, j] = col_median
            logger.debug("Filled %d NaNs in feature %d with median %.4f",
                        np.sum(mask_nan), j, col_median)

    return df, X, feature_cols


def _find_optimal_k(
    X: np.ndarray,
    k_min: int = K_MIN,
    k_max: int = K_MAX,
    sample_size: int = 10000,  # Sample for faster evaluation
) -> tuple[int, dict[str, Any]]:
    """Determine optimal k via elbow method and silhouette score.

    Args:
        X           : (N, D) feature matrix for clustering.
        k_min       : Minimum k to test.
        k_max       : Maximum k to test.
        sample_size : Use sample for silhouette evaluation (speeds up large N).

    Returns:
        Tuple of (best_k, metrics_dict)
            - best_k : Optimal k (maximizes silhouette score on sample)
            - metrics_dict : {k: {"inertia": ..., "silhouette": ...}, ...}
    """
    logger.info("Finding optimal k in range [%d, %d]", k_min, k_max)

    # Use sample for faster silhouette evaluation
    if len(X) > sample_size:
        sample_indices = np.random.RandomState(RANDOM_STATE).choice(
            len(X), size=sample_size, replace=False
        )
        X_sample = X[sample_indices]
        logger.info("Using sample of %d rows for k evaluation", sample_size)
    else:
        X_sample = X

    metrics = {}
    best_k = k_min
    best_silhouette = -1.0

    for k in range(k_min, k_max + 1):
        logger.info("Evaluating k=%d", k)

        kmeans = KMeans(
            n_clusters=k,
            init=INIT_METHOD,
            max_iter=MAX_ITER,
            n_init=N_INIT,
            random_state=RANDOM_STATE,
            verbose=0
        )
        labels = kmeans.fit_predict(X_sample)
        inertia = kmeans.inertia_
        silhouette = silhouette_score(X_sample, labels)

        metrics[k] = {
            "inertia": float(inertia),
            "silhouette": float(silhouette),
        }

        logger.info("  k=%d: inertia=%.2f, silhouette=%.4f", k, inertia, silhouette)

        if silhouette > best_silhouette:
            best_silhouette = silhouette
            best_k = k

    logger.info("Optimal k=%d (silhouette=%.4f)", best_k, best_silhouette)
    return best_k, metrics


def _fit_kmeans(
    X: np.ndarray,
    k: int,
) -> tuple[KMeans, np.ndarray]:
    """Fit K-means clustering.

    Args:
        X : (N, D) feature matrix.
        k : Number of clusters.

    Returns:
        Tuple of (fitted_kmeans, labels)
    """
    logger.info("Fitting K-means with k=%d", k)

    kmeans = KMeans(
        n_clusters=k,
        init=INIT_METHOD,
        max_iter=MAX_ITER,
        n_init=N_INIT,
        random_state=RANDOM_STATE,
        verbose=1
    )
    labels = kmeans.fit_predict(X)

    logger.info("K-means fitted: inertia=%.2f", kmeans.inertia_)
    return kmeans, labels


def _estimate_effective_n(
    values: np.ndarray,
    lag_max: int = 50,
) -> float:
    """Estimate effective sample size for autocorrelated data.

    Uses autocorrelation decay to estimate the number of independent samples
    from an autocorrelated time series.

    Formula: N_eff = N / (1 + 2 * sum(rho[k] for k in 1..lag_max))

    Args:
        values  : 1D array of time series values (may contain NaN).
        lag_max : Maximum lag to consider.

    Returns:
        Effective sample size (float).
    """
    # Remove NaNs
    values_clean = values[~np.isnan(values)]
    n = len(values_clean)

    if n < lag_max + 1:
        logger.warning("Not enough data for effective N estimation (n=%d)", n)
        return float(n)

    # Compute autocorrelation at lags 1..lag_max
    mean_val = np.mean(values_clean)
    c0 = np.var(values_clean, ddof=0)  # Variance (not sample variance)

    if c0 == 0:
        return float(n)

    acf = np.zeros(lag_max + 1)
    acf[0] = 1.0

    for k in range(1, lag_max + 1):
        ck = np.mean((values_clean[:-k] - mean_val) * (values_clean[k:] - mean_val)) / c0
        acf[k] = float(np.clip(ck, -1, 1))  # Clip to [-1, 1]

    # Sum of autocorrelations (excluding lag 0)
    rho_sum = np.sum(acf[1:])

    # Effective N
    n_eff = n / (1.0 + 2.0 * rho_sum)
    return max(1.0, n_eff)  # At least 1


def _profile_clusters(
    df: pl.DataFrame,
    labels: np.ndarray,
    k: int,
) -> dict[int, dict[str, Any]]:
    """Profile each cluster by MFE direction and compute significance.

    Args:
        df     : Original DataFrame with targets (long_mfe_fw360, short_mfe_fw360).
        labels : (N,) cluster labels from K-means.
        k      : Number of clusters.

    Returns:
        Dictionary: {cluster_id: {profile_stats...}}
    """
    logger.info("Profiling %d clusters", k)

    # Add cluster labels to dataframe
    df_with_labels = df.with_columns(pl.Series("cluster_id", labels))

    profiles = {}

    for cluster_id in range(k):
        cluster_mask = labels == cluster_id
        n_obs = np.sum(cluster_mask)

        logger.info("Cluster %d: %d observations", cluster_id, n_obs)

        # Extract long and short MFE targets for this cluster
        cluster_rows = df_with_labels.filter(pl.col("cluster_id") == cluster_id)

        long_mfe = cluster_rows["long_mfe_fw360"].drop_nulls().to_numpy()
        short_mfe = cluster_rows["short_mfe_fw360"].drop_nulls().to_numpy()

        # Summary statistics
        long_mean = float(np.mean(long_mfe)) if len(long_mfe) > 0 else np.nan
        long_std = float(np.std(long_mfe, ddof=1)) if len(long_mfe) > 1 else np.nan
        long_n = len(long_mfe)

        short_mean = float(np.mean(short_mfe)) if len(short_mfe) > 0 else np.nan
        short_std = float(np.std(short_mfe, ddof=1)) if len(short_mfe) > 1 else np.nan
        short_n = len(short_mfe)

        # Effective N and significance tests
        long_n_eff = _estimate_effective_n(long_mfe) if len(long_mfe) > 0 else np.nan
        short_n_eff = _estimate_effective_n(short_mfe) if len(short_mfe) > 0 else np.nan

        # T-test vs global mean (two-tailed)
        global_long_mean = df["long_mfe_fw360"].drop_nulls().mean()
        global_short_mean = df["short_mfe_fw360"].drop_nulls().mean()

        long_t_stat = np.nan
        long_p_value = np.nan
        if len(long_mfe) > 1 and long_std > 0:
            # Standard error
            se_long = long_std / np.sqrt(long_n_eff)
            long_t_stat = (long_mean - float(global_long_mean)) / se_long
            long_p_value = 2.0 * (1.0 - stats.t.cdf(abs(long_t_stat), long_n_eff - 1))

        short_t_stat = np.nan
        short_p_value = np.nan
        if len(short_mfe) > 1 and short_std > 0:
            # Standard error
            se_short = short_std / np.sqrt(short_n_eff)
            short_t_stat = (short_mean - float(global_short_mean)) / se_short
            short_p_value = 2.0 * (1.0 - stats.t.cdf(abs(short_t_stat), short_n_eff - 1))

        # Significance threshold (roughly 95% CI for |t| > 1.96)
        is_long_significant = abs(long_t_stat) > 1.96 if not np.isnan(long_t_stat) else False
        is_short_significant = abs(short_t_stat) > 1.96 if not np.isnan(short_t_stat) else False

        profiles[cluster_id] = {
            "n_obs": int(n_obs),
            "long_mfe": {
                "n": long_n,
                "mean": long_mean,
                "std": long_std,
                "n_eff": float(long_n_eff),
                "t_stat": float(long_t_stat),
                "p_value": float(long_p_value),
                "significant_at_95pct": bool(is_long_significant),
            },
            "short_mfe": {
                "n": short_n,
                "mean": short_mean,
                "std": short_std,
                "n_eff": float(short_n_eff),
                "t_stat": float(short_t_stat),
                "p_value": float(short_p_value),
                "significant_at_95pct": bool(is_short_significant),
            },
        }

        logger.info(
            "  Cluster %d: long_mean=%.6f (n_eff=%.0f, p=%.4f), "
            "short_mean=%.6f (n_eff=%.0f, p=%.4f)",
            cluster_id,
            long_mean, long_n_eff, long_p_value,
            short_mean, short_n_eff, short_p_value,
        )

    return profiles


def perform_clustering(
    window_repr_path: str | None = None,
    output_dir: str | None = None,
) -> dict[str, Any]:
    """Perform K-means clustering on window representation.

    Pipeline:
    1. Load window representation (104K rows × 70 features).
    2. Find optimal k via silhouette score.
    3. Fit K-means with optimal k.
    4. Profile each cluster by MFE (long/short direction bias, significance).
    5. Save cluster_labels.csv and cluster_profiles.json.

    Args:
        window_repr_path : Path to solusdt_window_repr.parquet. If None, uses default.
        output_dir       : Output directory. If None, uses artifacts/research_cluster_regime/.

    Returns:
        Dictionary with clustering results and metadata.
    """
    if output_dir is None:
        output_dir = "artifacts/research_cluster_regime"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Load data
    logger.info("Step 1: Loading window representation...")
    df, X, feature_cols = _load_window_representation(window_repr_path)

    # Step 2: Find optimal k
    logger.info("Step 2: Finding optimal k...")
    best_k, metrics = _find_optimal_k(X)

    # Step 3: Fit K-means
    logger.info("Step 3: Fitting K-means with k=%d...", best_k)
    kmeans, labels = _fit_kmeans(X, best_k)

    # Step 4: Profile clusters
    logger.info("Step 4: Profiling clusters...")
    profiles = _profile_clusters(df, labels, best_k)

    # Step 5: Save outputs
    logger.info("Step 5: Saving outputs...")

    # Save cluster labels
    df_labels = df.select(["open_time"]).with_columns(
        pl.Series("cluster_id", labels),
        pl.Series("long_mfe_fw360", df["long_mfe_fw360"]),
        pl.Series("short_mfe_fw360", df["short_mfe_fw360"]),
    )
    labels_path = output_dir / "cluster_labels.csv"
    df_labels.write_csv(str(labels_path))
    logger.info("Cluster labels saved to %s", labels_path)

    # Save cluster profiles
    profiles_path = output_dir / "cluster_profiles.json"
    with open(profiles_path, "w") as f:
        json.dump(profiles, f, indent=2)
    logger.info("Cluster profiles saved to %s", profiles_path)

    # Save metadata
    results = {
        "metadata": {
            "description": "K-means clustering on window representations (t866)",
            "date": str(np.datetime64("today")),
            "n_observations": len(df),
            "n_features": len(feature_cols),
            "feature_columns": feature_cols,
        },
        "algorithm": {
            "name": "K-means",
            "k_optimal": best_k,
            "k_range": [K_MIN, K_MAX],
            "init_method": INIT_METHOD,
            "random_state": RANDOM_STATE,
            "max_iter": MAX_ITER,
        },
        "metrics": metrics,
        "cluster_profiles": profiles,
        "output_files": {
            "cluster_labels": str(labels_path),
            "cluster_profiles": str(profiles_path),
        },
    }

    metadata_path = output_dir / "clustering_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(results, f, indent=2)
    logger.info("Metadata saved to %s", metadata_path)

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("K-means clustering on window representation...")
    results = perform_clustering()
    logger.info("Clustering complete. Results: k=%d", results["algorithm"]["k_optimal"])
    logger.info("Outputs:")
    for key, path in results["output_files"].items():
        logger.info("  %s: %s", key, path)
