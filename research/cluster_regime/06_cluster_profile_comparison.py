"""Analyze cluster profiles: Training vs. OOS comparison for 4 tradeable clusters.

Loads trained k=10 model, predicts clusters for full period (2021-2026),
then profiles each of the 4 tradeable clusters (0, 2, 3, 9) across train vs. OOS.
"""

from __future__ import annotations

import json
import logging
import pickle

import numpy as np
import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)

# Configuration
TRAIN_END = "2025-06-30"
OOS_START = "2025-07-01"
OOS_END = "2026-06-30"

TRADEABLE_CLUSTERS = [0, 2, 3, 9]

# Paths
MODEL_PATH = "artifacts/research_cluster_regime/kmeans_k10_trained_2021_2025_06.pkl"
WINDOW_REPR_PATH = "artifacts/research_cluster_regime/solusdt_window_repr.parquet"
OUTPUT_JSON = "artifacts/research_cluster_regime/cluster_profile_train_vs_oos.json"


def load_model_and_data() -> tuple[object, pd.DataFrame, np.ndarray, list[str]]:
    """Load trained model and window representation data."""
    # Load model
    with open(MODEL_PATH, "rb") as f:
        kmeans = pickle.load(f)
    logger.info("Loaded trained model from %s", MODEL_PATH)

    # Load data
    df = pl.read_parquet(WINDOW_REPR_PATH).to_pandas()
    df["open_time"] = pd.to_datetime(df["open_time"])

    # Extract features
    all_cols = df.columns.tolist()
    skip_prefixes = ("open_time", "long_mfe_fw", "short_mfe_fw")
    feature_cols = [c for c in all_cols if not any(c.startswith(p) for p in skip_prefixes)]

    X = df[feature_cols].values.astype(np.float64)

    # Fill NaNs
    for j in range(X.shape[1]):
        col_data = X[:, j]
        mask_nan = np.isnan(col_data)
        if np.any(mask_nan):
            col_median = np.nanmedian(col_data)
            X[mask_nan, j] = col_median

    logger.info("Loaded data: %d rows, %d features", len(df), len(feature_cols))
    return kmeans, df, X, feature_cols


def profile_clusters(
    df: pd.DataFrame,
    X: np.ndarray,
    kmeans: object,
    period_name: str,
) -> dict[int, dict]:
    """Profile clusters for a specific period."""
    # Predict clusters
    labels = kmeans.predict(X)
    df_labeled = df.copy()
    df_labeled["cluster"] = labels

    profiles = {}

    for cluster_id in TRADEABLE_CLUSTERS:
        mask = df_labeled["cluster"] == cluster_id
        n_obs = np.sum(mask)

        long_mfe = df_labeled.loc[mask, "long_mfe_fw360"].dropna().values
        short_mfe = df_labeled.loc[mask, "short_mfe_fw360"].dropna().values

        long_mean = float(np.mean(long_mfe)) if len(long_mfe) > 0 else np.nan
        long_std = float(np.std(long_mfe, ddof=1)) if len(long_mfe) > 1 else np.nan
        short_mean = float(np.mean(short_mfe)) if len(short_mfe) > 0 else np.nan
        short_std = float(np.std(short_mfe, ddof=1)) if len(short_mfe) > 1 else np.nan

        profiles[cluster_id] = {
            "n_observations": int(n_obs),
            "n_long_mfe": int(len(long_mfe)),
            "n_short_mfe": int(len(short_mfe)),
            "long_mfe": {
                "mean": long_mean,
                "std": long_std,
            },
            "short_mfe": {
                "mean": short_mean,
                "std": short_std,
            },
        }

        logger.info(
            "%s Cluster %d: n=%d, long_mfe=%.6f±%.6f, short_mfe=%.6f±%.6f",
            period_name, cluster_id, n_obs, long_mean, long_std, short_mean, short_std
        )

    return profiles


def main():
    """Main analysis."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Starting cluster profile comparison...")

    # Load model and data
    kmeans, df, X, feature_cols = load_model_and_data()

    # Split periods
    train_mask = df["open_time"] <= TRAIN_END
    oos_mask = (df["open_time"] >= OOS_START) & (df["open_time"] <= OOS_END)

    df_train = df[train_mask].reset_index(drop=True)
    X_train = X[train_mask]

    df_oos = df[oos_mask].reset_index(drop=True)
    X_oos = X[oos_mask]

    logger.info("Train: %d rows, OOS: %d rows", len(df_train), len(df_oos))

    # Profile each period
    logger.info("\n--- TRAINING PERIOD (2021-01 — 2025-06-30) ---")
    profiles_train = profile_clusters(df_train, X_train, kmeans, "TRAIN")

    logger.info("\n--- OOS PERIOD (2025-07-01 — 2026-06-30) ---")
    profiles_oos = profile_clusters(df_oos, X_oos, kmeans, "OOS")

    # Compile results
    results = {
        "metadata": {
            "date": pd.Timestamp.now().isoformat(),
            "model": "kmeans_k10_trained_2021_2025_06.pkl",
            "tradeable_clusters": TRADEABLE_CLUSTERS,
            "train_period": ["2021-01-01", TRAIN_END],
            "oos_period": [OOS_START, OOS_END],
        },
        "train": profiles_train,
        "oos": profiles_oos,
        "comparison": {},
    }

    # Compute comparison metrics
    logger.info("\n--- COMPARISON (Train → OOS) ---")
    for cluster_id in TRADEABLE_CLUSTERS:
        train_prof = profiles_train[cluster_id]
        oos_prof = profiles_oos[cluster_id]

        long_mfe_train = train_prof["long_mfe"]["mean"]
        long_mfe_oos = oos_prof["long_mfe"]["mean"]
        long_delta = long_mfe_oos - long_mfe_train

        short_mfe_train = train_prof["short_mfe"]["mean"]
        short_mfe_oos = oos_prof["short_mfe"]["mean"]
        short_delta = short_mfe_oos - short_mfe_train

        results["comparison"][cluster_id] = {
            "long_mfe": {
                "train": long_mfe_train,
                "oos": long_mfe_oos,
                "delta": long_delta,
                "pct_change": (long_delta / long_mfe_train * 100.0) if long_mfe_train != 0 else np.nan,
            },
            "short_mfe": {
                "train": short_mfe_train,
                "oos": short_mfe_oos,
                "delta": short_delta,
                "pct_change": (short_delta / short_mfe_train * 100.0) if short_mfe_train != 0 else np.nan,
            },
        }

        logger.info(
            "Cluster %d: long_mfe %+.6f → %+.6f (Δ %+.6f, %+.1f%%), "
            "short_mfe %+.6f → %+.6f (Δ %+.6f, %+.1f%%)",
            cluster_id,
            long_mfe_train, long_mfe_oos, long_delta, results["comparison"][cluster_id]["long_mfe"]["pct_change"],
            short_mfe_train, short_mfe_oos, short_delta, results["comparison"][cluster_id]["short_mfe"]["pct_change"],
        )

    # Save results
    with open(OUTPUT_JSON, "w") as f:
        json.dump(results, f, indent=2)
    logger.info("Results saved to %s", OUTPUT_JSON)

    # Print summary table
    print("\n" + "=" * 100)
    print("CLUSTER PROFILE COMPARISON: Training vs. OOS")
    print("=" * 100)
    print()
    print("TRAINING PERIOD (2021-01-01 — 2025-06-30):")
    print("-" * 100)
    for cluster_id in TRADEABLE_CLUSTERS:
        prof = profiles_train[cluster_id]
        print(f"  Cluster {cluster_id}: n={prof['n_observations']:6d} | "
              f"long_mfe={prof['long_mfe']['mean']:+.6f}±{prof['long_mfe']['std']:.6f} | "
              f"short_mfe={prof['short_mfe']['mean']:+.6f}±{prof['short_mfe']['std']:.6f}")
    print()
    print("OOS PERIOD (2025-07-01 — 2026-06-30):")
    print("-" * 100)
    for cluster_id in TRADEABLE_CLUSTERS:
        prof = profiles_oos[cluster_id]
        print(f"  Cluster {cluster_id}: n={prof['n_observations']:6d} | "
              f"long_mfe={prof['long_mfe']['mean']:+.6f}±{prof['long_mfe']['std']:.6f} | "
              f"short_mfe={prof['short_mfe']['mean']:+.6f}±{prof['short_mfe']['std']:.6f}")
    print()
    print("COMPARISON (OOS - Train):")
    print("-" * 100)
    for cluster_id in TRADEABLE_CLUSTERS:
        cmp = results["comparison"][cluster_id]
        print(f"  Cluster {cluster_id}: "
              f"long_mfe Δ={cmp['long_mfe']['delta']:+.6f} ({cmp['long_mfe']['pct_change']:+.1f}%) | "
              f"short_mfe Δ={cmp['short_mfe']['delta']:+.6f} ({cmp['short_mfe']['pct_change']:+.1f}%)")
    print("=" * 100)

    return results


if __name__ == "__main__":
    main()
