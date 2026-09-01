"""Per-cluster train vs. validation observation counts for the selected k (t872 follow-up).

Quick diagnostic requested by the coordinator: for the CV-selected k=13
model, refit each of the 4 walk-forward folds exactly as
04_cv_cluster_optimization.py did (same MiniBatchKMeans settings and
random_state, for consistency with the already-reported n_realized_clusters
per fold), and report per-cluster TRAIN n_obs alongside VALID n_obs.

Purpose: distinguish "cluster was already small/rare in training" from
"cluster looked normal in training but vanished only in validation" --
the two have different implications for whether cluster disappearance is a
genuine regime-shift signal or an artifact of an already-marginal cluster.

No new training methodology and no backtest -- reuses the exact fold
definitions and fitting logic from 04_cv_cluster_optimization.py.
"""

from __future__ import annotations

import importlib
import json
import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

_cv = importlib.import_module("research.cluster_regime.04_cv_cluster_optimization")

SELECTED_K = 13  # from cv_k_search_results.json's "optimal_k"
OUTPUT_PATH = Path("artifacts") / "research_cluster_regime" / "cv_per_cluster_train_valid_counts.json"


def main() -> dict:
    """Compute per-fold, per-cluster train/valid observation counts for SELECTED_K."""
    logging.basicConfig(level=logging.INFO)

    logger.info("Loading window representation...")
    df, feature_cols = _cv._load_window_representation()

    results: list[dict] = []

    for fold in _cv.FOLDS:
        df_train = _cv._slice_by_date(df, fold["train_start"], fold["train_end"])
        df_valid = _cv._slice_by_date(df, fold["valid_start"], fold["valid_end"])

        X_train = df_train[feature_cols].to_numpy(dtype=np.float64)
        X_valid = df_valid[feature_cols].to_numpy(dtype=np.float64)

        medians = _cv._fit_medians(X_train)
        X_train_filled = _cv._apply_medians(X_train, medians)
        X_valid_filled = _cv._apply_medians(X_valid, medians)

        kmeans = _cv.MiniBatchKMeans(
            n_clusters=SELECTED_K,
            random_state=_cv.RANDOM_STATE,
            n_init=_cv.SEARCH_N_INIT,
            max_iter=_cv.SEARCH_MAX_ITER,
            batch_size=_cv.SEARCH_BATCH_SIZE,
        )
        kmeans.fit(X_train_filled)

        labels_train = kmeans.labels_
        labels_valid = kmeans.predict(X_valid_filled)

        n_train_total = len(labels_train)
        n_valid_total = len(labels_valid)

        train_counts = np.bincount(labels_train, minlength=SELECTED_K)
        valid_counts = np.bincount(labels_valid, minlength=SELECTED_K)

        for cluster_id in range(SELECTED_K):
            train_n = int(train_counts[cluster_id])
            valid_n = int(valid_counts[cluster_id])
            train_pct = (train_n / n_train_total * 100.0) if n_train_total > 0 else 0.0
            valid_pct = (valid_n / n_valid_total * 100.0) if n_valid_total > 0 else 0.0

            results.append(
                {
                    "fold": fold["name"],
                    "cluster": cluster_id,
                    "train_n_obs": train_n,
                    "valid_n_obs": valid_n,
                    "train_pct": round(train_pct, 4),
                    "valid_pct": round(valid_pct, 4),
                    "train_total": n_train_total,
                    "valid_total": n_valid_total,
                }
            )

        logger.info(
            "%s: train_total=%d valid_total=%d -- computed %d cluster rows",
            fold["name"], n_train_total, n_valid_total, SELECTED_K,
        )

    output = {
        "metadata": {
            "description": "Per-fold, per-cluster train vs. validation observation counts (t872 follow-up)",
            "k": SELECTED_K,
            "note": "Refit per fold using the same MiniBatchKMeans settings/random_state as 04_cv_cluster_optimization.py's search grid.",
        },
        "rows": results,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    logger.info("Saved to %s", OUTPUT_PATH)

    # Print table
    print("\n" + "=" * 90)
    print(f"PER-CLUSTER TRAIN vs. VALID N_OBS (k={SELECTED_K})")
    print("=" * 90)
    print(f"{'Fold':<6}{'Cluster':<9}{'Train n_obs':<13}{'Valid n_obs':<13}{'Train %':<10}{'Valid %':<10}{'Flag':<20}")
    print("-" * 90)
    for row in results:
        flag = ""
        if row["valid_n_obs"] == 0 and row["train_pct"] < 1.0:
            flag = "small-in-train-too"
        elif row["valid_n_obs"] == 0 and row["train_pct"] >= 1.0:
            flag = "VANISHED (was normal train)"
        elif row["train_pct"] < 1.0:
            flag = "small-in-train"
        print(
            f"{row['fold']:<6}{row['cluster']:<9}{row['train_n_obs']:<13}{row['valid_n_obs']:<13}"
            f"{row['train_pct']:<10.3f}{row['valid_pct']:<10.3f}{flag:<20}"
        )
    print("=" * 90)

    return output


if __name__ == "__main__":
    main()
