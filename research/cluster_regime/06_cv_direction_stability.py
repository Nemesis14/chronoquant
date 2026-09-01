"""Direction assignment stability: TRAIN vs. VALID, per fold, per cluster (t872 follow-up).

Quick diagnostic requested by the coordinator: does a cluster's directional
signal (LONG/SHORT/NEUTRAL) survive the transition from its own fold's
training sample to that same fold's validation sample?

Previously, the paired-t-test direction classification (t871 methodology)
was only computed on Fold 4's validation split (`fold4_direction_report` in
cv_k_search_results.json). This extends the same test to BOTH the train and
validation samples of ALL 4 folds, so each cluster's train-time assignment
can be directly compared to its own fold's out-of-fold assignment.

Methodology (unchanged from t871 / 04_cv_cluster_optimization.py):
    diff_i = long_mfe_fw360_i + short_mfe_fw360_i   (paired, per observation)
    t_stat  = mean(diff) / (std(diff) / sqrt(n))
    Bonferroni alpha = 0.05 / k = 0.05 / 13
    assignment = LONG  if t_stat >  t_critical and mean(diff) > 0
                 SHORT if t_stat < -t_critical and mean(diff) < 0
                 NEUTRAL otherwise

For each fold, one KMeans model is fit on that fold's TRAIN split (same
MiniBatchKMeans settings as the CV search / 05_cv_per_cluster_counts.py,
for consistency). The TRAIN assignment uses that same model's labels on
its OWN training rows (in-sample); the VALID assignment uses the model's
predictions on the fold's validation rows (out-of-fold). Comparing the two
answers: does the training-sample direction signal replicate out-of-fold,
or does it weaken (-> NEUTRAL) or, in the worst case, flip sign?
"""

from __future__ import annotations

import importlib
import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

_cv = importlib.import_module("research.cluster_regime.04_cv_cluster_optimization")

SELECTED_K = 13  # from cv_k_search_results.json's "optimal_k"
ALPHA_BASE = 0.05
OUTPUT_PATH = Path("artifacts") / "research_cluster_regime" / "cv_direction_stability_train_vs_valid.json"


def _classify_split(df_split: pd.DataFrame, labels: np.ndarray, k: int, alpha_base: float = ALPHA_BASE) -> dict[int, dict[str, Any]]:
    """Paired t-test direction classification for one (data split, label array) pair.

    Args:
        df_split : DataFrame (train or valid rows) with long_mfe_fw360/short_mfe_fw360.
        labels   : Cluster labels aligned to df_split's rows (in-sample or predicted).
        k        : Number of clusters (for the Bonferroni correction denominator).
        alpha_base : Base alpha before Bonferroni correction.

    Returns:
        Dict of {cluster_id: {...}} classification results.
    """
    alpha_corrected = alpha_base / k
    report: dict[int, dict[str, Any]] = {}

    for cluster_id in range(k):
        mask = labels == cluster_id
        n_obs = int(np.sum(mask))

        paired = df_split.loc[mask, ["long_mfe_fw360", "short_mfe_fw360"]].dropna()
        long_mfe = paired["long_mfe_fw360"].to_numpy()
        short_mfe = paired["short_mfe_fw360"].to_numpy()
        n = len(long_mfe)

        if n < 2:
            report[cluster_id] = {
                "n_obs": n_obs, "n_paired": n, "t_stat": None, "p_value": None, "assignment": "MISSING",
            }
            continue

        diff = long_mfe + short_mfe
        diff_mean = float(np.mean(diff))
        diff_std = float(np.std(diff, ddof=1))
        dof = n - 1

        if diff_std == 0:
            t_stat = 0.0 if diff_mean == 0 else float("inf")
        else:
            t_stat = diff_mean / (diff_std / np.sqrt(n))

        p_value = float(2.0 * (1.0 - stats.t.cdf(abs(t_stat), dof)))
        t_critical = float(stats.t.ppf(1.0 - alpha_corrected / 2.0, dof))
        significant = p_value < alpha_corrected

        if not significant:
            assignment = "NEUTRAL"
        elif t_stat > t_critical and diff_mean > 0:
            assignment = "LONG"
        elif t_stat < -t_critical and diff_mean < 0:
            assignment = "SHORT"
        else:
            assignment = "NEUTRAL"

        report[cluster_id] = {
            "n_obs": n_obs,
            "n_paired": n,
            "diff_mean": diff_mean,
            "t_stat": float(t_stat),
            "p_value": p_value,
            "assignment": assignment,
        }

    return report


def main() -> dict:
    """Compute train-vs-valid direction stability for SELECTED_K across all 4 folds."""
    logging.basicConfig(level=logging.INFO)

    logger.info("Loading window representation...")
    df, feature_cols = _cv._load_window_representation()

    alpha_corrected = ALPHA_BASE / SELECTED_K
    rows: list[dict[str, Any]] = []
    match_count = 0
    total_count = 0
    flip_cases: list[dict[str, Any]] = []
    weaken_cases: list[dict[str, Any]] = []
    strengthen_cases: list[dict[str, Any]] = []

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

        train_report = _classify_split(df_train, labels_train, SELECTED_K)
        valid_report = _classify_split(df_valid, labels_valid, SELECTED_K)

        logger.info("%s: classified train (in-sample) and valid (out-of-fold)", fold["name"])

        for cluster_id in range(SELECTED_K):
            t_info = train_report[cluster_id]
            v_info = valid_report[cluster_id]

            train_assignment = t_info["assignment"]
            valid_assignment = v_info["assignment"]

            match = train_assignment == valid_assignment
            total_count += 1
            if match:
                match_count += 1

            # Classify the type of mismatch (if any)
            change_type = "match"
            if not match:
                is_directional = {"LONG", "SHORT"}
                if train_assignment in is_directional and valid_assignment in is_directional:
                    change_type = "FLIP"  # LONG<->SHORT reversal -- worst case
                elif train_assignment in is_directional and valid_assignment == "NEUTRAL":
                    change_type = "weaken"  # significant -> neutral
                elif train_assignment == "NEUTRAL" and valid_assignment in is_directional:
                    change_type = "strengthen"  # neutral -> significant (emerges OOS)
                elif train_assignment == "MISSING" or valid_assignment == "MISSING":
                    change_type = "missing_data"

            row = {
                "fold": fold["name"],
                "cluster": cluster_id,
                "train_assignment": train_assignment,
                "train_n_obs": t_info["n_obs"],
                "train_p_value": t_info["p_value"],
                "valid_assignment": valid_assignment,
                "valid_n_obs": v_info["n_obs"],
                "valid_p_value": v_info["p_value"],
                "match": match,
                "change_type": change_type,
            }
            rows.append(row)

            if change_type == "FLIP":
                flip_cases.append(row)
            elif change_type == "weaken":
                weaken_cases.append(row)
            elif change_type == "strengthen":
                strengthen_cases.append(row)

    summary = {
        "total_combinations": total_count,
        "match_count": match_count,
        "match_pct": round(match_count / total_count * 100.0, 2) if total_count > 0 else None,
        "mismatch_count": total_count - match_count,
        "flip_count": len(flip_cases),
        "weaken_count": len(weaken_cases),
        "strengthen_count": len(strengthen_cases),
        "flip_cases": flip_cases,
    }

    output = {
        "metadata": {
            "description": "Direction assignment stability: TRAIN (in-sample) vs. VALID (out-of-fold), per fold, per cluster (t872 follow-up)",
            "k": SELECTED_K,
            "alpha_base": ALPHA_BASE,
            "alpha_corrected_bonferroni": alpha_corrected,
            "methodology": "Paired t-test (diff_i = long_mfe_i + short_mfe_i) per split; TRAIN = in-sample labels from the fold's own fit, VALID = out-of-fold predicted labels.",
        },
        "summary": summary,
        "rows": rows,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    logger.info("Saved to %s", OUTPUT_PATH)

    # Print table
    print("\n" + "=" * 110)
    print(f"DIRECTION ASSIGNMENT STABILITY: TRAIN vs. VALID (k={SELECTED_K})")
    print("=" * 110)
    print(f"{'Fold':<6}{'Cl':<4}{'Train Assign':<14}{'Train p':<12}{'Valid Assign':<14}{'Valid p':<12}{'Match':<7}{'Change':<12}")
    print("-" * 110)
    for row in rows:
        tp = f"{row['train_p_value']:.5f}" if row["train_p_value"] is not None else "N/A"
        vp = f"{row['valid_p_value']:.5f}" if row["valid_p_value"] is not None else "N/A"
        print(
            f"{row['fold']:<6}{row['cluster']:<4}{row['train_assignment']:<14}{tp:<12}"
            f"{row['valid_assignment']:<14}{vp:<12}{'Yes' if row['match'] else 'No':<7}{row['change_type']:<12}"
        )
    print("-" * 110)
    print(f"Total combinations: {summary['total_combinations']}")
    print(f"Match: {summary['match_count']} ({summary['match_pct']}%)")
    print(f"Mismatch: {summary['mismatch_count']}")
    print(f"  -> FLIP (LONG<->SHORT reversal): {summary['flip_count']}")
    print(f"  -> weaken (significant -> NEUTRAL): {summary['weaken_count']}")
    print(f"  -> strengthen (NEUTRAL -> significant OOS): {summary['strengthen_count']}")
    print("=" * 110)

    return output


if __name__ == "__main__":
    main()
