"""CV-optimized cluster separation via walk-forward ANOVA F-score search (t872).

Replaces the fixed-k=10 approach from t863-t871 (which had no exploitable
trading edge under realistic execution, see
`_doc_/models_doc/6501_k10_validation_realistic_backtest.md`) with a
data-driven search over k=2..30 that selects the number of clusters whose
directional separation (long vs. short MFE signal) is STRONGEST and MOST
STABLE across time, evaluated strictly out-of-fold.

CV structure: 4-fold walk-forward (NOT random k-fold -- this is a time
series, random folds would leak future information into training):

    Fold 1: Train 2021-2022        -> Validate 2023
    Fold 2: Train 2021-2023        -> Validate 2024
    Fold 3: Train 2021-2024        -> Validate 2025 H1 (2025-01 -- 2025-06)
    Fold 4: Train 2021-2025 H1     -> Validate 2025 H2 -- 2026 H1 (2025-07 -- 2026-06)

Metric: one-way ANOVA F-statistic on the per-observation directional signal
    signal_i = long_mfe_fw360_i + short_mfe_fw360_i
grouped by predicted cluster label, ALWAYS measured on the validation split
(cluster model fit on train, predicted on validation) -- never on train,
where it would be trivially inflated.

    cv_score(k) = mean(F_fold1, F_fold2, F_fold3, F_fold4)
    cv_std(k)   = std(F_fold1, F_fold2, F_fold3, F_fold4)

Optimal k selection: highest cv_score, tie-broken by stability (low
cv_std/cv_score coefficient of variation); k values where a single fold
dominates the mean F (> 50% of the fold sum) are flagged as likely
overfit/unstable and deprioritized.

NO TRADING BACKTEST IN THIS SCRIPT -- see epic_065 ticket t872: this task
only optimizes clustering quality; a trading validation is a later task,
conditional on this one finding a stable, non-trivial separation.

Statistical caveat: the underlying signal series is autocorrelated (as
established in t867/t871), so f_oneway's classical p-values are optimistic
(effective N < nominal N). The F-statistic is used here purely as a
RELATIVE ranking metric across k and across folds -- consistent within this
comparison because the same autocorrelation-driven inflation applies
uniformly to every k -- not as an absolute significance test.
"""

from __future__ import annotations

import json
import logging
import pickle
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
from scipy import stats
from sklearn.cluster import KMeans, MiniBatchKMeans

logger = logging.getLogger(__name__)

# %% Configuration

WINDOW_REPR_PATH = Path("artifacts") / "research_cluster_regime" / "solusdt_window_repr.parquet"
OUTPUT_DIR = Path("artifacts") / "research_cluster_regime"

RANDOM_STATE = 42
K_MIN = 2
K_MAX = 30

# MiniBatchKMeans settings for the k-search grid (speed over the 29 x 4 = 116 fits)
SEARCH_N_INIT = 3
SEARCH_MAX_ITER = 100
SEARCH_BATCH_SIZE = 2048

# Full KMeans settings for the final selected-k model (fit once, quality over speed)
FINAL_N_INIT = 10
FINAL_MAX_ITER = 300

ALPHA_BASE = 0.05

FOLDS: list[dict[str, str]] = [
    {"name": "fold1", "train_start": "2021-01-01", "train_end": "2022-12-31", "valid_start": "2023-01-01", "valid_end": "2023-12-31"},
    {"name": "fold2", "train_start": "2021-01-01", "train_end": "2023-12-31", "valid_start": "2024-01-01", "valid_end": "2024-12-31"},
    {"name": "fold3", "train_start": "2021-01-01", "train_end": "2024-12-31", "valid_start": "2025-01-01", "valid_end": "2025-06-30"},
    {"name": "fold4", "train_start": "2021-01-01", "train_end": "2025-06-30", "valid_start": "2025-07-01", "valid_end": "2026-06-30"},
]

# Overall full-data range for the final selected-k model
FULL_DATA_START = "2021-01-01"
FULL_DATA_END = "2026-06-30"

# A single fold contributing more than this share of the total fold-F sum
# flags the k as likely overfit to one period rather than a stable pattern.
SINGLE_FOLD_DOMINANCE_THRESHOLD = 0.5


@dataclass
class FoldResult:
    """ANOVA F-score result for one (k, fold) combination."""
    fold_name: str
    k: int
    n_train: int
    n_valid: int
    n_realized_clusters: int
    f_stat: float
    p_value: float
    per_cluster_signal: dict[int, dict[str, float]] = field(default_factory=dict)


# %% Data loading and splitting


def _load_window_representation() -> tuple[pd.DataFrame, list[str]]:
    """Load the window representation parquet and identify feature columns.

    Returns:
        Tuple of (DataFrame, feature_column_names).
    """
    if not WINDOW_REPR_PATH.exists():
        raise FileNotFoundError(
            f"Window representation not found: {WINDOW_REPR_PATH}. "
            "Run 03_feature_selection.py, then 01_target_computation.py, "
            "then 02_window_representation.py first."
        )

    df = pl.read_parquet(str(WINDOW_REPR_PATH)).to_pandas()
    df["open_time"] = pd.to_datetime(df["open_time"])

    skip_prefixes = ("open_time", "long_mfe_fw", "short_mfe_fw")
    feature_cols = [c for c in df.columns if not any(c.startswith(p) for p in skip_prefixes)]

    logger.info("Loaded window representation: %d rows, %d feature columns", len(df), len(feature_cols))
    return df, feature_cols


def _slice_by_date(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """Return the subset of df with open_time in [start, end] (inclusive)."""
    mask = (df["open_time"] >= start) & (df["open_time"] <= end)
    return df.loc[mask].reset_index(drop=True)


def _fit_medians(X_train: np.ndarray) -> np.ndarray:
    """Compute per-column medians from TRAIN data only (for NaN imputation)."""
    medians = np.zeros(X_train.shape[1])
    for j in range(X_train.shape[1]):
        col = X_train[:, j]
        valid = col[~np.isnan(col)]
        medians[j] = np.median(valid) if len(valid) > 0 else 0.0
    return medians


def _apply_medians(X: np.ndarray, medians: np.ndarray) -> np.ndarray:
    """Fill NaNs in X using precomputed (train-only) per-column medians."""
    X_filled = X.copy()
    for j in range(X.shape[1]):
        mask_nan = np.isnan(X_filled[:, j])
        if np.any(mask_nan):
            X_filled[mask_nan, j] = medians[j]
    return X_filled


# %% ANOVA F-score evaluation


def _compute_signal(df: pd.DataFrame) -> pd.Series:
    """Per-observation directional signal: long_mfe_fw360 + short_mfe_fw360.

    Since short_mfe_fw360 < 0 typically, this equals |long_mfe| - |short_mfe|
    (positive => long-biased excursion, negative => short-biased excursion).
    """
    return df["long_mfe_fw360"] + df["short_mfe_fw360"]


def _evaluate_fold(
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    feature_cols: list[str],
    k: int,
    fold_name: str,
) -> FoldResult:
    """Fit a k-cluster model on train, evaluate ANOVA F-score on validation.

    Args:
        df_train     : Training-period window representation rows.
        df_valid     : Validation-period window representation rows.
        feature_cols : Feature column names for clustering.
        k            : Number of clusters.
        fold_name    : Fold identifier (for logging/results).

    Returns:
        FoldResult with the validation-only F-statistic and per-cluster signal stats.
    """
    X_train = df_train[feature_cols].to_numpy(dtype=np.float64)
    X_valid = df_valid[feature_cols].to_numpy(dtype=np.float64)

    medians = _fit_medians(X_train)
    X_train_filled = _apply_medians(X_train, medians)
    X_valid_filled = _apply_medians(X_valid, medians)

    kmeans = MiniBatchKMeans(
        n_clusters=k,
        random_state=RANDOM_STATE,
        n_init=SEARCH_N_INIT,
        max_iter=SEARCH_MAX_ITER,
        batch_size=SEARCH_BATCH_SIZE,
    )
    kmeans.fit(X_train_filled)

    labels_valid = kmeans.predict(X_valid_filled)
    signal_valid = _compute_signal(df_valid).to_numpy()

    valid_mask = ~np.isnan(signal_valid)
    labels_valid = labels_valid[valid_mask]
    signal_valid = signal_valid[valid_mask]

    unique_clusters = np.unique(labels_valid)
    groups = [signal_valid[labels_valid == c] for c in unique_clusters]
    groups = [g for g in groups if len(g) >= 1]

    per_cluster_signal = {
        int(c): {
            "n": int(len(g)),
            "mean": float(np.mean(g)) if len(g) > 0 else float("nan"),
            "std": float(np.std(g, ddof=1)) if len(g) > 1 else float("nan"),
        }
        for c, g in zip(unique_clusters, groups, strict=True)
    }

    if len(groups) < 2 or all(len(g) < 2 for g in groups):
        f_stat, p_value = float("nan"), float("nan")
    else:
        eligible_groups = [g for g in groups if len(g) >= 1]
        f_stat, p_value = stats.f_oneway(*eligible_groups)
        f_stat, p_value = float(f_stat), float(p_value)

    return FoldResult(
        fold_name=fold_name,
        k=k,
        n_train=len(df_train),
        n_valid=len(df_valid),
        n_realized_clusters=len(unique_clusters),
        f_stat=f_stat,
        p_value=p_value,
        per_cluster_signal=per_cluster_signal,
    )


# %% K search


def run_cv_k_search(df: pd.DataFrame, feature_cols: list[str]) -> dict[int, dict[str, Any]]:
    """Run the full walk-forward CV F-score search over k=K_MIN..K_MAX.

    Args:
        df           : Full window representation DataFrame.
        feature_cols : Feature columns used for clustering.

    Returns:
        Dict with per-k, per-fold results and aggregated cv_score/cv_std.
    """
    fold_data = []
    for fold in FOLDS:
        df_train = _slice_by_date(df, fold["train_start"], fold["train_end"])
        df_valid = _slice_by_date(df, fold["valid_start"], fold["valid_end"])
        fold_data.append((fold["name"], df_train, df_valid))
        logger.info(
            "%s: train=[%s, %s] n=%d, valid=[%s, %s] n=%d",
            fold["name"], fold["train_start"], fold["train_end"], len(df_train),
            fold["valid_start"], fold["valid_end"], len(df_valid),
        )

    k_results: dict[int, dict[str, Any]] = {}

    for k in range(K_MIN, K_MAX + 1):
        t0 = time.time()
        fold_results: list[FoldResult] = []

        for fold_name, df_train, df_valid in fold_data:
            fr = _evaluate_fold(df_train, df_valid, feature_cols, k, fold_name)
            fold_results.append(fr)

        f_stats = np.array([fr.f_stat for fr in fold_results])
        f_stats_valid = f_stats[~np.isnan(f_stats)]

        if len(f_stats_valid) == 0:
            cv_score, cv_std, cv_cv, dominance = float("nan"), float("nan"), float("nan"), float("nan")
        else:
            cv_score = float(np.mean(f_stats_valid))
            cv_std = float(np.std(f_stats_valid, ddof=1)) if len(f_stats_valid) > 1 else 0.0
            cv_cv = (cv_std / cv_score) if cv_score != 0 else float("inf")
            f_sum = float(np.sum(f_stats_valid))
            dominance = float(np.max(f_stats_valid) / f_sum) if f_sum > 0 else float("nan")

        elapsed = time.time() - t0
        logger.info(
            "k=%2d: cv_score=%.4f cv_std=%.4f cv_cv=%.4f dominance=%.3f (%.1fs)",
            k, cv_score, cv_std, cv_cv, dominance, elapsed,
        )

        k_results[k] = {
            "cv_score": cv_score,
            "cv_std": cv_std,
            "cv_coefficient_of_variation": cv_cv,
            "single_fold_dominance": dominance,
            "unstable_flag": bool(dominance > SINGLE_FOLD_DOMINANCE_THRESHOLD) if not np.isnan(dominance) else True,
            "folds": [
                {
                    "fold_name": fr.fold_name,
                    "n_train": fr.n_train,
                    "n_valid": fr.n_valid,
                    "n_realized_clusters": fr.n_realized_clusters,
                    "f_stat": fr.f_stat,
                    "p_value": fr.p_value,
                }
                for fr in fold_results
            ],
        }

    return k_results


def select_optimal_k(k_results: dict[int, dict[str, Any]]) -> tuple[int, str]:
    """Select the optimal k: highest cv_score among stable (non-dominated) candidates.

    Args:
        k_results : Per-k results from run_cv_k_search.

    Returns:
        Tuple of (optimal_k, rationale_text).
    """
    stable_candidates = {
        k: r for k, r in k_results.items()
        if not r["unstable_flag"] and not np.isnan(r["cv_score"])
    }

    if stable_candidates:
        best_k = max(stable_candidates, key=lambda k: stable_candidates[k]["cv_score"])
        rationale = (
            f"k={best_k} selected: highest cv_score ({stable_candidates[best_k]['cv_score']:.4f}) "
            f"among stable candidates (single-fold dominance <= {SINGLE_FOLD_DOMINANCE_THRESHOLD:.0%}). "
            f"cv_std={stable_candidates[best_k]['cv_std']:.4f}, "
            f"CV={stable_candidates[best_k]['cv_coefficient_of_variation']:.4f}."
        )
    else:
        all_valid = {k: r for k, r in k_results.items() if not np.isnan(r["cv_score"])}
        best_k = max(all_valid, key=lambda k: all_valid[k]["cv_score"])
        rationale = (
            f"k={best_k} selected: highest cv_score ({all_valid[best_k]['cv_score']:.4f}), "
            "but NO candidate passed the single-fold-dominance stability check -- "
            "every k tested shows F-score concentrated in one fold. Treat this result "
            "with caution; it may indicate no stable cross-period separation exists at any k."
        )

    return best_k, rationale


# %% Final model fit


def fit_final_model(df: pd.DataFrame, feature_cols: list[str], k: int) -> tuple[KMeans, np.ndarray]:
    """Fit the final full-batch KMeans model with the selected k on the FULL dataset.

    Args:
        df           : Full window representation DataFrame.
        feature_cols : Feature columns used for clustering.
        k            : Selected optimal number of clusters.

    Returns:
        Tuple of (fitted KMeans model, per-column medians used for NaN imputation).
    """
    df_full = _slice_by_date(df, FULL_DATA_START, FULL_DATA_END)
    X_full = df_full[feature_cols].to_numpy(dtype=np.float64)
    medians = _fit_medians(X_full)
    X_full_filled = _apply_medians(X_full, medians)

    logger.info("Fitting FINAL model: k=%d on full data (%d rows, %s..%s)", k, len(df_full), FULL_DATA_START, FULL_DATA_END)
    kmeans = KMeans(
        n_clusters=k,
        init="k-means++",
        n_init=FINAL_N_INIT,
        max_iter=FINAL_MAX_ITER,
        random_state=RANDOM_STATE,
    )
    kmeans.fit(X_full_filled)
    logger.info("Final model fitted: inertia=%.2f", kmeans.inertia_)

    return kmeans, medians


# %% Fold-4 out-of-fold per-cluster direction report (CV-validated, no leakage)


def build_fold4_direction_report(
    df: pd.DataFrame,
    feature_cols: list[str],
    k: int,
) -> dict[str, Any]:
    """Build a CV-validated per-cluster direction report using Fold 4's out-of-fold split.

    Fold 4 (train=2021-01..2025-06, validate=2025-07..2026-06) is the only
    fold whose validation window is genuinely "future" relative to its
    training window and covers the most recent regime -- it is used here
    (rather than averaging across folds, since KMeans cluster labels are not
    guaranteed aligned across independently-fit folds) to report per-cluster
    direction with a corrected PAIRED t-test (see t871's
    08_paired_ttest_classification.py methodology): diff_i = long_mfe_i +
    short_mfe_i, Bonferroni-corrected alpha = 0.05/k.

    Args:
        df           : Full window representation DataFrame.
        feature_cols : Feature columns used for clustering.
        k            : Selected optimal number of clusters.

    Returns:
        Dict of per-cluster direction classification, fit strictly on Fold 4's
        training window and evaluated strictly on its validation window.
    """
    fold4 = FOLDS[-1]
    df_train = _slice_by_date(df, fold4["train_start"], fold4["train_end"])
    df_valid = _slice_by_date(df, fold4["valid_start"], fold4["valid_end"])

    X_train = df_train[feature_cols].to_numpy(dtype=np.float64)
    X_valid = df_valid[feature_cols].to_numpy(dtype=np.float64)
    medians = _fit_medians(X_train)
    X_train_filled = _apply_medians(X_train, medians)
    X_valid_filled = _apply_medians(X_valid, medians)

    kmeans_fold4 = KMeans(n_clusters=k, init="k-means++", n_init=FINAL_N_INIT, max_iter=FINAL_MAX_ITER, random_state=RANDOM_STATE)
    kmeans_fold4.fit(X_train_filled)
    labels_valid = kmeans_fold4.predict(X_valid_filled)

    alpha_corrected = ALPHA_BASE / k

    report: dict[str, Any] = {}
    for cluster_id in range(k):
        mask = labels_valid == cluster_id
        n_obs = int(np.sum(mask))

        paired = df_valid.loc[mask, ["long_mfe_fw360", "short_mfe_fw360"]].dropna()
        long_mfe = paired["long_mfe_fw360"].to_numpy()
        short_mfe = paired["short_mfe_fw360"].to_numpy()
        n = len(long_mfe)

        if n < 2:
            report[str(cluster_id)] = {"n_obs": n_obs, "n_paired": n, "assignment": "MISSING"}
            continue

        diff = long_mfe + short_mfe
        diff_mean = float(np.mean(diff))
        diff_std = float(np.std(diff, ddof=1))
        df_dof = n - 1

        if diff_std == 0:
            t_stat = 0.0 if diff_mean == 0 else float("inf")
        else:
            t_stat = diff_mean / (diff_std / np.sqrt(n))

        p_value = float(2.0 * (1.0 - stats.t.cdf(abs(t_stat), df_dof)))
        t_critical = float(stats.t.ppf(1.0 - alpha_corrected / 2.0, df_dof))
        significant = p_value < alpha_corrected

        if not significant:
            assignment = "NEUTRAL"
        elif t_stat > t_critical and diff_mean > 0:
            assignment = "LONG"
        elif t_stat < -t_critical and diff_mean < 0:
            assignment = "SHORT"
        else:
            assignment = "NEUTRAL"

        report[str(cluster_id)] = {
            "n_obs": n_obs,
            "n_paired": n,
            "long_mfe_mean": float(np.mean(long_mfe)),
            "short_mfe_mean": float(np.mean(short_mfe)),
            "diff_mean": diff_mean,
            "t_stat": float(t_stat),
            "p_value": p_value,
            "alpha_corrected": alpha_corrected,
            "significant_bonferroni": bool(significant),
            "assignment": assignment,
        }

    return {
        "methodology": (
            "Paired t-test (diff_i = long_mfe_i + short_mfe_i), Bonferroni alpha=0.05/k. "
            "Model fit on Fold 4 train (2021-01..2025-06), evaluated on Fold 4 "
            "validation (2025-07..2026-06) -- genuinely out-of-fold, no leakage."
        ),
        "fold": "fold4",
        "train_period": [fold4["train_start"], fold4["train_end"]],
        "valid_period": [fold4["valid_start"], fold4["valid_end"]],
        "k": k,
        "alpha_base": ALPHA_BASE,
        "per_cluster": report,
    }


# %% Orchestration


def main() -> dict[str, Any]:
    """Run the full CV-optimized cluster separation pipeline."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Step 1: Loading window representation...")
    df, feature_cols = _load_window_representation()

    logger.info("Step 2: Running walk-forward CV k-search (k=%d..%d)...", K_MIN, K_MAX)
    k_results = run_cv_k_search(df, feature_cols)

    logger.info("Step 3: Selecting optimal k...")
    optimal_k, rationale = select_optimal_k(k_results)
    logger.info("Optimal k = %d. %s", optimal_k, rationale)

    logger.info("Step 4: Fitting final model (full data, k=%d)...", optimal_k)
    final_model, medians = fit_final_model(df, feature_cols, optimal_k)

    logger.info("Step 5: Building Fold-4 out-of-fold direction report...")
    direction_report = build_fold4_direction_report(df, feature_cols, optimal_k)

    logger.info("Step 6: Saving outputs...")

    search_results = {
        "metadata": {
            "description": "CV-optimized cluster separation via walk-forward ANOVA F-score search (t872)",
            "k_range": [K_MIN, K_MAX],
            "random_state": RANDOM_STATE,
            "n_folds": len(FOLDS),
            "folds": FOLDS,
            "search_algorithm": "MiniBatchKMeans",
            "search_params": {"n_init": SEARCH_N_INIT, "max_iter": SEARCH_MAX_ITER, "batch_size": SEARCH_BATCH_SIZE},
            "final_algorithm": "KMeans",
            "final_params": {"n_init": FINAL_N_INIT, "max_iter": FINAL_MAX_ITER},
            "signal_formula": "signal_i = long_mfe_fw360_i + short_mfe_fw360_i",
            "metric": "one-way ANOVA F-statistic, measured on validation split only",
            "single_fold_dominance_threshold": SINGLE_FOLD_DOMINANCE_THRESHOLD,
            "statistical_caveat": (
                "F-statistic used for RELATIVE ranking across k, not absolute "
                "significance -- underlying signal is autocorrelated, so classical "
                "p-values are optimistic (see t867/t871 effective-N discussion)."
            ),
        },
        "k_results": {str(k): v for k, v in k_results.items()},
        "optimal_k": optimal_k,
        "optimal_k_rationale": rationale,
        "fold4_direction_report": direction_report,
    }

    search_results_path = OUTPUT_DIR / "cv_k_search_results.json"
    with open(search_results_path, "w") as f:
        json.dump(search_results, f, indent=2)
    logger.info("CV k-search results saved to %s", search_results_path)

    model_path = OUTPUT_DIR / "cv_optimal_k_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(
            {
                "model": final_model,
                "feature_columns": feature_cols,
                "medians": medians,
                "k": optimal_k,
                "train_period": [FULL_DATA_START, FULL_DATA_END],
            },
            f,
        )
    logger.info("Final optimal-k model saved to %s", model_path)

    print("\n" + "=" * 90)
    print("t872 CV-OPTIMIZED CLUSTER SEPARATION -- SEARCH SUMMARY")
    print("=" * 90)
    print(f"K range tested:    {K_MIN}..{K_MAX}")
    print(f"Optimal k:         {optimal_k}")
    print(f"Rationale:         {rationale}")
    print("-" * 90)
    print(f"{'k':<4} {'cv_score':<12} {'cv_std':<12} {'cv_cv':<10} {'dominance':<10} {'unstable':<10}")
    for k in sorted(k_results.keys()):
        r = k_results[k]
        marker = " <== SELECTED" if k == optimal_k else ""
        print(
            f"{k:<4} {r['cv_score']:<12.4f} {r['cv_std']:<12.4f} "
            f"{r['cv_coefficient_of_variation']:<10.4f} {r['single_fold_dominance']:<10.3f} "
            f"{str(r['unstable_flag']):<10}{marker}"
        )
    print("=" * 90)

    return search_results


if __name__ == "__main__":
    main()
