# =============================================================================
# LightGBM hyperparameter search — DuckDB-native (snap ⋈ model.__sample)
# =============================================================================
# Inputs:
#   artifact_dir/feature_engineering/feature_set.json  → selected feature list
#   snap."<snapshot_id>" ⋈ model."<model_id>__sample"  → CV data (fold_id, target, feat_*)
#   config/models.json (sampling section)               → fold time windows + purge_minutes
# Outputs:
#   artifact_dir/search/search_best.json   — full best trial record
#   artifact_dir/search/best_params.json   — best hyperparameter dict only
#   artifact_dir/search/search_trials.jsonl, search_summary.csv, trial_logs/, trial_curves/
#
# Entry point: run_search(model_id, stage, n_trials, ...)
# =============================================================================

import contextlib
import gc
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

import utils
from modeling.sampling import generate_walk_forward_folds
from modeling.training.datasets import ModelingDataset
from modeling.training.training_windows import DatasetSplit

logger = logging.getLogger("lgbm_search")

# ─── Fixed LightGBM parameters (not tuned) ───────────────────────────────────
_FIXED_PARAMS: dict = {
    "objective":      "regression",
    "boosting_type":  "gbdt",
    "metric":         "rmse",
    "n_estimators":   3000,
    "subsample_freq": 1,
    "force_col_wise": True,
    "verbosity":      -1,
    "n_jobs":         4,
}

# ─── Objective: Top10 Lift with fold-stability penalty ───────────────────────
# objective = mean(top10_lift_folds) - LIFT_LAMBDA * std(top10_lift_folds)
# Higher is better → negate for Optuna minimize (objective_score = -objective)
_LIFT_LAMBDA = 0.5

_ES_ROUNDS        = 100
_CURVE_MAX_POINTS = 100

try:
    import optuna  # type: ignore[import-not-found]
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAS_OPTUNA = True
except ImportError:
    _HAS_OPTUNA = False


# =============================================================================
# Search dataset
# =============================================================================
@dataclass(frozen=True)
class _SearchDataset:
    dataset:  ModelingDataset
    fold_ids: pd.Series   # Int8 1-4 — aligned with dataset rows


# =============================================================================
# run_search  —  main entry point
# =============================================================================
def run_search(
    model_id:      str,
    stage:         str          = "smoke",
    n_trials:      int          = 60,
    timeout_hours: float | None = None,
    row_stride:    int | None   = None,
    fold_limit:    int | None   = None,
    retry_failed:  bool         = False,
) -> dict:
    """
    Hyperparameter search for a yearly-sample LightGBM model.

    Reads feature list from artifact_dir/feature_engineering/feature_set.json
    and CV structure from sample_dir/metadata.json (fold_week_assignments, n_folds).

    Stages
    ------
    smoke    5 trials / 2 folds  — pipeline sanity check
    explore  60 trials / all folds — broad region search
    refine   30 trials / all folds — narrow best regions
    """
    _setup_logging()

    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta         = models_cfg["models"][model_id]
    target_col   = meta["target_name"]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))
    search_dir   = artifact_dir / "search"
    search_dir.mkdir(parents=True, exist_ok=True)

    n_trials, row_stride, fold_limit = _apply_stage_defaults(
        stage, n_trials, row_stride, fold_limit
    )

    feature_cols  = _load_feature_cols(artifact_dir)
    # Load fold metadata from model config + DuckDB (DuckDB-native path, plan 5.1).
    sample_meta = _load_model_sample_meta(model_id, meta)
    purge_minutes     = sample_meta.get("purge_minutes", 240)

    # Champion models always use walk-forward CV (metadata always has fold_time_windows).
    fold_time_windows     = sample_meta.get("fold_time_windows")
    fold_week_assignments = sample_meta.get("fold_week_assignments")
    use_walk_forward      = fold_time_windows is not None

    n_folds = len(fold_time_windows) if use_walk_forward else sample_meta.get("n_folds", 4)

    all_fold_ids    = list(range(1, n_folds + 1))
    fold_ids_to_run = all_fold_ids[:fold_limit] if fold_limit else all_fold_ids

    logger.info("=" * 72)
    logger.info(f"LGBM SEARCH  model={model_id}")
    logger.info(f"  target={target_col}  stage={stage}")
    logger.info(f"  cv_mode={'walk_forward' if use_walk_forward else 'weekly'}")
    logger.info(f"  n_trials={n_trials}  row_stride={row_stride}  folds={len(fold_ids_to_run)}/{n_folds}")
    logger.info(f"  engine={'optuna-TPE' if _HAS_OPTUNA else 'seeded-random'}")
    logger.info(f"  search_dir={search_dir}")
    logger.info("=" * 72)

    sd = _load_search_dataset(
        model_id     = model_id,
        meta         = meta,
        target_col   = target_col,
        feature_cols = feature_cols,
        row_stride   = row_stride,
    )
    logger.info(
        f"[Data] {len(sd.dataset.y):,} rows  "
        f"target_mean={float(sd.dataset.y.mean()):.6f}  "
        f"n_features={len(feature_cols)}"
    )

    done_hashes, fail_hashes = _load_existing_hashes(search_dir)
    if retry_failed:
        fail_hashes = set()
    logger.info(
        f"[Resume] {len(done_hashes)} completed, {len(fail_hashes)} failed in log"
    )

    if _HAS_OPTUNA:
        best = _search_optuna(
            model_id, sd, fold_ids_to_run, fold_week_assignments, purge_minutes,
            search_dir, feature_cols, row_stride,
            n_trials, timeout_hours, stage, done_hashes, fail_hashes,
            fold_time_windows=fold_time_windows,
        )
    else:
        best = _search_random(
            model_id, sd, fold_ids_to_run, fold_week_assignments, purge_minutes,
            search_dir, feature_cols, row_stride,
            n_trials, timeout_hours, stage, done_hashes, fail_hashes,
            fold_time_windows=fold_time_windows,
        )

    if best:
        _write_best_params(search_dir, best)

    _register_search_provenance(model_id, stage, best, search_dir)

    _print_final_summary(best, search_dir)
    return best


def _register_search_provenance(
    model_id:   str,
    stage:      str,
    best:       dict,
    search_dir: Path,
) -> None:
    """Record the search run in reg.search_runs + its log files in reg.artifacts.

    Best-effort: a registry failure must not lose a completed search, so the write
    is wrapped — the search artifacts on disk remain the source of truth.
    """
    if not best:
        return
    try:
        from modeling import provenance
        provenance.register_search_run(model_id, stage, best)
        provenance.register_artifacts(
            model_id,
            [
                ("search_best",    search_dir / "search_best.json"),
                ("search_trials",  search_dir / "search_trials.jsonl"),
                ("search_summary", search_dir / "search_summary.csv"),
            ],
        )
    except Exception as exc:  # noqa: BLE001 — provenance must never abort the search
        logger.warning("[search] registry provenance write failed: %s", exc)


# =============================================================================
# Stage defaults
# =============================================================================
def _apply_stage_defaults(
    stage:      str,
    n_trials:   int,
    row_stride: int | None,
    fold_limit: int | None,
) -> tuple[int, int, int | None]:
    if stage == "smoke":
        return min(n_trials, 5), row_stride or 1, fold_limit or 2
    if stage == "explore":
        return min(n_trials, 60), row_stride or 1, fold_limit
    if stage == "refine":
        return min(n_trials, 30), row_stride or 1, fold_limit
    return n_trials, row_stride or 1, fold_limit


# =============================================================================
# Feature loading
# =============================================================================
def _load_feature_cols(artifact_dir: Path) -> list[str]:
    fe_json = artifact_dir / "feature_engineering" / "feature_set.json"
    if not fe_json.exists():
        raise FileNotFoundError(
            f"feature_set.json not found: {fe_json}\n"
            "Run the feature_engineering step first:\n"
            "  pipeline.py --model <model_id> --step feature_engineering"
        )
    data = json.loads(fe_json.read_text(encoding="utf-8"))
    cols = data.get("selected", [])
    if not cols:
        raise ValueError(f"feature_set.json 'selected' list is empty: {fe_json}")
    return list(cols)


# =============================================================================
# Fold metadata — derived from model config (DuckDB-native path)
# =============================================================================
def _load_model_sample_meta(model_id: str, meta: dict) -> dict:
    """Build sample metadata (fold_time_windows, purge_minutes, n_folds) from model config.

    Replaces the old ``load_yearly_sample`` / metadata.json path.  The fold windows
    are derived deterministically from the sampling section of the model config,
    using the same ``generate_walk_forward_folds`` as ``create_model_sample``.

    Args:
        model_id : Model key (used for log messages).
        meta     : Model config dict from config/models.json.

    Returns:
        Dict with ``fold_time_windows``, ``purge_minutes``, ``n_folds``.
    """
    sampling_meta = meta.get("sampling", {})
    year          = _anchor_year_from_meta(sampling_meta)
    train_months  = int(sampling_meta.get("train_months", 9))
    valid_months  = int(sampling_meta.get("valid_months", 3))
    shift_months  = int(sampling_meta.get("shift_months", 3))
    n_folds       = int(sampling_meta.get("n_folds", 4))
    purge_minutes = int(sampling_meta.get("purge_minutes", 240))

    fold_time_windows = generate_walk_forward_folds(
        year          = year,
        train_months  = train_months,
        valid_months  = valid_months,
        shift_months  = shift_months,
        purge_minutes = purge_minutes,
        n_folds       = n_folds,
    )
    logger.info(
        "[sample_meta] model=%s year=%d n_folds=%d purge_minutes=%d",
        model_id, year, n_folds, purge_minutes,
    )
    return {
        "fold_time_windows": fold_time_windows,
        "purge_minutes":     purge_minutes,
        "n_folds":           n_folds,
    }


def _anchor_year_from_meta(sampling_meta: dict) -> int:
    """Resolve the anchor calendar year from sampling metadata."""
    if "year" in sampling_meta:
        return int(sampling_meta["year"])
    sample_id = sampling_meta.get("sample_id", "")
    if "_yearly_" in sample_id:
        return int(sample_id.split("_yearly_")[-1])
    if sample_id:
        tail = sample_id.split("_")[-1]
        if tail.isdigit():
            return int(tail)
    return 2023


# =============================================================================
# Dataset loading — snap ⋈ model.__sample JOIN (DuckDB-native, plan 5.1)
# =============================================================================
def _load_search_dataset(
    model_id:     str,
    meta:         dict,
    target_col:   str,
    feature_cols: list[str],
    row_stride:   int,
) -> _SearchDataset:
    """Load the CV dataset from snap ⋈ model.__sample (DuckDB-native path, plan 5.1).

    The sample table carries open_time + target + fold_id; the snapshot carries all
    feat_* columns.  The join is on open_time and returns only training-set fold
    rows (fold_id >= 0 — both train-only and walk-forward folds are included so
    that all fold splits are available at search time).

    Args:
        model_id     : Model key (used to resolve snapshot_id and table names).
        meta         : Model config dict.
        target_col   : Target column name.
        feature_cols : Selected feature columns.
        row_stride   : Sub-sampling stride (1 = all rows).

    Returns:
        _SearchDataset ready for fold splitting.
    """
    asset_id    = meta.get("asset_id")
    snapshot_id = meta.get("sampling", {}).get("snapshot_id")

    conn = utils.open_lab_connection(asset_id)
    try:
        if not snapshot_id:
            row = conn.execute(
                "SELECT snapshot_id FROM reg.models WHERE model_id = ?", [model_id]
            ).fetchone()
            if row:
                snapshot_id = row[0]
        if not snapshot_id:
            raise ValueError(
                f"Cannot resolve snapshot_id for model {model_id} — set "
                "sampling.snapshot_id in config/models.json or run the sample step first."
            )

        feat_cols_sql = ", ".join(f's."{c}"' for c in feature_cols)
        sql = f"""
            SELECT
                s.open_time,
                m."{target_col}",
                m.fold_id,
                {feat_cols_sql}
            FROM snap."{snapshot_id}" AS s
            INNER JOIN model."{model_id}__sample" AS m ON s.open_time = m.open_time
            ORDER BY s.open_time
        """
        df = conn.execute(sql).df()

        # I2 invariant logging: joined rows should equal model.__sample rowcount.
        sample_row = conn.execute(
            f'SELECT COUNT(*) FROM model."{model_id}__sample"'
        ).fetchone()
        sample_count = int(sample_row[0]) if sample_row else -1
    finally:
        conn.close()

    df["open_time"] = pd.to_datetime(df["open_time"])

    if row_stride > 1:
        df = df.iloc[::row_stride].copy().reset_index(drop=True)

    logger.info(
        "[Data] target=%s  joined_rows=%d  sample_rows=%d  (I2: snap ⋈ model.__sample)",
        target_col, len(df), sample_count,
    )

    dataset = ModelingDataset(
        open_time    = pd.Series(df["open_time"]),
        X            = pd.DataFrame(df[feature_cols]),
        y            = pd.Series(df[target_col].astype(float), name=target_col),
        target_col   = target_col,
        feature_cols = feature_cols,
    )
    return _SearchDataset(
        dataset  = dataset,
        fold_ids = pd.Series(df["fold_id"].astype("int8"), name="fold_id"),
    )


# =============================================================================
# Fold split — 4-fold stratified by calendar week
# =============================================================================
def _fold_split_4fold(
    sd:                    _SearchDataset,
    fold_k:                int,
    fold_week_assignments: dict,
    purge_minutes:         int,
) -> DatasetSplit:
    valid_mask = (sd.fold_ids == fold_k).values
    valid_mask = pd.Series(valid_mask, index=sd.dataset.open_time.index)

    delta      = pd.Timedelta(minutes=purge_minutes)
    purge_mask = pd.Series(False, index=sd.dataset.open_time.index)

    weeks = fold_week_assignments.get(fold_k, fold_week_assignments.get(str(fold_k), []))
    for week in weeks:
        vs   = pd.Timestamp(week["start"])
        ve   = pd.Timestamp(week["end"]) + pd.Timedelta(hours=23, minutes=59)
        pre  = (sd.dataset.open_time >= vs - delta) & (sd.dataset.open_time < vs)
        post = (sd.dataset.open_time > ve)           & (sd.dataset.open_time <= ve + delta)
        purge_mask = purge_mask | pre | post

    train_mask = ~valid_mask & ~purge_mask

    if not valid_mask.any():
        raise ValueError(f"Empty validation set for fold {fold_k}")

    return DatasetSplit(
        X_train = sd.dataset.X.loc[train_mask],
        y_train = sd.dataset.y.loc[train_mask],
        X_eval  = sd.dataset.X.loc[valid_mask],
        y_eval  = sd.dataset.y.loc[valid_mask],
    )


def _fold_split_walk_forward(
    sd               : _SearchDataset,
    fold_k           : int,
    fold_time_windows: list[dict],
    purge_minutes    : int,
) -> DatasetSplit:
    """Split dataset into train/valid using explicit time windows for walk-forward CV.

    Args:
        sd                : Search dataset with open_time aligned to X/y.
        fold_k            : Fold number (1-based) matching a fold_id in fold_time_windows.
        fold_time_windows : List of fold dicts from generate_walk_forward_folds().
        purge_minutes     : Minutes to exclude around fold boundaries.

    Returns:
        DatasetSplit with X_train, y_train, X_eval, y_eval.

    Raises:
        ValueError: If fold_k not found or validation set is empty.
    """
    fw = next((f for f in fold_time_windows if f["fold_id"] == fold_k), None)
    if fw is None:
        raise ValueError(f"fold_id {fold_k} not in fold_time_windows")

    valid_start = pd.Timestamp(fw["valid_start"])
    valid_end   = pd.Timestamp(fw["valid_end"]) + pd.Timedelta(hours=23, minutes=59)
    train_end   = pd.Timestamp(fw["train_end"]) + pd.Timedelta(hours=23, minutes=59)

    delta      = pd.Timedelta(minutes=purge_minutes)
    valid_mask = (
        (sd.dataset.open_time >= valid_start)
        & (sd.dataset.open_time <= valid_end)
    )
    # Purge zone: between train_end and valid_start, and after valid_end
    purge_mask = (
        (sd.dataset.open_time > train_end)
        & (sd.dataset.open_time < valid_start)
    ) | (
        (sd.dataset.open_time > valid_end)
        & (sd.dataset.open_time <= valid_end + delta)
    )

    train_mask = ~valid_mask & ~purge_mask

    valid_mask = pd.Series(valid_mask.values, index=sd.dataset.open_time.index)
    train_mask = pd.Series(train_mask.values, index=sd.dataset.open_time.index)

    if not valid_mask.any():
        raise ValueError(f"Empty validation set for fold {fold_k}")

    return DatasetSplit(
        X_train = sd.dataset.X.loc[train_mask],
        y_train = sd.dataset.y.loc[train_mask],
        X_eval  = sd.dataset.X.loc[valid_mask],
        y_eval  = sd.dataset.y.loc[valid_mask],
    )


# =============================================================================
# Rank audit metrics
# =============================================================================

def _compute_top10_lift(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Top-decile lift: mean y_true among top 10% scores minus overall mean.

    Args:
        y_true  : Ground-truth target values.
        y_score : Model predicted scores.

    Returns:
        Float lift value.  Returns 0.0 if the top-decile mask is empty.
    """
    threshold = np.percentile(y_score, 90)
    mask      = y_score >= threshold
    if mask.sum() == 0:
        return 0.0
    return float(np.mean(y_true[mask]) - np.mean(y_true))


def _compute_decile_monotonicity(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Fraction of adjacent decile pairs where mean y_true increases with score decile.

    Args:
        y_true  : Ground-truth target values.
        y_score : Model predicted scores.

    Returns:
        Float in [0.0, 1.0].  Returns 0.0 if fewer than 2 deciles exist.
    """
    n_deciles = 10
    labels    = pd.qcut(pd.Series(y_score), q=n_deciles, labels=False, duplicates="drop")
    means     = pd.Series(y_true).groupby(labels).mean().sort_index()  # type: ignore[arg-type]
    if len(means) < 2:
        return 0.0
    diffs = np.diff(means.to_numpy(dtype=float))
    return float((diffs > 0).mean())


# =============================================================================
# Single trial execution
# =============================================================================
def _run_one_trial(
    trial_no             : int,
    params               : dict,
    sd                   : _SearchDataset,
    fold_ids             : list[int],
    fold_week_assignments: dict | None,
    purge_minutes        : int,
    search_dir           : Path,
    fold_time_windows    : list[dict] | None = None,
) -> dict:
    full_params  = {**_FIXED_PARAMS, **params, "random_state": 42}
    fold_results = []
    use_walk_forward = fold_time_windows is not None

    for fold_k in fold_ids:
        if use_walk_forward:
            split = _fold_split_walk_forward(sd, fold_k, fold_time_windows, purge_minutes)
            fold_label = _fold_label_walk_forward(fold_k, fold_time_windows)
        else:
            split      = _fold_split_4fold(sd, fold_k, fold_week_assignments or {}, purge_minutes)
            weeks      = (fold_week_assignments or {}).get(fold_k, (fold_week_assignments or {}).get(str(fold_k), []))
            fold_label = f"{weeks[0]['start']}_{weeks[-1]['end']}" if weeks else ""

        eval_result: dict = {}
        callbacks = [
            lgb.early_stopping(stopping_rounds=_ES_ROUNDS, verbose=False),
            lgb.log_evaluation(period=-1),
            lgb.record_evaluation(eval_result),
        ]

        model = lgb.LGBMRegressor(**full_params)
        model.fit(
            split.X_train, split.y_train,
            eval_set   = [(split.X_train, split.y_train), (split.X_eval, split.y_eval)],
            eval_names = ["train", "valid"],
            callbacks  = callbacks,
        )

        best_iter  = getattr(model, "best_iteration_", None) or full_params["n_estimators"]
        train_pred = pd.Series(model.predict(split.X_train))
        valid_pred = pd.Series(model.predict(split.X_eval))

        y_tr = split.y_train.to_numpy(dtype=float)
        y_va = split.y_eval.to_numpy(dtype=float)
        p_tr = train_pred.to_numpy(dtype=float)
        p_va = valid_pred.to_numpy(dtype=float)

        train_rmse = float(np.sqrt(np.mean((y_tr - p_tr) ** 2)))
        valid_rmse = float(np.sqrt(np.mean((y_va - p_va) ** 2)))
        train_mae  = float(np.mean(np.abs(y_tr - p_tr)))
        valid_mae  = float(np.mean(np.abs(y_va - p_va)))

        # Rank audit metrics
        top10_lift          = _compute_top10_lift(y_va, p_va)
        spearman_result      = spearmanr(y_va, p_va)
        spearman_rho         = float(spearman_result[0])  # type: ignore[arg-type]
        decile_monotonicity = _compute_decile_monotonicity(y_va, p_va)

        fi = _feature_importance(model, sd.dataset.feature_cols)

        curves_path = (
            search_dir / "trial_curves"
            / f"trial_{trial_no:04d}_fold_{fold_k:02d}.json"
        )
        curves_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(curves_path, _compact_curves(eval_result, trial_no, fold_k))

        fold_results.append({
            "fold":                fold_k,
            "fold_week":           fold_label,
            "train_rmse":          train_rmse,
            "valid_rmse":          valid_rmse,
            "train_mae":           train_mae,
            "valid_mae":           valid_mae,
            "top10_lift":          top10_lift,
            "spearman_rho":        spearman_rho if np.isfinite(spearman_rho) else None,
            "decile_monotonicity": decile_monotonicity,
            "train_n":             int(len(split.y_train)),
            "valid_n":             int(len(split.y_eval)),
            "best_iteration":      best_iter,
            "top20_features":      fi,
        })

        del model, train_pred, valid_pred
        gc.collect()

    return {"fold_metrics": fold_results}


def _fold_label_walk_forward(fold_k: int, fold_time_windows: list[dict]) -> str:
    """Return a human-readable label for a walk-forward fold."""
    fw = next((f for f in fold_time_windows if f["fold_id"] == fold_k), None)
    if fw is None:
        return f"fold_{fold_k}"
    return f"{fw['valid_start']}_{fw['valid_end']}"


def _feature_importance(model: lgb.LGBMRegressor, feature_cols: list[str]) -> list[dict]:
    split_imp = model.booster_.feature_importance(importance_type="split")
    gain_imp  = model.booster_.feature_importance(importance_type="gain")
    rows = [
        {"feature": f, "split": int(s), "gain": float(g)}
        for f, s, g in zip(feature_cols, split_imp, gain_imp, strict=False)
    ]
    rows.sort(key=lambda x: x["gain"], reverse=True)
    return rows[:20]


def _compact_curves(eval_result: dict, trial_no: int, fold_no: int) -> dict:
    out: dict = {"trial": trial_no, "fold": fold_no}
    for dset, metrics in eval_result.items():
        out[dset] = {}
        for metric, values in metrics.items():
            n    = len(values)
            step = max(1, n // _CURVE_MAX_POINTS)
            out[dset][metric] = values[::step]
    return out


# =============================================================================
# Objective computation
# =============================================================================
def _compute_objective(fold_metrics: list[dict]) -> dict:
    """Compute Top10 Lift objective with fold-stability penalty.

    objective = mean(top10_lift_folds) - LIFT_LAMBDA * std(top10_lift_folds)
    Higher objective is better → objective_score = -objective (lower is better
    for Optuna minimize direction).

    Also aggregates RMSE/MAE and rank audit metrics for logging/persistence.

    Args:
        fold_metrics : List of per-fold metric dicts from _run_one_trial.

    Returns:
        Dict with ``objective_score`` (lower=better) and aggregated metrics.
    """
    lifts = [f["top10_lift"]   for f in fold_metrics if f.get("top10_lift")   is not None]
    if not lifts:
        return {"objective_score": float("inf")}

    mean_lift = float(np.mean(lifts))
    std_lift  = float(np.std(lifts))
    objective = mean_lift - _LIFT_LAMBDA * std_lift
    score     = -objective  # Lower is better for Optuna minimize

    spearmans:   list[float] = [float(f["spearman_rho"])        for f in fold_metrics if f.get("spearman_rho")        is not None]
    monots:      list[float] = [float(f["decile_monotonicity"]) for f in fold_metrics if f.get("decile_monotonicity") is not None]
    valid_rmses: list[float] = [float(f["valid_rmse"])          for f in fold_metrics if f.get("valid_rmse")          is not None]
    train_rmses: list[float] = [float(f["train_rmse"])          for f in fold_metrics if f.get("train_rmse")          is not None]
    valid_maes:  list[float] = [float(f["valid_mae"])           for f in fold_metrics if f.get("valid_mae")           is not None]
    train_maes:  list[float] = [float(f["train_mae"])           for f in fold_metrics if f.get("train_mae")           is not None]

    return {
        "objective_score":           score,
        "mean_top10_lift":           mean_lift,
        "std_top10_lift":            std_lift,
        "mean_spearman_rho":         float(np.mean(spearmans)) if spearmans else None,
        "mean_decile_monotonicity":  float(np.mean(monots))    if monots    else None,
        "mean_valid_rmse":           float(np.mean(valid_rmses)) if valid_rmses else None,
        "std_valid_rmse":            float(np.std(valid_rmses))  if valid_rmses else None,
        "mean_train_rmse":           float(np.mean(train_rmses)) if train_rmses else None,
        "mean_valid_mae":            float(np.mean(valid_maes))  if valid_maes  else None,
        "mean_train_mae":            float(np.mean(train_maes))  if train_maes  else None,
    }


# =============================================================================
# Seeded random search
# =============================================================================
def _search_random(
    model_id:              str,
    sd:                    _SearchDataset,
    fold_ids:              list[int],
    fold_week_assignments: dict | None,
    purge_minutes:         int,
    search_dir:            Path,
    feature_cols:          list[str],
    row_stride:            int,
    n_trials:              int,
    timeout_hours:         float | None,
    stage:                 str,
    done_hashes:           set,
    fail_hashes:           set,
    fold_time_windows:     list[dict] | None = None,
) -> dict:
    rng = np.random.default_rng(seed=42)

    best              = _load_best(search_dir)
    completed_trials  = _load_completed_trials(search_dir)
    timeout_s         = timeout_hours * 3600 if timeout_hours else None
    t0                = time.time()
    completed_count   = 0
    consecutive_fails = 0
    attempt           = 0

    while completed_count < n_trials:
        if timeout_s and (time.time() - t0) > timeout_s:
            logger.info(f"[Search] Timeout after {completed_count} completed trials")
            break

        attempt += 1
        guide    = _build_tpe_guide(completed_trials)
        params   = _sample_params_random(rng, stage, guide)
        h        = _make_param_hash(params, len(feature_cols), row_stride)

        if h in done_hashes or h in fail_hashes:
            continue

        trial_no = len(done_hashes) + len(fail_hashes) + 1
        logger.info(
            f"\n[Trial {trial_no:04d}] attempt={attempt}  "
            f"completed={completed_count}/{n_trials}  "
            f"guide={'yes' if guide else 'random'}"
        )
        logger.info(f"  Params: {_format_params(params)}")

        t_trial = time.time()
        try:
            result  = _run_one_trial(trial_no, params, sd, fold_ids, fold_week_assignments, purge_minutes, search_dir, fold_time_windows=fold_time_windows)
            obj     = _compute_objective(result["fold_metrics"])
            elapsed = time.time() - t_trial

            trial_record = {
                "trial_no":     trial_no,
                "param_hash":   h,
                "params":       params,
                "elapsed_s":    round(elapsed, 1),
                "fold_metrics": result["fold_metrics"],
                **obj,
            }

            _log_trial_result(trial_no, params, trial_record, best)
            _persist_completed(search_dir, trial_no, trial_record, feature_cols, row_stride)
            done_hashes.add(h)
            completed_trials.append(trial_record)
            completed_count   += 1
            consecutive_fails  = 0
            best = _update_best(search_dir, trial_record, best)

        except MemoryError as exc:
            consecutive_fails += 1
            logger.warning(f"[Trial {trial_no}] MemoryError — {exc}")
            _persist_failed(search_dir, attempt, params, h, "MemoryError", str(exc))
            fail_hashes.add(h)
            gc.collect()
            if consecutive_fails >= 3:
                logger.error("[Search] 3 consecutive memory failures — aborting")
                break

        except Exception as exc:
            consecutive_fails += 1
            logger.warning(f"[Trial {trial_no}] {type(exc).__name__}: {exc}")
            _persist_failed(search_dir, attempt, params, h, type(exc).__name__, str(exc))
            fail_hashes.add(h)
            gc.collect()

    return best or {}


# =============================================================================
# Optuna TPE search
# =============================================================================
def _search_optuna(
    model_id:              str,
    sd:                    _SearchDataset,
    fold_ids:              list[int],
    fold_week_assignments: dict | None,
    purge_minutes:         int,
    search_dir:            Path,
    feature_cols:          list[str],
    row_stride:            int,
    n_trials:              int,
    timeout_hours:         float | None,
    stage:                 str,
    done_hashes:           set,
    fail_hashes:           set,
    fold_time_windows:     list[dict] | None = None,
) -> dict:
    import optuna  # type: ignore[import-not-found]

    storage_path = search_dir / "optuna_study.db"
    storage      = f"sqlite:///{storage_path}"
    sampler      = optuna.samplers.TPESampler(seed=42, n_startup_trials=20, multivariate=True)
    study        = optuna.create_study(
        study_name     = model_id,
        storage        = storage,
        sampler        = sampler,
        direction      = "minimize",
        load_if_exists = True,
    )

    best             = _load_best(search_dir)
    completed_trials = _load_completed_trials(search_dir)

    def objective(trial: "optuna.Trial") -> float:
        params = _suggest_optuna_params(trial, stage)
        h      = _make_param_hash(params, len(feature_cols), row_stride)

        if h in done_hashes or h in fail_hashes:
            raise optuna.exceptions.TrialPruned()

        trial_no = trial.number + 1
        logger.info(f"\n[Trial {trial_no:04d}]  Params: {_format_params(params)}")

        t_trial      = time.time()
        result       = _run_one_trial(trial_no, params, sd, fold_ids, fold_week_assignments, purge_minutes, search_dir, fold_time_windows=fold_time_windows)
        obj          = _compute_objective(result["fold_metrics"])
        elapsed      = time.time() - t_trial
        trial_record = {
            "trial_no":     trial_no,
            "param_hash":   h,
            "params":       params,
            "elapsed_s":    round(elapsed, 1),
            "fold_metrics": result["fold_metrics"],
            **obj,
        }

        nonlocal best
        _log_trial_result(trial_no, params, trial_record, best)
        _persist_completed(search_dir, trial_no, trial_record, feature_cols, row_stride)
        done_hashes.add(h)
        completed_trials.append(trial_record)
        best = _update_best(search_dir, trial_record, best)

        return obj.get("objective_score", float("inf"))

    timeout_s = timeout_hours * 3600 if timeout_hours else None
    try:
        study.optimize(
            objective,
            n_trials          = n_trials,
            timeout           = timeout_s,
            catch             = (Exception,),
            show_progress_bar = False,
        )
    except KeyboardInterrupt:
        logger.info("[Search] Interrupted by user")

    return best or {}


# =============================================================================
# Parameter sampling
# =============================================================================
def _suggest_optuna_params(trial: "optuna.Trial", stage: str) -> dict:
    leaves_hi  = 31 if stage == "smoke" else 63
    num_leaves = trial.suggest_int("num_leaves", 3, leaves_hi, log=True)
    max_depth  = trial.suggest_categorical("max_depth", [-1, 2, 3, 4, 5, 6, 8])

    if max_depth > 0:
        num_leaves = min(num_leaves, 2 ** max_depth)

    zero_gain      = trial.suggest_categorical("zero_gain", [True, False])
    min_split_gain = (
        0.0 if zero_gain
        else trial.suggest_float("min_split_gain_nz", 1e-5, 0.1, log=True)
    )

    return {
        "num_leaves":        num_leaves,
        "max_depth":         max_depth,
        "min_child_samples": trial.suggest_int("min_child_samples", 200, 8000, log=True),
        "min_child_weight":  trial.suggest_float("min_child_weight", 1e-4, 1e-1, log=True),
        "min_split_gain":    min_split_gain,
        "reg_alpha":         trial.suggest_float("reg_alpha", 1e-3, 10.0, log=True),
        "reg_lambda":        trial.suggest_float("reg_lambda", 1.0, 100.0, log=True),
        "subsample":         trial.suggest_float("subsample", 0.45, 0.95),
        "colsample_bytree":  trial.suggest_float("colsample_bytree", 0.35, 0.95),
        "learning_rate":     trial.suggest_float("learning_rate", 0.005, 0.05, log=True),
        "max_bin":           trial.suggest_categorical("max_bin", [63, 127]),
        "path_smooth":       trial.suggest_float("path_smooth", 1e-3, 10.0, log=True),
        "extra_trees":       trial.suggest_categorical("extra_trees", [False, True]),
    }


def _sample_params_random(
    rng:       np.random.Generator,
    stage:     str,
    tpe_guide: dict | None = None,
) -> dict:
    def log_uni(lo: float, hi: float, center: float | None = None, scale_frac: float | None = None) -> float:
        if center and scale_frac:
            log_c  = np.log(center)
            log_lo = max(np.log(lo), log_c - scale_frac)
            log_hi = min(np.log(hi), log_c + scale_frac)
            return float(np.exp(rng.uniform(log_lo, log_hi)))
        return float(np.exp(rng.uniform(np.log(lo), np.log(hi))))

    def int_log_uni(lo: int, hi: int, center: float | None = None, scale_frac: float | None = None) -> int:
        return max(lo, min(hi, int(round(log_uni(lo, hi, center, scale_frac)))))

    def uni(lo: float, hi: float, center: float | None = None, scale_frac: float | None = None) -> float:
        if center and scale_frac:
            half = (hi - lo) * scale_frac
            return float(rng.uniform(max(lo, center - half), min(hi, center + half)))
        return float(rng.uniform(lo, hi))

    g         = tpe_guide or {}
    leaves_hi = 31 if stage == "smoke" else 63
    num_leaves = int_log_uni(3, leaves_hi, g.get("num_leaves"), g.get("_frac"))
    max_depth  = int(rng.choice([-1, 2, 3, 4, 5, 6, 8]))
    if max_depth > 0:
        num_leaves = min(num_leaves, 2 ** max_depth)

    zero_gain      = rng.random() < 0.20
    min_split_gain = 0.0 if zero_gain else log_uni(1e-5, 0.1)

    return {
        "num_leaves":        num_leaves,
        "max_depth":         max_depth,
        "min_child_samples": int_log_uni(200, 8000, g.get("min_child_samples"), g.get("_frac")),
        "min_child_weight":  log_uni(1e-4, 1e-1, g.get("min_child_weight"),  g.get("_frac")),
        "min_split_gain":    min_split_gain,
        "reg_alpha":         log_uni(1e-3, 10.0, g.get("reg_alpha"),         g.get("_frac")),
        "reg_lambda":        log_uni(1.0, 100.0, g.get("reg_lambda"),        g.get("_frac")),
        "subsample":         uni(0.45, 0.95,     g.get("subsample"),         g.get("_frac")),
        "colsample_bytree":  uni(0.35, 0.95,     g.get("colsample_bytree"),  g.get("_frac")),
        "learning_rate":     log_uni(0.005, 0.05, g.get("learning_rate"),    g.get("_frac")),
        "max_bin":           int(rng.choice([63, 127])),
        "path_smooth":       log_uni(1e-3, 10.0),
        "extra_trees":       bool(rng.choice([False, True])),
    }


def _build_tpe_guide(completed: list[dict], n_startup: int = 20) -> dict | None:
    if len(completed) < n_startup:
        return None
    scores = [t["objective_score"] for t in completed if t.get("objective_score") is not None]
    if not scores:
        return None
    threshold = np.percentile(scores, 25)
    top = [t for t in completed if t.get("objective_score", float("inf")) <= threshold]
    if not top:
        return None

    def _median(key: str) -> float | None:
        vals = [t["params"].get(key) for t in top if t["params"].get(key) is not None]
        return float(np.median(vals)) if vals else None

    guide = {k: _median(k) for k in (
        "num_leaves", "min_child_samples", "min_child_weight",
        "reg_alpha", "reg_lambda", "subsample",
        "colsample_bytree", "learning_rate",
    )}
    guide["_frac"] = 0.5
    return guide


def _make_param_hash(params: dict, n_features: int, row_stride: int) -> str:
    payload = {
        "params":     {k: str(v) for k, v in sorted(params.items())},
        "n_features": n_features,
        "row_stride": row_stride,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()[:16]


# =============================================================================
# Persistence
# =============================================================================
def _persist_completed(
    search_dir:   Path,
    trial_no:     int,
    record:       dict,
    feature_cols: list[str],
    row_stride:   int,
) -> None:
    logs_dir = search_dir / "trial_logs"
    logs_dir.mkdir(exist_ok=True)
    log_record                = {k: v for k, v in record.items() if k != "fold_metrics"}
    log_record["n_features"]  = len(feature_cols)
    log_record["row_stride"]  = row_stride
    log_record["fold_summary"] = [
        {k: v for k, v in f.items() if k != "top20_features"}
        for f in record.get("fold_metrics", [])
    ]
    _write_json(logs_dir / f"trial_{trial_no:04d}.json", log_record)

    compact = {
        "trial_no":                  record["trial_no"],
        "param_hash":                record["param_hash"],
        "params":                    record["params"],
        "objective_score":           record.get("objective_score"),
        "mean_top10_lift":           record.get("mean_top10_lift"),
        "std_top10_lift":            record.get("std_top10_lift"),
        "mean_spearman_rho":         record.get("mean_spearman_rho"),
        "mean_decile_monotonicity":  record.get("mean_decile_monotonicity"),
        "mean_valid_rmse":           record.get("mean_valid_rmse"),
        "std_valid_rmse":            record.get("std_valid_rmse"),
        "mean_train_rmse":           record.get("mean_train_rmse"),
        "mean_valid_mae":            record.get("mean_valid_mae"),
        "elapsed_s":                 record.get("elapsed_s"),
    }
    _append_jsonl(search_dir / "search_trials.jsonl", compact)
    _update_summary_csv(search_dir, compact, record["params"])
    _log_aggregated_feature_importance(trial_no, record.get("fold_metrics", []))


def _persist_failed(
    search_dir: Path,
    attempt:    int,
    params:     dict,
    h:          str,
    exc_type:   str,
    exc_msg:    str,
) -> None:
    _append_jsonl(search_dir / "failed_trials.jsonl", {
        "attempt":  attempt,
        "hash":     h,
        "params":   params,
        "exc_type": exc_type,
        "exc_msg":  exc_msg[:500],
    })


def _update_best(search_dir: Path, record: dict, current_best: dict | None) -> dict:
    score      = record.get("objective_score", float("inf"))
    best_score = (current_best or {}).get("objective_score", float("inf"))
    if score < best_score:
        best = {k: v for k, v in record.items() if k != "fold_metrics"}
        best["fold_summary"] = [
            {k: v for k, v in f.items() if k != "top20_features"}
            for f in record.get("fold_metrics", [])
        ]
        _write_json(search_dir / "search_best.json", best)
        logger.info(f"  *** NEW BEST  score={score:.6f} ***")
        return best
    return current_best or {}


def _load_best(search_dir: Path) -> dict | None:
    p = search_dir / "search_best.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def _load_existing_hashes(search_dir: Path) -> tuple[set, set]:
    done: set = set()
    fail: set = set()
    for line in _iter_jsonl(search_dir / "search_trials.jsonl"):
        h = line.get("param_hash")
        if h:
            done.add(h)
    for line in _iter_jsonl(search_dir / "failed_trials.jsonl"):
        h = line.get("hash")
        if h:
            fail.add(h)
    return done, fail


def _load_completed_trials(search_dir: Path) -> list[dict]:
    return [
        line for line in _iter_jsonl(search_dir / "search_trials.jsonl")
        if line.get("objective_score") is not None
    ]


def _update_summary_csv(search_dir: Path, compact: dict, params: dict) -> None:
    row      = {**compact, **{f"p_{k}": v for k, v in params.items()}}
    csv_path = search_dir / "search_summary.csv"
    df_new   = pd.DataFrame([row])
    if csv_path.exists():
        df_old = pd.read_csv(csv_path)
        df_new = pd.concat([df_old, df_new], ignore_index=True)
    df_new.to_csv(csv_path, index=False)


def _write_best_params(search_dir: Path, best: dict) -> None:
    params = best.get("params", {})
    _write_json(search_dir / "best_params.json", params)
    logger.info(f"[Output] best_params.json written → {search_dir / 'best_params.json'}")


# =============================================================================
# Logging helpers
# =============================================================================
def _log_trial_result(
    trial_no: int, params: dict, record: dict, best: dict | None
) -> None:
    lift  = record.get("mean_top10_lift")
    slift = record.get("std_top10_lift")
    rho   = record.get("mean_spearman_rho")
    mono  = record.get("mean_decile_monotonicity")
    vrmse = record.get("mean_valid_rmse")
    vmae  = record.get("mean_valid_mae")
    obj   = record.get("objective_score")
    best_obj = (best or {}).get("objective_score")

    logger.info(
        f"  top10_lift={_fmt(lift)}  std_lift={_fmt(slift)}  objective={_fmt(obj)}"
    )
    logger.info(
        f"  spearman_rho={_fmt(rho)}  decile_monotonicity={_fmt(mono)}"
    )
    logger.info(
        f"  valid_rmse={_fmt(vrmse)}  valid_mae={_fmt(vmae)}"
    )

    if best_obj is not None:
        delta  = (obj or float("inf")) - best_obj
        marker = "  << NEW BEST" if delta < 0 else f"  (Δ{delta:+.5f} vs best)"
        logger.info(f"  elapsed={record.get('elapsed_s')}s{marker}")
    else:
        logger.info(f"  elapsed={record.get('elapsed_s')}s")

    for f in record.get("fold_metrics", []):
        logger.info(
            f"    fold {f['fold']} ({f.get('fold_week', '')})  "
            f"top10_lift={_fmt(f.get('top10_lift'))}  "
            f"spearman={_fmt(f.get('spearman_rho'))}  "
            f"mono={_fmt(f.get('decile_monotonicity'))}  "
            f"valid_rmse={_fmt(f.get('valid_rmse'))}  "
            f"best_iter={f.get('best_iteration')}  "
            f"n_valid={f.get('valid_n', 0):,}"
        )


def _log_aggregated_feature_importance(trial_no: int, fold_metrics: list[dict]) -> None:
    agg: dict[str, float] = {}
    for fold in fold_metrics:
        for row in fold.get("top20_features", []):
            agg[row["feature"]] = agg.get(row["feature"], 0.0) + row["gain"]
    if not agg:
        return
    n_folds = max(1, len(fold_metrics))
    ranked  = sorted(agg.items(), key=lambda x: x[1], reverse=True)[:20]
    logger.info(f"  Feature importance (mean gain, top 20 across {n_folds} folds):")
    for feat, gain in ranked:
        logger.info(f"    {feat:<55}  gain={gain / n_folds:.1f}")


def _print_final_summary(best: dict | None, search_dir: Path) -> None:
    logger.info("\n" + "=" * 72)
    logger.info("SEARCH COMPLETE")
    logger.info("=" * 72)

    if not best:
        logger.info("No completed trials.")
        return

    logger.info(f"Best trial: #{best.get('trial_no')}  hash={best.get('param_hash')}")
    logger.info(f"  objective_score          = {_fmt(best.get('objective_score'))}  (lower=better)")
    logger.info(f"  mean_top10_lift          = {_fmt(best.get('mean_top10_lift'))}  std={_fmt(best.get('std_top10_lift'))}")
    logger.info(f"  mean_spearman_rho        = {_fmt(best.get('mean_spearman_rho'))}")
    logger.info(f"  mean_decile_monotonicity = {_fmt(best.get('mean_decile_monotonicity'))}")
    logger.info(f"  mean_valid_rmse          = {_fmt(best.get('mean_valid_rmse'))}  std={_fmt(best.get('std_valid_rmse'))}")
    logger.info(f"  mean_valid_mae           = {_fmt(best.get('mean_valid_mae'))}")
    logger.info(f"  elapsed_s                = {best.get('elapsed_s')}")
    logger.info("\nBest parameters:")
    for k, v in sorted(best.get("params", {}).items()):
        logger.info(f"  {k:<25} = {v}")
    logger.info("\nPer-fold breakdown:")
    for f in best.get("fold_summary", []):
        logger.info(
            f"  fold {f['fold']}  "
            f"top10_lift={_fmt(f.get('top10_lift'))}  "
            f"spearman={_fmt(f.get('spearman_rho'))}  "
            f"mono={_fmt(f.get('decile_monotonicity'))}  "
            f"valid_rmse={_fmt(f.get('valid_rmse'))}"
        )

    csv_path = search_dir / "search_summary.csv"
    if csv_path.exists():
        with contextlib.suppress(Exception):
            df = pd.read_csv(csv_path)
            if "objective_score" in df.columns and len(df) > 1:
                top = df.nsmallest(min(10, len(df)), "objective_score")
                logger.info(f"\nTop-{len(top)} trials by objective score:")
                for _, row in top.iterrows():
                    logger.info(
                        f"  #{int(row['trial_no']):04d}  "  # type: ignore[arg-type]
                        f"obj={_fmt(row.get('objective_score'))}  "
                        f"lift={_fmt(row.get('mean_top10_lift'))}  "
                        f"rho={_fmt(row.get('mean_spearman_rho'))}"
                    )

    logger.info("\nArtifacts:")
    logger.info(f"  {search_dir / 'search_best.json'}")
    logger.info(f"  {search_dir / 'best_params.json'}")
    logger.info(f"  {search_dir / 'search_summary.csv'}")
    logger.info(f"  {search_dir / 'search_trials.jsonl'}")


# =============================================================================
# Low-level I/O
# =============================================================================
def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=_json_serial), encoding="utf-8")


def _append_jsonl(path: Path, record: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=_json_serial) + "\n")


def _iter_jsonl(path: Path):
    if not path.exists():
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                with contextlib.suppress(json.JSONDecodeError):
                    yield json.loads(line)


def _json_serial(obj: object) -> int | float | bool:
    if isinstance(obj, np.integer):
        return int(obj)  # type: ignore[arg-type]
    if isinstance(obj, np.floating):
        return float(obj)  # type: ignore[arg-type]
    if isinstance(obj, np.bool_):
        return bool(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


# =============================================================================
# Formatting
# =============================================================================
def _fmt(v: object) -> str:
    if v is None:
        return "N/A"
    return f"{v:.6f}"  # type: ignore[arg-type]


def _format_params(p: dict) -> str:
    return "  ".join(
        f"{k}={v:.4g}" if isinstance(v, float) else f"{k}={v}"
        for k, v in sorted(p.items())
    )


def _setup_logging() -> None:
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s", "%H:%M:%S"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
