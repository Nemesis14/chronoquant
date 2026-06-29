"""LightGBM hyperparameter search — train/valid split, valid ratio_p925 objective.

Objective: valid_ratio_p925 = mean(y_true | score >= p92.5) / mean(y_true)
Loss:      quantile regression, alpha=0.925 (asymmetric focus on top-tail bars)

Inputs:
  artifact_dir/feature_engineering/feature_set.json  → selected feature list
  snap."<snapshot_id>" ⋈ model."<model_id>__sample"  → train/valid rows (split col)
Outputs:
  artifact_dir/search/search_best.json   — full best trial record
  artifact_dir/search/best_params.json   — best hyperparameter dict only
  artifact_dir/search/search_trials.jsonl, search_summary.csv, trial_logs/, trial_curves/

Entry point: run_search(model_id, stage, n_trials, ...)
"""

import contextlib
import gc
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

import utils
from modeling.training.training_windows import DatasetSplit

logger = logging.getLogger("lgbm_search")

# ─── Fixed LightGBM parameters (not tuned) ───────────────────────────────────
_FIXED_PARAMS: dict = {
    "objective":      "quantile",
    "alpha":          0.925,          # asymmetric loss focusing on top-tail bars
    "boosting_type":  "gbdt",
    "metric":         "quantile",
    "n_estimators":   3000,
    "subsample_freq": 1,
    "force_col_wise": True,
    "verbosity":      -1,
    "n_jobs":         os.cpu_count() or 4,
}

_ES_ROUNDS        = 100
_CURVE_MAX_POINTS = 100
_MAX_TRIALS       = 100

try:
    import optuna  # type: ignore[import-not-found]
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAS_OPTUNA = True
except ImportError:
    _HAS_OPTUNA = False


# =============================================================================
# Search dataset — single train/valid split (split col: 0=train, 1=valid)
# =============================================================================
@dataclass(frozen=True)
class _SearchDataset:
    train: DatasetSplit   # pre-built from split==0 / split==1
    train_n: int
    valid_n: int


# =============================================================================
# run_search  —  main entry point
# =============================================================================
def run_search(
    model_id:          str,
    stage:             str          = "smoke",
    n_trials:          int          = _MAX_TRIALS,
    timeout_hours:     float | None = None,
    row_stride:        int | None   = None,
    retry_failed:      bool         = False,
    feature_key:       str          = "selected",
    feature_selection: str          = "joint",
    gap_penalty:       float        = 0.0,
    search_tag:        str | None   = None,
    direction:         str          = "long",
) -> dict:
    """Hyperparameter search for a LightGBM model using a fixed train/valid split.

    Reads feature list from artifact_dir/feature_engineering/feature_set.json
    and the model.__sample table (split col: 0=train, 1=valid).

    Objective: maximise valid_ratio_p925 (mean top 7.5% / mean all).
    Stopping:  max 100 trials (no patience early-stopping).

    Stages
    ------
    smoke    5 trials  — pipeline sanity check
    explore  60 trials — broad region search
    refine   30 trials — narrow best regions

    Args:
        model_id          : Model key from config/models.json.
        stage             : Search stage (smoke / explore / refine).
        n_trials          : Trial count cap (further capped by stage defaults).
        timeout_hours     : Wall-clock timeout; None = no timeout.
        row_stride        : Sub-sampling stride (None = 1).
        retry_failed      : If True, retry previously failed param hashes.
        feature_key       : Key in feature_set.json to use as feature list
                            (e.g. "selected", "top10"). Ignored when
                            feature_selection="joint".
        feature_selection : "fixed"  — use feature_key list, static feature set.
                            "joint"  — load gain_ranked list and add feature_k
                            as an additional Optuna parameter (forward selection).
                            Requires run_gain_rank() to have been called first.

    Returns:
        Best trial record dict.
    """
    _setup_logging()

    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta         = models_cfg["models"][model_id]
    target_col   = meta["target_name"]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))

    n_trials, row_stride = _apply_stage_defaults(stage, n_trials, row_stride)

    if feature_selection == "joint":
        feature_cols   = _load_feature_cols(artifact_dir, "gain_ranked")
        n_features_max = len(feature_cols)
        if search_tag:
            search_dir = artifact_dir / f"search_{search_tag}"
        else:
            tag        = f"joint_reg_gp{int(gap_penalty * 100):02d}" if gap_penalty > 0 else "joint"
            search_dir = artifact_dir / f"search_{tag}"
    else:
        feature_cols   = _load_feature_cols(artifact_dir, feature_key)
        n_features_max = None
        search_dir     = artifact_dir / (
            f"search_{search_tag}" if search_tag
            else ("search" if feature_key == "selected" else f"search_{feature_key}")
        )

    search_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 72)
    if feature_selection == "joint":
        logger.info(f"LGBM SEARCH  model={model_id}  mode=JOINT (feature_k in Optuna)")
        logger.info(f"  n_features_max={n_features_max}  gap_penalty={gap_penalty}  stage={stage}")
    else:
        logger.info(f"LGBM SEARCH  model={model_id}  feature_key={feature_key}")
        logger.info(f"  target={target_col}  stage={stage}")
    logger.info(f"  n_trials={n_trials}  row_stride={row_stride}  n_features={len(feature_cols)}")
    logger.info(f"  max={_MAX_TRIALS}  (no patience early-stopping)")
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
        f"[Data] train_n={sd.train_n:,}  valid_n={sd.valid_n:,}  n_features={len(feature_cols)}"
    )

    done_hashes, fail_hashes = _load_existing_hashes(search_dir)
    if retry_failed:
        fail_hashes = set()
    logger.info(
        f"[Resume] {len(done_hashes)} completed, {len(fail_hashes)} failed in log"
    )

    if _HAS_OPTUNA:
        best = _search_optuna(
            model_id, sd, search_dir, feature_cols, row_stride,
            n_trials, timeout_hours, stage, done_hashes, fail_hashes,
            n_features_max=n_features_max, gap_penalty=gap_penalty, direction=direction,
        )
    else:
        best = _search_random(
            model_id, sd, search_dir, feature_cols, row_stride,
            n_trials, timeout_hours, stage, done_hashes, fail_hashes,
            n_features_max=n_features_max, gap_penalty=gap_penalty, direction=direction,
        )

    if best:
        _write_best_params(search_dir, best)

    _register_search_provenance(model_id, stage, best, search_dir)

    _print_final_summary(best, search_dir)
    return best


# =============================================================================
# run_prune  —  post-search zero-split feature pruning
# =============================================================================
def run_prune(
    model_id:   str,
    feature_key: str         = "selected",
    search_tag:  str | None  = None,
) -> dict:
    """Fit one model with search best params, drop features with zero split importance.

    Trains on the train split (split==0) with early stopping on valid (split==1).
    Reads feature list from feature_set.json[feature_key], fits LightGBM, computes
    full feature importances, then removes any feature whose split count == 0.

    When best_params.json contains 'feature_k' (joint search mode), automatically
    uses gain_ranked[:feature_k] as the feature list instead of feature_key.

    Writes:
      artifact_dir/search_{tag}/pruning_report.json — full per-feature table
      artifact_dir/feature_engineering/feature_set.json — adds
          "pruned_{tag}" and "zero_split_{tag}" keys.

    Args:
        model_id    : Model key from config/models.json.
        feature_key : Key in feature_set.json — used when no search_tag and no
                      feature_k in best_params.
        search_tag  : If provided, overrides the search directory name
                      (artifact_dir/search_{search_tag}) and the feature_set.json
                      key prefix.

    Returns:
        Dict with n_selected, n_zero_split, n_pruned_selected, lists of each.
    """
    _setup_logging()

    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta         = models_cfg["models"][model_id]
    target_col   = meta["target_name"]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))

    tag = search_tag or feature_key
    if search_tag:
        search_dir = artifact_dir / f"search_{search_tag}"
    elif feature_key == "selected":
        search_dir = artifact_dir / "search"
    else:
        search_dir = artifact_dir / f"search_{feature_key}"

    best_params_path = search_dir / "best_params.json"
    if not best_params_path.exists():
        raise FileNotFoundError(
            f"best_params.json not found at {best_params_path}. Run search first."
        )
    best_params = json.loads(best_params_path.read_text(encoding="utf-8"))

    # Auto-detect joint mode: feature_k present in best_params
    feature_k_best = best_params.pop("feature_k", None)
    if feature_k_best is not None:
        all_gain_ranked = _load_feature_cols(artifact_dir, "gain_ranked")
        feature_cols    = all_gain_ranked[:feature_k_best]
    else:
        feature_cols = _load_feature_cols(artifact_dir, feature_key)

    logger.info("=" * 72)
    logger.info(f"LGBM PRUNE  model={model_id}  feature_key={feature_key}")
    logger.info(f"  n_features={len(feature_cols)}  best_params loaded from {search_dir.name}")
    logger.info("=" * 72)

    sd = _load_search_dataset(
        model_id     = model_id,
        meta         = meta,
        target_col   = target_col,
        feature_cols = feature_cols,
        row_stride   = 1,
    )

    full_params = {**_FIXED_PARAMS, **best_params, "random_state": 42}
    split       = sd.train

    model = lgb.LGBMRegressor(**full_params)
    model.fit(
        split.X_train, split.y_train,
        eval_set   = [(split.X_eval, split.y_eval)],
        eval_names = ["valid"],
        callbacks  = [
            lgb.early_stopping(stopping_rounds=_ES_ROUNDS, verbose=False),
            lgb.log_evaluation(period=-1),
        ],
    )

    split_imp = model.booster_.feature_importance(importance_type="split")
    gain_imp  = model.booster_.feature_importance(importance_type="gain")

    all_fi: list[dict] = [
        {"feature": f, "split": int(s), "gain": float(g)}
        for f, s, g in zip(feature_cols, split_imp, gain_imp, strict=False)
    ]
    all_fi.sort(key=lambda x: x["gain"], reverse=True)

    zero_split      = [row["feature"] for row in all_fi if row["split"] == 0]
    pruned_selected = [f for f in feature_cols if f not in set(zero_split)]

    logger.info(f"[Prune] selected={len(feature_cols)}  zero_split={len(zero_split)}  pruned_selected={len(pruned_selected)}")
    if zero_split:
        logger.info(f"[Prune] Removed: {zero_split}")

    # Update feature_set.json — use backward-compat keys for "selected", namespaced for others
    pruned_key     = "pruned_selected"     if tag == "selected" else f"pruned_{tag}"
    zero_key       = "zero_split_features" if tag == "selected" else f"zero_split_{tag}"
    provenance_key = "pruning"             if tag == "selected" else f"pruning_{tag}"

    fs_path = artifact_dir / "feature_engineering" / "feature_set.json"
    fs      = json.loads(fs_path.read_text(encoding="utf-8"))
    fs[pruned_key] = pruned_selected
    fs[zero_key]   = zero_split
    fs.setdefault("provenance", {})[provenance_key] = {
        "feature_key":       feature_key,
        "n_selected":        len(feature_cols),
        "n_zero_split":      len(zero_split),
        "n_pruned_selected": len(pruned_selected),
        "best_params":       best_params,
    }
    fs_path.write_text(json.dumps(fs, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"[Prune] feature_set.json updated → {pruned_key}={len(pruned_selected)} features")

    # Save full importance report
    _write_json(search_dir / "pruning_report.json", {
        "model_id":              model_id,
        "feature_key":           feature_key,
        "n_selected":            len(feature_cols),
        "n_zero_split":          len(zero_split),
        "n_pruned_selected":     len(pruned_selected),
        "zero_split_features":   zero_split,
        "all_feature_importance": all_fi,
    })
    logger.info(f"[Prune] pruning_report.json written → {search_dir / 'pruning_report.json'}")

    del model
    gc.collect()

    return {
        "n_selected":        len(feature_cols),
        "n_zero_split":      len(zero_split),
        "n_pruned_selected": len(pruned_selected),
        "pruned_selected":   pruned_selected,
        "zero_split_features": zero_split,
    }


# =============================================================================
# run_gain_rank  —  gain-importance ordering for joint feature+param search
# =============================================================================
def run_gain_rank(model_id: str) -> list[str]:
    """Rank all selected features by gain importance for use in joint search.

    Fits one LightGBM model with all selected features (using best_params from
    the regular search if available, else reasonable defaults) with
    colsample_bytree=1.0 so every feature gets a chance to be ranked.

    Saves gain_ranked to feature_set.json["gain_ranked"]. This ordering is
    consumed by run_search(..., feature_selection="joint") so that
    gain_ranked[:feature_k] gives the top-K features for each Optuna trial.

    Args:
        model_id : Model key from config/models.json.

    Returns:
        List of feature names ordered by gain importance (highest first).
    """
    _setup_logging()

    models_cfg   = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta         = models_cfg["models"][model_id]
    target_col   = meta["target_name"]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))
    feature_cols = _load_feature_cols(artifact_dir, "selected")

    best_params_path = artifact_dir / "search" / "best_params.json"
    if best_params_path.exists():
        base = json.loads(best_params_path.read_text(encoding="utf-8"))
        base.pop("feature_k", None)
        logger.info(f"[GainRank] Using best_params from {best_params_path}")
    else:
        base = {
            "num_leaves":        31,
            "min_child_samples": 500,
            "learning_rate":     0.05,
            "subsample":         0.8,
            "reg_lambda":        5.0,
        }
        logger.info("[GainRank] No best_params.json found — using defaults")

    # colsample_bytree=1.0 so ALL features are candidates in every tree
    rank_params = {**base, "colsample_bytree": 1.0, "n_estimators": 1000}

    logger.info("=" * 72)
    logger.info(f"LGBM GAIN RANK  model={model_id}  n_features={len(feature_cols)}")
    logger.info("=" * 72)

    sd = _load_search_dataset(
        model_id     = model_id,
        meta         = meta,
        target_col   = target_col,
        feature_cols = feature_cols,
        row_stride   = 1,
    )

    full_params = {**_FIXED_PARAMS, **rank_params, "random_state": 42}
    model       = lgb.LGBMRegressor(**full_params)
    model.fit(
        sd.train.X_train, sd.train.y_train,
        eval_set   = [(sd.train.X_eval, sd.train.y_eval)],
        eval_names = ["valid"],
        callbacks  = [
            lgb.early_stopping(stopping_rounds=_ES_ROUNDS, verbose=False),
            lgb.log_evaluation(period=-1),
        ],
    )

    split_imp = model.booster_.feature_importance(importance_type="split")
    gain_imp  = model.booster_.feature_importance(importance_type="gain")
    ranked = sorted(
        [{"feature": f, "split": int(s), "gain": float(g)}
         for f, s, g in zip(feature_cols, split_imp, gain_imp, strict=False)],
        key=lambda x: x["gain"], reverse=True,
    )
    gain_ranked = [r["feature"] for r in ranked]

    fs_path = artifact_dir / "feature_engineering" / "feature_set.json"
    fs = json.loads(fs_path.read_text(encoding="utf-8"))
    fs["gain_ranked"] = gain_ranked
    fs.setdefault("provenance", {})["gain_rank"] = {
        "source":   "best_params" if best_params_path.exists() else "defaults",
        "n_ranked": len(gain_ranked),
    }
    fs_path.write_text(json.dumps(fs, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info(f"[GainRank] feature_set.json → gain_ranked ({len(gain_ranked)} features)")
    logger.info(f"[GainRank] Top 10: {gain_ranked[:10]}")

    del model
    gc.collect()
    return gain_ranked


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
) -> tuple[int, int]:
    """Return (n_trials, row_stride) with stage-specific caps applied.

    Args:
        stage      : Search stage name.
        n_trials   : Requested trial count.
        row_stride : Requested row sub-sampling stride (None = 1).

    Returns:
        Tuple of (n_trials, row_stride).
    """
    stride = row_stride or 1
    if stage == "smoke":
        return min(n_trials, 5), stride
    if stage == "explore":
        return min(n_trials, 60), stride
    if stage == "refine":
        return min(n_trials, 30), stride
    return n_trials, stride


# =============================================================================
# Feature loading
# =============================================================================
def _load_feature_cols(artifact_dir: Path, feature_key: str = "selected") -> list[str]:
    """Load feature columns from feature_set.json[feature_key].

    Args:
        artifact_dir : Model artifact directory.
        feature_key  : Key in feature_set.json (default "selected").

    Returns:
        List of feature column names.

    Raises:
        FileNotFoundError : If feature_set.json does not exist.
        ValueError        : If the feature list is empty or key missing.
    """
    fe_json = artifact_dir / "feature_engineering" / "feature_set.json"
    if not fe_json.exists():
        raise FileNotFoundError(
            f"feature_set.json not found: {fe_json}\n"
            "Run the feature_engineering step first:\n"
            "  pipeline.py --model <model_id> --step feature_engineering"
        )
    data = json.loads(fe_json.read_text(encoding="utf-8"))
    cols = data.get(feature_key, [])
    if not cols:
        raise ValueError(f"feature_set.json '{feature_key}' list is empty or missing: {fe_json}")
    return list(cols)


# =============================================================================
# Dataset loading — snap ⋈ model.__sample JOIN, split col (0=train, 1=valid)
# =============================================================================
def _load_search_dataset(
    model_id:     str,
    meta:         dict,
    target_col:   str,
    feature_cols: list[str],
    row_stride:   int,
) -> _SearchDataset:
    """Load train/valid split from snap ⋈ model.__sample (split col: 0=train, 1=valid).

    The sample table carries open_time + target + split; the snapshot carries all
    feat_* columns.  The join is on open_time.

    Args:
        model_id     : Model key (used to resolve snapshot_id and table names).
        meta         : Model config dict.
        target_col   : Target column name.
        feature_cols : Selected feature columns.
        row_stride   : Sub-sampling stride (1 = all rows).

    Returns:
        _SearchDataset with pre-built DatasetSplit.

    Raises:
        ValueError : If snapshot_id cannot be resolved, or split column is missing.
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
                m.split,
                {feat_cols_sql}
            FROM snap."{snapshot_id}" AS s
            INNER JOIN model."{model_id}__sample" AS m ON s.open_time = m.open_time
            ORDER BY s.open_time
        """
        df = conn.execute(sql).df()

        sample_row   = conn.execute(
            f'SELECT COUNT(*) FROM model."{model_id}__sample"'
        ).fetchone()
        sample_count = int(sample_row[0]) if sample_row else -1
    finally:
        conn.close()
        gc.collect()   # force DuckDB file handle release on Windows

    if "split" not in df.columns:
        raise ValueError(
            f"model.{model_id}__sample has no 'split' column — "
            "run sampling with mode='train_valid_split' first (task t2)."
        )

    df["open_time"] = pd.to_datetime(df["open_time"])

    if row_stride > 1:
        df = df.iloc[::row_stride].copy().reset_index(drop=True)

    logger.info(
        "[Data] target=%s  joined_rows=%d  sample_rows=%d  (snap ⋈ model.__sample)",
        target_col, len(df), sample_count,
    )

    train_mask = df["split"] == 0
    valid_mask = df["split"] == 1

    train_n = int(train_mask.sum())
    valid_n = int(valid_mask.sum())

    if train_n == 0:
        raise ValueError(f"No training rows (split==0) for model {model_id}")
    if valid_n == 0:
        raise ValueError(f"No validation rows (split==1) for model {model_id}")

    logger.info("[Data] train_n=%d  valid_n=%d", train_n, valid_n)

    split = DatasetSplit(
        X_train = pd.DataFrame(df.loc[train_mask, feature_cols]).reset_index(drop=True),
        y_train = pd.Series(df.loc[train_mask, target_col].astype(float).values, name=target_col),
        X_eval  = pd.DataFrame(df.loc[valid_mask, feature_cols]).reset_index(drop=True),
        y_eval  = pd.Series(df.loc[valid_mask, target_col].astype(float).values, name=target_col),
    )
    return _SearchDataset(train=split, train_n=train_n, valid_n=valid_n)


# =============================================================================
# Rank audit metrics
# =============================================================================
def _compute_ratio_p925(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Top-7.5% ratio: mean y_true among top 7.5% scores divided by overall mean.

    For long direction: high scores → high MFE bars → ratio > 1 means good ranking.
    Returns 0.0 if mask empty or overall mean is non-positive (use for long targets only).

    Args:
        y_true  : Ground-truth target values (long_mfe_fw60, positive).
        y_score : Model predicted scores.

    Returns:
        Float ratio.  Returns 0.0 if mask empty or overall mean is non-positive.
    """
    threshold = np.percentile(y_score, 92.5)
    mask      = y_score >= threshold
    if mask.sum() == 0:
        return 0.0
    overall = float(np.mean(y_true))
    if overall <= 0:
        return 0.0
    return float(np.mean(y_true[mask])) / overall


def _compute_ratio_p075(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Bottom-7.5% ratio: mean y_true among bottom 7.5% scores divided by overall mean.

    For short direction: low scores → more negative MFE bars (profitable shorts) →
    ratio > 1 when dividing two negative numbers means the bottom-scored bars have
    a larger-magnitude (more profitable) target than the population average.
    Returns 0.0 if mask empty or overall mean is exactly zero.

    Args:
        y_true  : Ground-truth target values (short_mfe_fw60, negative).
        y_score : Model predicted scores.

    Returns:
        Float ratio.  Returns 0.0 if mask empty or overall mean is zero.
    """
    threshold = np.percentile(y_score, 7.5)
    mask      = y_score <= threshold
    if mask.sum() == 0:
        return 0.0
    overall = float(np.mean(y_true))
    if overall == 0.0:
        return 0.0
    return float(np.mean(y_true[mask])) / overall


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
    trial_no  : int,
    params    : dict,
    sd        : _SearchDataset,
    search_dir: Path,
) -> dict:
    """Fit one LightGBM model on the train split, evaluate on both train and valid.

    Args:
        trial_no   : Trial sequence number (for artifact naming).
        params     : Hyperparameter dict (tuned params; _FIXED_PARAMS merged in).
        sd         : Pre-built train/valid search dataset.
        search_dir : Directory for curve artifacts.

    Returns:
        Dict with trial metrics: train_ratio_p925, valid_ratio_p925, and audit metrics.
    """
    # feature_k is a meta-param for joint search — not passed to LightGBM
    feature_k   = params.get("feature_k")
    lgbm_params = {k: v for k, v in params.items() if k != "feature_k"}
    full_params  = {**_FIXED_PARAMS, **lgbm_params, "random_state": 42}

    split = sd.train
    if feature_k is not None:
        X_train = split.X_train.iloc[:, :feature_k]
        X_eval  = split.X_eval.iloc[:, :feature_k]
    else:
        X_train = split.X_train
        X_eval  = split.X_eval

    eval_result: dict = {}
    callbacks = [
        lgb.early_stopping(stopping_rounds=_ES_ROUNDS, verbose=False),
        lgb.log_evaluation(period=-1),
        lgb.record_evaluation(eval_result),
    ]

    model = lgb.LGBMRegressor(**full_params)
    model.fit(
        X_train, split.y_train,
        eval_set   = [(X_train, split.y_train), (X_eval, split.y_eval)],
        eval_names = ["train", "valid"],
        callbacks  = callbacks,
    )

    best_iter  = getattr(model, "best_iteration_", None) or full_params["n_estimators"]
    train_pred = pd.Series(model.predict(X_train))
    valid_pred = pd.Series(model.predict(X_eval))

    y_tr = split.y_train.to_numpy(dtype=float)
    y_va = split.y_eval.to_numpy(dtype=float)
    p_tr = train_pred.to_numpy(dtype=float)
    p_va = valid_pred.to_numpy(dtype=float)

    train_rmse = float(np.sqrt(np.mean((y_tr - p_tr) ** 2)))
    valid_rmse = float(np.sqrt(np.mean((y_va - p_va) ** 2)))
    train_mae  = float(np.mean(np.abs(y_tr - p_tr)))
    valid_mae  = float(np.mean(np.abs(y_va - p_va)))

    train_ratio_p925 = _compute_ratio_p925(y_tr, p_tr)
    valid_ratio_p925 = _compute_ratio_p925(y_va, p_va)
    train_ratio_p075 = _compute_ratio_p075(y_tr, p_tr)
    valid_ratio_p075 = _compute_ratio_p075(y_va, p_va)

    spearman_result     = spearmanr(y_va, p_va)
    spearman_rho        = float(spearman_result[0])  # type: ignore[arg-type]
    decile_monotonicity = _compute_decile_monotonicity(y_va, p_va)

    fi = _feature_importance(model, X_train.columns.tolist())

    curves_path = search_dir / "trial_curves" / f"trial_{trial_no:04d}.json"
    curves_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(curves_path, _compact_curves(eval_result, trial_no))

    del model, train_pred, valid_pred
    gc.collect()

    return {
        "train_ratio_p925":    train_ratio_p925,
        "valid_ratio_p925":    valid_ratio_p925,
        "train_ratio_p075":    train_ratio_p075,
        "valid_ratio_p075":    valid_ratio_p075,
        "train_valid_gap":     train_ratio_p925 - valid_ratio_p925,
        "train_rmse":          train_rmse,
        "valid_rmse":          valid_rmse,
        "train_mae":           train_mae,
        "valid_mae":           valid_mae,
        "spearman_rho":        spearman_rho if np.isfinite(spearman_rho) else None,
        "decile_monotonicity": decile_monotonicity,
        "train_n":             int(len(split.y_train)),
        "valid_n":             int(len(split.y_eval)),
        "best_iteration":      best_iter,
        "top20_features":      fi,
    }


def _feature_importance(model: lgb.LGBMRegressor, feature_cols: list[str]) -> list[dict]:
    """Return top-20 features by gain importance.

    Args:
        model        : Fitted LGBMRegressor.
        feature_cols : Feature column names aligned to model inputs.

    Returns:
        List of dicts with keys feature, split, gain — sorted by gain descending.
    """
    split_imp = model.booster_.feature_importance(importance_type="split")
    gain_imp  = model.booster_.feature_importance(importance_type="gain")
    rows = [
        {"feature": f, "split": int(s), "gain": float(g)}
        for f, s, g in zip(feature_cols, split_imp, gain_imp, strict=False)
    ]
    rows.sort(key=lambda x: x["gain"], reverse=True)
    return rows[:20]


def _compact_curves(eval_result: dict, trial_no: int) -> dict:
    """Compact learning curves to at most _CURVE_MAX_POINTS per metric.

    Args:
        eval_result : LightGBM eval_result dict from record_evaluation callback.
        trial_no    : Trial number for labelling.

    Returns:
        Dict with trial number and downsampled per-dataset metric arrays.
    """
    out: dict = {"trial": trial_no}
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
def _compute_objective(
    trial_metrics: dict,
    gap_penalty:   float = 0.0,
    direction:     str   = "long",
) -> dict:
    """Compute the search objective from a single trial's metrics.

    Long:  objective = -valid_ratio_p925 (top 7.5% by score → high MFE for long).
    Short: objective = -valid_ratio_p075 (bottom 7.5% by score → most-negative
           short_mfe for short; dividing two negatives gives ratio > 1 for a good model).
    With gap_penalty > 0: -(valid_ratio - gap_penalty * max(0, gap)).

    Args:
        trial_metrics : Metrics dict from _run_one_trial.
        gap_penalty   : Weight on train–valid gap penalty (0 = no regularization).
        direction     : "long" or "short" — selects the ratio metric.

    Returns:
        Dict with objective_score (lower=better) and aggregated metrics.
    """
    ratio_key       = "valid_ratio_p925" if direction == "long" else "valid_ratio_p075"
    train_ratio_key = "train_ratio_p925" if direction == "long" else "train_ratio_p075"

    valid_ratio = trial_metrics.get(ratio_key)
    if valid_ratio is None:
        return {"objective_score": float("inf")}

    train_ratio = float(trial_metrics.get(train_ratio_key, 0.0))
    gap         = train_ratio - float(valid_ratio)
    penalized   = float(valid_ratio) - gap_penalty * max(0.0, gap)

    return {
        "objective_score":     -penalized,
        "valid_ratio_p925":    float(trial_metrics.get("valid_ratio_p925", 0.0)),
        "valid_ratio_p075":    float(trial_metrics.get("valid_ratio_p075", 0.0)),
        "penalized_ratio":     penalized,
        "train_ratio_p925":    float(trial_metrics.get("train_ratio_p925", 0.0)),
        "train_ratio_p075":    float(trial_metrics.get("train_ratio_p075", 0.0)),
        "train_valid_gap":     gap,
        "spearman_rho":        trial_metrics.get("spearman_rho"),
        "decile_monotonicity": trial_metrics.get("decile_monotonicity"),
        "valid_rmse":          trial_metrics.get("valid_rmse"),
        "train_rmse":          trial_metrics.get("train_rmse"),
        "valid_mae":           trial_metrics.get("valid_mae"),
        "train_mae":           trial_metrics.get("train_mae"),
    }


# =============================================================================
# Patience stopping helper
# =============================================================================
def _check_patience(
    completed_trials: list[dict],
    patience:         int = 20,
    epsilon:          float = 0.001,
) -> bool:
    """Return True if patience stopping condition is met.

    Condition: the last `patience` completed trials contain no improvement
    of at least `epsilon` over the best valid_top10_lift seen before them.

    Args:
        completed_trials : List of completed trial records (chronological order).
        patience         : Number of trials to look back.
        epsilon          : Minimum improvement threshold.

    Returns:
        True if stopping is warranted, False otherwise.
    """
    if len(completed_trials) < patience:
        return False

    recent = completed_trials[-patience:]

    def _score(t: dict) -> float:
        v = t.get("valid_top10_lift")
        return float(v) if v is not None else float("-inf")

    best_before = max((_score(t) for t in completed_trials[:-patience]), default=float("-inf"))
    best_recent = max((_score(t) for t in recent), default=float("-inf"))
    return (best_recent - best_before) < epsilon


# =============================================================================
# Best trial selection — valid max + gap filter
# =============================================================================
def _select_best_trial(completed_trials: list[dict]) -> dict | None:
    """Select the best trial by valid_top10_lift (higher = better), gap as tiebreaker.

    Selects the top-5 trials by valid_top10_lift descending, then among those
    picks the one with the smallest train_valid_gap.

    Args:
        completed_trials : List of completed trial records.

    Returns:
        The best trial record, or None if no valid trials exist.
    """
    valid = [t for t in completed_trials if t.get("valid_top10_lift") is not None]
    if not valid:
        return None

    # Sort by valid_top10_lift descending (higher = better)
    valid.sort(key=lambda t: t.get("valid_top10_lift", float("-inf")), reverse=True)

    # Top-5 candidates (or fewer if less available)
    top_n    = min(5, len(valid))
    top_pool = valid[:top_n]

    # Among the top pool, pick the one with smallest train-valid gap
    best = min(top_pool, key=lambda t: abs(t.get("train_valid_gap", float("inf"))))
    return best


# =============================================================================
# Seeded random search
# =============================================================================
def _search_random(
    model_id:       str,
    sd:             _SearchDataset,
    search_dir:     Path,
    feature_cols:   list[str],
    row_stride:     int,
    n_trials:       int,
    timeout_hours:  float | None,
    stage:          str,
    done_hashes:    set,
    fail_hashes:    set,
    n_features_max: int | None  = None,
    gap_penalty:    float       = 0.0,
    direction:      str         = "long",
) -> dict:
    rng = np.random.default_rng(seed=42)

    best             = _load_best(search_dir)
    completed_trials = _load_completed_trials(search_dir)
    timeout_s        = timeout_hours * 3600 if timeout_hours else None
    t0               = time.time()
    completed_count  = 0
    cons_fails       = 0
    attempt          = 0

    while completed_count < min(n_trials, _MAX_TRIALS):
        if timeout_s and (time.time() - t0) > timeout_s:
            logger.info(f"[Search] Timeout after {completed_count} completed trials")
            break

        attempt += 1
        guide    = _build_tpe_guide(completed_trials)
        params   = _sample_params_random(rng, stage, guide, n_features_max)
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
            result       = _run_one_trial(trial_no, params, sd, search_dir)
            obj          = _compute_objective(result, gap_penalty, direction)
            elapsed      = time.time() - t_trial
            trial_record = {
                "trial_no":   trial_no,
                "param_hash": h,
                "params":     params,
                "elapsed_s":  round(elapsed, 1),
                **obj,
                "best_iteration":      result.get("best_iteration"),
                "top20_features":      result.get("top20_features"),
            }

            _log_trial_result(trial_no, params, trial_record, best)
            _persist_completed(search_dir, trial_no, trial_record, feature_cols, row_stride)
            done_hashes.add(h)
            completed_trials.append(trial_record)
            completed_count += 1
            cons_fails       = 0
            best = _update_best(search_dir, trial_record, best, completed_trials)

        except MemoryError as exc:
            cons_fails += 1
            logger.warning(f"[Trial {trial_no}] MemoryError — {exc}")
            _persist_failed(search_dir, attempt, params, h, "MemoryError", str(exc))
            fail_hashes.add(h)
            gc.collect()
            if cons_fails >= 3:
                logger.error("[Search] 3 consecutive memory failures — aborting")
                break

        except Exception as exc:
            cons_fails += 1
            logger.warning(f"[Trial {trial_no}] {type(exc).__name__}: {exc}")
            _persist_failed(search_dir, attempt, params, h, type(exc).__name__, str(exc))
            fail_hashes.add(h)
            gc.collect()

    return best or {}


# =============================================================================
# Optuna TPE search
# =============================================================================
def _search_optuna(
    model_id:       str,
    sd:             _SearchDataset,
    search_dir:     Path,
    feature_cols:   list[str],
    row_stride:     int,
    n_trials:       int,
    timeout_hours:  float | None,
    stage:          str,
    done_hashes:    set,
    fail_hashes:    set,
    n_features_max: int | None  = None,
    gap_penalty:    float       = 0.0,
    direction:      str         = "long",
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
    effective_trials = min(n_trials, _MAX_TRIALS)

    def objective(trial: "optuna.Trial") -> float:
        params = _suggest_optuna_params(trial, stage, n_features_max)
        h      = _make_param_hash(params, len(feature_cols), row_stride)

        if h in done_hashes or h in fail_hashes:
            raise optuna.exceptions.TrialPruned()

        trial_no = trial.number + 1
        logger.info(f"\n[Trial {trial_no:04d}]  Params: {_format_params(params)}")

        t_trial      = time.time()
        result       = _run_one_trial(trial_no, params, sd, search_dir)
        obj          = _compute_objective(result, gap_penalty, direction)
        elapsed      = time.time() - t_trial
        trial_record = {
            "trial_no":            trial_no,
            "param_hash":          h,
            "params":              params,
            "elapsed_s":           round(elapsed, 1),
            **obj,
            "best_iteration":      result.get("best_iteration"),
            "top20_features":      result.get("top20_features"),
        }

        nonlocal best
        _log_trial_result(trial_no, params, trial_record, best)
        _persist_completed(search_dir, trial_no, trial_record, feature_cols, row_stride)
        done_hashes.add(h)
        completed_trials.append(trial_record)
        best = _update_best(search_dir, trial_record, best, completed_trials)

        return obj.get("objective_score", float("inf"))

    timeout_s = timeout_hours * 3600 if timeout_hours else None
    try:
        study.optimize(
            objective,
            n_trials          = effective_trials,
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
def _suggest_optuna_params(
    trial:          "optuna.Trial",
    stage:          str,
    n_features_max: int | None = None,
) -> dict:
    """Suggest hyperparameters for an Optuna trial.

    Args:
        trial          : Optuna trial object.
        stage          : Search stage (affects num_leaves range).
        n_features_max : When provided (joint search mode), also suggests
                         feature_k — number of top gain-ranked features to use.

    Returns:
        Dict of hyperparameter name → value.
    """
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

    params = {
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

    if n_features_max is not None:
        params["feature_k"] = trial.suggest_int("feature_k", 3, n_features_max, log=True)

    return params


def _sample_params_random(
    rng:            np.random.Generator,
    stage:          str,
    tpe_guide:      dict | None = None,
    n_features_max: int | None  = None,
) -> dict:
    """Sample hyperparameters randomly (fallback when Optuna is unavailable).

    Args:
        rng       : NumPy random generator.
        stage     : Search stage (affects num_leaves range).
        tpe_guide : Optional guide dict from _build_tpe_guide for guided sampling.

    Returns:
        Dict of hyperparameter name → value.
    """
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

    g          = tpe_guide or {}
    leaves_hi  = 31 if stage == "smoke" else 63
    num_leaves = int_log_uni(3, leaves_hi, g.get("num_leaves"), g.get("_frac"))
    max_depth  = int(rng.choice([-1, 2, 3, 4, 5, 6, 8]))
    if max_depth > 0:
        num_leaves = min(num_leaves, 2 ** max_depth)

    zero_gain      = rng.random() < 0.20
    min_split_gain = 0.0 if zero_gain else log_uni(1e-5, 0.1)

    result = {
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
    if n_features_max is not None:
        result["feature_k"] = int_log_uni(3, n_features_max, g.get("feature_k"), g.get("_frac"))
    return result


def _build_tpe_guide(completed: list[dict], n_startup: int = 20) -> dict | None:
    """Build a parameter guide from the top-quartile completed trials.

    Args:
        completed  : List of completed trial records.
        n_startup  : Minimum trials before building a guide.

    Returns:
        Guide dict with median params of top-quartile trials, or None.
    """
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
        "colsample_bytree", "learning_rate", "feature_k",
    )}
    guide["_frac"] = 0.5
    return guide


def _make_param_hash(params: dict, n_features: int, row_stride: int) -> str:
    """Compute a short hash for a parameter set to detect duplicates.

    Args:
        params     : Hyperparameter dict.
        n_features : Number of features (part of the hash key).
        row_stride : Row sub-sampling stride (part of the hash key).

    Returns:
        16-character hex hash string.
    """
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
    log_record               = {k: v for k, v in record.items() if k != "top20_features"}
    log_record["n_features"] = len(feature_cols)
    log_record["row_stride"] = row_stride
    _write_json(logs_dir / f"trial_{trial_no:04d}.json", log_record)

    # search_trials.jsonl — compact record, includes train/valid ratio for analyst notebook
    compact = {
        "trial_no":            record["trial_no"],
        "param_hash":          record["param_hash"],
        "params":              record["params"],
        "feature_k":           record["params"].get("feature_k"),
        "objective_score":     record.get("objective_score"),
        "valid_ratio_p925":    record.get("valid_ratio_p925"),
        "train_ratio_p925":    record.get("train_ratio_p925"),
        "train_valid_gap":     record.get("train_valid_gap"),
        "spearman_rho":        record.get("spearman_rho"),
        "decile_monotonicity": record.get("decile_monotonicity"),
        "valid_rmse":          record.get("valid_rmse"),
        "train_rmse":          record.get("train_rmse"),
        "valid_mae":           record.get("valid_mae"),
        "elapsed_s":           record.get("elapsed_s"),
        "best_iteration":      record.get("best_iteration"),
    }
    _append_jsonl(search_dir / "search_trials.jsonl", compact)
    _update_summary_csv(search_dir, compact, record["params"])
    _log_aggregated_feature_importance(trial_no, record.get("top20_features", []))


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


def _update_best(
    search_dir:       Path,
    record:           dict,
    current_best:     dict | None,
    completed_trials: list[dict],
) -> dict:
    """Re-select best trial using valid max + gap secondary filter.

    Args:
        search_dir       : Directory to write search_best.json.
        record           : Latest completed trial record.
        current_best     : Previously tracked best record.
        completed_trials : All completed trial records (for re-selection).

    Returns:
        Updated best trial record.
    """
    selected = _select_best_trial(completed_trials)
    if selected is None:
        return current_best or {}

    prev_obj = (current_best or {}).get("objective_score", float("inf"))
    curr_obj = selected.get("objective_score", float("inf"))

    if curr_obj < prev_obj or current_best is None:
        best_to_write = {k: v for k, v in selected.items() if k != "top20_features"}
        _write_json(search_dir / "search_best.json", best_to_write)
        curr_penalized = selected.get("penalized_ratio") or selected.get("valid_ratio_p925", 0.0)
        logger.info(
            f"  *** NEW BEST  penalized={curr_penalized:.6f}  "
            f"valid_ratio={selected.get('valid_ratio_p925', 0.0):.6f}  "
            f"gap={selected.get('train_valid_gap', 0.0):.6f} ***"
        )
        return best_to_write

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
    valid_ratio = record.get("valid_ratio_p925")
    train_ratio = record.get("train_ratio_p925")
    gap         = record.get("train_valid_gap")
    rho         = record.get("spearman_rho")
    mono        = record.get("decile_monotonicity")
    vrmse       = record.get("valid_rmse")
    vmae        = record.get("valid_mae")
    obj         = record.get("objective_score")
    best_valid  = (best or {}).get("valid_ratio_p925")

    feature_k = params.get("feature_k")
    k_str     = f"  feature_k={feature_k}" if feature_k is not None else ""
    logger.info(
        f"  valid_ratio_p925={_fmt(valid_ratio)}  train_ratio_p925={_fmt(train_ratio)}  gap={_fmt(gap)}{k_str}"
    )
    logger.info(
        f"  objective={_fmt(obj)}  spearman_rho={_fmt(rho)}  decile_monotonicity={_fmt(mono)}"
    )
    logger.info(f"  valid_rmse={_fmt(vrmse)}  valid_mae={_fmt(vmae)}")

    if best_valid is not None:
        delta  = (valid_ratio or float("-inf")) - best_valid
        marker = "  << NEW BEST" if delta > 0 else f"  (Δ{delta:+.5f} vs best)"
        logger.info(f"  elapsed={record.get('elapsed_s')}s{marker}")
    else:
        logger.info(f"  elapsed={record.get('elapsed_s')}s")


def _log_aggregated_feature_importance(trial_no: int, top20: list[dict]) -> None:
    if not top20:
        return
    ranked = sorted(top20, key=lambda x: x["gain"], reverse=True)[:20]
    logger.info(f"  Feature importance (gain, top 20) — trial {trial_no:04d}:")
    for row in ranked:
        logger.info(f"    {row['feature']:<55}  gain={row['gain']:.1f}")


def _print_final_summary(best: dict | None, search_dir: Path) -> None:
    logger.info("\n" + "=" * 72)
    logger.info("SEARCH COMPLETE")
    logger.info("=" * 72)

    if not best:
        logger.info("No completed trials.")
        return

    logger.info(f"Best trial: #{best.get('trial_no')}  hash={best.get('param_hash')}")
    logger.info(f"  objective_score     = {_fmt(best.get('objective_score'))}  (lower=better)")
    logger.info(f"  valid_ratio_p925    = {_fmt(best.get('valid_ratio_p925'))}")
    logger.info(f"  train_ratio_p925    = {_fmt(best.get('train_ratio_p925'))}  (diagnostic)")
    logger.info(f"  train_valid_gap     = {_fmt(best.get('train_valid_gap'))}")
    logger.info(f"  spearman_rho        = {_fmt(best.get('spearman_rho'))}")
    logger.info(f"  decile_monotonicity = {_fmt(best.get('decile_monotonicity'))}")
    logger.info(f"  valid_rmse          = {_fmt(best.get('valid_rmse'))}")
    logger.info(f"  valid_mae           = {_fmt(best.get('valid_mae'))}")
    logger.info(f"  elapsed_s           = {best.get('elapsed_s')}")
    logger.info("\nBest parameters:")
    for k, v in sorted(best.get("params", {}).items()):
        logger.info(f"  {k:<25} = {v}")

    csv_path = search_dir / "search_summary.csv"
    if csv_path.exists():
        with contextlib.suppress(Exception):
            df = pd.read_csv(csv_path)
            if "valid_ratio_p925" in df.columns and len(df) > 1:
                top = df.nlargest(min(10, len(df)), "valid_ratio_p925")
                logger.info(f"\nTop-{len(top)} trials by valid_ratio_p925:")
                for _, row in top.iterrows():
                    logger.info(
                        f"  #{int(row['trial_no']):04d}  "  # type: ignore[arg-type]
                        f"valid_ratio={_fmt(row.get('valid_ratio_p925'))}  "
                        f"train_ratio={_fmt(row.get('train_ratio_p925'))}  "
                        f"gap={_fmt(row.get('train_valid_gap'))}  "
                        f"rho={_fmt(row.get('spearman_rho'))}"
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
