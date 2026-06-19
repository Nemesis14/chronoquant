# =============================================================================
# Distribution-based LightGBM hyperparameter search
# =============================================================================
# Purpose:
#  - Stage 0: Feature audit — null rates, near-constants, known duplicates
#  - Stages 1-3: Optuna TPE (preferred) or seeded random search fallback
#  - Primary objective: stability-penalized validation log loss
#  - Guardrail: PR AUC must not regress more than 5% vs champion
#  - All trials persisted immediately for resume on interruption
#
# Entry point:
#   run_search(model_id, stage="smoke"|"explore"|"refine", ...)
# =============================================================================

import contextlib
import gc
import hashlib
import json
import logging
import time
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

import utils
from modeling.sampling import load_sample_definition, validate_sample_definition
from modeling.training.datasets import load_modeling_dataset
from modeling.training.metrics import binary_classification_metrics
from modeling.training.training_windows import fold_split

logger = logging.getLogger("lgbm_search")

# ─── Known duplicate features from the solusdt_fw60 redundancy map ───────────
_KNOWN_DUPLICATES = frozenset({
    "feat_returns_sma_14",  # r=1.0 with feat_roc_14
    "feat_ohlc_range",      # r=1.0 with feat_hml_range
    "feat_williams_r_14",   # r=1.0 with feat_stoch_k_14
    "feat_ad_line",         # r=0.9999 with feat_obv
    "feat_atr_14",          # r=0.974 with feat_natr_14
    "feat_wma_ratio_14",    # r=0.967 with feat_ema_ratio_14
})

# ─── Fixed LightGBM parameters (not tuned) ───────────────────────────────────
_FIXED_PARAMS = {
    "objective":      "binary",
    "boosting_type":  "gbdt",
    "metric":         "binary_logloss",
    "n_estimators":   3000,
    "subsample_freq": 1,
    "force_col_wise": True,
    "verbosity":      -1,
    "n_jobs":         4,
}

# ─── Objective penalty weights ────────────────────────────────────────────────
# score = mean(valid_ll) + STAB_W * std(valid_ll) + GAP_W * max(0, gap - ALLOWED_GAP)
_ALLOWED_GAP   = 0.03
_STAB_W        = 0.25
_GAP_W         = 0.10

_ES_ROUNDS         = 100   # early stopping patience
_CURVE_MAX_POINTS  = 100   # max training curve points saved per fold

try:
    import optuna  # type: ignore[import-not-found]
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAS_OPTUNA = True
except ImportError:
    _HAS_OPTUNA = False


# =============================================================================
# run_search  —  main entry point
# =============================================================================
def run_search(
    model_id:      str,
    n_trials:      int          = 60,
    timeout_hours: float | None = None,
    row_stride:    int | None   = None,
    fold_limit:    int | None   = None,
    stage:         str          = "explore",
    retry_failed:  bool         = False,
    asset_id:      str | None   = None,
) -> dict:
    """
    Run the distribution-based LightGBM search for model_id.

    Stages
    ------
    smoke   5 trials / row_stride=60 / 2 folds  — verify pipeline
    explore 60 trials / row_stride=60 / all folds — broad region search
    refine  30 trials / row_stride=10 / all folds — narrow best regions
    """
    _setup_logging()

    models_cfg = utils.load_models_config()
    if model_id not in models_cfg.get("models", {}):
        raise ValueError(f"Model not found in config/models.json: {model_id}")

    meta       = models_cfg["models"][model_id]
    target_col = meta["target_name"]
    asset_id   = asset_id or meta.get("asset_id")
    sample_dir = Path(utils._resolve_path(meta["sampling"]["sample_dir"]))
    model_dir  = Path(utils._resolve_path(meta["artifact_dir"]))
    search_dir = model_dir / "search"
    search_dir.mkdir(parents=True, exist_ok=True)

    n_trials, row_stride, fold_limit = _apply_stage_defaults(
        stage, n_trials, row_stride, fold_limit
    )

    sample = load_sample_definition(sample_dir)
    validate_sample_definition(sample)
    folds  = sample["folds"][:fold_limit] if fold_limit else sample["folds"]

    logger.info("=" * 72)
    logger.info(f"LGBM SEARCH  model={model_id}")
    logger.info(f"  target={target_col}  stage={stage}")
    logger.info(f"  n_trials={n_trials}  row_stride={row_stride}  folds={len(folds)}")
    logger.info(f"  search_engine={'optuna-TPE' if _HAS_OPTUNA else 'seeded-random'}")
    logger.info(f"  search_dir={search_dir}")
    logger.info("=" * 72)

    # ── Stage 0: Feature audit ────────────────────────────────────────────────
    feat_file = search_dir / "features_search.json"
    if feat_file.exists():
        feat_data    = json.loads(feat_file.read_text(encoding="utf-8"))
        feature_cols = feat_data["features"]
        logger.info(f"[Stage 0] Reusing existing feature list — {len(feature_cols)} features")
    else:
        feature_cols = _run_feature_audit(asset_id, search_dir)

    # ── Load dataset ──────────────────────────────────────────────────────────
    logger.info(f"[Data] Loading dataset (row_stride={row_stride}) …")
    t0 = time.time()
    dataset = load_modeling_dataset(
        target_col   = target_col,
        feature_cols = feature_cols,
        start        = sample["data"]["start"],
        end          = sample["data"]["end"],
        row_stride   = row_stride,
        asset_id     = asset_id,
    )
    pos_rate = float(dataset.y.mean())
    logger.info(
        f"[Data] {len(dataset.y):,} rows  pos_rate={pos_rate:.4f}  "
        f"n_features={len(feature_cols)}  elapsed={time.time()-t0:.1f}s"
    )
    mem_mb = len(dataset.y) * len(feature_cols) * 4 / (1024**2)
    logger.info(f"[Data] Feature matrix ≈{mem_mb:.0f} MB (float32 estimate)")

    # ── Champion PR AUC guardrail ─────────────────────────────────────────────
    champion_prauc = _load_champion_prauc(model_id, model_dir)
    if champion_prauc is not None:
        logger.info(
            f"[Guardrail] Champion mean valid PR AUC = {champion_prauc:.4f}  "
            f"(5% floor = {champion_prauc * 0.95:.4f})"
        )
    else:
        logger.info("[Guardrail] No champion found — PR AUC guardrail disabled")

    # ── Resume: load already-seen hashes ─────────────────────────────────────
    done_hashes, fail_hashes = _load_existing_hashes(search_dir)
    if retry_failed:
        fail_hashes = set()
    logger.info(
        f"[Resume] {len(done_hashes)} completed, {len(fail_hashes)} failed trials in log"
    )

    # ── Run search ────────────────────────────────────────────────────────────
    if _HAS_OPTUNA:
        best = _search_optuna(
            model_id, dataset, folds, search_dir, feature_cols, row_stride,
            n_trials, timeout_hours, stage, champion_prauc,
            done_hashes, fail_hashes,
        )
    else:
        best = _search_random(
            model_id, dataset, folds, search_dir, feature_cols, row_stride,
            n_trials, timeout_hours, stage, champion_prauc,
            done_hashes, fail_hashes,
        )

    _print_final_summary(best, search_dir)
    return best


# =============================================================================
# Stage defaults
# =============================================================================
def _apply_stage_defaults(
    stage: str,
    n_trials: int,
    row_stride: int | None,
    fold_limit: int | None,
) -> tuple[int, int, int | None]:
    if stage == "smoke":
        return min(n_trials, 5), row_stride or 60, fold_limit or 2
    if stage == "explore":
        return min(n_trials, 60), row_stride or 60, fold_limit
    if stage == "refine":
        return min(n_trials, 30), row_stride or 10, fold_limit
    return n_trials, row_stride or 60, fold_limit


# =============================================================================
# Stage 0 — Feature audit
# =============================================================================
def _run_feature_audit(asset_id: str | None, search_dir: Path) -> list[str]:
    """
    Audit the feature table and build the search feature list.

    Removes:
      - features with null rate > 1%
      - near-constant features (std < 0.001)
      - known redundant duplicates from the solusdt_fw60 redundancy map

    Writes features_search.json to search_dir.
    """
    import duckdb

    cfg      = utils.load_asset_config(asset_id)["database"]
    data_dir = cfg["data_dir"]
    glob_path = str(Path(data_dir) / "features" / "*.parquet")

    AUDIT_ROWS = 200_000
    logger.info(f"[Stage 0] Auditing features dataset (sample {AUDIT_ROWS:,} rows) …")

    conn = duckdb.connect()
    try:
        _count_row = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{glob_path}', union_by_name=true)"
        ).fetchone()
        total = int(_count_row[0]) if _count_row else 0
        df = conn.execute(
            f"SELECT * FROM read_parquet('{glob_path}', union_by_name=true) ORDER BY open_time DESC LIMIT {AUDIT_ROWS}"
        ).df()
    finally:
        conn.close()
    logger.info(f"[Stage 0] Dataset: {total:,} rows total, {AUDIT_ROWS:,} sampled for audit")

    all_feat = sorted(c for c in df.columns if c.startswith("feat_"))
    logger.info(f"[Stage 0] Total feat_ columns: {len(all_feat)}")

    null_rates = df[all_feat].isna().mean()
    stds       = df[all_feat].std()

    removed_null  = sorted(c for c in all_feat if null_rates[c] > 0.01)
    # Remove only truly constant features (float-precision zero variance).
    # A threshold of 1e-6 avoids discarding small-valued ratio features
    # (natr, gk_vol, parkinson, lr_slope, return_mean) whose std is small
    # in absolute terms but informative relative to their own scale.
    removed_const = sorted(c for c in all_feat if stds[c] < 1e-6 and c not in removed_null)  # type: ignore[index]
    removed_dupes = sorted(c for c in all_feat if c in _KNOWN_DUPLICATES
                           and c not in removed_null and c not in removed_const)

    if removed_null:
        logger.info(f"[Stage 0] Removed {len(removed_null)} high-null features (>1% null):")
        for c in removed_null:
            logger.info(f"  REMOVED (null={null_rates[c]*100:.1f}%)  {c}")
    else:
        logger.info("[Stage 0] No high-null features")

    if removed_const:
        logger.info(f"[Stage 0] Removed {len(removed_const)} near-constant features:")
        for c in removed_const:
            logger.info(f"  REMOVED (std={stds[c]:.6f})  {c}")  # type: ignore[index]
    else:
        logger.info("[Stage 0] No near-constant features")

    if removed_dupes:
        logger.info(f"[Stage 0] Removed {len(removed_dupes)} known redundant duplicates:")
        for c in removed_dupes:
            logger.info(f"  REMOVED (duplicate)  {c}")
    else:
        logger.info("[Stage 0] No known duplicates found in table")

    # Exclude features marked unusable in feature_taxonomy (KAN-32/KAN-34)
    unusable       = set(utils.load_unusable_features())
    removed_unusable = sorted(c for c in all_feat if c in unusable
                              and c not in removed_null and c not in removed_const
                              and c not in removed_dupes)
    if removed_unusable:
        logger.info(
            "[Stage 0] Removed %d unusable features (feature_taxonomy):", len(removed_unusable)
        )
        for c in removed_unusable:
            logger.info(f"  REMOVED (unusable)  {c}")

    excluded     = set(removed_null) | set(removed_const) | set(removed_dupes) | unusable
    feature_cols = sorted(c for c in all_feat if c not in excluded)

    logger.info(
        f"[Stage 0] Final: {len(feature_cols)} features "
        f"(removed {len(excluded)} / {len(all_feat)})"
    )

    # ── Activity feature null-rate summary ────────────────────────────────────
    activity_kw = ("quote_volume", "trade_count", "avg_trade", "taker_buy", "taker_flow")
    act_cols    = [c for c in feature_cols if any(kw in c for kw in activity_kw)]
    if act_cols:
        logger.info(f"[Stage 0] Activity features ({len(act_cols)}) null rates:")
        for c in act_cols:
            r = null_rates.get(c, 0.0) or 0.0
            logger.info(f"  {c:<55}  null={r*100:.2f}%")

    result = {
        "features":                feature_cols,
        "n_features":              len(feature_cols),
        "total_in_table":          len(all_feat),
        "removed_high_null":       removed_null,
        "removed_near_constant":   removed_const,
        "removed_known_duplicates": removed_dupes,
        "audit_sample_rows":       AUDIT_ROWS,
    }
    _write_json(search_dir / "features_search.json", result)
    logger.info("[Stage 0] features_search.json written")
    return feature_cols


# =============================================================================
# Parameter sampling
# =============================================================================
def _sample_params_random(
    rng:   np.random.Generator,
    stage: str,
    tpe_guide: dict | None = None,
) -> dict:
    """
    Sample one parameter set from the search distributions.

    When tpe_guide is provided (after n_startup_trials), parameters are
    sampled from a narrower range centred on the top-trial observations
    (crude TPE approximation for the Optuna-free fallback).
    """

    def log_uni(lo, hi, center=None, scale_frac=None):
        if center and scale_frac:
            log_c  = np.log(center)
            log_lo = max(np.log(lo), log_c - scale_frac)
            log_hi = min(np.log(hi), log_c + scale_frac)
            return float(np.exp(rng.uniform(log_lo, log_hi)))
        return float(np.exp(rng.uniform(np.log(lo), np.log(hi))))

    def int_log_uni(lo, hi, center=None, scale_frac=None):
        return max(lo, min(hi, int(round(log_uni(lo, hi, center, scale_frac)))))

    def uni(lo, hi, center=None, scale_frac=None):
        if center and scale_frac:
            half = (hi - lo) * scale_frac
            return float(rng.uniform(max(lo, center - half), min(hi, center + half)))
        return float(rng.uniform(lo, hi))

    g = tpe_guide or {}

    leaves_hi = 31 if stage == "smoke" else 63
    num_leaves = int_log_uni(3, leaves_hi,
                             g.get("num_leaves"), g.get("_frac"))
    max_depth  = int(rng.choice([-1, 2, 3, 4, 5, 6, 8]))
    if max_depth > 0:
        num_leaves = min(num_leaves, 2 ** max_depth)

    zero_gain  = rng.random() < 0.20
    min_split_gain = 0.0 if zero_gain else log_uni(1e-5, 0.1)

    return {
        "num_leaves":        num_leaves,
        "max_depth":         max_depth,
        "min_child_samples": int_log_uni(200, 8000, g.get("min_child_samples"), g.get("_frac")),
        "min_child_weight":  log_uni(1e-4, 1e-1,   g.get("min_child_weight"),  g.get("_frac")),
        "min_split_gain":    min_split_gain,
        "reg_alpha":         log_uni(1e-3, 10.0,   g.get("reg_alpha"),         g.get("_frac")),
        "reg_lambda":        log_uni(1.0,  100.0,  g.get("reg_lambda"),        g.get("_frac")),
        "subsample":         uni(0.45, 0.95,        g.get("subsample"),         g.get("_frac")),
        "colsample_bytree":  uni(0.35, 0.95,        g.get("colsample_bytree"),  g.get("_frac")),
        "learning_rate":     log_uni(0.005, 0.05,   g.get("learning_rate"),     g.get("_frac")),
        "max_bin":           int(rng.choice([63, 127])),
        "path_smooth":       log_uni(1e-3, 10.0),
        "extra_trees":       bool(rng.choice([False, True])),
    }


def _build_tpe_guide(completed: list[dict], n_startup: int = 20) -> dict | None:
    """
    After n_startup random trials, build a crude guide from top-quartile trials.
    Returns None during the random startup phase.
    """
    if len(completed) < n_startup:
        return None
    scores   = [t["objective_score"] for t in completed if t.get("objective_score") is not None]
    if not scores:
        return None
    threshold = np.percentile(scores, 25)  # top 25%
    top       = [t for t in completed
                 if t.get("objective_score", float("inf")) <= threshold]
    if not top:
        return None

    def _median(key):
        vals = [t["params"].get(key) for t in top if t["params"].get(key) is not None]
        return float(np.median(vals)) if vals else None

    guide = {k: _median(k) for k in (
        "num_leaves", "min_child_samples", "min_child_weight",
        "reg_alpha", "reg_lambda", "subsample",
        "colsample_bytree", "learning_rate",
    )}
    guide["_frac"] = 0.5  # sample within ±50% of log range around median
    return guide


def _make_param_hash(params: dict, n_features: int, row_stride: int) -> str:
    """Stable 16-char hex hash for deduplication."""
    payload = {
        "params":     {k: str(v) for k, v in sorted(params.items())},
        "n_features": n_features,
        "row_stride": row_stride,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()[:16]


# =============================================================================
# Single trial execution
# =============================================================================
def _run_one_trial(
    trial_no:   int,
    params:     dict,
    dataset,
    folds:      list[dict],
    search_dir: Path,
) -> dict:
    """
    Train one parameter set on all folds.
    Returns fold metrics, feature importances, and training curves.
    """
    full_params = {**_FIXED_PARAMS, **params, "random_state": 42}
    fold_results = []

    for fold in folds:
        split = fold_split(dataset, fold)
        if len(split.y_train) == 0 or len(split.y_eval) == 0:
            raise ValueError(f"Empty split on fold {fold['fold']}")

        eval_result: dict = {}
        callbacks = [
            lgb.early_stopping(stopping_rounds=_ES_ROUNDS, verbose=False),
            lgb.log_evaluation(period=-1),
            lgb.record_evaluation(eval_result),
        ]

        model = lgb.LGBMClassifier(**full_params)
        model.fit(
            split.X_train, split.y_train,
            eval_set=[
                (split.X_train, split.y_train),
                (split.X_eval,  split.y_eval),
            ],
            eval_names=["train", "valid"],
            callbacks=callbacks,
        )

        best_iter = getattr(model, "best_iteration_", None) or full_params["n_estimators"]

        train_pred = model.predict_proba(split.X_train)[:, 1]  # type: ignore[index]
        valid_pred = model.predict_proba(split.X_eval)[:, 1]  # type: ignore[index]
        tm = binary_classification_metrics(split.y_train, train_pred)
        vm = binary_classification_metrics(split.y_eval,  valid_pred)

        fi = _feature_importance(model, dataset.feature_cols)

        curves_path = (
            search_dir / "trial_curves"
            / f"trial_{trial_no:04d}_fold_{fold['fold']}.json"
        )
        curves_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(curves_path, _compact_curves(eval_result, trial_no, fold["fold"]))

        fold_results.append({
            "fold":           fold["fold"],
            "train_log_loss": tm["log_loss"],
            "valid_log_loss": vm["log_loss"],
            "train_pr_auc":   tm["pr_auc"],
            "valid_pr_auc":   vm["pr_auc"],
            "train_roc_auc":  tm["roc_auc"],
            "valid_roc_auc":  vm["roc_auc"],
            "train_brier":    tm["brier_score"],
            "valid_brier":    vm["brier_score"],
            "train_n":        int(len(split.y_train)),
            "valid_n":        int(len(split.y_eval)),
            "best_iteration": best_iter,
            "lift_1pct":      vm["lift"].get("top_1pct", {}).get("lift"),
            "lift_5pct":      vm["lift"].get("top_5pct", {}).get("lift"),
            "lift_10pct":     vm["lift"].get("top_10pct", {}).get("lift"),
            "top20_features": fi,
        })

        del model, train_pred, valid_pred
        gc.collect()

    return {"fold_metrics": fold_results}


def _feature_importance(model, feature_cols: list[str]) -> list[dict]:
    """Top-20 features by gain importance from this fold's model."""
    split_imp = model.booster_.feature_importance(importance_type="split")
    gain_imp  = model.booster_.feature_importance(importance_type="gain")
    rows = [
        {"feature": f, "split": int(s), "gain": float(g)}
        for f, s, g in zip(feature_cols, split_imp, gain_imp, strict=False)
    ]
    rows.sort(key=lambda x: x["gain"], reverse=True)
    return rows[:20]


def _compact_curves(eval_result: dict, trial_no: int, fold_no: int) -> dict:
    """Downsample LightGBM eval curves to at most _CURVE_MAX_POINTS points."""
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
    """
    score = mean(valid_ll) + 0.25 * std(valid_ll)
            + 0.10 * max(0, mean(valid_ll - train_ll) - 0.03)

    A lower score is better (matches Optuna direction='minimize').
    """
    valid_lls  = [f["valid_log_loss"] for f in fold_metrics if f["valid_log_loss"] is not None]
    train_lls  = [f["train_log_loss"] for f in fold_metrics if f["train_log_loss"] is not None]
    valid_prcs = [f["valid_pr_auc"]   for f in fold_metrics if f["valid_pr_auc"]   is not None]
    train_prcs = [f["train_pr_auc"]   for f in fold_metrics if f["train_pr_auc"]   is not None]

    if not valid_lls:
        return {"objective_score": float("inf")}

    mean_v  = float(np.mean(valid_lls))
    std_v   = float(np.std(valid_lls))
    mean_t  = float(np.mean(train_lls)) if train_lls else None
    mean_gap = float(mean_v - mean_t) if mean_t is not None else 0.0
    penalty  = max(0.0, mean_gap - _ALLOWED_GAP)
    score    = mean_v + _STAB_W * std_v + _GAP_W * penalty

    return {
        "mean_valid_ll":    mean_v,
        "std_valid_ll":     std_v,
        "mean_train_ll":    mean_t,
        "mean_gap":         mean_gap,
        "gap_penalty":      penalty,
        "objective_score":  score,
        "mean_valid_prauc": float(np.mean(valid_prcs)) if valid_prcs else None,
        "mean_train_prauc": float(np.mean(train_prcs)) if train_prcs else None,
        "mean_valid_roc":   float(np.mean([f["valid_roc_auc"] for f in fold_metrics
                                           if f.get("valid_roc_auc")])) or None,
        "mean_lift_5pct":   float(np.mean([f["lift_5pct"] for f in fold_metrics
                                           if f.get("lift_5pct")])) or None,
        "mean_lift_10pct":  float(np.mean([f["lift_10pct"] for f in fold_metrics
                                           if f.get("lift_10pct")])) or None,
    }


# =============================================================================
# Seeded random search loop
# =============================================================================
def _search_random(
    model_id, dataset, folds, search_dir, feature_cols, row_stride,
    n_trials, timeout_hours, stage, champion_prauc,
    done_hashes, fail_hashes,
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
            logger.info(f"[Search] Timeout reached after {completed_count} completed trials")
            break

        attempt  += 1
        guide     = _build_tpe_guide(completed_trials)
        params    = _sample_params_random(rng, stage, guide)
        h         = _make_param_hash(params, len(feature_cols), row_stride)

        if h in done_hashes:
            continue
        if h in fail_hashes:
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
            result   = _run_one_trial(trial_no, params, dataset, folds, search_dir)
            obj      = _compute_objective(result["fold_metrics"])
            elapsed  = time.time() - t_trial

            guardrail_ok = _check_guardrail(obj, champion_prauc)

            trial_record = {
                "trial_no":       trial_no,
                "param_hash":     h,
                "params":         params,
                "elapsed_s":      round(elapsed, 1),
                "guardrail_ok":   guardrail_ok,
                "fold_metrics":   result["fold_metrics"],
                **obj,
            }

            _log_trial_result(trial_no, params, trial_record, best)
            _persist_completed(search_dir, trial_no, trial_record, feature_cols, row_stride)
            done_hashes.add(h)
            completed_trials.append(trial_record)
            completed_count   += 1
            consecutive_fails  = 0

            if guardrail_ok or champion_prauc is None:
                best = _update_best(search_dir, trial_record, best)
            else:
                logger.info(
                    f"  [GUARDRAIL FAIL] PR AUC {obj.get('mean_valid_prauc', 0.0):.4f} < "
                    f"floor {champion_prauc * 0.95:.4f} — not eligible for best"
                )

        except MemoryError as exc:
            consecutive_fails += 1
            logger.warning(f"[Trial {trial_no}] MemoryError — {exc}")
            _persist_failed(search_dir, attempt, params, h, "MemoryError", str(exc))
            fail_hashes.add(h)
            gc.collect()
            if consecutive_fails >= 3:
                logger.error("[Search] 3 consecutive memory failures — aborting search")
                break

        except Exception as exc:
            consecutive_fails += 1
            logger.warning(f"[Trial {trial_no}] {type(exc).__name__}: {exc}")
            _persist_failed(search_dir, attempt, params, h, type(exc).__name__, str(exc))
            fail_hashes.add(h)
            gc.collect()

    return best or {}


# =============================================================================
# Optuna search loop
# =============================================================================
def _search_optuna(
    model_id, dataset, folds, search_dir, feature_cols, row_stride,
    n_trials, timeout_hours, stage, champion_prauc,
    done_hashes, fail_hashes,
) -> dict:
    import optuna  # type: ignore[import-not-found]

    storage_path = search_dir / "optuna_study.db"
    storage      = f"sqlite:///{storage_path}"
    sampler      = optuna.samplers.TPESampler(
        seed=42, n_startup_trials=20, multivariate=True
    )
    study = optuna.create_study(
        study_name    = model_id,
        storage       = storage,
        sampler       = sampler,
        direction     = "minimize",
        load_if_exists= True,
    )

    best             = _load_best(search_dir)
    completed_trials = _load_completed_trials(search_dir)

    def objective(trial):
        params = _suggest_optuna_params(trial, stage)
        h      = _make_param_hash(params, len(feature_cols), row_stride)

        if h in done_hashes or h in fail_hashes:
            raise optuna.exceptions.TrialPruned()

        trial_no = trial.number + 1
        logger.info(f"\n[Trial {trial_no:04d}]  Params: {_format_params(params)}")

        t_trial = time.time()
        result  = _run_one_trial(trial_no, params, dataset, folds, search_dir)
        obj     = _compute_objective(result["fold_metrics"])
        elapsed = time.time() - t_trial

        guardrail_ok  = _check_guardrail(obj, champion_prauc)
        trial_record  = {
            "trial_no":     trial_no,
            "param_hash":   h,
            "params":       params,
            "elapsed_s":    round(elapsed, 1),
            "guardrail_ok": guardrail_ok,
            "fold_metrics": result["fold_metrics"],
            **obj,
        }

        nonlocal best
        _log_trial_result(trial_no, params, trial_record, best)
        _persist_completed(search_dir, trial_no, trial_record, feature_cols, row_stride)
        done_hashes.add(h)
        completed_trials.append(trial_record)

        if guardrail_ok or champion_prauc is None:
            best = _update_best(search_dir, trial_record, best)
        else:
            logger.info("  [GUARDRAIL FAIL] PR AUC below 5% floor")

        return obj.get("objective_score", float("inf"))

    timeout_s = timeout_hours * 3600 if timeout_hours else None
    try:
        study.optimize(
            objective,
            n_trials  = n_trials,
            timeout   = timeout_s,
            catch      = (Exception,),
            show_progress_bar = False,
        )
    except KeyboardInterrupt:
        logger.info("[Search] Interrupted by user")

    return best or {}


def _suggest_optuna_params(trial, stage: str) -> dict:

    leaves_hi  = 31 if stage == "smoke" else 63
    num_leaves = trial.suggest_int("num_leaves", 3, leaves_hi, log=True)
    max_depth  = trial.suggest_categorical("max_depth", [-1, 2, 3, 4, 5, 6, 8])

    if max_depth > 0:
        num_leaves = min(num_leaves, 2 ** max_depth)

    zero_gain     = trial.suggest_categorical("zero_gain", [True, False])
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


# =============================================================================
# Guardrail check
# =============================================================================
def _check_guardrail(obj: dict, champion_prauc: float | None) -> bool:
    if champion_prauc is None:
        return True
    vp = obj.get("mean_valid_prauc")
    if vp is None:
        return True
    return vp >= champion_prauc * 0.95


# =============================================================================
# Persistence helpers
# =============================================================================
def _persist_completed(
    search_dir:   Path,
    trial_no:     int,
    record:       dict,
    feature_cols: list[str],
    row_stride:   int,
) -> None:
    # ── trial_logs/trial_NNNN.json ────────────────────────────────────────────
    logs_dir = search_dir / "trial_logs"
    logs_dir.mkdir(exist_ok=True)
    log_record = {k: v for k, v in record.items() if k != "fold_metrics"}
    log_record["n_features"]  = len(feature_cols)
    log_record["row_stride"]  = row_stride
    # Add per-fold summary (without top20_features to keep size small)
    log_record["fold_summary"] = [
        {k: v for k, v in f.items() if k != "top20_features"}
        for f in record.get("fold_metrics", [])
    ]
    _write_json(logs_dir / f"trial_{trial_no:04d}.json", log_record)

    # ── search_trials.jsonl ───────────────────────────────────────────────────
    compact = {
        "trial_no":        record["trial_no"],
        "param_hash":      record["param_hash"],
        "params":          record["params"],
        "objective_score": record.get("objective_score"),
        "mean_valid_ll":   record.get("mean_valid_ll"),
        "std_valid_ll":    record.get("std_valid_ll"),
        "mean_train_ll":   record.get("mean_train_ll"),
        "mean_gap":        record.get("mean_gap"),
        "mean_valid_prauc":record.get("mean_valid_prauc"),
        "guardrail_ok":    record.get("guardrail_ok"),
        "elapsed_s":       record.get("elapsed_s"),
    }
    _append_jsonl(search_dir / "search_trials.jsonl", compact)

    # ── search_summary.csv ────────────────────────────────────────────────────
    _update_summary_csv(search_dir, compact, record["params"])

    # ── Feature importance log (top 20 aggregated across folds) ───────────────
    _log_aggregated_feature_importance(
        trial_no, record.get("fold_metrics", [])
    )


def _persist_failed(
    search_dir: Path,
    attempt:    int,
    params:     dict,
    h:          str,
    exc_type:   str,
    exc_msg:    str,
) -> None:
    record = {
        "attempt":  attempt,
        "hash":     h,
        "params":   params,
        "exc_type": exc_type,
        "exc_msg":  exc_msg[:500],
    }
    _append_jsonl(search_dir / "failed_trials.jsonl", record)


def _update_best(search_dir: Path, record: dict, current_best: dict | None) -> dict:
    score = record.get("objective_score", float("inf"))
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
    return current_best  # type: ignore[return-value]


def _load_best(search_dir: Path) -> dict | None:
    p = search_dir / "search_best.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return None


def _load_existing_hashes(search_dir: Path) -> tuple[set, set]:
    done, fail = set(), set()
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
    trials = []
    for line in _iter_jsonl(search_dir / "search_trials.jsonl"):
        if line.get("objective_score") is not None:
            trials.append(line)
    return trials


def _update_summary_csv(search_dir: Path, compact: dict, params: dict) -> None:
    row = {**compact, **{f"p_{k}": v for k, v in params.items()}}
    csv_path = search_dir / "search_summary.csv"
    df_new   = pd.DataFrame([row])
    if csv_path.exists():
        df_old = pd.read_csv(csv_path)
        df_new = pd.concat([df_old, df_new], ignore_index=True)
    df_new.to_csv(csv_path, index=False)


def _load_champion_prauc(model_id: str, model_dir: Path) -> float | None:
    """
    Try to load the champion's mean valid PR AUC from cv_results.csv.
    Maps local_v2 → stable_v1 champion for the same direction/asset.
    """
    champion_id = model_id.replace("local_v2", "stable_v1")
    if champion_id == model_id:
        return None

    candidate_dir = model_dir.parent / champion_id
    csv_path      = candidate_dir / "cv_results.csv"
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path)
        if "valid_pr_auc" in df.columns:
            return float(df["valid_pr_auc"].mean())  # type: ignore[arg-type]
    except Exception:
        pass
    return None


# =============================================================================
# Logging helpers
# =============================================================================
def _log_trial_result(
    trial_no: int, params: dict, record: dict, best: dict | None
) -> None:
    vll  = record.get("mean_valid_ll")
    tll  = record.get("mean_train_ll")
    gap  = record.get("mean_gap")
    vprc = record.get("mean_valid_prauc")
    tprc = record.get("mean_train_prauc")
    obj  = record.get("objective_score")
    std  = record.get("std_valid_ll")
    best_obj = (best or {}).get("objective_score")

    logger.info(
        f"  valid_ll={_fmt(vll)}  train_ll={_fmt(tll)}  "
        f"gap={_fmt(gap)}  std={_fmt(std)}"
    )
    logger.info(
        f"  valid_prauc={_fmt(vprc)}  train_prauc={_fmt(tprc)}  "
        f"objective={_fmt(obj)}"
    )
    if best_obj is not None:
        delta = (obj or float("inf")) - best_obj
        marker = "  << NEW BEST" if delta < 0 else f"  (Δ+{delta:.5f} vs best)"
        logger.info(f"  elapsed={record.get('elapsed_s')}s  "
                    f"guardrail={'OK' if record.get('guardrail_ok') else 'FAIL'}"
                    f"{marker}")
    else:
        logger.info(f"  elapsed={record.get('elapsed_s')}s  guardrail={'OK' if record.get('guardrail_ok') else 'FAIL'}")

    # Per-fold breakdown
    for f in record.get("fold_metrics", []):
        logger.info(
            f"    fold {f['fold']}  "
            f"valid_ll={_fmt(f.get('valid_log_loss'))}  "
            f"train_ll={_fmt(f.get('train_log_loss'))}  "
            f"valid_prauc={_fmt(f.get('valid_pr_auc'))}  "
            f"best_iter={f.get('best_iteration')}  "
            f"n_valid={f.get('valid_n'):,}"
        )


def _log_aggregated_feature_importance(
    trial_no: int, fold_metrics: list[dict]
) -> None:
    """Log the mean gain importance across all folds for this trial."""
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
        logger.info(f"    {feat:<55}  gain={gain/n_folds:.1f}")


def _print_final_summary(best: dict | None, search_dir: Path) -> None:
    logger.info("\n" + "=" * 72)
    logger.info("SEARCH COMPLETE")
    logger.info("=" * 72)

    if not best:
        logger.info("No completed trials found.")
        return

    logger.info(f"Best trial: #{best.get('trial_no')}  hash={best.get('param_hash')}")
    logger.info(
        f"  objective_score = {_fmt(best.get('objective_score'))}"
        f"  (lower=better)"
    )
    logger.info(
        f"  mean_valid_ll   = {_fmt(best.get('mean_valid_ll'))}"
        f"  std_valid_ll = {_fmt(best.get('std_valid_ll'))}"
    )
    logger.info(
        f"  mean_train_ll   = {_fmt(best.get('mean_train_ll'))}"
        f"  mean_gap     = {_fmt(best.get('mean_gap'))}"
    )
    logger.info(
        f"  mean_valid_prauc= {_fmt(best.get('mean_valid_prauc'))}"
        f"  mean_train_prauc = {_fmt(best.get('mean_train_prauc'))}"
    )
    logger.info(f"  elapsed_s       = {best.get('elapsed_s')}")
    logger.info("\nBest parameters:")
    for k, v in sorted(best.get("params", {}).items()):
        logger.info(f"  {k:<25} = {v}")

    logger.info("\nPer-fold breakdown:")
    for f in best.get("fold_summary", []):
        logger.info(
            f"  fold {f['fold']}  "
            f"valid_ll={_fmt(f.get('valid_log_loss'))}  "
            f"train_ll={_fmt(f.get('train_log_loss'))}  "
            f"valid_prauc={_fmt(f.get('valid_pr_auc'))}"
        )

    # Top-20 summary from search_summary.csv
    csv_path = search_dir / "search_summary.csv"
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path)
            if "objective_score" in df.columns and len(df) > 1:
                top = df.nsmallest(min(10, len(df)), "objective_score")
                logger.info(f"\nTop-{len(top)} trials by objective score:")
                for _, row in top.iterrows():
                    logger.info(
                        f"  #{int(row['trial_no']):04d}  "  # type: ignore[arg-type]
                        f"obj={_fmt(row.get('objective_score'))}  "
                        f"valid_ll={_fmt(row.get('mean_valid_ll'))}  "
                        f"valid_prauc={_fmt(row.get('mean_valid_prauc'))}"
                    )
        except Exception:
            pass

    logger.info("\nArtifacts:")
    logger.info(f"  {search_dir / 'search_best.json'}")
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


def _json_serial(obj):
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


# =============================================================================
# Formatting helpers
# =============================================================================
def _fmt(v) -> str:
    if v is None:
        return "N/A"
    return f"{v:.6f}"


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
