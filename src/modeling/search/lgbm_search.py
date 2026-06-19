# =============================================================================
# LightGBM hyperparameter search — yearly sample edition
# =============================================================================
# Inputs:
#   artifact_dir/feature_engineering/feature_set.json  → selected feature list
#   sample_dir/sample_train_valid.parquet + metadata.json  → CV folds + target
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
import re
import time
from dataclasses import dataclass
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import polars as pl

import utils
from data_handling.store.duckdb_query import query_range_pl
from modeling.sampling import load_yearly_sample
from modeling.training.datasets import ModelingDataset
from modeling.training.metrics import binary_classification_metrics
from modeling.training.training_windows import DatasetSplit

logger = logging.getLogger("lgbm_search")

# ─── Fixed LightGBM parameters (not tuned) ───────────────────────────────────
_FIXED_PARAMS: dict = {
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
_ALLOWED_GAP  = 0.03
_STAB_W       = 0.25
_GAP_W        = 0.10

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
    segments: pd.Series   # "train" | "valid" | "purge" — aligned with dataset rows


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
    and CV structure from sample_dir/metadata.json (selected_valid_weeks).

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
    asset_id     = meta["asset_id"]
    artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))
    sample_dir   = Path(utils._resolve_path(meta["sampling"]["sample_dir"]))
    search_dir   = artifact_dir / "search"
    search_dir.mkdir(parents=True, exist_ok=True)

    n_trials, row_stride, fold_limit = _apply_stage_defaults(
        stage, n_trials, row_stride, fold_limit
    )

    feature_cols  = _load_feature_cols(artifact_dir)
    q_threshold   = _parse_quantile(model_id)
    sample_meta   = load_yearly_sample(sample_dir)
    all_weeks     = sample_meta["selected_valid_weeks"]
    folds         = all_weeks[:fold_limit] if fold_limit else all_weeks

    logger.info("=" * 72)
    logger.info(f"LGBM SEARCH  model={model_id}")
    logger.info(f"  target={target_col}  stage={stage}  q_threshold={q_threshold:.2f}")
    logger.info(f"  n_trials={n_trials}  row_stride={row_stride}  folds={len(folds)}/{len(all_weeks)}")
    logger.info(f"  engine={'optuna-TPE' if _HAS_OPTUNA else 'seeded-random'}")
    logger.info(f"  search_dir={search_dir}")
    logger.info("=" * 72)

    sd = _load_search_dataset(
        sample_dir   = sample_dir,
        target_col   = target_col,
        feature_cols = feature_cols,
        asset_id     = asset_id,
        row_stride   = row_stride,
        q_threshold  = q_threshold,
    )
    pos_rate = float(sd.dataset.y.mean())
    logger.info(
        f"[Data] {len(sd.dataset.y):,} rows  pos_rate={pos_rate:.4f}  "
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
            model_id, sd, folds, search_dir, feature_cols, row_stride,
            n_trials, timeout_hours, stage, done_hashes, fail_hashes,
        )
    else:
        best = _search_random(
            model_id, sd, folds, search_dir, feature_cols, row_stride,
            n_trials, timeout_hours, stage, done_hashes, fail_hashes,
        )

    if best:
        _write_best_params(search_dir, best)

    _print_final_summary(best, search_dir)
    return best


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
# Quantile parsing
# =============================================================================
def _parse_quantile(model_id: str) -> float:
    m = re.search(r"_q(\d+)(?:_|$)", model_id)
    if not m:
        raise ValueError(
            f"Cannot parse quantile from model_id '{model_id}'. "
            "Expected pattern _q{{N}}_ (e.g. _q90_ or _q10_)"
        )
    return int(m.group(1)) / 100.0


# =============================================================================
# Dataset loading — sample parquet × DuckDB features
# =============================================================================
def _load_search_dataset(
    sample_dir:   Path,
    target_col:   str,
    feature_cols: list[str],
    asset_id:     str,
    row_stride:   int,
    q_threshold:  float,
) -> _SearchDataset:
    parquet_path = sample_dir / "sample_train_valid.parquet"
    sample_pl = pl.read_parquet(parquet_path, columns=["open_time", "segment", target_col])

    if row_stride > 1:
        sample_pl = sample_pl[::row_stride]

    meta_path = sample_dir / "metadata.json"
    year      = json.loads(meta_path.read_text(encoding="utf-8"))["year"]

    db_path     = utils.load_asset_config(asset_id)["database"]["db_path"]
    feat_cols   = ["open_time"] + feature_cols
    feat_pl     = query_range_pl(
        db_path, "feat_ohlcv_quant",
        start   = f"{year}-01-01 00:00:00",
        end     = f"{year}-12-31 23:59:59",
        columns = feat_cols,
    )

    merged_pl = sample_pl.join(feat_pl, on="open_time", how="inner")
    merged    = merged_pl.to_pandas().reset_index(drop=True)
    merged["open_time"] = pd.to_datetime(merged["open_time"])

    # Binary label — threshold from train segment only (no leakage from valid)
    train_mask = merged["segment"] == "train"
    train_y    = merged.loc[train_mask, target_col]
    threshold  = float(np.quantile(train_y, q_threshold))

    if q_threshold >= 0.5:  # long model: top (1-q)% → y >= threshold
        binary_y = (merged[target_col] >= threshold).astype(np.int8)
    else:                   # short model: bottom q% → y <= threshold
        binary_y = (merged[target_col] <= threshold).astype(np.int8)

    pos_train = float(binary_y[train_mask].mean())
    logger.info(
        f"[Data] threshold={threshold:.5f} (q{q_threshold:.0%} of train rows)  "
        f"pos_rate_train={pos_train:.4f}  total_rows={len(merged):,}"
    )

    dataset = ModelingDataset(
        open_time    = pd.Series(merged["open_time"]),
        X            = pd.DataFrame(merged[feature_cols]),
        y            = pd.Series(binary_y, name=target_col),
        target_col   = target_col,
        feature_cols = feature_cols,
    )
    return _SearchDataset(dataset=dataset, segments=pd.Series(merged["segment"]))


# =============================================================================
# Fold split — one validation week from selected_valid_weeks
# =============================================================================
def _fold_split_yearly(
    sd:         _SearchDataset,
    valid_week: dict,
    fold_idx:   int,
) -> DatasetSplit:
    start = pd.Timestamp(valid_week["start"])
    end   = pd.Timestamp(valid_week["end"]) + pd.Timedelta(hours=23, minutes=59, seconds=59)

    train_mask = sd.segments == "train"
    valid_mask = (
        (sd.segments == "valid")
        & (sd.dataset.open_time >= start)
        & (sd.dataset.open_time <= end)
    )

    if not valid_mask.any():
        raise ValueError(
            f"Empty validation set — fold {fold_idx + 1} "
            f"({valid_week['start']} – {valid_week['end']})"
        )

    return DatasetSplit(
        X_train = sd.dataset.X.loc[train_mask],
        y_train = sd.dataset.y.loc[train_mask],
        X_eval  = sd.dataset.X.loc[valid_mask],
        y_eval  = sd.dataset.y.loc[valid_mask],
    )


# =============================================================================
# Single trial execution
# =============================================================================
def _run_one_trial(
    trial_no:   int,
    params:     dict,
    sd:         _SearchDataset,
    folds:      list[dict],
    search_dir: Path,
) -> dict:
    full_params  = {**_FIXED_PARAMS, **params, "random_state": 42}
    fold_results = []

    for fold_idx, fold in enumerate(folds):
        split = _fold_split_yearly(sd, fold, fold_idx)

        eval_result: dict = {}
        callbacks = [
            lgb.early_stopping(stopping_rounds=_ES_ROUNDS, verbose=False),
            lgb.log_evaluation(period=-1),
            lgb.record_evaluation(eval_result),
        ]

        model = lgb.LGBMClassifier(**full_params)
        model.fit(
            split.X_train, split.y_train,
            eval_set   = [(split.X_train, split.y_train), (split.X_eval, split.y_eval)],
            eval_names = ["train", "valid"],
            callbacks  = callbacks,
        )

        best_iter  = getattr(model, "best_iteration_", None) or full_params["n_estimators"]
        train_pred = model.predict_proba(split.X_train)[:, 1]  # type: ignore[index]
        valid_pred = model.predict_proba(split.X_eval)[:, 1]   # type: ignore[index]
        tm = binary_classification_metrics(split.y_train, train_pred)
        vm = binary_classification_metrics(split.y_eval,  valid_pred)

        fi = _feature_importance(model, sd.dataset.feature_cols)

        curves_path = (
            search_dir / "trial_curves"
            / f"trial_{trial_no:04d}_fold_{fold_idx + 1:02d}.json"
        )
        curves_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(curves_path, _compact_curves(eval_result, trial_no, fold_idx + 1))

        fold_results.append({
            "fold":           fold_idx + 1,
            "fold_week":      f"{fold['start']}_{fold['end']}",
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


def _feature_importance(model: lgb.LGBMClassifier, feature_cols: list[str]) -> list[dict]:
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
    """
    score = mean(valid_ll) + 0.25 * std(valid_ll)
            + 0.10 * max(0, mean(valid_ll - train_ll) - 0.03)

    Lower is better (matches Optuna direction='minimize').
    """
    valid_lls  = [f["valid_log_loss"] for f in fold_metrics if f["valid_log_loss"] is not None]
    train_lls  = [f["train_log_loss"] for f in fold_metrics if f["train_log_loss"] is not None]
    valid_prcs = [f["valid_pr_auc"]   for f in fold_metrics if f["valid_pr_auc"]   is not None]
    train_prcs = [f["train_pr_auc"]   for f in fold_metrics if f["train_pr_auc"]   is not None]

    if not valid_lls:
        return {"objective_score": float("inf")}

    mean_v   = float(np.mean(valid_lls))
    std_v    = float(np.std(valid_lls))
    mean_t   = float(np.mean(train_lls)) if train_lls else None
    mean_gap = float(mean_v - mean_t) if mean_t is not None else 0.0
    penalty  = max(0.0, mean_gap - _ALLOWED_GAP)
    score    = mean_v + _STAB_W * std_v + _GAP_W * penalty

    return {
        "mean_valid_ll":     mean_v,
        "std_valid_ll":      std_v,
        "mean_train_ll":     mean_t,
        "mean_gap":          mean_gap,
        "gap_penalty":       penalty,
        "objective_score":   score,
        "mean_valid_prauc":  float(np.mean(valid_prcs)) if valid_prcs else None,
        "mean_train_prauc":  float(np.mean(train_prcs)) if train_prcs else None,
        "mean_valid_roc":    float(np.mean([f["valid_roc_auc"] for f in fold_metrics
                                            if f.get("valid_roc_auc")])) or None,
        "mean_lift_5pct":    float(np.mean([f["lift_5pct"] for f in fold_metrics
                                            if f.get("lift_5pct")])) or None,
        "mean_lift_10pct":   float(np.mean([f["lift_10pct"] for f in fold_metrics
                                            if f.get("lift_10pct")])) or None,
    }


# =============================================================================
# Seeded random search
# =============================================================================
def _search_random(
    model_id:      str,
    sd:            _SearchDataset,
    folds:         list[dict],
    search_dir:    Path,
    feature_cols:  list[str],
    row_stride:    int,
    n_trials:      int,
    timeout_hours: float | None,
    stage:         str,
    done_hashes:   set,
    fail_hashes:   set,
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
            result  = _run_one_trial(trial_no, params, sd, folds, search_dir)
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
    model_id:      str,
    sd:            _SearchDataset,
    folds:         list[dict],
    search_dir:    Path,
    feature_cols:  list[str],
    row_stride:    int,
    n_trials:      int,
    timeout_hours: float | None,
    stage:         str,
    done_hashes:   set,
    fail_hashes:   set,
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
        result       = _run_one_trial(trial_no, params, sd, folds, search_dir)
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
        "trial_no":         record["trial_no"],
        "param_hash":       record["param_hash"],
        "params":           record["params"],
        "objective_score":  record.get("objective_score"),
        "mean_valid_ll":    record.get("mean_valid_ll"),
        "std_valid_ll":     record.get("std_valid_ll"),
        "mean_train_ll":    record.get("mean_train_ll"),
        "mean_gap":         record.get("mean_gap"),
        "mean_valid_prauc": record.get("mean_valid_prauc"),
        "elapsed_s":        record.get("elapsed_s"),
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
    vll  = record.get("mean_valid_ll")
    tll  = record.get("mean_train_ll")
    gap  = record.get("mean_gap")
    vprc = record.get("mean_valid_prauc")
    obj  = record.get("objective_score")
    std  = record.get("std_valid_ll")
    best_obj = (best or {}).get("objective_score")

    logger.info(
        f"  valid_ll={_fmt(vll)}  train_ll={_fmt(tll)}  "
        f"gap={_fmt(gap)}  std={_fmt(std)}"
    )
    logger.info(f"  valid_prauc={_fmt(vprc)}  objective={_fmt(obj)}")

    if best_obj is not None:
        delta  = (obj or float("inf")) - best_obj
        marker = "  << NEW BEST" if delta < 0 else f"  (Δ+{delta:.5f} vs best)"
        logger.info(
            f"  elapsed={record.get('elapsed_s')}s{marker}"
        )
    else:
        logger.info(f"  elapsed={record.get('elapsed_s')}s")

    for f in record.get("fold_metrics", []):
        logger.info(
            f"    fold {f['fold']} ({f.get('fold_week', '')})  "
            f"valid_ll={_fmt(f.get('valid_log_loss'))}  "
            f"train_ll={_fmt(f.get('train_log_loss'))}  "
            f"valid_prauc={_fmt(f.get('valid_pr_auc'))}  "
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
    logger.info(f"  objective_score  = {_fmt(best.get('objective_score'))}  (lower=better)")
    logger.info(f"  mean_valid_ll    = {_fmt(best.get('mean_valid_ll'))}  std={_fmt(best.get('std_valid_ll'))}")
    logger.info(f"  mean_train_ll    = {_fmt(best.get('mean_train_ll'))}  gap={_fmt(best.get('mean_gap'))}")
    logger.info(f"  mean_valid_prauc = {_fmt(best.get('mean_valid_prauc'))}")
    logger.info(f"  elapsed_s        = {best.get('elapsed_s')}")
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
                        f"valid_ll={_fmt(row.get('mean_valid_ll'))}  "
                        f"valid_prauc={_fmt(row.get('mean_valid_prauc'))}"
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
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
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
