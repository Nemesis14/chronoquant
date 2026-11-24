# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false, reportGeneralTypeIssues=false, reportReturnType=false
"""General report ``wave_predictiveness_gate``: image-vs-scalar predictiveness go / no-go (epic_072 / t920).

Methodology: ``_doc_/methodology_doc/5200_wave_start_image_model.md`` (v2), "Go / no-go" section. Cell ``W4h_H8h`` only,
TRAIN rows only (``load_events(split="train")``; the 1m price series is cut at the train end, nothing from the OOS /
holdout-2 period is ever loaded).

PRE-REGISTRATION (fixed before any result was looked at):

* Model A = ``S0 + C`` (6 scalar features), Model B = ``S0 + C + representation``; same learner, L2 logistic regression with a
  FIXED strength ``C = 0.1`` (no inner tuning: tuning would be label dependent and would have to be refitted per permutation).
* PRIMARY test = representation ``ae`` (16-dim autoencoder latent). 5200 does not name the representation, so the primary is
  the AE embedding. SENSITIVITY only (never the decision): cluster one-hot k = 10 (t919 choice), cluster one-hot k = 5 (most
  stable partition), LR strength 0.01 / 1.0, gradient boosting (point estimate only).
* Statistic: pooled out-of-fold ``dAUC = AUC(B) - AUC(A)`` over the 6 purged + embargoed (``H``) CV blocks; AE, k-means and the
  render axis are fitted PER FOLD on the fold-train events only (:func:`reports.builders.wave_image_model.fit_fold`).
* p-value: calendar-day-block permutation of the labels, ``N = 1000``, ``p = (1 + #{null >= observed}) / (N + 1)``; one cell ->
  Holm = identity.
* Go criteria in order: (1) minimum counts, (2) ``dAUC >= 0.02``, (3) ``p < 0.01``, (4) ``dAUC > 0`` in >= 4 / 6 blocks,
  (5) OOS sign check: needs >= 100 unique OOS t0; the t925 count is 75 -> the check does not run, best outcome "conditional GO".

Permutation efficiency (exact, not an approximation): the AE and k-means are UNSUPERVISED, so the per-fold embeddings / clusters
do not depend on the labels. They are computed once (and cached); in every permutation only the labels are permuted and
EVERY label-dependent component (the fold-train fits of Model A and of every Model B) is refitted on the same folds.

Regenerate::

    uv run --group research python -m reports.builders.wave_predictiveness_gate     (cwd: repo root)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from reports import ReportBuilder, theme
from reports.builders import wave_events as we
from reports.builders import wave_image_model as wm
from reports.builders._common import source_info, themed
from reports.builders.sr_detection import load_ohlcv_1m

logger = logging.getLogger(__name__)

SLUG = "wave_predictiveness_gate"
GENERATOR = "reports.builders.wave_predictiveness_gate"
PRIMARY = "ae"
REPS = ("ae", "cluster_k10", "cluster_k5")
REP_HU = {"ae": "AE latens (16 dim) — elsődleges", "cluster_k10": "klaszter one-hot k = 10 (t919)", "cluster_k5": "klaszter one-hot k = 5"}
CLUSTER_KS = {"cluster_k10": 10, "cluster_k5": 5}
LR_C = 0.1
LR_C_SENS = (0.01, 1.0)
N_PERM = 1000
N_BOOT = 2000
DELTA_MIN = 0.02
P_MAX = 0.01
BLOCKS_MIN = 4
OOS_UNIQUE_T0 = 75          # t925 (5200 v2 decision log): unique OOS t0 events; < 100 -> the OOS sign check does not run
OOS_MIN_T0 = 100
SCALAR_COLS = tuple(we.FEATURE_COLS)
FOLD_CACHE_VERSION = 1


# --------------------------------------------------------------------------- per-fold, label-free design data


@dataclass
class FoldData:
    """One purged / embargoed fold: row positions (into the usable events) and label-free design matrices."""

    block: int
    train_idx: np.ndarray
    test_idx: np.ndarray
    xa_tr: np.ndarray                                    # scalar S0 + C, standardised with the fold-train mean / sd
    xa_te: np.ndarray
    rep_tr: dict[str, np.ndarray]
    rep_te: dict[str, np.ndarray]
    n_purged: int = 0
    n_embargoed: int = 0
    n_straddle: int = 0
    _design: dict[str, tuple[np.ndarray, np.ndarray]] = field(default_factory=dict, repr=False)

    def design(self, rep: str | None) -> tuple[np.ndarray, np.ndarray]:
        """(train, test) design matrix of Model A (``rep=None``) or Model B with ``rep`` (cached)."""
        key = rep or "_A"
        if key not in self._design:
            if rep is None:
                self._design[key] = (self.xa_tr, self.xa_te)
            else:
                self._design[key] = (np.hstack([self.xa_tr, self.rep_tr[rep]]), np.hstack([self.xa_te, self.rep_te[rep]]))
        return self._design[key]


def usable_events(ev: pl.DataFrame) -> pl.DataFrame:
    """The events that take part in the CV (train, not straddling a block boundary), time ordered."""
    return ev.filter((pl.col("split") == "train") & ~pl.col("cv_straddle")).sort("t0")


def _t0(df: pl.DataFrame) -> np.ndarray:
    return df["t0"].to_numpy().astype("datetime64[us]")


def prepare_folds(ev: pl.DataFrame, pre: wm.Prefixes, bounds: list[datetime], embargo_min: int, *, cfg: wm.AEConfig | None = None,
                  seed: int = wm.SEED, ks: tuple[int, ...] = tuple(CLUSTER_KS.values())) -> list[FoldData]:
    """Per-fold label-free design data: AE + k-means fitted on the fold-train events only (``wm.fit_fold``).

    ``ev`` = ONE cell's train events (v2 table). Labels are never read here.
    """
    use = usable_events(ev)
    index = pd.Index(_t0(use))
    folds: list[FoldData] = []
    for blk in range(len(bounds) - 1):
        ff = wm.fit_fold(ev, pre, blk, bounds, embargo_min, ks, cfg=cfg, seed=seed)
        tr_idx = index.get_indexer(pd.Index(_t0(ff.fold.train)))
        te_idx = index.get_indexer(pd.Index(_t0(ff.fold.test)))
        if (tr_idx < 0).any() or (te_idx < 0).any():
            raise KeyError("fold events outside the usable set")
        x_tr = ff.fold.train.select(list(SCALAR_COLS)).to_numpy().astype(float)
        x_te = ff.fold.test.select(list(SCALAR_COLS)).to_numpy().astype(float)
        mu, sd = x_tr.mean(axis=0), x_tr.std(axis=0)
        sd = np.where(sd > 1e-12, sd, 1.0)
        rep_tr: dict[str, np.ndarray] = {"ae": ff.train_z}
        rep_te: dict[str, np.ndarray] = {"ae": ff.test_z}
        for name, k in CLUSTER_KS.items():
            if k in ks:
                rep_tr[name] = np.eye(k)[ff.train_labels[k]]
                rep_te[name] = np.eye(k)[ff.test_labels[k]]
        folds.append(FoldData(blk, tr_idx, te_idx, (x_tr - mu) / sd, (x_te - mu) / sd, rep_tr, rep_te,
                              ff.fold.n_purged, ff.fold.n_embargoed, ff.fold.n_straddle))
        logger.info("fold %d/%d prepared (train %d, test %d)", blk + 1, len(bounds) - 1, tr_idx.size, te_idx.size)
    return folds


def _cache_key(ev: pl.DataFrame, cfg: wm.AEConfig, seed: int, embargo_min: int) -> str:
    h = hashlib.sha256()
    h.update(_t0(usable_events(ev)).tobytes())
    h.update(json.dumps([cfg.__dict__, seed, embargo_min, FOLD_CACHE_VERSION, sorted(CLUSTER_KS.items())], sort_keys=True).encode())
    return h.hexdigest()[:16]


def save_folds(folds: list[FoldData], path: Path, key: str, fit_seconds: float = float("nan")) -> None:
    arrs: dict[str, Any] = {"_key": np.array(key), "_n": np.array(len(folds)), "_fit_s": np.array(fit_seconds)}
    for f in folds:
        p = f"f{f.block}_"
        arrs |= {p + "tr_idx": f.train_idx, p + "te_idx": f.test_idx, p + "xa_tr": f.xa_tr, p + "xa_te": f.xa_te,
                 p + "meta": np.array([f.n_purged, f.n_embargoed, f.n_straddle]), p + "reps": np.array(list(f.rep_tr))}
        for r in f.rep_tr:
            arrs[p + "rtr_" + r], arrs[p + "rte_" + r] = f.rep_tr[r], f.rep_te[r]
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(path, **arrs)


def cached_fit_seconds(path: Path) -> float:
    """Wall time of the original per-fold AE + k-means fitting stored next to the cache (nan if unknown)."""
    z = np.load(path, allow_pickle=False)
    return float(z["_fit_s"]) if "_fit_s" in z.files else float("nan")


def load_folds(path: Path, key: str) -> list[FoldData] | None:
    if not path.exists():
        return None
    z = np.load(path, allow_pickle=False)
    if str(z["_key"]) != key:
        return None
    out = []
    for b in range(int(z["_n"])):
        p = f"f{b}_"
        reps = [str(r) for r in z[p + "reps"]]
        meta = z[p + "meta"]
        out.append(FoldData(b, z[p + "tr_idx"], z[p + "te_idx"], z[p + "xa_tr"], z[p + "xa_te"],
                            {r: z[p + "rtr_" + r] for r in reps}, {r: z[p + "rte_" + r] for r in reps},
                            int(meta[0]), int(meta[1]), int(meta[2])))
    return out


# --------------------------------------------------------------------------- label-dependent part: models, OOF, AUC


def _lr(x_tr: np.ndarray, y_tr: np.ndarray, x_te: np.ndarray, c: float) -> np.ndarray:
    from sklearn.linear_model import LogisticRegression

    return LogisticRegression(C=c, max_iter=2000).fit(x_tr, y_tr).predict_proba(x_te)[:, 1]


def _gbm(x_tr: np.ndarray, y_tr: np.ndarray, x_te: np.ndarray, seed: int) -> np.ndarray:
    from sklearn.ensemble import HistGradientBoostingClassifier
    from threadpoolctl import threadpool_limits

    with threadpool_limits(limits=1):
        m = HistGradientBoostingClassifier(max_depth=3, learning_rate=0.05, max_iter=150, min_samples_leaf=20,
                                           l2_regularization=1.0, random_state=seed)
        return m.fit(x_tr, y_tr).predict_proba(x_te)[:, 1]


def oof_predictions(folds: list[FoldData], y: np.ndarray, reps: tuple[str, ...] = REPS, *, c: float = LR_C,
                    learner: str = "lr", seed: int = wm.SEED) -> dict[str, np.ndarray]:
    """Out-of-fold probabilities (every usable event is in exactly one test fold) for Model A (``"A"``) and each Model B.

    Everything label dependent is refitted on the fold-train rows: standardisation is label free, the learner is fitted
    with ``y[train_idx]`` only.
    """
    n = max(int(f.test_idx.max()) for f in folds) + 1
    out = {k: np.full(n, np.nan) for k in ("A", *reps)}
    for f in folds:
        for key, rep in (("A", None), *((r, r) for r in reps)):
            xtr, xte = f.design(rep)
            ytr = y[f.train_idx]
            out[key][f.test_idx] = _lr(xtr, ytr, xte, c) if learner == "lr" else _gbm(xtr, ytr, xte, seed)
    return out


def auc(y: np.ndarray, p: np.ndarray) -> float:
    from sklearn.metrics import roc_auc_score

    return float(roc_auc_score(y, p))


def pooled_delta(y: np.ndarray, oof: dict[str, np.ndarray], rep: str) -> tuple[float, float, float]:
    """(AUC_A, AUC_B, dAUC) pooled over all out-of-fold events."""
    a, b = auc(y, oof["A"]), auc(y, oof[rep])
    return a, b, b - a


def block_table(folds: list[FoldData], y: np.ndarray, oof: dict[str, np.ndarray], rep: str) -> pd.DataFrame:
    rows = []
    for f in folds:
        yt = y[f.test_idx]
        a, b = auc(yt, oof["A"][f.test_idx]), auc(yt, oof[rep][f.test_idx])
        rows.append({"block": f.block + 1, "n_test": int(yt.size), "n_pos": int(yt.sum()), "n_train": int(f.train_idx.size),
                     "auc_a": a, "auc_b": b, "delta_auc": b - a, "sign": "+" if b - a > 0 else "−"})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- day-block permutation


@dataclass
class DayBlocks:
    """Calendar-day blocks of the usable events, grouped by block size (permutation units)."""

    groups: dict[int, np.ndarray]            # size -> (n_days_of_that_size, size) event positions (time ordered inside a day)
    n_days: int
    day_of: np.ndarray                       # event position -> day code


def day_blocks(t0: np.ndarray) -> DayBlocks:
    days = t0.astype("datetime64[D]")
    _, code = np.unique(days, return_inverse=True)
    order = np.lexsort((np.arange(code.size), code))                  # stable: events of a day stay time ordered
    sorted_code = code[order]
    starts = np.flatnonzero(np.r_[True, sorted_code[1:] != sorted_code[:-1]])
    ends = np.r_[starts[1:], code.size]
    by_size: dict[int, list[np.ndarray]] = {}
    for s, e in zip(starts, ends, strict=True):
        by_size.setdefault(int(e - s), []).append(order[s:e])
    return DayBlocks({k: np.stack(v) for k, v in by_size.items()}, int(starts.size), code)


def permute_day_blocks(y: np.ndarray, blocks: DayBlocks, rng: np.random.Generator) -> np.ndarray:
    """Permute the labels in whole calendar-day blocks: a day's label vector moves, intact, to another day with the SAME
    number of events (so the block structure and the overall label counts are preserved exactly)."""
    out = y.copy()
    for g in blocks.groups.values():
        out[g] = y[g[rng.permutation(g.shape[0])]]
    return out


def permutation_null(folds: list[FoldData], y: np.ndarray, blocks: DayBlocks, *, n_perm: int = N_PERM, seed: int = wm.SEED,
                     reps: tuple[str, ...] = REPS, c: float = LR_C) -> dict[str, np.ndarray]:
    """Null of the pooled dAUC (per representation) and of AUC(A): labels permuted by day blocks, all models refitted."""
    null = {r: np.zeros(n_perm) for r in reps}
    null["auc_a"] = np.zeros(n_perm)
    t = time.perf_counter()
    for i in range(n_perm):
        rng = np.random.default_rng([seed, i])
        yp = permute_day_blocks(y, blocks, rng)
        oof = oof_predictions(folds, yp, reps, c=c)
        a = auc(yp, oof["A"])
        null["auc_a"][i] = a
        for r in reps:
            null[r][i] = auc(yp, oof[r]) - a
        if (i + 1) % 100 == 0:
            logger.info("permutation %d/%d (%.0f s)", i + 1, n_perm, time.perf_counter() - t)
    return null


def perm_p(observed: float, null: np.ndarray) -> float:
    return float((1 + np.sum(null >= observed)) / (null.size + 1))


# --------------------------------------------------------------------------- bootstrap, calibration


def bootstrap_delta(y: np.ndarray, oof: dict[str, np.ndarray], rep: str, blocks: DayBlocks, *, n_boot: int = N_BOOT,
                    seed: int = wm.SEED) -> tuple[float, float, np.ndarray]:
    """Day-block bootstrap (resample calendar days with replacement) of the pooled dAUC; percentile 95% CI."""
    day_idx = [np.flatnonzero(blocks.day_of == d) for d in range(blocks.n_days)]
    rng = np.random.default_rng([seed, 999_001])
    vals = []
    for _ in range(n_boot):
        pick = rng.integers(0, blocks.n_days, blocks.n_days)
        idx = np.concatenate([day_idx[d] for d in pick])
        yb = y[idx]
        if yb.min() == yb.max():
            continue
        vals.append(auc(yb, oof[rep][idx]) - auc(yb, oof["A"][idx]))
    arr = np.array(vals)
    return float(np.quantile(arr, 0.025)), float(np.quantile(arr, 0.975)), arr


def calibration(y: np.ndarray, p: np.ndarray, n_bins: int = 5) -> tuple[pd.DataFrame, dict[str, float]]:
    """Quantile-binned reliability table + Brier, log-loss, calibration slope / intercept (logit scale)."""
    from sklearn.linear_model import LogisticRegression

    pc = np.clip(p, 1e-6, 1 - 1e-6)
    order = np.argsort(pc, kind="stable")
    rows = []
    for b, idx in enumerate(np.array_split(order, n_bins), start=1):
        rows.append({"bin": b, "n": int(idx.size), "mean_pred": float(pc[idx].mean()), "obs_rate": float(y[idx].mean())})
    lg = np.log(pc / (1 - pc))[:, None]
    cal = LogisticRegression(C=1e6, max_iter=2000).fit(lg, y)
    stats = {"brier": float(np.mean((pc - y) ** 2)), "logloss": wm.logloss(pc, y), "slope": float(cal.coef_[0, 0]),
             "intercept": float(cal.intercept_[0]), "mean_pred": float(pc.mean()), "obs_rate": float(y.mean())}
    return pd.DataFrame(rows), stats


def fold_prior_logloss(folds: list[FoldData], y: np.ndarray) -> float:
    """Pooled OOF log-loss of the constant (fold-train prior) predictor."""
    p = np.full(max(int(f.test_idx.max()) for f in folds) + 1, np.nan)
    for f in folds:
        p[f.test_idx] = y[f.train_idx].mean()
    return wm.logloss(p, y)


# --------------------------------------------------------------------------- go / no-go (5200 v2, in order)


@dataclass
class Criterion:
    no: int
    name: str
    rule: str
    value: str
    passed: bool | None            # None = not run


def evaluate_gate(counts: dict[str, Any], n_train: int, delta: float, p: float, blocks_pos: int, n_blocks: int, *,
                  oos_unique_t0: int = OOS_UNIQUE_T0) -> tuple[list[Criterion], str]:
    """Apply the 5200 v2 criteria in order; the verdict is NO-GO at the first failure, else conditional GO / GO."""
    min_ok = bool(n_train >= 500 and counts["block_ok"] and counts["pooled_ok"])
    crit = [
        Criterion(1, "Minimum eseményszám", "train ≥ 500; blokkonként ≥ 40; összevontan ≥ 100 impulzus és ≥ 100 nem-impulzus",
                  f"train {n_train}; min. blokk {counts['min_block_events']}; impulzus {counts['pooled_impulse']}, nem-impulzus {counts['pooled_non_impulse']}", min_ok),
        Criterion(2, "ΔAUC (CV, összevont OOF)", f"≥ {DELTA_MIN:.2f}", f"{delta:+.4f}", bool(delta >= DELTA_MIN)),
        Criterion(3, "Permutációs p (nap-blokkos, 1 cella → Holm = nyers p)", f"< {P_MAX:.2f}", f"{p:.4f}", bool(p < P_MAX)),
        Criterion(4, "Blokk-előjel", f"ΔAUC > 0 legalább {BLOCKS_MIN} / {n_blocks} blokkban", f"{blocks_pos} / {n_blocks}", bool(blocks_pos >= BLOCKS_MIN)),
    ]
    oos_runs = oos_unique_t0 >= OOS_MIN_T0
    crit.append(Criterion(5, "OOS megerősítő előjel", f"csak ha ≥ {OOS_MIN_T0} egyedi t0 OOS esemény",
                          f"egyedi OOS t0 = {oos_unique_t0} → " + ("lefutna (nem implementált itt)" if oos_runs else "NEM fut le"), None))
    failed = [c for c in crit[:4] if not c.passed]
    if failed:
        return crit, "NO-GO"
    return crit, ("GO (OOS-ellenőrzés szükséges)" if oos_runs else "feltételes GO")


# --------------------------------------------------------------------------- full analysis (pure, testable)


@dataclass
class GateResult:
    y: np.ndarray
    t0: np.ndarray
    folds: list[FoldData]
    oof: dict[str, np.ndarray]
    summary: pd.DataFrame                    # per representation: auc_a, auc_b, delta, ci, p, blocks_pos
    blocks: dict[str, pd.DataFrame]
    null: dict[str, np.ndarray]
    boot: dict[str, np.ndarray]
    sens: pd.DataFrame
    cal: dict[str, tuple[pd.DataFrame, dict[str, float]]]
    prior_ll: float
    auc_a_p: float
    criteria: list[Criterion]
    verdict: str
    counts: dict[str, Any]
    timings: dict[str, float] = field(default_factory=dict)


def analyse(ev: pl.DataFrame, folds: list[FoldData], bounds: list[datetime], *, n_perm: int = N_PERM, n_boot: int = N_BOOT,
            seed: int = wm.SEED, with_sensitivity: bool = True, reps: tuple[str, ...] = REPS) -> GateResult:
    """Everything label dependent: OOF predictions, dAUC, block signs, permutation p, bootstrap CI, calibration, sensitivity."""
    use = usable_events(ev)
    y = use["label_impulse"].to_numpy().astype(float)
    t0 = _t0(use)
    blocks = day_blocks(t0)
    tm: dict[str, float] = {}
    t = time.perf_counter()
    oof = oof_predictions(folds, y, reps)
    tm["oof"] = time.perf_counter() - t
    t = time.perf_counter()
    null = permutation_null(folds, y, blocks, n_perm=n_perm, seed=seed, reps=reps)
    tm["permutation"] = time.perf_counter() - t
    rows, blk, boot = [], {}, {}
    for r in reps:
        a, b, d = pooled_delta(y, oof, r)
        lo, hi, arr = bootstrap_delta(y, oof, r, blocks, n_boot=n_boot, seed=seed)
        bt = block_table(folds, y, oof, r)
        blk[r], boot[r] = bt, arr
        rows.append({"rep": r, "auc_a": a, "auc_b": b, "delta": d, "ci_lo": lo, "ci_hi": hi, "p": perm_p(d, null[r]),
                     "blocks_pos": int((bt["delta_auc"] > 0).sum()), "null_mean": float(null[r].mean()),
                     "null_q99": float(np.quantile(null[r], 0.99))})
    summ = pd.DataFrame(rows).set_index("rep")
    t = time.perf_counter()
    sens_rows: list[dict[str, Any]] = []
    if with_sensitivity:
        for c in LR_C_SENS:
            o = oof_predictions(folds, y, reps, c=c)
            for r in reps:
                a, b, d = pooled_delta(y, o, r)
                bt = block_table(folds, y, o, r)
                sens_rows.append({"learner": f"logisztikus, C = {c:g}", "rep": r, "auc_a": a, "auc_b": b, "delta": d,
                                  "blocks_pos": int((bt["delta_auc"] > 0).sum())})
        og = oof_predictions(folds, y, reps, learner="gbm", seed=seed)
        for r in reps:
            a, b, d = pooled_delta(y, og, r)
            bt = block_table(folds, y, og, r)
            sens_rows.append({"learner": "gradiens-boosting (csak pont-becslés)", "rep": r, "auc_a": a, "auc_b": b, "delta": d,
                              "blocks_pos": int((bt["delta_auc"] > 0).sum())})
    tm["sensitivity"] = time.perf_counter() - t
    cal = {"A": calibration(y, oof["A"]), **{r: calibration(y, oof[r]) for r in reps}}
    _, counts = we.cv_summary(ev, bounds)
    prim = summ.loc[PRIMARY]
    crit, verdict = evaluate_gate(counts, int(ev.filter(pl.col("split") == "train").height), float(prim["delta"]), float(prim["p"]),
                                  int(prim["blocks_pos"]), len(folds))
    return GateResult(y, t0, folds, oof, summ, blk, null, boot, pd.DataFrame(sens_rows), cal, fold_prior_logloss(folds, y),
                      perm_p(float(summ.loc[PRIMARY, "auc_a"]), null["auc_a"]), crit, verdict, counts, tm)


# --------------------------------------------------------------------------- figures


def _null_fig(res: GateResult) -> go.Figure:
    r = PRIMARY
    fig = go.Figure(go.Histogram(x=res.null[r], nbinsx=50, marker={"color": theme.NEUTRAL}, name="permutációs null",
                                 hovertemplate="ΔAUC %{x:.3f}: %{y}<extra></extra>"))
    obs = float(res.summary.loc[r, "delta"])
    fig.add_vline(x=obs, line={"color": theme.ACCENT, "width": 2}, annotation_text=f"megfigyelt {obs:+.4f}", annotation_position="top right")
    fig.add_vline(x=DELTA_MIN, line={"color": theme.LONG, "width": 1, "dash": "dash"}, annotation_text="küszöb +0,02", annotation_position="top left")
    fig.update_xaxes(title_text="összevont OOF ΔAUC (B − A) a permutált címkéken")
    fig.update_yaxes(title_text="permutációk száma")
    return themed(fig, height=340, showlegend=False)


def _block_fig(res: GateResult) -> go.Figure:
    bt = res.blocks[PRIMARY]
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Blokkonkénti ΔAUC (B − A)", "Blokkonkénti AUC"), horizontal_spacing=0.1)
    cols = [theme.LONG if v > 0 else theme.SHORT for v in bt["delta_auc"]]
    fig.add_trace(go.Bar(x=[f"B{b}" for b in bt["block"]], y=bt["delta_auc"], marker={"color": cols}, showlegend=False,
                         hovertemplate="%{x}: %{y:+.4f}<extra></extra>"), row=1, col=1)
    fig.add_hline(y=0, line={"color": theme.NEUTRAL, "width": 1}, row=1, col=1)
    fig.add_trace(go.Bar(x=[f"B{b}" for b in bt["block"]], y=bt["auc_a"], name="Modell A (S0 + C)", marker={"color": theme.BLUE}), row=1, col=2)
    fig.add_trace(go.Bar(x=[f"B{b}" for b in bt["block"]], y=bt["auc_b"], name="Modell B (+ AE)", marker={"color": theme.ORANGE}), row=1, col=2)
    fig.add_hline(y=0.5, line={"color": theme.NEUTRAL, "width": 1, "dash": "dash"}, row=1, col=2)
    fig.update_layout(barmode="group")
    fig.update_yaxes(title_text="ΔAUC", col=1)
    fig.update_yaxes(title_text="AUC", col=2, range=[0.3, 0.85])
    fig.update_annotations(font_size=11)
    return themed(fig, height=340, hovermode="closest")


def _cal_fig(res: GateResult) -> go.Figure:
    fig = go.Figure()
    top = 0.0
    for key, name, color in (("A", "Modell A (S0 + C)", theme.BLUE), (PRIMARY, "Modell B (+ AE)", theme.ORANGE)):
        tb = res.cal[key][0]
        top = max(top, float(tb["mean_pred"].max()), float(tb["obs_rate"].max()))
        fig.add_trace(go.Scatter(x=tb["mean_pred"], y=tb["obs_rate"], mode="lines+markers", name=name, line={"color": color},
                                 hovertemplate="becsült %{x:.1%} → megfigyelt %{y:.1%}<extra>" + name + "</extra>"))
    top = min(1.0, top * 1.15 + 0.02)
    fig.add_trace(go.Scatter(x=[0, top], y=[0, top], mode="lines", name="tökéletes kalibráció",
                             line={"color": theme.NEUTRAL, "dash": "dash", "width": 1}, hoverinfo="skip"))
    fig.update_xaxes(title_text="átlagos becsült P(impulzus) (kvantilis-bin, OOF)", tickformat=".0%")
    fig.update_yaxes(title_text="megfigyelt impulzus-arány", tickformat=".0%")
    return themed(fig, height=340)


def _sens_fig(res: GateResult) -> go.Figure:
    fig = go.Figure()
    s = res.summary
    fig.add_trace(go.Bar(x=[REP_HU[r] for r in s.index], y=s["delta"], error_y={"type": "data", "symmetric": False,
                         "array": s["ci_hi"] - s["delta"], "arrayminus": s["delta"] - s["ci_lo"]},
                         marker={"color": [theme.ORANGE if r == PRIMARY else theme.NEUTRAL for r in s.index]},
                         hovertemplate="%{x}: %{y:+.4f}<extra></extra>"))
    fig.add_hline(y=0, line={"color": theme.NEUTRAL, "width": 1})
    fig.add_hline(y=DELTA_MIN, line={"color": theme.LONG, "width": 1, "dash": "dash"}, annotation_text="küszöb +0,02")
    fig.update_yaxes(title_text="összevont OOF ΔAUC, 95% nap-blokk bootstrap CI")
    return themed(fig, height=340, showlegend=False)


# --------------------------------------------------------------------------- build


def _fmt(x: float, d: int = 4) -> str:
    return f"{x:+.{d}f}"


def build(root: Path | None = None, *, df_1m: pl.DataFrame | None = None, events: pl.DataFrame | None = None,
          params: we.WaveParams | None = None, cell: str = wm.CELL, cfg: wm.AEConfig | None = None, seed: int = wm.SEED,
          n_perm: int = N_PERM, n_boot: int = N_BOOT, cache_dir: Path | None = None, asset_id: str | None = None) -> Path | None:
    """Fit per-fold embeddings on train only, run the 5200 v2 go / no-go and write the ``wave_predictiveness_gate`` report."""
    t_start = time.perf_counter()
    p = params or we.params_v2()
    oos0 = datetime.fromisoformat(p.oos_start)
    cfg = cfg or wm.AEConfig()
    _, w_min, h_min = next(c for c in p.cells if c[0] == cell)
    if events is None:
        events = we.load_events(asset_id, split="train", version="v2")       # SQL-filtered: no OOS / holdout-2 row is loaded
    ev = events.filter((pl.col("cell_id") == cell) & (pl.col("split") == "train")).sort("t0")
    if df_1m is None:
        df_1m = load_ohlcv_1m(asset_id)
        if df_1m is None:
            logger.error("live db unavailable")
            return None
    df_1m = df_1m.filter(pl.col("open_time") < oos0)                          # nothing from the OOS period enters memory
    m = we.to_minute_grid(df_1m)
    b15 = we.to_bars15(m, p)
    pre = wm.build_prefixes(b15, ev)
    bounds = we.cv_block_bounds(p)

    t = time.perf_counter()
    key = _cache_key(ev, cfg, seed, h_min)
    cpath = (cache_dir / f"{SLUG}_{cell}_folds.npz") if cache_dir else None
    folds = load_folds(cpath, key) if cpath else None
    cached = folds is not None
    if folds is None:
        folds = prepare_folds(ev, pre, bounds, h_min, cfg=cfg, seed=seed)
        if cpath:
            save_folds(folds, cpath, key, time.perf_counter() - t)
    t_prep = time.perf_counter() - t
    fit_s = cached_fit_seconds(cpath) if (cached and cpath) else t_prep
    logger.info("fold embeddings %s (%.0f s)", "loaded from cache" if cached else "fitted", t_prep)

    res = analyse(ev, folds, bounds, n_perm=n_perm, n_boot=n_boot, seed=seed)
    res.timings["fold_embeddings"] = fit_s
    res.timings["total"] = time.perf_counter() - t_start
    return _write_report(res, ev, folds, root, cell=cell, w_min=w_min, h_min=h_min, p=p, oos0=oos0, n_perm=n_perm, n_boot=n_boot,
                         cfg=cfg, seed=seed, cached=cached)


def _write_report(res: GateResult, ev: pl.DataFrame, folds: list[FoldData], root: Path | None, *, cell: str, w_min: int, h_min: int,
                  p: we.WaveParams, oos0: datetime, n_perm: int, n_boot: int, cfg: wm.AEConfig, seed: int, cached: bool) -> Path:
    s = res.summary
    prim = s.loc[PRIMARY]
    y = res.y
    n = int(y.size)
    n_pos = int(y.sum())
    prior = n_pos / n
    a_cal, a_stats = res.cal["A"]
    b_cal, b_stats = res.cal[PRIMARY]
    first_fail = next((c for c in res.criteria[:4] if not c.passed), None)
    rb = ReportBuilder(
        scope="general", slug=SLUG, order=32, root=None if root is None else root,
        title=f"Hullámkezdet előrejelző-erő kapu — kép vs skalár alap, purged CV ({cell}, train)",
        summary=f"5200 v2 / t920: a {cell} cella Modell B (S0 + C + AE-kép) vs Modell A (S0 + C), összevont OOF ΔAUC a 6 blokkos purged CV-n, "
                f"nap-blokkos permutációs teszt (N = {n_perm}), blokk-előjel, érzékenységi vizsgálatok. Döntés: {res.verdict}.",
        tags=["wave", "epic_072", "go-no-go", "permutation", "v2"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél és döntés\n\n"
        "**t920 (epic_072):** a `_doc_/methodology_doc/5200_wave_start_image_model.md` (v2) go/no-go szabálya szerint megmérjük, hogy a "
        "prefix-kép (AE-beágyazás) inkrementális előrejelző erőt ad-e a skalár alap (S0 + C) fölött az impulzus-címkére (bináris fő teszt). "
        f"Cella: `{cell}` (W = {w_min // 60}h, H = {h_min // 60}h), **{ev.height} train esemény**, ebből **{n} vesz részt a CV-ben** "
        f"(nem lóg át blokkhatáron), {n_pos} impulzus ({100 * prior:.1f}%).\n\n"
        f"> **Cella-döntés: {res.verdict}.**"
        + (f" Az első nem teljesülő feltétel: **{first_fail.no}. {first_fail.name}** ({first_fail.value}; szabály: {first_fail.rule})." if first_fail else
           " A 1–4. feltétel teljesül; az OOS megerősítő előjel-ellenőrzés nem futhat le (< 100 egyedi OOS t0), ezért legfeljebb feltételes GO.")
        + "\n\n> ⚠️ **OOS-tisztaság:** az elemzés kizárólag a train eseményeit használja (`load_events(split=\"train\")`, az 1m árfolyam a train végén levágva); "
        "az OOS és a holdout-2 sorok nem kerültek a memóriába. Az OOS-előjel nem lett lefuttatva (egyedi OOS t0 = 75 < 100, t925).")
    rb.add_markdown(
        "## Előre rögzített terv (az eredmények megtekintése előtt)\n\n"
        "- **Modell A** = `S0 + C` (a_sigma, velocity_1h, pretrend_3d, log_sigma_1m, drop_depth, volume_ratio; fold-train átlaggal / szórással standardizálva). "
        "**Modell B** = A + reprezentáció. Azonos tanuló: L2-regularizált logisztikus regresszió, **rögzített erősség C = "
        f"{LR_C:g}** (belső hangolás nincs: címke-függő lenne, és minden permutációban újra kellene futnia).\n"
        "- **Elsődleges (döntési) teszt: AE latens embedding** (16 dim). Az 5200 nem nevez meg reprezentációt, ezért az alapértelmezés az AE-latens, "
        "ez rögzítve lett az eredmények megtekintése előtt. **Érzékenység (nem döntési, nincs cherry-pick):** klaszter one-hot k = 10 (t919 választása), "
        "klaszter one-hot k = 5 (legstabilabb partíció), C = 0,01 / 1, gradiens-boosting (csak pont-becslés).\n"
        "- **Statisztika:** a 6 blokkos purged + embargós (`H`) CV összevont out-of-fold `ΔAUC = AUC(B) − AUC(A)`; az AE, a k-means és a kép-tengely "
        "**foldonként, csak a fold-train eseményein** illesztett (`wave_image_model.fit_fold`); a végső modell 17,16 ATR-es konstansa nem használt.\n"
        f"- **p-érték:** naptári nap-blokkos permutáció, **N = {n_perm}**, `p = (1 + #{{null ≥ megfigyelt}}) / (N + 1)`; 1 cella → Holm = nyers p.\n"
        "- **Feltételek sorrendben:** (1) minimum eseményszám, (2) ΔAUC ≥ 0,02, (3) p < 0,01, (4) ΔAUC > 0 legalább 4 / 6 blokkban, "
        "(5) OOS előjel (csak ≥ 100 egyedi t0; itt 75 → nem fut, a legjobb kimenet: feltételes GO).\n"
        "- **Permutáció — pontos újraillesztés:** az AE és a k-means felügyelet nélküli, a per-fold beágyazások nem függnek a címkéktől, ezért egyszer "
        "készülnek el (és gyorsítótárba kerülnek). Minden permutációban **csak a címkék** permutálódnak (egy nap címke-vektora érintetlenül egy másik, "
        "azonos esemény-számú napra kerül, így a blokk-szerkezet és a címke-összeg pontosan megmarad), és **minden címke-függő komponens** — "
        "A és minden B modell fold-train illesztése — újra tanul ugyanazokon a foldokon. Ez a teljes CV-pipeline egzakt újrafuttatása.")
    crit = pd.DataFrame([{"no": c.no, "name": c.name, "rule": c.rule, "value": c.value,
                          "status": "nem fut" if c.passed is None else ("teljesül" if c.passed else "NEM teljesül")} for c in res.criteria])
    rb.add_table("criteria", crit, title="Go/no-go feltételek sorrendben (elsődleges: AE embedding)",
                 columns={"no": "#", "name": "Feltétel", "rule": "Szabály", "value": "Érték", "status": "Állapot"})

    # 1. headline numbers
    rb.add_markdown(
        "## 1. Fő eredmény: összevont OOF ΔAUC\n\n"
        f"Az elsődleges (AE) Modell B összevont OOF AUC-ja **{prim['auc_b']:.4f}**, a Modell A-é **{prim['auc_a']:.4f}**, így "
        f"**ΔAUC = {_fmt(prim['delta'])}** (95% nap-blokk bootstrap CI [{_fmt(prim['ci_lo'])}; {_fmt(prim['ci_hi'])}]). A permutációs null átlaga "
        f"{_fmt(prim['null_mean'])}, 99. percentilise {_fmt(prim['null_q99'])}; **p = {prim['p']:.4f}** (N = {n_perm}, a legkisebb elérhető érték "
        f"{1 / (n_perm + 1):.4f}). Blokk-előjel: **{int(prim['blocks_pos'])} / {len(folds)}** blokkban pozitív a ΔAUC.")
    rb.add_chart("null", _null_fig(res), title="Nap-blokkos permutációs null (elsődleges, AE) a megfigyelt ΔAUC-val", height=340)
    summ_t = s.reset_index().assign(rep=lambda d: d["rep"].map(REP_HU))
    rb.add_table("summary", summ_t[["rep", "auc_a", "auc_b", "delta", "ci_lo", "ci_hi", "p", "blocks_pos"]],
                 title="Reprezentációk: összevont OOF AUC, ΔAUC, CI, p (csak az első sor döntési)",
                 columns={"rep": "Reprezentáció (Modell B)", "auc_a": {"label": "AUC A", "digits": 4}, "auc_b": {"label": "AUC B", "digits": 4},
                          "delta": {"label": "ΔAUC", "digits": 4}, "ci_lo": {"label": "CI alsó", "digits": 4}, "ci_hi": {"label": "CI felső", "digits": 4},
                          "p": {"label": "p (perm.)", "digits": 4}, "blocks_pos": "Pozitív blokk (6-ból)"})

    # 2. blocks
    bt = res.blocks[PRIMARY]
    rb.add_markdown(
        "## 2. Blokkonkénti stabilitás\n\n"
        "Minden blokk egyszer teszt; a modellek a többi blokk (purge + embargó után) eseményein tanulnak. A blokkonkénti ΔAUC kis mintán "
        f"({int(bt['n_test'].min())}–{int(bt['n_test'].max())} esemény, {int(bt['n_pos'].min())}–{int(bt['n_pos'].max())} impulzus) zajos; a 4. feltétel azt védi, hogy egyetlen időszak ne hordozza az eredményt.")
    rb.add_chart("blocks", _block_fig(res), title="Blokkonkénti ΔAUC és AUC (elsődleges, AE)", height=340)
    bt2 = bt.copy()
    fd = pd.DataFrame([{"block": f.block + 1, "n_purged": f.n_purged, "n_embargoed": f.n_embargoed, "n_straddle": f.n_straddle} for f in folds])
    bt2 = bt2.merge(fd, on="block")
    rb.add_table("blocks_table", bt2, title="Blokk-táblázat (elsődleges, AE)",
                 columns={"block": "Teszt-blokk", "n_test": "Teszt-esemény", "n_pos": "Impulzus", "n_train": "Fold-train",
                          "auc_a": {"label": "AUC A", "digits": 4}, "auc_b": {"label": "AUC B", "digits": 4},
                          "delta_auc": {"label": "ΔAUC", "digits": 4}, "sign": "Előjel", "n_purged": "Purge-olt", "n_embargoed": "Embargó-zott",
                          "n_straddle": "Blokkhatáron átlógó"})

    # 3. skalár alap
    rb.add_markdown(
        "## 3. A skalár alap (Modell A) erőssége\n\n"
        f"A Modell A önmagában **AUC = {prim['auc_a']:.4f}** (összevont OOF; 0,5 = véletlen); a nap-blokkos permutációs null szerint p = {res.auc_a_p:.4f}. "
        f"Log-loss: A **{a_stats['logloss']:.4f}** vs konstans (fold-train prior) **{res.prior_ll:.4f}** (Δ = {_fmt(a_stats['logloss'] - res.prior_ll)}); "
        f"Brier A {a_stats['brier']:.4f}. Az S0 + C tehát "
        + ("érdemben előrejelez az impulzus-arány fölött" if prim['auc_a'] >= 0.6 else "csak gyenge előrejelzést ad a prior fölött")
        + " — ez a mérce, amit a képnek meg kell haladnia. (A t919 szerint a klaszterek nagyrészt a rebound méretét, `A/σ_W`-t különítik el, "
        "ami az S0 része.)")

    # 4. calibration
    rb.add_markdown(
        "## 4. Kalibráció (összevont OOF)\n\n"
        f"Kvantilis-bin (5 bin) megbízhatósági tábla. Kalibrációs meredekség (a logit-becslésre illesztett logisztikus): A **{a_stats['slope']:.2f}**, "
        f"B **{b_stats['slope']:.2f}** (1 = tökéletes, < 1 = túl magabiztos); átlagos becslés vs megfigyelt arány: A {a_stats['mean_pred']:.3f} / {a_stats['obs_rate']:.3f}, "
        f"B {b_stats['mean_pred']:.3f} / {b_stats['obs_rate']:.3f}. Log-loss: A {a_stats['logloss']:.4f}, B {b_stats['logloss']:.4f}; Brier: A {a_stats['brier']:.4f}, B {b_stats['brier']:.4f}.")
    rb.add_chart("calibration", _cal_fig(res), title="Megbízhatósági görbe: Modell A vs Modell B (elsődleges, AE)", height=340)
    ct = pd.concat([a_cal.assign(model="A (S0 + C)"), b_cal.assign(model="B (+ AE)")])
    rb.add_table("calibration_table", ct[["model", "bin", "n", "mean_pred", "obs_rate"]], title="Kalibrációs bin-ek",
                 columns={"model": "Modell", "bin": "Bin", "n": "Esemény", "mean_pred": {"label": "Átl. becslés", "type": "pct", "digits": 1},
                          "obs_rate": {"label": "Megfigyelt arány", "type": "pct", "digits": 1}})

    # 5. sensitivity
    rb.add_markdown(
        "## 5. Érzékenységi vizsgálatok (nem döntési)\n\n"
        "Az alábbiak a döntést **nem** befolyásolják; azt mutatják, hogy az elsődleges eredmény függ-e a reprezentációtól vagy a tanulótól. "
        "A klaszter-reprezentációk permutációs p-értékei is a táblában vannak (ugyanaz a N = "
        f"{n_perm}), de a döntés kizárólag az AE-teszten alapul; a három teszt között nincs szelekció.")
    rb.add_chart("sensitivity", _sens_fig(res), title="ΔAUC reprezentációnként (C = 0,1) bootstrap CI-vel", height=340)
    sd = res.sens.copy()
    if not sd.empty:
        sd["rep"] = sd["rep"].map(REP_HU)
        rb.add_table("sensitivity_table", sd, title="Tanuló- és reprezentáció-érzékenység (pont-becslés)",
                     columns={"learner": "Tanuló", "rep": "Reprezentáció", "auc_a": {"label": "AUC A", "digits": 4}, "auc_b": {"label": "AUC B", "digits": 4},
                              "delta": {"label": "ΔAUC", "digits": 4}, "blocks_pos": "Pozitív blokk"})
    tm = res.timings
    rb.add_table("runtime", pd.DataFrame([
        ("Teljes futás", tm.get("total", float("nan"))),
        (f"Per-fold AE + k-means illesztés (egyszeri, {'a gyorsítótárba mentett eredeti idő' if cached else 'most illesztve'})", tm.get("fold_embeddings", float("nan"))),
        (f"Permutációs null (N = {n_perm}, teljes címke-függő refit)", tm.get("permutation", float("nan"))),
        ("Érzékenységi futások", tm.get("sensitivity", float("nan"))),
    ], columns=["step", "seconds"]), title="Futási idő (CPU, 1 szál)", columns={"step": "Lépés", "seconds": {"label": "mp", "digits": 1}})

    # interpretation
    sec = [r for r in REPS if r != PRIMARY]
    sec_txt = "; ".join(f"{REP_HU[r]}: ΔAUC {_fmt(s.loc[r, 'delta'])}, p = {s.loc[r, 'p']:.3f}, {int(s.loc[r, 'blocks_pos'])}/6 blokk" for r in sec)
    gb = res.sens[res.sens["learner"].str.startswith("gradiens") & (res.sens["rep"] == PRIMARY)]
    gb_txt = f"A gradiens-boosting érzékenység (AE): ΔAUC {_fmt(float(gb.iloc[0]['delta']))}." if len(gb) else ""
    null_note = (f"A permutációs null szélesebb és pozitív irányba eltolt (átlag {_fmt(prim['null_mean'])}, 99. percentilis {_fmt(prim['null_q99'])}): "
                 "a többlet-jellemzők zaj-illesztése is hoz pozitív összevont ΔAUC-ot, a megfigyelt értéknek ehhez képest kell kiemelkednie.")
    big = bt.loc[bt["delta_auc"].idxmax()]
    n_tiny = int(((bt["delta_auc"] > 0) & (bt["delta_auc"] <= 0.005)).sum())
    block_note = "" if n_tiny < 2 else (f"**Blokk-szerkezet:** a ΔAUC a B{int(big['block'])} blokkban a legnagyobb ({_fmt(float(big['delta_auc']))}), a pozitív blokkok közül "
                  f"{n_tiny} gyakorlatilag nulla (≤ 0,005); a {int(prim['blocks_pos'])}/6-os előjel-szám tehát gyengébb bizonyíték, mint amilyennek látszik.")
    if prim["delta"] >= DELTA_MIN and prim["p"] >= P_MAX:
        meaning = (f"a ΔAUC pont-becslése ({_fmt(prim['delta'])}) eléri a 0,02-es küszöböt, de a permutációs p ({prim['p']:.3f}) "
                   f"a {P_MAX:g} felett van" + (" és a bootstrap CI a nullát is tartalmazza" if prim["ci_lo"] <= 0 else "")
                   + f", így az eredmény nem különböztethető meg a zajtól. {null_note}")
    elif prim["delta"] < DELTA_MIN:
        meaning = f"a ΔAUC ({_fmt(prim['delta'])}) a 0,02-es küszöb alatt marad. {null_note}"
    else:
        meaning = f"a ΔAUC ({_fmt(prim['delta'])}) és a p ({prim['p']:.4f}) a küszöböket teljesíti. {null_note}"
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"**Cella-döntés ({cell}): {res.verdict}.** Elsődleges, előre rögzített teszt (AE latens, logisztikus C = {LR_C:g}): ΔAUC = {_fmt(prim['delta'])} "
        f"[{_fmt(prim['ci_lo'])}; {_fmt(prim['ci_hi'])}], p = {prim['p']:.4f}, pozitív blokkok {int(prim['blocks_pos'])}/6; "
        f"AUC A = {prim['auc_a']:.4f}, AUC B = {prim['auc_b']:.4f}.\n\n"
        + ("**A kép nem ad kimutatható inkrementális információt a skalár alap fölött** ezen a cellán: "
           if res.verdict == "NO-GO" else "**A kép inkrementális többlete a feltételes GO feltételeit teljesíti, de OOS-megerősítés nélkül:** ")
        + (f"az első nem teljesülő feltétel a {first_fail.no}. ({first_fail.name}: {first_fail.value})." if first_fail else
           "a CV-feltételek 1–4 teljesülnek; az OOS-előjel a kevés egyedi OOS-esemény miatt nem ellenőrizhető, a holdout-2 védett marad.")
        + f"\n\n**Érzékenység (nem döntési):** {sec_txt}. {gb_txt}\n\n"
        f"**Mit jelent ez:** {meaning} "
        f"{block_note} Érvényes NO-GO nem hiba (5200: erős skalár alap kockázat). "
        f"**Korlátok:** egyetlen cella, {n} CV-esemény és {n_pos} impulzus (blokkonként {int(bt['n_pos'].min())}–{int(bt['n_pos'].max())}) — a blokkonkénti ΔAUC zajos; "
        "a logisztikus tanuló rögzített erősséggel fut; az embargó-érzékenység (2H) nem futott; az AE egy seeddel illesztett. A holdout-2 és az OOS érintetlen maradt."
    )
    return rb.write()


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or SLUG).split("\n")[0])
    ap.add_argument("--asset-id", default=None)
    ap.add_argument("--epochs", type=int, default=wm.AEConfig().epochs)
    ap.add_argument("--n-perm", type=int, default=N_PERM)
    ap.add_argument("--root", default=None, help="report root (default: _doc_/reports)")
    ap.add_argument("--cache-dir", default=None, help="directory for the per-fold embedding cache")
    args = ap.parse_args()
    out = build(Path(args.root) if args.root else None, asset_id=args.asset_id, cfg=wm.AEConfig(epochs=args.epochs),
                n_perm=args.n_perm, cache_dir=Path(args.cache_dir) if args.cache_dir else None)
    print(out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
