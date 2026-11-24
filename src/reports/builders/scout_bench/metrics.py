"""Metrics of the Impulse Scout benchmark (5300 "Metrikák"): all on the pooled out-of-fold sample."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

EPS = 1e-6


def auc(y: np.ndarray, s: np.ndarray) -> float:
    """Rank-based ROC AUC (ties averaged); NaN when one class is missing."""
    y = np.asarray(y)
    pos = y > 0.5
    n1, n0 = int(pos.sum()), int((~pos).sum())
    if n1 == 0 or n0 == 0:
        return float("nan")
    r = pd.Series(s).rank(method="average").to_numpy()
    return float((r[pos].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def logloss_binary(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(p, EPS, 1 - EPS)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean())


def logloss_binary_each(y: np.ndarray, p: np.ndarray) -> np.ndarray:
    p = np.clip(p, EPS, 1 - EPS)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def logloss_3class(cls: np.ndarray, proba: np.ndarray) -> float:
    """Multiclass log-loss; ``cls`` = class index per event, ``proba`` (n, 3)."""
    p = np.clip(proba, EPS, 1.0)
    p = p / p.sum(axis=1, keepdims=True)
    return float(-np.log(p[np.arange(cls.size), cls]).mean())


def top_q(y: np.ndarray, score: np.ndarray, thr: np.ndarray | float, base_rate: float | None = None) -> dict[str, float]:
    """Precision / lift of the signals ``score >= thr`` (``thr`` scalar or per event, e.g. from the fold-train)."""
    sel = score >= thr
    n = int(sel.sum())
    base = float(y.mean()) if base_rate is None else base_rate
    prec = float(y[sel].mean()) if n else float("nan")
    return {"n": n, "precision": prec, "lift": prec / base if n and base > 0 else float("nan")}


def fold_rank_select(score: np.ndarray, groups: np.ndarray, q: float) -> np.ndarray:
    """Primary top-q view (epic_073 decision 2026-11-21): inside every group (= test fold / CV block) the
    ``ceil(q * n_g)`` highest scores are selected by rank; no label and no train-fold threshold is used, so every fold
    contributes the same share of signals. Ties are broken by the (time-ordered) row position, deterministically."""
    score = np.asarray(score, dtype=float)
    groups = np.asarray(groups)
    sel = np.zeros(score.size, dtype=bool)
    for g in pd.unique(groups):
        idx = np.flatnonzero(groups == g)
        k = int(np.ceil(q * idx.size - 1e-9))
        if k <= 0:
            continue
        order = np.lexsort((idx, -score[idx]))                         # score desc, then position asc
        sel[idx[order[:k]]] = True
    return sel


def top_q_mask(y: np.ndarray, sel: np.ndarray, base_rate: float | None = None) -> dict[str, float]:
    """Precision / lift of an explicit signal mask (e.g. :func:`fold_rank_select`)."""
    n = int(sel.sum())
    base = float(y.mean()) if base_rate is None else base_rate
    prec = float(y[sel].mean()) if n else float("nan")
    return {"n": n, "precision": prec, "lift": prec / base if n and base > 0 else float("nan")}


def calibration(y: np.ndarray, p: np.ndarray, n_bins: int = 5) -> tuple[pd.DataFrame, dict[str, float]]:
    """Quantile-binned reliability table + Brier, log-loss, calibration slope / intercept (logit scale)."""
    from sklearn.linear_model import LogisticRegression

    pc = np.clip(p, EPS, 1 - EPS)
    order = np.argsort(pc, kind="stable")
    rows = []
    for b, idx in enumerate(np.array_split(order, n_bins), start=1):
        rows.append({"bin": b, "n": int(idx.size), "mean_pred": float(pc[idx].mean()), "obs_rate": float(y[idx].mean())})
    stats: dict[str, float] = {"brier": float(np.mean((pc - y) ** 2)), "logloss": logloss_binary(y, pc),
                               "mean_pred": float(pc.mean()), "obs_rate": float(y.mean()),
                               "slope": float("nan"), "intercept": float("nan")}
    if 0 < y.sum() < y.size:
        lg = np.log(pc / (1 - pc))[:, None]
        cal = LogisticRegression(C=1e6, max_iter=2000).fit(lg, y)
        stats["slope"], stats["intercept"] = float(cal.coef_[0, 0]), float(cal.intercept_[0])
    return pd.DataFrame(rows), stats


def breakdown_auc(y: np.ndarray, s_model: np.ndarray, s_base: np.ndarray, groups: np.ndarray) -> pd.DataFrame:
    """Per-group (block / asset) AUC of the model and the baseline and their difference."""
    rows = []
    for g in pd.unique(groups):
        m = groups == g
        a_m, a_b = auc(y[m], s_model[m]), auc(y[m], s_base[m])
        rows.append({"group": g, "n": int(m.sum()), "n_impulse": int(y[m].sum()), "auc_model": a_m, "auc_base": a_b,
                     "delta_auc": a_m - a_b})
    return pd.DataFrame(rows)


def best_block_removed(y: np.ndarray, s_model: np.ndarray, s_base: np.ndarray, blocks: np.ndarray) -> dict[str, Any]:
    """5300 go-criterion 6: drop the block with the largest dAUC; report the mean per-block dAUC of the rest and the
    pooled dAUC without that block's events."""
    tab = breakdown_auc(y, s_model, s_base, blocks)
    d = tab["delta_auc"].to_numpy()
    if d.size < 2 or not np.isfinite(d).any():
        return {"best_block": None, "mean_wo_best": float("nan"), "pooled_wo_best": float("nan")}
    k = int(np.nanargmax(d))
    best = tab["group"].iloc[k]
    keep = blocks != best
    return {"best_block": best, "mean_wo_best": float(np.nanmean(np.delete(d, k))),
            "pooled_wo_best": auc(y[keep], s_model[keep]) - auc(y[keep], s_base[keep])}
