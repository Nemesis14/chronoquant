"""Nested, purged, time-ordered modelling of the candidate targets (epic_079 / t972).

Targets (pre-registered): ``big`` (leg reaches >= k sigma, judged *beyond* the analytic random-walk offset), ``hit1`` (+1R before the
structural stop), ``fr_h05`` / ``fr_h1`` / ``fr_h2`` (forward return after the confirmation in sigma_T, horizons 0.5T / 1T / 2T).

Protocol
--------
* Rows: clean candidates (no flash / gap / trivial), all scales T = 15 ... 960 pooled, **unweighted** (cluster weights are hindsight-
  contaminated, see ``make_data``); dependence between rows is handled by the day-cluster bootstrap and by per-scale reporting.  Outer folds = half-year test blocks 2023H1 ... 2025H2; a block trains on every candidate whose *label was already known*
  before the block started (purge on ``label_idx``).
* Preprocessing is fitted on the outer-train rows only: 0.5 / 99.5 % clipping, rank-gauss, correlation pruning (|rho| > 0.92, group order).
* Learners: M0 baseline (binary: recalibrated random-walk offset ``logit p_rw``; ``hit1`` / returns: weighted mean), M1 ridge (logit with
  offset / ridge), M2 shallow LightGBM (``init_score`` = baseline), M3 ExtraTrees on the baseline residual, M4 = average of the shifts of
  M1-M3.  Hyper-parameters and the feature count are chosen **only inside** the outer-train rows by an inner purged expanding CV
  (<= 3 half-year blocks) from a small fixed grid; if no config beats the baseline there, the learner falls back to the baseline.
* Metrics (day-cluster bootstrap): binary - log-loss gain over M0, AUC, top-decile lift; returns - Spearman IC, top-decile mean.

``uv run python -m reports.builders.cand_v2_model``
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from typing import Any

import lightgbm as lgb
import numpy as np
import polars as pl
from scipy.optimize import minimize
from scipy.special import expit, logit
from scipy.stats import rankdata
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import QuantileTransformer

from reports.builders import cand_v2_features as fe
from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an

logger = logging.getLogger(__name__)

TARGETS = ("big", "hit1", "fr_h05", "fr_h1", "fr_h2")
KIND = {"big": "bin", "hit1": "bin", "fr_h05": "reg", "fr_h1": "reg", "fr_h2": "reg"}
LEARNERS = ("M1", "M2", "M3")
BLOCKS = tuple((y, h) for y in (2023, 2024, 2025) for h in (1, 2))
PRUNE_RHO = 0.92
MIN_TRAIN = 2500
TABLE_METRICS = "cand_v2_metrics"
TABLE_OOF = "cand_v2_oof"
TABLE_SELECTED = "cand_v2_selected"
TABLE_SLICES = "cand_v2_slices"
GRID: dict[str, list[dict[str, Any]]] = {
    "M1": [{"lam": lam, "k": k} for lam in (300.0, 3000.0) for k in (20, 0)],
    "M2": [{"leaves": 4, "n": 80}, {"leaves": 4, "n": 250}, {"leaves": 8, "n": 150}],
    "M3": [{"leaf": 100}, {"leaf": 300}],
}


# --------------------------------------------------------------------------------------------------------------
# data
# --------------------------------------------------------------------------------------------------------------

@dataclass
class Data:
    names: list[str]
    x: np.ndarray                     # raw features, (n, p), NaN allowed
    w: np.ndarray
    off: np.ndarray                   # logit of the random-walk offset
    scale: np.ndarray
    day: np.ndarray                   # integer day id
    label_idx: np.ndarray
    conf_idx: np.ndarray
    ts: np.ndarray                    # conf_time as epoch minutes
    y: dict[str, np.ndarray]          # NaN = not available
    seg: dict[str, np.ndarray]
    m0: int


def make_data(df: pl.DataFrame) -> Data:
    d = df.filter((~pl.col("flash")) & (~pl.col("gap")) & (~pl.col("trivial")) & (pl.col("year") <= 2025))
    names = fe.feature_names(d)
    ts = d["conf_time"].dt.epoch("s").to_numpy() // 60
    m0 = int(ts[0] - d["conf_idx"].to_numpy()[0])
    w = np.ones(d.height)          # cluster weights are NOT used: 1 / cluster size is a hindsight quantity (big moves are seen at
                                   # more scales, so positives get smaller weights) and inflated the weighted AUC (0.606 vs 0.526 unweighted)
    y = {"big": d["y_big"].cast(pl.Float64).to_numpy(),
         "hit1": d["y_hit1"].cast(pl.Float64).fill_null(float("nan")).to_numpy()}
    for h in ("h05", "h1", "h2"):
        y[f"fr_{h}"] = d[f"fr_{h}"].to_numpy().astype(float)
    er = d["r_er10T"].to_numpy()
    scale = d["scale_min"].to_numpy()
    seg = {"all": np.ones(d.height, bool), "chain0": d["z_chain"].to_numpy() == 0, "chain1p": d["z_chain"].to_numpy() >= 1,
           "up30d": d["r_ret30d"].to_numpy() > 0, "down30d": d["r_ret30d"].to_numpy() <= 0}
    clean = np.zeros(d.height, bool)
    for s in np.unique(scale):
        m = scale == s
        clean[m] = er[m] >= np.nanmedian(er[m])
    seg["trendy"], seg["choppy"] = clean, ~clean
    return Data(names=names, x=d.select(names).to_numpy().astype(np.float64), w=w / w.mean(), off=logit(d["p_rw"].to_numpy()),
                scale=scale, day=ts // 1440, label_idx=d["label_idx"].to_numpy(), conf_idx=d["conf_idx"].to_numpy(), ts=ts, y=y, seg=seg, m0=m0)


def block_bounds(year: int, half: int) -> tuple[np.datetime64, np.datetime64]:
    a = np.datetime64(f"{year}-{'01' if half == 1 else '07'}-01", "m")
    b = np.datetime64(f"{year}-07-01" if half == 1 else f"{year + 1}-01-01", "m")
    return a, b


def block_idx(data: Data, year: int, half: int) -> tuple[int, int]:
    a, b = block_bounds(year, half)
    return int(a.astype("int64")) - data.m0, int(b.astype("int64")) - data.m0


# --------------------------------------------------------------------------------------------------------------
# preprocessing
# --------------------------------------------------------------------------------------------------------------

@dataclass
class Prep:
    keep: np.ndarray
    lo: np.ndarray
    hi: np.ndarray
    qt: QuantileTransformer

    def transform(self, x: np.ndarray) -> np.ndarray:
        z = np.clip(x, self.lo, self.hi)
        out = np.asarray(self.qt.transform(z), dtype=float)
        out[~np.isfinite(out)] = 0.0
        return out[:, self.keep]


def fit_prep(x: np.ndarray, rho: float = PRUNE_RHO) -> Prep:
    lo, hi = np.nanpercentile(x, 0.5, axis=0), np.nanpercentile(x, 99.5, axis=0)
    z = np.clip(x, lo, hi)
    qt = QuantileTransformer(n_quantiles=200, output_distribution="normal", subsample=200_000, random_state=0)
    t = qt.fit_transform(z)
    t[~np.isfinite(t)] = 0.0
    c = np.corrcoef(t, rowvar=False)
    c = np.nan_to_num(c)
    keep: list[int] = []
    for j in range(t.shape[1]):
        if t[:, j].std() < 1e-9:
            continue
        if not keep or np.max(np.abs(c[j, keep])) <= rho:
            keep.append(j)
    return Prep(np.array(keep), lo, hi, qt)


# --------------------------------------------------------------------------------------------------------------
# metrics
# --------------------------------------------------------------------------------------------------------------

def wauc(y: np.ndarray, p: np.ndarray, w: np.ndarray) -> float:
    o = np.argsort(p, kind="stable")
    y, w = y[o], w[o]
    wn = np.cumsum(w * (1 - y))
    tp, tn = float((w * y).sum()), float((w * (1 - y)).sum())
    if tp <= 0 or tn <= 0:
        return float("nan")
    return float((w * y * wn).sum() / (tp * tn))


def wspearman(a: np.ndarray, b: np.ndarray, w: np.ndarray) -> float:
    ra, rb = rankdata(a), rankdata(b)
    ma, mb = np.average(ra, weights=w), np.average(rb, weights=w)
    cov = np.average((ra - ma) * (rb - mb), weights=w)
    va, vb = np.average((ra - ma) ** 2, weights=w), np.average((rb - mb) ** 2, weights=w)
    return float(cov / np.sqrt(va * vb)) if va > 0 and vb > 0 else float("nan")


def logloss_vec(y: np.ndarray, p: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def top_lift(y: np.ndarray, p: np.ndarray, w: np.ndarray, frac: float = 0.1) -> float:
    o = np.argsort(-p, kind="stable")
    cw = np.cumsum(w[o])
    top = o[cw <= frac * w.sum()]
    if top.size == 0:
        return float("nan")
    return float(np.average(y[top], weights=w[top]) / np.average(y, weights=w))


def top_mean(y: np.ndarray, p: np.ndarray, w: np.ndarray, frac: float = 0.1) -> float:
    o = np.argsort(-p, kind="stable")
    cw = np.cumsum(w[o])
    top = o[cw <= frac * w.sum()]
    if top.size == 0:
        return float("nan")
    return float(np.average(y[top], weights=w[top]) - np.average(y, weights=w))


# --------------------------------------------------------------------------------------------------------------
# learners
# --------------------------------------------------------------------------------------------------------------

def baseline_fit(kind: str, target: str, y: np.ndarray, w: np.ndarray, off: np.ndarray) -> Any:
    """Weighted baseline: ``big`` -> logistic on the offset (slope + intercept), ``hit1`` -> constant, returns -> weighted mean."""
    if kind == "reg":
        return ("const", float(np.average(y, weights=w)))
    if target == "hit1":
        return ("const", float(logit(np.clip(np.average(y, weights=w), 1e-3, 1 - 1e-3))))
    x = np.column_stack([np.ones_like(off), off])

    def f(b: np.ndarray) -> tuple[float, np.ndarray]:
        z = x @ b
        ll = np.sum(w * (np.logaddexp(0, z) - y * z))
        g = x.T @ (w * (expit(z) - y))
        return float(ll), g

    b = minimize(f, np.array([0.0, 1.0]), jac=True, method="L-BFGS-B").x
    return ("offset", b)


def baseline_pred(model: Any, n: int, off: np.ndarray) -> np.ndarray:
    """Baseline prediction on the logit scale (binary) or the value scale (returns)."""
    kind, par = model
    if kind == "const":
        return np.full(n, par)
    return par[0] + par[1] * off


def ridge_logit(x: np.ndarray, y: np.ndarray, w: np.ndarray, base: np.ndarray, lam: float) -> tuple[np.ndarray, float]:
    n, p = x.shape

    def f(theta: np.ndarray) -> tuple[float, np.ndarray]:
        z = base + theta[0] + x @ theta[1:]
        r = expit(z) - y
        ll = np.sum(w * (np.logaddexp(0, z) - y * z)) + 0.5 * lam * float(theta[1:] @ theta[1:])
        g = np.concatenate(([np.sum(w * r)], x.T @ (w * r) + lam * theta[1:]))
        return float(ll), g

    th = minimize(f, np.zeros(p + 1), jac=True, method="L-BFGS-B", options={"maxiter": 200}).x
    return th[1:], float(th[0])


def select_k(x: np.ndarray, resid: np.ndarray, w: np.ndarray, k: int) -> np.ndarray:
    if k <= 0 or k >= x.shape[1]:
        return np.arange(x.shape[1])
    xm = x - np.average(x, axis=0, weights=w)
    rm = resid - np.average(resid, weights=w)
    c = (w[:, None] * xm * rm[:, None]).sum(0) / np.sqrt((w[:, None] * xm**2).sum(0) * (w * rm**2).sum() + 1e-12)
    return np.argsort(-np.abs(c))[:k]


def fit_predict(learner: str, cfg: dict[str, Any], kind: str, xtr: np.ndarray, ytr: np.ndarray, wtr: np.ndarray, btr: np.ndarray,
                xte: np.ndarray, bte: np.ndarray, n_jobs: int, seed: int = 0) -> np.ndarray:
    """Shift of the prediction relative to the baseline: logit shift (binary) or value shift (returns); ``btr`` / ``bte`` = baseline."""
    if learner == "M1":
        if kind == "bin":
            resid = ytr - expit(btr)
            idx = select_k(xtr, resid, wtr, cfg["k"])
            beta, b0 = ridge_logit(xtr[:, idx], ytr, wtr, btr, cfg["lam"])
            return b0 + xte[:, idx] @ beta
        idx = select_k(xtr, ytr - btr, wtr, cfg["k"])
        m = Ridge(alpha=cfg["lam"], random_state=seed).fit(xtr[:, idx], ytr - btr, sample_weight=wtr)
        return m.predict(xte[:, idx])
    if learner == "M2":
        params = {"objective": "binary" if kind == "bin" else "regression", "num_leaves": cfg["leaves"], "learning_rate": 0.03,
                  "min_data_in_leaf": 150, "lambda_l2": 20.0, "feature_fraction": 0.5, "bagging_fraction": 0.7, "bagging_freq": 1,
                  "verbose": -1, "seed": seed, "num_threads": n_jobs}
        if kind == "bin":
            ds = lgb.Dataset(xtr, ytr, weight=wtr, init_score=btr, free_raw_data=False)
        else:
            ds = lgb.Dataset(xtr, ytr - btr, weight=wtr, free_raw_data=False)
        bst = lgb.train(params, ds, num_boost_round=cfg["n"])
        return np.asarray(bst.predict(xte, raw_score=True), dtype=float)
    if learner == "M3":
        tgt = (ytr - expit(btr)) if kind == "bin" else (ytr - btr)
        m = ExtraTreesRegressor(n_estimators=150, min_samples_leaf=cfg["leaf"], max_features=0.3, max_samples=0.6, bootstrap=True,
                                n_jobs=n_jobs, random_state=seed).fit(xtr, tgt, sample_weight=wtr)
        pr = m.predict(xte)
        if kind == "bin":                                    # residual on the probability scale -> logit shift
            p0 = expit(bte)
            return logit(np.clip(p0 + pr, 0.005, 0.995)) - bte
        return pr
    raise ValueError(learner)


def to_pred(kind: str, shift: np.ndarray, base: np.ndarray) -> np.ndarray:
    return expit(base + shift) if kind == "bin" else base + shift


def score(kind: str, y: np.ndarray, shift: np.ndarray, base: np.ndarray, w: np.ndarray) -> float:
    """Selection score: weighted log-loss gain over the baseline (binary) or weighted Spearman IC (returns)."""
    if kind == "bin":
        return float(np.average(logloss_vec(y, expit(base)) - logloss_vec(y, expit(base + shift)), weights=w))
    return wspearman(shift + base, y, w) if np.ptp(shift) > 0 else 0.0


# --------------------------------------------------------------------------------------------------------------
# nested evaluation
# --------------------------------------------------------------------------------------------------------------

def inner_blocks(data: Data, start_idx: int, n_blocks: int = 3) -> list[tuple[int, int]]:
    """Up to ``n_blocks`` half-year blocks that end at ``start_idx`` (the outer block start), most recent last."""
    out = []
    ref = np.datetime64(int(start_idx + data.m0), "m")
    year, month = int(str(ref.astype("datetime64[M]"))[:4]), int(str(ref.astype("datetime64[M]"))[5:7])
    for _ in range(n_blocks):
        if month == 1:
            year, half = year - 1, 2
        else:
            half = 1
        out.append(block_idx(data, year, half))
        month = 1 if half == 1 else 7
    return out[::-1]


def run_target(data: Data, target: str, *, n_jobs: int = 4, learners: tuple[str, ...] = LEARNERS, seed: int = 0,
               y_override: np.ndarray | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Out-of-fold predictions (all outer blocks) and the selected configs for one target."""
    kind = KIND[target]
    y_all = (data.y[target] if y_override is None else y_override).copy()
    ok_all = np.isfinite(y_all)
    n = len(y_all)
    preds: list[pl.DataFrame] = []
    sel: list[dict[str, Any]] = []
    for year, half in BLOCKS:
        a, b = block_idx(data, year, half)
        te = ok_all & (data.conf_idx >= a) & (data.conf_idx < b)
        tr = ok_all & (data.conf_idx < a) & (data.label_idx < a)
        if te.sum() == 0 or tr.sum() < MIN_TRAIN:
            continue
        prep = fit_prep(data.x[tr])
        z = np.zeros((n, len(prep.keep)))
        z[tr | te] = prep.transform(data.x[tr | te])
        # winsorise / scale the return target on the training rows only
        y = y_all.copy()
        if kind == "reg":
            lo_, hi_ = np.percentile(y[tr], [1, 99])
            y = np.clip(y, lo_, hi_)
            sd = y[tr].std()
            y = (y - y[tr].mean()) / (sd if sd > 0 else 1.0)
        bm = baseline_fit(kind, target, y[tr], data.w[tr], data.off[tr])
        base = baseline_pred(bm, n, data.off)
        row_idx = np.nonzero(te)[0]
        shifts: dict[str, np.ndarray] = {}
        for lrn in learners:
            best_cfg, best_sc = None, 0.0
            for cfg in GRID[lrn]:
                ys, ss, bs, ws = [], [], [], []
                for ia, ib in inner_blocks(data, a):
                    itr = ok_all & (data.conf_idx < ia) & (data.label_idx < ia) & tr
                    ite = ok_all & (data.conf_idx >= ia) & (data.conf_idx < ib) & tr
                    if itr.sum() < MIN_TRAIN or ite.sum() < 200:
                        continue
                    ibm = baseline_fit(kind, target, y[itr], data.w[itr], data.off[itr])
                    ib_all = baseline_pred(ibm, n, data.off)
                    sh = fit_predict(lrn, cfg, kind, z[itr], y[itr], data.w[itr], ib_all[itr], z[ite], ib_all[ite], n_jobs, seed)
                    ys.append(y[ite])
                    ss.append(sh)
                    bs.append(ib_all[ite])
                    ws.append(data.w[ite])
                if not ys:
                    continue
                sc = score(kind, np.concatenate(ys), np.concatenate(ss), np.concatenate(bs), np.concatenate(ws))
                if sc > best_sc:
                    best_cfg, best_sc = cfg, sc
            if best_cfg is None:
                shifts[lrn] = np.zeros(te.sum())
            else:
                shifts[lrn] = fit_predict(lrn, best_cfg, kind, z[tr], y[tr], data.w[tr], base[tr], z[te], base[te], n_jobs, seed)
            sel.append({"target": target, "block": f"{year}H{half}", "learner": lrn, "inner_score": best_sc,
                        "config": "baseline" if best_cfg is None else str(best_cfg), "n_train": int(tr.sum()), "n_keep": int(len(prep.keep))})
        if kind == "bin":
            shifts["M4"] = np.mean([shifts[k] for k in learners], axis=0)
        else:
            rk = [(rankdata(shifts[k]) / len(shifts[k]) - 0.5) for k in learners]
            shifts["M4"] = np.mean(rk, axis=0) * np.std(np.concatenate([shifts[k] for k in learners])) * 2
        for lrn, sh in shifts.items():
            preds.append(pl.DataFrame({"target": target, "learner": lrn, "block": f"{year}H{half}", "row": row_idx,
                                       "shift": sh, "base": base[te], "y": y[te]}))
        logger.info("%s %dH%d train=%d test=%d keep=%d", target, year, half, int(tr.sum()), int(te.sum()), len(prep.keep))
    return pl.concat(preds), pl.DataFrame(sel)


def bootstrap_days(day: np.ndarray, n_boot: int, seed: int) -> list[np.ndarray]:
    rng = np.random.default_rng(seed)
    ud, inv = np.unique(day, return_inverse=True)
    by_day = [np.nonzero(inv == i)[0] for i in range(ud.size)]
    return [np.concatenate([by_day[i] for i in rng.integers(0, ud.size, ud.size)]) for _ in range(n_boot)]


def eval_pred(kind: str, y: np.ndarray, p_shift: np.ndarray, base: np.ndarray, w: np.ndarray) -> dict[str, float]:
    if kind == "bin":
        p, p0 = expit(base + p_shift), expit(base)
        return {"gain": float(np.average(logloss_vec(y, p0) - logloss_vec(y, p), weights=w)), "auc": wauc(y, p, w),
                "auc_base": wauc(y, p0, w) if np.ptp(base) > 0 else 0.5, "lift10": top_lift(y, p, w)}
    return {"ic": wspearman(p_shift + base, y, w), "top10": top_mean(y, p_shift + base, w)}


def metrics_table(data: Data, oof: pl.DataFrame, *, n_boot: int = 300, seed: int = 5) -> pl.DataFrame:
    rows = []
    for (target, lrn), g in oof.group_by(["target", "learner"], maintain_order=True):
        kind = KIND[target]
        idx = g["row"].to_numpy()
        y, sh, bs, w, day = g["y"].to_numpy(), g["shift"].to_numpy(), g["base"].to_numpy(), data.w[idx], data.day[idx]
        full = eval_pred(kind, y, sh, bs, w)
        boots = bootstrap_days(day, n_boot, seed)
        keys = list(full)
        bv = {k: np.array([eval_pred(kind, y[b], sh[b], bs[b], w[b])[k] for b in boots]) for k in keys[:1]}
        row = {"target": target, "learner": lrn, "n": int(idx.size), **full,
               **{f"{k}_lo": float(np.nanpercentile(v, 2.5)) for k, v in bv.items()}, **{f"{k}_hi": float(np.nanpercentile(v, 97.5)) for k, v in bv.items()}}
        rows.append(row)
    return pl.DataFrame(rows)


def slice_table(data: Data, oof: pl.DataFrame) -> pl.DataFrame:
    """The same metrics per scale, per outer block and per pre-registered segment (unweighted for the scale view)."""
    rows = []
    for (target, lrn), g in oof.group_by(["target", "learner"], maintain_order=True):
        kind = KIND[target]
        idx = g["row"].to_numpy()
        y, sh, bs = g["y"].to_numpy(), g["shift"].to_numpy(), g["base"].to_numpy()
        w = data.w[idx]
        key = "gain" if kind == "bin" else "ic"
        groups: list[tuple[str, str, np.ndarray, bool]] = []
        for s in np.unique(data.scale[idx]):
            groups.append(("scale", str(int(s)), data.scale[idx] == s, False))
        for blk in g["block"].unique().sort().to_list():
            groups.append(("block", blk, (g["block"] == blk).to_numpy(), True))
        for nm, m in data.seg.items():
            groups.append(("segment", nm, m[idx], True))
        for kind_g, name, msk, weighted in groups:
            if msk.sum() < 100:
                continue
            ww = w[msk] if weighted else np.ones(int(msk.sum()))
            r = eval_pred(kind, y[msk], sh[msk], bs[msk], ww)
            rows.append({"target": target, "learner": lrn, "slice": kind_g, "name": name, "n": int(msk.sum()), "metric": key, "value": r[key]})
    return pl.DataFrame(rows)


def run_all(data: Data, targets: tuple[str, ...] = TARGETS, n_jobs: int = 4) -> tuple[pl.DataFrame, pl.DataFrame]:
    oofs, sels = [], []
    for t in targets:
        t0 = time.time()
        o, s = run_target(data, t, n_jobs=n_jobs)
        oofs.append(o)
        sels.append(s)
        logger.info("target %s done in %.0fs", t, time.time() - t0)
    return pl.concat(oofs), pl.concat(sels)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--targets", default=",".join(TARGETS))
    ap.add_argument("--jobs", type=int, default=4)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    df = mc.load_table(a.asset_id, fe.TABLE_FEATURES)
    data = make_data(df)
    logger.info("clean rows=%d features=%d", len(data.w), len(data.names))
    oof, sel = run_all(data, tuple(a.targets.split(",")), a.jobs)
    met = metrics_table(data, oof)
    sl = slice_table(data, oof)
    an._write(a.asset_id, {TABLE_OOF: oof, TABLE_SELECTED: sel, TABLE_METRICS: met, TABLE_SLICES: sl})
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=220):
        print(met.with_columns(pl.col(pl.Float64).round(4)))
        print(sel.group_by("target", "learner").agg((pl.col("config") == "baseline").sum().alias("fallback"), pl.len().alias("blocks")))


if __name__ == "__main__":
    main()
