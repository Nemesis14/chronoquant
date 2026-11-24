"""Forward group selection + time-ordered validation of the candidate predictors (epic_078 / t962).

Question: does a variable group raise the out-of-sample ranking of "this candidate becomes a big move" above the
base rate?  Everything is out-of-time: expanding-window yearly folds, test years 2023 / 2024 / 2025, training only on
candidates whose *label* was already known before the test year started (purge).  Model: L2 logistic regression
(C = 0.1, fixed, no tuning) on winsorised, standardised features + scale dummies.

Experiments (all on the same out-of-fold rows):

* ``base``     — scale dummies only (≈ base rate)
* ``only_<g>`` — baseline + one group (no selection, 6 numbers)
* ``cum_<g>``  — groups added in the *pre-registered* order z, v, w, f, q, s
* ``all``      — all groups;  ``gbm_all`` — small gradient boosting on all features (comparison only)
* ``greedy``   — forward selection of groups on an *inner* time-CV inside the training years only (stop when the inner
  AUC gain < 0.005) — the only experiment that selects, and it never sees the outer test year.

Targets: ``big`` (the up-leg reaches >= k sigma_T) and ``hit1`` (+1R before the structural stop; censored rows dropped).
Metrics: AUC, lift in the best decile, log-loss skill vs the per-scale base rate; day-cluster bootstrap CIs and paired
differences to ``base``.

``uv run python -m reports.builders.cand_pred_model``
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

import numpy as np
import polars as pl
from scipy.stats import rankdata
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression

from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an
from reports.builders.cand_pred_features import GROUPS, SCALES, TABLE_FEATURES

logger = logging.getLogger(__name__)

TEST_YEARS = (2023, 2024, 2025)
INNER_FIRST_YEAR = 2022
ORDER = ("z", "v", "w", "f", "q", "s")
TARGETS = ("big", "hit1")
C_REG = 0.1
MIN_GAIN = 0.005
TABLE_METRICS = "cand_pred_v1_metrics"
TABLE_OOF = "cand_pred_v1_oof"
TABLE_SELECTED = "cand_pred_v1_selected"
TABLE_UNIVAR = "cand_pred_v1_univar"


def feature_columns(df: pl.DataFrame) -> dict[str, list[str]]:
    """Feature columns per group letter (``qd_`` belongs to the quant group ``q``)."""
    out: dict[str, list[str]] = {g: [] for g in GROUPS}
    for c in df.columns:
        if c.startswith("qd_"):
            out["q"].append(c)
        elif len(c) > 2 and c[1] == "_" and c[0] in GROUPS:
            out[c[0]].append(c)
    return out


# --------------------------------------------------------------------------------------------------------------
# data + splits
# --------------------------------------------------------------------------------------------------------------

def add_targets(df: pl.DataFrame) -> pl.DataFrame:
    """``y_big`` / ``y_hit1`` (null = censored), ``label_idx`` (bar at which both labels are surely known), ``year``."""
    return df.with_columns(
        pl.col("big").cast(pl.Int8).alias("y_big"),
        pl.when(pl.col("res1") == 1).then(1).when(pl.col("res1") == -1).then(0).otherwise(None).cast(pl.Int8).alias("y_hit1"),
        pl.max_horizontal(pl.col("end_conf_idx"), pl.col("conf_idx") + pl.col("resolve_bars").clip(lower_bound=0)).alias("label_idx"),
        pl.col("conf_time").dt.year().alias("year"))


def year_start_idx(df: pl.DataFrame, year: int) -> int:
    """Bar index of ``year``-01-01 on the candidates' bar clock."""
    r = df.row(0, named=True)
    m0 = int(np.datetime64(r["conf_time"], "m").astype("int64")) - int(r["conf_idx"])
    return int(np.datetime64(f"{year}-01-01", "m").astype("int64")) - m0


def split(df: pl.DataFrame, year: int, target: str) -> tuple[np.ndarray, np.ndarray]:
    """(train mask, test mask): test = the year, train = earlier candidates whose labels were known before the year began."""
    cut = year_start_idx(df, year)
    ok = df[f"y_{target}"].is_not_null().to_numpy()
    tr = ok & (df["year"].to_numpy() < year) & (df["label_idx"].to_numpy() < cut)
    te = ok & (df["year"].to_numpy() == year)
    return tr, te


# --------------------------------------------------------------------------------------------------------------
# model
# --------------------------------------------------------------------------------------------------------------

def _design(x_tr: np.ndarray, x_te: np.ndarray, sc_tr: np.ndarray, sc_te: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if x_tr.shape[1]:
        lo, hi = np.nanpercentile(x_tr, 1, axis=0), np.nanpercentile(x_tr, 99, axis=0)
        a, b = np.clip(x_tr, lo, hi), np.clip(x_te, lo, hi)
        mu, sd = a.mean(0), a.std(0)
        sd[sd < 1e-12] = 1.0
        a, b = (a - mu) / sd, (b - mu) / sd
    else:
        a, b = np.empty((len(sc_tr), 0)), np.empty((len(sc_te), 0))
    dum = [s for s in SCALES[1:]]
    d_tr = np.column_stack([(sc_tr == s).astype(float) for s in dum])
    d_te = np.column_stack([(sc_te == s).astype(float) for s in dum])
    return np.hstack([a, d_tr]), np.hstack([b, d_te])


def fit_predict(x_tr: np.ndarray, y_tr: np.ndarray, sc_tr: np.ndarray, x_te: np.ndarray, sc_te: np.ndarray, *,
                gbm: bool = False) -> np.ndarray:
    if gbm:
        a = np.column_stack([x_tr, sc_tr])
        b = np.column_stack([x_te, sc_te])
        m = HistGradientBoostingClassifier(max_depth=3, learning_rate=0.05, max_iter=150, l2_regularization=5.0,
                                           min_samples_leaf=40, random_state=0)
        return m.fit(a, y_tr).predict_proba(b)[:, 1]
    a, b = _design(x_tr, x_te, sc_tr, sc_te)
    m = LogisticRegression(C=C_REG, max_iter=2000)
    return m.fit(a, y_tr).predict_proba(b)[:, 1]


def auc(y: np.ndarray, p: np.ndarray) -> float:
    pos = y == 1
    n1, n0 = int(pos.sum()), int((~pos).sum())
    if n1 == 0 or n0 == 0:
        return float("nan")
    r = rankdata(p)
    return float((r[pos].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def lift_top(y: np.ndarray, p: np.ndarray, frac: float = 0.10) -> float:
    n = max(int(round(frac * y.size)), 1)
    top = np.argsort(-p, kind="stable")[:n]
    base = y.mean()
    return float(y[top].mean() / base) if base > 0 else float("nan")


def logloss_skill(y: np.ndarray, p: np.ndarray, sc: np.ndarray, base_rate: dict[int, float]) -> float:
    """1 - logloss(model) / logloss(per-scale base rate of the training years)."""
    eps = 1e-9
    pm = np.clip(p, eps, 1 - eps)
    ll = -np.mean(y * np.log(pm) + (1 - y) * np.log(1 - pm))
    pb = np.clip(np.array([base_rate[int(s)] for s in sc]), eps, 1 - eps)
    lb = -np.mean(y * np.log(pb) + (1 - y) * np.log(1 - pb))
    return float(1 - ll / lb)


# --------------------------------------------------------------------------------------------------------------
# experiments
# --------------------------------------------------------------------------------------------------------------

def inner_auc(df: pl.DataFrame, target: str, outer_year: int, cols: list[str]) -> float:
    """Pooled AUC of the inner expanding-window folds (test years INNER_FIRST_YEAR .. outer_year - 1), training years only."""
    ys, ps = [], []
    for y in range(INNER_FIRST_YEAR, outer_year):
        tr, te = split(df, y, target)
        if tr.sum() < 200 or te.sum() < 50:
            continue
        x = df.select(cols).to_numpy() if cols else np.empty((df.height, 0))
        sc = df["scale_min"].to_numpy()
        yy = df[f"y_{target}"].to_numpy()
        ps.append(fit_predict(x[tr], yy[tr], sc[tr], x[te], sc[te]))
        ys.append(yy[te])
    if not ys:
        return float("nan")
    return auc(np.concatenate(ys), np.concatenate(ps))


def greedy_groups(df: pl.DataFrame, target: str, outer_year: int, fcols: dict[str, list[str]]) -> tuple[list[str], list[float]]:
    chosen: list[str] = []
    cols: list[str] = []
    cur = inner_auc(df, target, outer_year, cols)
    trace = [cur]
    left = list(ORDER)
    while left:
        best_g, best_a = None, -1.0
        for g in left:
            a = inner_auc(df, target, outer_year, cols + fcols[g])
            if np.isfinite(a) and a > best_a:
                best_g, best_a = g, a
        if best_g is None or not np.isfinite(cur) or best_a - cur < MIN_GAIN:
            break
        chosen.append(best_g)
        cols += fcols[best_g]
        left.remove(best_g)
        cur = best_a
        trace.append(cur)
    return chosen, trace


def run_models(df: pl.DataFrame, target: str, fcols: dict[str, list[str]], *, with_greedy: bool = True,
               with_gbm: bool = True) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """Out-of-fold predictions of every experiment (rows = the test years 2023-2025 pooled)."""
    specs: dict[str, list[str]] = {"base": []}
    for g in ORDER:
        specs[f"only_{g}"] = list(fcols[g])
    run: list[str] = []
    for g in ORDER:
        run += fcols[g]
        specs[f"cum_{g}"] = list(run)
    specs["all"] = list(run)
    x_all = df.select(run).to_numpy()
    col_pos = {c: i for i, c in enumerate(run)}
    sc = df["scale_min"].to_numpy()
    yy = df[f"y_{target}"].to_numpy()
    parts: list[pl.DataFrame] = []
    selected: list[dict[str, Any]] = []
    for year in TEST_YEARS:
        tr, te = split(df, year, target)
        rows = np.nonzero(te)[0]
        out: dict[str, np.ndarray] = {}
        for name, cols in specs.items():
            ix = [col_pos[c] for c in cols]
            out[name] = fit_predict(x_all[tr][:, ix], yy[tr], sc[tr], x_all[te][:, ix], sc[te])
        if with_gbm:
            out["gbm_all"] = fit_predict(x_all[tr], yy[tr], sc[tr], x_all[te], sc[te], gbm=True)
        if with_greedy:
            chosen, trace = greedy_groups(df, target, year, fcols)
            cols = [c for g in chosen for c in fcols[g]]
            ix = [col_pos[c] for c in cols]
            out["greedy"] = fit_predict(x_all[tr][:, ix], yy[tr], sc[tr], x_all[te][:, ix], sc[te])
            selected.append({"target": target, "test_year": year, "groups": "".join(chosen), "inner_auc_trace": trace})
        for name, p in out.items():
            parts.append(pl.DataFrame({"target": target, "model": name, "test_year": year, "row": rows, "p": p}))
        logger.info("%s %d done (train %d, test %d)", target, year, int(tr.sum()), int(te.sum()))
    return pl.concat(parts), selected


def base_rates(df: pl.DataFrame, target: str) -> dict[int, float]:
    d = df.filter(pl.col(f"y_{target}").is_not_null() & (pl.col("year") < TEST_YEARS[0]))
    return {int(s): float(np.mean(d.filter(pl.col("scale_min") == s)[f"y_{target}"].to_numpy())) for s in SCALES}


def metrics(df: pl.DataFrame, oof: pl.DataFrame, target: str, *, n_boot: int = 300, seed: int = 5) -> pl.DataFrame:
    """Per model: AUC, lift, log-loss skill (+ day-cluster bootstrap CIs, paired differences to ``base``)."""
    yy = df[f"y_{target}"].to_numpy()
    sc_all = df["scale_min"].to_numpy()
    day_all = df["conf_time"].dt.date().cast(pl.Utf8).to_numpy()
    br = base_rates(df, target)
    models = oof["model"].unique().sort().to_list()
    first = oof.filter(pl.col("model") == "base").sort(["test_year", "row"])
    rows = first["row"].to_numpy()
    y, sc, day = yy[rows].astype(int), sc_all[rows], day_all[rows]
    yr = first["test_year"].to_numpy()
    preds = {m: oof.filter(pl.col("model") == m).sort(["test_year", "row"])["p"].to_numpy() for m in models}
    rng = np.random.default_rng(seed)
    ud, inv = np.unique(day, return_inverse=True)
    by_day = [np.nonzero(inv == i)[0] for i in range(ud.size)]
    boots = [np.concatenate([by_day[i] for i in rng.integers(0, ud.size, ud.size)]) for _ in range(n_boot)]
    b_auc = {m: np.array([auc(y[b], preds[m][b]) for b in boots]) for m in models}
    b_lift = {m: np.array([lift_top(y[b], preds[m][b]) for b in boots]) for m in models}
    out = []
    for m in models:
        p = preds[m]
        d_boot = b_auc[m] - b_auc["base"]
        row: dict[str, Any] = {
            "target": target, "model": m, "n": int(y.size), "base_rate": float(y.mean()),
            "auc": auc(y, p), "auc_lo": float(np.nanpercentile(b_auc[m], 2.5)), "auc_hi": float(np.nanpercentile(b_auc[m], 97.5)),
            "dauc": auc(y, p) - auc(y, preds["base"]),
            "dauc_lo": float(np.nanpercentile(d_boot, 2.5)), "dauc_hi": float(np.nanpercentile(d_boot, 97.5)),
            "lift10": lift_top(y, p), "lift_lo": float(np.nanpercentile(b_lift[m], 2.5)),
            "lift_hi": float(np.nanpercentile(b_lift[m], 97.5)), "ll_skill": logloss_skill(y, p, sc, br)}
        for t in TEST_YEARS:
            k = yr == t
            row[f"auc_{t}"] = auc(y[k], p[k])
        for s in SCALES:
            k = sc == s
            row[f"auc_T{s}"] = auc(y[k], p[k])
        out.append(row)
    return pl.DataFrame(out)


def univariate(df: pl.DataFrame, target: str, fcols: dict[str, list[str]]) -> pl.DataFrame:
    """Single-variable AUC on the pooled test years (raw value as the score, no fitting) — direction = sign of AUC - 0.5."""
    te = np.zeros(df.height, bool)
    for y in TEST_YEARS:
        te |= split(df, y, target)[1]
    yy = df[f"y_{target}"].to_numpy()[te].astype(int)
    rows = []
    for g, cols in fcols.items():
        for c in cols:
            x = df[c].to_numpy()[te]
            rows.append({"target": target, "group": g, "feature": c, "auc": auc(yy, x), "n": int(yy.size)})
    return pl.DataFrame(rows).with_columns((pl.col("auc") - 0.5).abs().alias("abs_dev")).sort("abs_dev", descending=True)


def evaluate(df: pl.DataFrame, *, with_ci: bool = True, with_greedy: bool = True, with_gbm: bool = True,
             targets: tuple[str, ...] = TARGETS) -> dict[str, pl.DataFrame]:
    d = add_targets(df)
    fcols = feature_columns(d)
    m_parts, o_parts, s_parts, u_parts = [], [], [], []
    for t in targets:
        oof, sel = run_models(d, t, fcols, with_greedy=with_greedy, with_gbm=with_gbm)
        o_parts.append(oof)
        if sel:
            s_parts.append(pl.DataFrame([{**r, "inner_auc_trace": ",".join(f"{v:.3f}" for v in r["inner_auc_trace"])} for r in sel]))
        u_parts.append(univariate(d, t, fcols))
        if with_ci:
            m_parts.append(metrics(d, oof, t))
    out = {TABLE_OOF: pl.concat(o_parts), TABLE_UNIVAR: pl.concat(u_parts)}
    if m_parts:
        out[TABLE_METRICS] = pl.concat(m_parts)
    if s_parts:
        out[TABLE_SELECTED] = pl.concat(s_parts)
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    df = mc.load_table(a.asset_id, TABLE_FEATURES)
    res = evaluate(df)
    an._write(a.asset_id, res)
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=250):
        for t in TARGETS:
            m = res[TABLE_METRICS].filter(pl.col("target") == t)
            print(m.select(["model", "n", "base_rate", "auc", "auc_lo", "auc_hi", "dauc", "dauc_lo", "dauc_hi", "lift10",
                            "ll_skill"]).with_columns(pl.col(pl.Float64).round(3)))
        print(res[TABLE_SELECTED])
        print(res[TABLE_UNIVAR].head(14))


if __name__ == "__main__":
    main()
