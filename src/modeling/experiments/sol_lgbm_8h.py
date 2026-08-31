#!/usr/bin/env python3
"""LightGBM at 8 hours, regularisation searched, walked forward (epic_064).

One target, one model family, no transforms in between. The 8-hour barrier
label, every candidate feature, a regularisation grid chosen on purged folds
inside the training window, and then a monthly refit across 24 months.

Two things are carried over from what went wrong earlier, because without them
the numbers cannot be read:

**A random baseline runs alongside.** Five features drawn at random -- never
screened -- produce a validation IC around +0.017 on this data, because
ordinary fitting on heavily overlapping observations manufactures agreement out
of nothing and the features are correlated enough that any five carry the same
components. A result is only interesting if it clears that line, not zero.

**A constant prediction scores zero, not NaN.** When regularisation flattens the
model, the correlation is undefined; treating that as missing quietly drops the
month from the average and flatters the result. Those months carry exactly the
information they found, which is none.

⚠️ The regularisation grid is chosen on folds inside the training half, never on
the walk-forward months. The label spans 8 hours, so every fold seam is purged
by that much.

Usage:
    python -m modeling.experiments.sol_lgbm_8h
"""

import argparse
import logging
import os
import sys
import warnings

import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

import utils
from modeling.experiments.sol_short_horizon import (
    DB_REL,
    EVAL_END,
    TABLE,
    TRAIN_END,
    build_labels,
)

logger = logging.getLogger(__name__)

HORIZON_MIN = 480
PURGE_H = HORIZON_MIN // 60
SEED = 42
EVAL_START = "2024-08"
N_RANDOM = 10

FIXED: dict = {
    "objective": "regression",
    "learning_rate": 0.02,
    "subsample": 0.7,
    "subsample_freq": 1,
    "verbosity": -1,
    "n_jobs": 4,
    "random_state": SEED,
}
# The effective sample is a few thousand observations, so the grid stays at the
# heavily-regularised end. Anything looser was already shown to memorise.
GRID = [
    {"num_leaves": 3, "min_child_samples": 4000, "reg_lambda": 200.0,
     "colsample_bytree": 0.3, "n_estimators": 200},
    {"num_leaves": 3, "min_child_samples": 2000, "reg_lambda": 100.0,
     "colsample_bytree": 0.5, "n_estimators": 300},
    {"num_leaves": 4, "min_child_samples": 4000, "reg_lambda": 200.0,
     "colsample_bytree": 0.3, "n_estimators": 200},
    {"num_leaves": 4, "min_child_samples": 2000, "reg_lambda": 50.0,
     "colsample_bytree": 0.5, "n_estimators": 300},
    {"num_leaves": 4, "min_child_samples": 1000, "reg_lambda": 20.0,
     "colsample_bytree": 0.7, "n_estimators": 400},
    {"num_leaves": 8, "min_child_samples": 2000, "reg_lambda": 200.0,
     "colsample_bytree": 0.5, "n_estimators": 300},
    {"num_leaves": 8, "min_child_samples": 1000, "reg_lambda": 50.0,
     "colsample_bytree": 0.7, "n_estimators": 400},
    {"num_leaves": 16, "min_child_samples": 2000, "reg_lambda": 100.0,
     "colsample_bytree": 0.5, "n_estimators": 300},
]


def purged_folds(n: int, k: int, purge: int) -> list[tuple[np.ndarray, np.ndarray]]:
    out, block = [], n // (k + 1)
    for i in range(1, k + 1):
        end = block * i
        stop = min(end + block, n)
        if stop - end < 200:
            continue
        out.append((np.arange(0, max(end - purge, 1)), np.arange(end, stop)))
    return out


def _fit(params: dict, x, y) -> lgb.LGBMRegressor:
    model = lgb.LGBMRegressor(**{**FIXED, **params})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model.fit(x, y)
    return model


def _metrics(pred: np.ndarray, y: np.ndarray) -> dict:
    # A flat prediction found nothing; that is a zero, not a missing value.
    ic = float(np.corrcoef(pred, y)[0, 1]) if pred.std() > 1e-12 else 0.0
    out = {"ic": ic, "auc": 0.5, "lift": 0.0}
    mask = y != 0
    if pred.std() > 1e-12 and mask.sum() >= 60 and len(np.unique(y[mask])) > 1:
        p, yy = pred[mask], y[mask]
        out["auc"] = float(roc_auc_score((yy > 0).astype(int), p))
        sel = p >= float(np.quantile(p, 0.90))
        if sel.sum() >= 20:
            out["lift"] = float((yy[sel] == 1).mean() - (yy == 1).mean()) * 100
    return out


def _walk(d: pd.DataFrame, dts: pd.Series, feats: list[str],
          params: dict) -> pd.DataFrame:
    rows = []
    for period in pd.period_range(EVAL_START, EVAL_END, freq="M"):
        m0, m1 = period.to_timestamp(), (period + 1).to_timestamp()
        tr = d.loc[dts < m0 - pd.Timedelta(minutes=HORIZON_MIN)]
        te = d.loc[(dts >= m0) & (dts < m1)]
        if len(tr) < 8000 or len(te) < 200:
            continue
        ya, yb = tr["y"].to_numpy(), te["y"].to_numpy()
        m = _fit(params, tr[feats], ya)
        a = _metrics(np.asarray(m.predict(tr[feats])), ya)
        b = _metrics(np.asarray(m.predict(te[feats])), yb)
        rows.append({"ho": str(period),
                     **{f"{k}_tr": v for k, v in a.items()}, **b})
    return pd.DataFrame(rows)


def _t(v: np.ndarray, centre: float = 0.0) -> float:
    v = v[np.isfinite(v)]
    return (float((v - centre).mean() / (v.std(ddof=1) / np.sqrt(len(v))))
            if len(v) > 1 else float("nan"))


def run(null_shift: int = 0) -> None:
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)
    try:
        cols = [r[0] for r in conn.execute(f"DESCRIBE {TABLE}").fetchall()
                if r[0].startswith("feat_")]
        sel = ", ".join(f'"{c}"' for c in cols)
        feats = conn.execute(
            f"SELECT open_time, {sel} FROM {TABLE} ORDER BY open_time").df()
    finally:
        conn.close()
    cols = [c for c in cols if feats[c].notna().mean() > 0.5]

    labels = build_labels(TRAIN_END)
    df = feats.merge(labels, on="open_time", how="inner")
    d = df.dropna(subset=[f"y{HORIZON_MIN}", *cols]).reset_index(drop=True)
    dts = pd.to_datetime(d["open_time"])
    d = d.rename(columns={f"y{HORIZON_MIN}": "y"})
    if null_shift:
        # Rotate the label forward. The features keep their structure, the label
        # keeps its autocorrelation and class mix, and the pairing between them
        # is destroyed. Anything the model still scores here is machinery, not
        # signal -- that is the number the real run has to clear.
        d["y"] = np.roll(d["y"].to_numpy(), null_shift)
    init = d.loc[dts <= TRAIN_END].reset_index(drop=True)

    print("\n" + "=" * 88)
    print(f"LIGHTGBM, {HORIZON_MIN // 60} ORAS TARGET — SOLUSDT")
    print("=" * 88)
    print(f"  {len(d):,} orai sor, {len(cols)} feature, semmilyen elozetes szures")
    print(f"  train ablak a regularizacio-keresesre: {len(init):,} sor "
          f"({TRAIN_END}-ig)")
    print(f"  gorduló meres: {EVAL_START} .. {EVAL_END}, havi ujratanitas\n")

    folds = purged_folds(len(init), 4, PURGE_H)
    print("=== REGULARIZACIO — purge-olt CV a train ablakon belul ===")
    print(f"  {'levelek':>8}{'min_leaf':>10}{'lambda':>8}{'colsamp':>9}{'fak':>6}"
          f"{'CV IC':>10}{'CV AUC':>9}")
    best, best_ic = None, -np.inf
    for params in GRID:
        ics, aucs = [], []
        for i, j in folds:
            m = _fit(params, init.iloc[i][cols], init.iloc[i]["y"].to_numpy())
            mm = _metrics(np.asarray(m.predict(init.iloc[j][cols])),
                          init.iloc[j]["y"].to_numpy())
            ics.append(mm["ic"])
            aucs.append(mm["auc"])
        ic = float(np.mean(ics))
        print(f"  {params['num_leaves']:>8}{params['min_child_samples']:>10}"
              f"{params['reg_lambda']:>8.0f}{params['colsample_bytree']:>9.1f}"
              f"{params['n_estimators']:>6}{ic:>+10.4f}{np.mean(aucs):>9.4f}")
        if ic > best_ic:
            best, best_ic = params, ic
    assert best is not None
    print(f"\n  valasztott: {best}   (CV IC {best_ic:+.4f})\n")

    print("=== GORDULO MERES ===")
    res = _walk(d, dts, cols, best)
    ic, auc, lf = (res["ic"].to_numpy(), res["auc"].to_numpy(),
                   res["lift"].to_numpy())
    print(f"  LIGHTGBM ({len(res)} honap)")
    print(f"    IC       train {res['ic_tr'].mean():+.4f}   valid {ic.mean():+.4f}"
          f"   szoras {ic.std(ddof=1):.4f}   t {_t(ic):5.2f}"
          f"   pozitiv {int((ic > 0).sum())}/{len(ic)}")
    print(f"    AUC      train {res['auc_tr'].mean():.4f}   valid {auc.mean():.4f}"
          f"   t {_t(auc, 0.5):5.2f}")
    print(f"    p90 lift train {res['lift_tr'].mean():+.2f}pp  "
          f"valid {lf.mean():+.2f}pp   t {_t(lf):5.2f}"
          f"   pozitiv {int((lf > 0).sum())}/{len(lf)}")

    rng = np.random.default_rng(SEED)
    acc = []
    for _ in range(N_RANDOM):
        pick = list(rng.choice(cols, size=5, replace=False))
        r = _walk(d, dts, pick, best)
        if not r.empty:
            acc.append(r[["ic", "auc", "lift"]].mean())
    base = pd.DataFrame(acc)
    print(f"\n  VELETLEN 5 feature, ugyanaz a modell ({len(base)} huzas)")
    print(f"    IC   valid {base['ic'].mean():+.4f}   "
          f"tartomany [{base['ic'].min():+.4f}, {base['ic'].max():+.4f}]")
    print(f"    AUC  valid {base['auc'].mean():.4f}   "
          f"p90 lift {base['lift'].mean():+.2f}pp")

    print("\n  --- osszevetes ---")
    for nev, a, b in (("IC", ic.mean(), base["ic"].mean()),
                      ("AUC-0,5", auc.mean() - 0.5, base["auc"].mean() - 0.5),
                      ("p90 lift", lf.mean(), base["lift"].mean())):
        verdict = "VERI" if a > b else "nem veri"
        print(f"    {nev:<9} modell {a:+.4f}   baseline {b:+.4f}   -> {verdict}")


def main() -> None:
    parser = argparse.ArgumentParser(description="LightGBM 8h, gorduló")
    parser.add_argument("--null-shift", type=int, default=0,
                        help="cimke elforgatasa orakban (null teszt)")
    args = parser.parse_args()
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.null_shift)


if __name__ == "__main__":
    main()
