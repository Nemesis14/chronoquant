#!/usr/bin/env python3
"""Lasso instead of screen-then-fit, with a random baseline (epic_064).

Two problems with the previous run, both raised and both real.

**The pipeline was a chain of arbitrary cuts.** Mutual information above 0.001,
correlation below 0.90, p below 0.01, then an unpenalised OLS on whatever
survived. Three thresholds, none of them derived from anything, and the fit
itself knew nothing about how its inputs had been chosen. Lasso does selection
and shrinkage in one step, with the penalty picked by cross-validation instead
of by hand.

**There was no yardstick.** Five features drawn at random -- never screened,
never tested -- produce a train IC of +0.030 and a validation IC of +0.017 on
this data, because ordinary least squares on heavily overlapping observations
manufactures in-sample fit out of nothing, and the features are correlated
enough that any five carry the same few components. The selected model scored
+0.031 / +0.016. It did not beat picking at random, and nothing here found that
out until it was measured.

So the random baseline now runs alongside every model, and a result only counts
if it clears that line rather than zero.

⚠️ Alpha is chosen on purged time-series folds inside the training window.
sklearn's default KFold would put a fold boundary in the middle of an 8-hour
label and let the answer leak across it.

Usage:
    python -m modeling.experiments.sol_lasso
"""

import argparse
import logging
import os
import sys
import warnings

import duckdb
import numpy as np
import pandas as pd
from sklearn.linear_model import Lasso, LassoCV
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler

import utils
from modeling.experiments.sol_short_horizon import (
    DB_REL,
    EVAL_END,
    HORIZONS_MIN,
    TABLE,
    TRAIN_END,
    build_labels,
)

logger = logging.getLogger(__name__)

SEED = 42
N_ALPHAS = 40
N_RANDOM = 20          # draws for the baseline
EVAL_START = "2024-08"


def purged_folds(n: int, k: int, purge: int) -> list[tuple[np.ndarray, np.ndarray]]:
    """Expanding windows with the label horizon removed at each seam."""
    out, block = [], n // (k + 1)
    for i in range(1, k + 1):
        end = block * i
        stop = min(end + block, n)
        if stop - end < 200:
            continue
        out.append((np.arange(0, max(end - purge, 1)), np.arange(end, stop)))
    return out


def pick_alpha(x: np.ndarray, y: np.ndarray, purge: int) -> float:
    """Cross-validated penalty, on folds that respect the label horizon."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        m = LassoCV(cv=purged_folds(len(y), 4, purge), n_alphas=N_ALPHAS,
                    random_state=SEED, max_iter=20000)
        m.fit(x, y)
    return float(m.alpha_)


def _metrics(pred: np.ndarray, y: np.ndarray) -> dict:
    out = {"ic": float(np.corrcoef(pred, y)[0, 1]) if pred.std() > 1e-12 else 0.0}
    mask = y != 0
    if mask.sum() >= 60 and len(np.unique(y[mask])) > 1:
        p, yy = pred[mask], y[mask]
        out["auc"] = float(roc_auc_score((yy > 0).astype(int), p))
        sel = p >= float(np.quantile(p, 0.90))
        out["lift"] = (float((yy[sel] == 1).mean() - (yy == 1).mean()) * 100
                       if sel.sum() >= 20 else np.nan)
    else:
        out["auc"], out["lift"] = np.nan, np.nan
    return out


def _walk(d: pd.DataFrame, dts: pd.Series, feats: list[str], h: int,
          alpha: float | None) -> pd.DataFrame:
    """Monthly refit. alpha=None means an unpenalised fit on the given columns."""
    rows = []
    for period in pd.period_range(EVAL_START, EVAL_END, freq="M"):
        m0, m1 = period.to_timestamp(), (period + 1).to_timestamp()
        tr = d[dts < m0 - pd.Timedelta(minutes=h)]
        te = d[(dts >= m0) & (dts < m1)]
        if len(tr) < 8000 or len(te) < 200:
            continue
        sc = StandardScaler().fit(tr[feats].to_numpy("float64"))
        xa, xb = sc.transform(tr[feats]), sc.transform(te[feats])
        ya, yb = tr["y"].to_numpy(), te["y"].to_numpy()
        model = Lasso(alpha=alpha if alpha else 1e-9, max_iter=20000,
                      random_state=SEED)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(xa, ya)
        rows.append({
            "ho": str(period), "nz": int((model.coef_ != 0).sum()),
            **{f"{k}_tr": v for k, v in _metrics(model.predict(xa), ya).items()},
            **_metrics(model.predict(xb), yb),
        })
    return pd.DataFrame(rows)


def _summary(name: str, res: pd.DataFrame) -> None:
    def t(v, centre=0.0):
        v = v[np.isfinite(v)]
        return (float((v - centre).mean() / (v.std(ddof=1) / np.sqrt(len(v))))
                if len(v) > 1 else np.nan)

    ic, auc, lf = (res["ic"].to_numpy(), res["auc"].to_numpy(),
                   res["lift"].to_numpy())
    print(f"      {name}")
    print(f"        valtozo (median nem-nulla): {res['nz'].median():.0f}")
    print(f"        IC       train {res['ic_tr'].mean():+.4f}   "
          f"valid {ic.mean():+.4f}   t {t(ic):5.2f}   "
          f"pozitiv {int((ic > 0).sum())}/{len(ic)}")
    print(f"        AUC      train {res['auc_tr'].mean():.4f}   "
          f"valid {np.nanmean(auc):.4f}   t {t(auc, 0.5):5.2f}")
    print(f"        p90 lift train {np.nanmean(res['lift_tr']):+.2f}pp  "
          f"valid {np.nanmean(lf):+.2f}pp   t {t(lf):5.2f}")


def run() -> None:
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
    df = feats.merge(build_labels(TRAIN_END), on="open_time", how="inner")

    print("\n" + "=" * 92)
    print("LASSO vs VELETLEN BASELINE — SOLUSDT")
    print("=" * 92)
    print(f"  {len(cols)} jelolt feature, semmilyen elozetes szures")
    print("  alpha: purge-olt idosoros CV a train ablakon belul")
    print(f"  baseline: {N_RANDOM} huzas 5 veletlen valtozobol, buntetes nelkul\n")

    for h in HORIZONS_MIN:
        d = df.dropna(subset=[f"y{h}", *cols]).reset_index(drop=True)
        dts = pd.to_datetime(d["open_time"])
        d = d.rename(columns={f"y{h}": "y"})
        init = d[dts <= TRAIN_END]
        sc = StandardScaler().fit(init[cols].to_numpy("float64"))
        alpha = pick_alpha(sc.transform(init[cols]), init["y"].to_numpy(), h // 60)

        print(f"  --- horizont {h} perc ({h // 60} ora) ---   alpha = {alpha:.5f}")
        _summary("LASSO (mind a jelolt)", _walk(d, dts, cols, h, alpha))

        rng = np.random.default_rng(SEED)
        acc = []
        for _ in range(N_RANDOM):
            pick = list(rng.choice(cols, size=5, replace=False))
            r = _walk(d, dts, pick, h, None)
            if not r.empty:
                acc.append(r[["ic", "auc", "lift", "ic_tr"]].mean())
        base = pd.DataFrame(acc)
        print(f"      VELETLEN 5 valtozo ({len(base)} huzas)")
        print(f"        IC       train {base['ic_tr'].mean():+.4f}   "
              f"valid {base['ic'].mean():+.4f}   "
              f"tartomany [{base['ic'].min():+.4f}, {base['ic'].max():+.4f}]")
        print(f"        AUC      valid {base['auc'].mean():.4f}   "
              f"p90 lift valid {base['lift'].mean():+.2f}pp\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Lasso + veletlen baseline")
    parser.parse_args()
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()


if __name__ == "__main__":
    main()
