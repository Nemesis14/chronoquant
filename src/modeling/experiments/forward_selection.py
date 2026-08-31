#!/usr/bin/env python3
"""Greedy forward selection for a small tree (epic_064, coinonkenti modell).

Both previous attempts failed on size rather than on model family. A 79-feature
LightGBM kept none of its training IC (0.219 -> 0.000) even at three leaves and
lambda 200, while a three-coefficient OLS kept 94% of its own (0.041 -> 0.038).
With roughly 2,350 independent observations behind 32,576 hourly rows, the
number of parameters is the whole game.

So features are added one at a time, best-first, and each candidate has to earn
its place. The order of consideration comes from mutual information; whether a
candidate is actually accepted comes from cross-validated out-of-sample IC.

⚠️ **Selection never touches the real validation period.** Judging candidates
on it would fit the model to the very window meant to test it, which is how
this project has been fooled before. Instead the training half is cut into
purged time-series folds: each fold trains on its earlier part and scores on a
later block, with the label horizon removed at the seam. A feature is accepted
only if it raises the mean fold IC by more than MIN_GAIN, so noise-sized
improvements cannot accumulate into a large model.

The real validation is scored once, at the end, and reported with a block
bootstrap that assumes nothing about independence.

Usage:
    python -m modeling.experiments.forward_selection --symbol XRPUSDT
    python -m modeling.experiments.forward_selection --symbol XRPUSDT --linear
"""

import argparse
import logging
import sys
import warnings

import lightgbm as lgb
import numpy as np
import pandas as pd
import statsmodels.api as sm

from modeling.experiments.single_coin_regression import (
    PURGE_H,
    TRAIN_FRAC,
    load,
)

logger = logging.getLogger(__name__)

SEED = 42
N_FOLDS = 4
MAX_FEATURES = 12
MAX_CANDIDATES = 30       # how far down the MI ranking each round looks
MIN_GAIN = 0.002          # a candidate must beat this to be accepted
MI_SAMPLE = 40_000

TREE: dict = {
    "objective": "regression",
    "n_estimators": 300,
    "learning_rate": 0.02,
    "num_leaves": 4,
    "min_child_samples": 2000,
    "reg_lambda": 100.0,
    "subsample": 0.7,
    "subsample_freq": 1,
    "colsample_bytree": 0.8,
    "verbosity": -1,
    "n_jobs": 4,
    "random_state": SEED,
}


def mi_order(tr: pd.DataFrame, cols: list[str]) -> list[str]:
    """Candidate order: mutual information, computed on the training half."""
    from sklearn.feature_selection import (
        mutual_info_classif,
        mutual_info_regression,
    )
    samp = tr.sample(min(MI_SAMPLE, len(tr)), random_state=SEED)
    y = samp["y"].to_numpy()
    # The ordered -1/0/+1 label is a classification target; the forward return
    # is not, and the classifier version refuses it outright.
    est = (mutual_info_classif
           if set(np.unique(y)) <= {-1.0, 0.0, 1.0}
           else mutual_info_regression)
    mi = est(samp[cols].to_numpy(dtype="float32"), y,
             discrete_features="auto", random_state=SEED)
    return [c for c, _ in sorted(zip(cols, mi, strict=True), key=lambda kv: -kv[1])]


def purged_folds(n: int, k: int, purge: int) -> list[tuple[np.ndarray, np.ndarray]]:
    """Expanding-window folds with the label horizon cut out at each seam."""
    out = []
    block = n // (k + 1)
    for i in range(1, k + 1):
        tr_end = block * i
        te_start, te_end = tr_end, min(tr_end + block, n)
        if te_end - te_start < 200:
            continue
        out.append((np.arange(0, max(tr_end - purge, 1)),
                    np.arange(te_start, te_end)))
    return out


def _cv_ic(df: pd.DataFrame, feats: list[str], folds, linear: bool) -> float:
    """Mean out-of-fold IC -- the only thing a candidate is judged on."""
    ics = []
    for tr_idx, te_idx in folds:
        a, b = df.iloc[tr_idx], df.iloc[te_idx]
        y_a, y_b = a["y"].to_numpy(), b["y"].to_numpy()
        if linear:
            x_a = sm.add_constant(a[feats].to_numpy(dtype="float64"),
                                  has_constant="add")
            x_b = sm.add_constant(b[feats].to_numpy(dtype="float64"),
                                  has_constant="add")
            pred = np.asarray(sm.OLS(y_a, x_a).fit().predict(x_b))
        else:
            model = lgb.LGBMRegressor(**TREE)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                model.fit(a[feats], y_a)
            pred = np.asarray(model.predict(b[feats]))
        if pred.std() < 1e-12:
            ics.append(0.0)
        else:
            ics.append(float(np.corrcoef(pred, y_b)[0, 1]))
    return float(np.mean(ics))


def run(symbol: str, linear: bool, target: str = "y") -> None:
    df, cols = load(symbol)
    df = df.rename(columns={target: "_y"}).assign(y=lambda d: d["_y"])
    ts = pd.to_datetime(df["open_time"])
    cut = ts.iloc[int(len(df) * TRAIN_FRAC)]
    tr = df[ts < cut - pd.Timedelta(hours=PURGE_H)].reset_index(drop=True)
    va = df[ts >= cut].reset_index(drop=True)

    order = mi_order(tr, cols)
    folds = purged_folds(len(tr), N_FOLDS, PURGE_H)

    kind = "OLS" if linear else "LightGBM"
    kind = f"{kind}, target={target}"
    print("\n" + "=" * 82)
    print(f"FORWARD SELECTION — {symbol}, {kind}")
    print("=" * 82)
    print(f"  TRAIN {len(tr):>7,} sor    VALID {len(va):>7,} sor")
    print(f"  {len(folds)} purge-olt CV fold a trainen belul, "
          f"foldonkent ~{len(folds[0][1]):,} teszt sor")
    print(f"  jelolt: {len(cols)} valtozo, MI szerint rendezve; "
          f"koronkent az elso {MAX_CANDIDATES}-t probaljuk")
    print(f"  elfogadas: a CV IC-t legalabb {MIN_GAIN} -tel javitsa\n")

    chosen: list[str] = []
    best_ic = 0.0
    print(f"  {'kor':>4}{'valtozo':<38}{'CV IC':>10}{'javulas':>10}")
    for _ in range(MAX_FEATURES):
        cands = [c for c in order if c not in chosen][:MAX_CANDIDATES]
        scored = [(c, _cv_ic(tr, [*chosen, c], folds, linear)) for c in cands]
        cand, ic = max(scored, key=lambda kv: kv[1])
        gain = ic - best_ic
        if gain < MIN_GAIN:
            print(f"  {'-':>4}{'(nincs tobb javito valtozo)':<38}"
                  f"{ic:>+10.4f}{gain:>+10.4f}")
            break
        chosen.append(cand)
        best_ic = ic
        print(f"  {len(chosen):>4}{cand:<38}{ic:>+10.4f}{gain:>+10.4f}")

    if not chosen:
        print("\n  Egyetlen valtozo sem javit a CV-n. Nincs modell.")
        return

    print(f"\n  vegleges keszlet: {len(chosen)} valtozo   CV IC {best_ic:+.4f}")

    # One pass on the untouched validation period.
    y_tr, y_va = tr["y"].to_numpy(), va["y"].to_numpy()
    if linear:
        x_tr = sm.add_constant(tr[chosen].to_numpy(dtype="float64"),
                               has_constant="add")
        x_va = sm.add_constant(va[chosen].to_numpy(dtype="float64"),
                               has_constant="add")
        model = sm.OLS(y_tr, x_tr).fit()
        p_tr, p_va = np.asarray(model.predict(x_tr)), np.asarray(model.predict(x_va))
    else:
        model = lgb.LGBMRegressor(**TREE)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(tr[chosen], y_tr)
        p_tr = np.asarray(model.predict(tr[chosen]))
        p_va = np.asarray(model.predict(va[chosen]))

    print("\n=== TRAIN vs VALID ===")
    for nev, pred, y in (("TRAIN", p_tr, y_tr), ("VALID", p_va, y_va)):
        ic = float(np.corrcoef(pred, y)[0, 1])
        print(f"  {nev:<7}{len(y):>8,} sor   IC {ic:>+7.4f}")
        for q, side, sign in ((0.95, "top  p95", 1), (0.05, "also p05", -1)):
            thr = float(np.quantile(pred, q))
            sel = pred >= thr if sign == 1 else pred <= thr
            if sel.sum() < 30:
                continue
            if set(np.unique(y)) <= {-1.0, 0.0, 1.0}:
                base = float((y == sign).mean())
                hit = float((y[sel] == sign).mean())
                print(f"          {side}  n={int(sel.sum()):>5}  "
                      f"talalat {hit * 100:>5.1f}%   alap {base * 100:>5.1f}%"
                      f"   lift {(hit - base) * 100:>+6.1f}pp")
            else:
                # For a continuous target the tradeable number is the mean
                # outcome in the slice, signed by the side being taken.
                print(f"          {side}  n={int(sel.sum()):>5}  "
                      f"atlagos kimenet {y[sel].mean():>+9.1f}   "
                      f"osszes atlag {y.mean():>+9.1f}   "
                      f"tobblet {sign * (y[sel].mean() - y.mean()):>+9.1f}")

    rng = np.random.default_rng(SEED)
    block = 30 * 24
    n_blocks = max(1, len(y_va) // block)
    boots = np.array([
        np.corrcoef(p_va[i], y_va[i])[0, 1]
        for i in (
            np.concatenate([np.arange(s, s + block)
                            for s in rng.integers(0, len(y_va) - block, n_blocks)])
            for _ in range(2000)
        )
    ])
    ic = float(np.corrcoef(p_va, y_va)[0, 1])
    print("\n  blokk-bootstrap a valid IC-re (30 napos blokkok):")
    print(f"    IC {ic:+.4f}   szoras {boots.std(ddof=1):.4f}   "
          f"t = {ic / boots.std(ddof=1):.2f}   P(IC>0) = {(boots > 0).mean():.1%}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Forward selection egy coinon")
    parser.add_argument("--symbol", default="XRPUSDT")
    parser.add_argument("--target", default="y",
                        choices=["y", "y_ret", "y_ret_norm"],
                        help="y = a -1/0/+1 cimke, y_ret = nyers 24h "
                             "log hozam bps-ben, y_ret_norm = ugyanaz "
                             "a sajat korlatjara normalizalva")
    parser.add_argument("--linear", action="store_true",
                        help="OLS a LightGBM helyett, ugyanazzal a valogatassal")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.symbol, args.linear, args.target)


if __name__ == "__main__":
    main()
