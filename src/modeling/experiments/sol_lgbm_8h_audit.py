#!/usr/bin/env python3
"""Why is the train lift +12.31pp and the validation lift +2.22pp? (epic_064)

A 5.6x gap between the two sides of the same metric is either a real regime
break or a sign that the two sides are not measuring the same thing. This
module separates the four explanations that all produce that picture.

**Are they the same quantity?** The p90 threshold is a quantile of the model's
own predictions, and this model is deliberately coarse -- three leaves, 4000
observations minimum per leaf -- so its output is lumpy. `pred >= quantile(.90)`
selects more than a tenth whenever predictions tie at the cut, and it can tie
differently on 25,000 training rows than on 600 validation rows. The realised
selection fraction and the base rate are reported for both sides.

**Is it the same estimator?** The train lift is one number over one large
sample. The validation lift is the mean of 24 monthly numbers, each computed on
roughly 600 rows against that month's own base rate. Those are different
estimators, so the pooled validation lift is reported alongside the monthly
mean.

**In-sample optimism, or a regime break?** This is the one that matters and the
one that was missing. The walk-forward is extended backwards so that months
*inside the training era* are also scored out-of-sample. If the model earns
~2pp there too, the +12.31pp is pure in-sample optimism and nothing changed. If
it earns much more there and collapses after 2024-08, the regime moved.

⚠️ The backwards extension is a diagnostic, not a clean result: the
regularisation grid was chosen on data through 2024-07-31, so the train-era
months are contaminated by that choice. It is fit to answer "did the world
change", not "how much money is there".

Usage:
    python -m modeling.experiments.sol_lgbm_8h_audit
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

import utils
from modeling.experiments.sol_lgbm_8h import FIXED, HORIZON_MIN
from modeling.experiments.sol_lgbm_8h_econ import BEST
from modeling.experiments.sol_short_horizon import (
    DB_REL,
    EVAL_END,
    TABLE,
    TRAIN_END,
    build_labels,
)

logger = logging.getLogger(__name__)

BACK_START = "2023-10"     # earliest month with enough history to fit
MIN_TRAIN = 8000


def _detail(pred: np.ndarray, y: np.ndarray, w: np.ndarray) -> dict:
    """Lift, plus everything needed to check it means the same on both sides."""
    mask = y != 0
    p, yy, ww = pred[mask], y[mask], w[mask]
    if len(yy) < 60 or len(np.unique(yy)) < 2 or p.std() <= 1e-12:
        return {}
    cut = float(np.quantile(p, 0.90))
    sel = p >= cut
    base = float((yy == 1).mean())
    return {
        "n": len(yy),
        "n_sel": int(sel.sum()),
        "frac": float(sel.mean()),          # should be ~0.10; more means ties
        "base": base * 100,
        "hit": float((yy[sel] == 1).mean()) * 100,
        "lift": (float((yy[sel] == 1).mean()) - base) * 100,
        "timeout": float((y == 0).mean()) * 100,
        "cut": cut,
        "p_mean": float(p.mean()),
        "p_std": float(p.std()),
        "w_med": float(np.median(ww)),
    }


def _load() -> tuple[pd.DataFrame, pd.Series, list[str]]:
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
    y, w = f"y{HORIZON_MIN}", f"w{HORIZON_MIN}"
    d = (feats.merge(build_labels(TRAIN_END), on="open_time", how="inner")
         .dropna(subset=[y, *cols]).reset_index(drop=True)
         .rename(columns={y: "y", w: "w"}))
    return d, pd.to_datetime(d["open_time"]), cols


def run() -> None:
    d, dts, cols = _load()
    print("\n" + "=" * 94)
    print("MIERT +12,31pp A TRAINEN ES +2,22pp A VALIDON? — AUDIT")
    print("=" * 94)
    print(f"  {len(d):,} orai sor, {dts.min()} .. {dts.max()}")
    print(f"  modell: {BEST}\n")

    rows, oos_all = [], []
    for period in pd.period_range(BACK_START, EVAL_END, freq="M"):
        m0, m1 = period.to_timestamp(), (period + 1).to_timestamp()
        tr = d.loc[dts < m0 - pd.Timedelta(minutes=HORIZON_MIN)]
        te = d.loc[(dts >= m0) & (dts < m1)]
        if len(tr) < MIN_TRAIN or len(te) < 200:
            continue
        model = lgb.LGBMRegressor(**{**FIXED, **BEST})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(tr[cols], tr["y"].to_numpy())
        a = _detail(np.asarray(model.predict(tr[cols])),
                    tr["y"].to_numpy(), tr["w"].to_numpy())
        pv = np.asarray(model.predict(te[cols]))
        b = _detail(pv, te["y"].to_numpy(), te["w"].to_numpy())
        if not a or not b:
            continue
        era = "train-kor" if m0 <= pd.Timestamp(TRAIN_END) else "valid-kor"
        rows.append({"ho": str(period), "era": era,
                     **{f"{k}_tr": v for k, v in a.items()}, **b})
        oos_all.append(te[["y", "w"]].assign(pred=pv, mo=str(period), era=era))
    r = pd.DataFrame(rows)
    pool = pd.concat(oos_all, ignore_index=True)

    # ---- 1. is it the same quantity on both sides? ------------------------
    print("=== 1. UGYANAZT A SZAMOT SZAMOLJUK? ===")
    print(f"  {'':<12}{'kivalasztott hanyad':>21}{'n_sel':>9}"
          f"{'alaparany (+1%)':>18}{'timeout %':>12}")
    for side, tag in (("_tr", "train"), ("", "valid")):
        print(f"  {tag:<12}{r['frac' + side].mean():>20.3f}"
              f"{r['n_sel' + side].mean():>9.0f}{r['base' + side].mean():>18.2f}"
              f"{r['timeout' + side].mean():>12.2f}")
    print("  (a hanyadnak 0,100 korul kell lennie; tobb = holtverseny a kuszobon)\n")

    # ---- 2. same estimator? ------------------------------------------------
    v = r.loc[r["era"] == "valid-kor"]
    pm = _detail(pool["pred"].to_numpy(), pool["y"].to_numpy(),
                 pool["w"].to_numpy())
    vp = pd.concat([o for o, e in zip(oos_all, r["era"], strict=True) if e == "valid-kor"],
                   ignore_index=True)
    pv_ = _detail(vp["pred"].to_numpy(), vp["y"].to_numpy(), vp["w"].to_numpy())
    print("=== 2. UGYANAZ A BECSLO? (havi atlag vs poololt) ===")
    print(f"  valid-kor, havi liftek atlaga : {v['lift'].mean():+6.2f}pp "
          f"({len(v)} honap, szoras {v['lift'].std(ddof=1):.2f})")
    print(f"  valid-kor, poololt lift       : {pv_['lift']:+6.2f}pp "
          f"({pv_['n']:,} iranyos sor, kivalasztva {pv_['n_sel']:,})")
    print(f"  teljes OOS, poololt lift      : {pm['lift']:+6.2f}pp\n")

    # ---- 3. in-sample optimism, or a regime break? -------------------------
    print("=== 3. IN-SAMPLE OPTIMIZMUS VAGY REZSIMVALTAS? ===")
    print(f"  {'korszak':<12}{'honap':>7}{'lift IN-SAMPLE':>17}"
          f"{'lift OUT-OF-SAMPLE':>21}{'szoras':>9}")
    for era, g in r.groupby("era", sort=True):
        print(f"  {era:<12}{len(g):>7}{g['lift_tr'].mean():>+16.2f}pp"
              f"{g['lift'].mean():>+20.2f}pp{g['lift'].std(ddof=1):>9.2f}")
    a_ = r.loc[r["era"] == "train-kor", "lift"].to_numpy()
    b_ = r.loc[r["era"] == "valid-kor", "lift"].to_numpy()
    opt = float(r.loc[r["era"] == "train-kor", "lift_tr"].mean() - a_.mean())
    se = float(np.sqrt(a_.var(ddof=1) / len(a_) + b_.var(ddof=1) / len(b_)))
    print(f"\n  in-sample optimizmus (in-sample - OOS, train-kor): {opt:+.2f}pp")
    print(f"  korszakok kozti tovabbi eses                     : "
          f"{b_.mean() - a_.mean():+.2f}pp   Welch t = "
          f"{(b_.mean() - a_.mean()) / se:.2f}")
    print("  (|t| < 2 -> a korszakok kulonbsege nem valik el a zajtol)\n")

    # ---- 4. what actually drifted -----------------------------------------
    print("=== 4. MI VALTOZOTT A KET KORSZAK KOZOTT (valid oldal) ===")
    print(f"  {'felev':<10}{'honap':>6}{'+1 arany':>10}{'timeout':>9}"
          f"{'barrier bps':>13}{'pred szoras':>13}{'lift OOS':>11}")
    half = pd.PeriodIndex(r["ho"], freq="M").asfreq("Q").astype(str)
    for k, g in r.groupby(half, sort=True):
        print(f"  {k:<10}{len(g):>6}{g['base'].mean():>9.1f}%"
              f"{g['timeout'].mean():>8.1f}%{g['w_med'].mean():>13.0f}"
              f"{g['p_std'].mean():>13.4f}{g['lift'].mean():>+10.2f}pp")

    print("\n=== 5. HAVI OOS LIFT ===")
    for _, x in r.iterrows():
        bar = "#" * max(int(round(x["lift"] / 2)), 0)
        print(f"  {x['ho']}  {x['era']:<10}{x['lift']:>+7.2f}pp  {bar}")

    # ---- 6. does a longer evaluation block change the answer? --------------
    # The total evidence is fixed -- 24 months of out-of-sample predictions --
    # so no aggregation creates information. What a longer block changes is
    # where the p90 cut falls (across the block instead of within each month)
    # and how the uncertainty is estimated. If the answer moves with the block
    # length, the monthly slicing was doing something; if not, it was not.
    print("\n=== 6. MEKKORA LEGYEN AZ ERTEKELESI BLOKK? (valid-kor) ===")
    vo = pd.concat([o for o in oos_all if o["era"].iloc[0] == "valid-kor"],
                   ignore_index=True)
    months = sorted(vo["mo"].unique())
    print(f"  {'blokk':<10}{'db':>5}{'sor/blokk':>11}{'n_sel':>8}"
          f"{'atlag lift':>13}{'szoras':>9}{'SE':>8}{'t':>7}")
    for k in (1, 3, 6, 12):
        lifts, nsel, nrow = [], [], []
        for i in range(0, len(months) - k + 1, k):
            blk = vo.loc[vo["mo"].isin(months[i:i + k])]
            det = _detail(blk["pred"].to_numpy(), blk["y"].to_numpy(),
                          blk["w"].to_numpy())
            if det:
                lifts.append(det["lift"])
                nsel.append(det["n_sel"])
                nrow.append(det["n"])
        a = np.array(lifts)
        sd = float(a.std(ddof=1)) if len(a) > 1 else float("nan")
        se = sd / np.sqrt(len(a)) if len(a) > 1 else float("nan")
        print(f"  {str(k) + ' honap':<10}{len(a):>5}{np.mean(nrow):>11.0f}"
              f"{np.mean(nsel):>8.0f}{a.mean():>+12.2f}pp{sd:>9.2f}"
              f"{se:>8.2f}{a.mean() / se if se == se else float('nan'):>7.2f}")

    det = _detail(vo["pred"].to_numpy(), vo["y"].to_numpy(), vo["w"].to_numpy())
    print(f"  {'poololt':<10}{1:>5}{det['n']:>11}{det['n_sel']:>8}"
          f"{det['lift']:>+12.2f}pp{'—':>9}{'—':>8}{'—':>7}")
    print("\n  A blokkon beluli p90 mas szabaly: a 12 honapos blokk az ev")
    print("  legjobb 10%-at valasztja, nem minden honap sajat legjobb 10%-at.")


def main() -> None:
    argparse.ArgumentParser(description="train/valid lift audit").parse_args()
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()


if __name__ == "__main__":
    main()
