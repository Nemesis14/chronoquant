#!/usr/bin/env python3
"""Linear baseline on the 30-coin panel (epic_064, alapmodell).

A deliberately weak model, chosen because the gradient-boosted one memorised
its training window: 92-98% hit rate on train against 43-58% on validation. An
OLS with a fixed feature set cannot do that, so whatever edge it shows is a
floor rather than an artefact of its own flexibility.

The target is the barrier label read as an ordered number:

    -1  lower barrier first    0  neither inside 24h    +1  upper barrier first

Ordered, so a linear fit is meaningful, and bounded, so one +2000 bps outlier
cannot drag the coefficients the way a raw forward return would.

⚠️ **Ordinary standard errors would be fiction here.** The label spans 24 hours
and the panel is sampled hourly, so consecutive residuals overlap in 23 of
their 24 hours, and the thirty coins move together at about 0.65 correlation
within any hour. Plain OLS assumes independence and would shrink every standard
error several-fold, letting essentially every feature clear p < 0.01 and making
the filter select nothing. Standard errors are therefore clustered by calendar
day: one cluster holds all thirty coins across all twenty-four hours, absorbing
both the cross-sectional and the serial dependence.

The train/validation seam is purged by the label horizon, so no training row
describes a future that reaches into the validation period.

Features arrive as within-coin rolling percentiles in [0, 1], so coefficients
are directly comparable without further standardisation.

Usage:
    python -m modeling.experiments.baseline_regression --fit
"""

import argparse
import json
import logging
import os
import pathlib
import sys

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

import utils
from modeling.experiments.baseline_features import (
    DB_REL,
    TABLE,
    exceeds_horizon,
    is_calendar,
)

logger = logging.getLogger(__name__)

SEARCH_START = "2021-12-01"
SEARCH_END = "2023-06-30"
EVAL_START = "2023-07-01"
EVAL_END = "2026-07-31"
P_MAX = 0.01
PURGE_H = 24
SELECTED_JSON = os.path.join("artifacts", "_experiments", "e064_base_selected.json")
OUT_JSON = os.path.join("artifacts", "_experiments", "e064_base_regression.json")


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)


def load(cols: list[str], start: str = SEARCH_START,
         end: str = SEARCH_END) -> pd.DataFrame:
    conn = _conn()
    try:
        sel = ", ".join(f'f."{c}"' for c in cols)
        return conn.execute(f"""
            SELECT f.symbol, f.open_time, {sel},
                   t.tb_label_1440_vol::DOUBLE AS y
            FROM {TABLE} f JOIN target_5m t USING (symbol, open_time)
            WHERE t.tb_label_1440_vol IS NOT NULL
              AND f.open_time >= '{start}' AND f.open_time <= '{end}'
            ORDER BY f.open_time, f.symbol
        """).df()
    finally:
        conn.close()


def _split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:  # noqa: ANN401
    """Chronological 80/20 split with the label horizon purged at the seam."""
    ts = pd.to_datetime(df["open_time"])
    cut_at = ts.iloc[int(len(df) * 0.80)]
    tr = df[ts < cut_at - pd.Timedelta(hours=PURGE_H)]
    va = df[ts >= cut_at]
    return tr, va


def _report_fit(name: str, model, x, y) -> None:
    """The same diagnostics for train and validation, side by side."""
    pred = np.asarray(model.predict(x))
    ic = float(np.corrcoef(pred, y)[0, 1])
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot
    print(f"  {name:<7}{len(y):>9,} sor   R^2 {r2:>8.5f}   IC {ic:>7.4f}")

    # What an entry rule would actually consume: the hit rate in the extreme
    # slices of the score, against the base rate of that direction.
    for q, side, target in ((0.99, "top    p99", 1), (0.01, "bottom p01", -1)):
        thr = float(np.quantile(pred, q))
        sel = pred >= thr if target == 1 else pred <= thr
        if sel.sum() < 50:
            continue
        base = float((y == target).mean())
        hit = float((y[sel] == target).mean())
        print(f"          {side}  n={int(sel.sum()):>6}  talalat {hit * 100:>5.1f}%"
              f"   alap {base * 100:>5.1f}%   lift {(hit - base) * 100:>+6.1f}pp")


def fit() -> None:
    conn = _conn()
    try:
        cols = [
            r[0] for r in conn.execute(f"DESCRIBE {TABLE}").fetchall()
            if r[0].startswith("feat_")
            and not is_calendar(r[0]) and not exceeds_horizon(r[0])
        ]
    finally:
        conn.close()

    path = pathlib.Path(utils._repo_root()) / SELECTED_JSON
    if path.exists():
        cols = json.loads(path.read_text(encoding="utf-8"))
        logger.info("a feature-lista az MI-szuresbol: %d valtozo", len(cols))

    df = load(cols).dropna().reset_index(drop=True)
    tr, va = _split(df)
    logger.info("train %s sor, valid %s sor (purge %dh)",
                f"{len(tr):,}", f"{len(va):,}", PURGE_H)

    y_tr = tr["y"].to_numpy()
    x_tr = sm.add_constant(tr[cols].to_numpy(dtype="float64"), has_constant="add")
    day_tr = pd.to_datetime(tr["open_time"]).dt.normalize().to_numpy()

    full = sm.OLS(y_tr, x_tr).fit(
        cov_type="cluster", cov_kwds={"groups": day_tr}, use_t=True
    )
    res = pd.DataFrame({
        "feature": ["const", *cols],
        "coef": full.params, "t": full.tvalues, "p": full.pvalues,
    }).iloc[1:]

    keep = res[res["p"] < P_MAX].sort_values("p")["feature"].tolist()
    bonf = P_MAX / len(cols)
    n_strict = int((res["p"] < bonf).sum())
    naive = sm.OLS(y_tr, x_tr).fit()

    ts = pd.to_datetime(df["open_time"])
    print("\n" + "=" * 78)
    print("LINEARIS ALAPMODELL — OLS, naponkenti klaszter-robusztus SE")
    print("=" * 78)
    print(f"  ablak    {ts.min()}  ..  {ts.max()}")
    print(f"  train    {len(tr):>10,} sor   {len(np.unique(day_tr)):,} nap-klaszter")
    print(f"  valid    {len(va):>10,} sor   (purge {PURGE_H}h a hataron)")
    print(f"  jelolt   {len(cols):>10} valtozo (MI-szures utan)")

    print(f"\n  p < {P_MAX} klaszterelt SE-vel: {len(keep):>4} / {len(cols)}"
          "   <-- ezek maradnak")
    print(f"  p < {P_MAX} naiv SE-vel:        "
          f"{int((naive.pvalues[1:] < P_MAX).sum()):>4} / {len(cols)}"
          "   (ennyit tartana a hibas modszer)")
    print(f"  p < {bonf:.1e} (Bonferroni):  {n_strict:>4} / {len(cols)}")

    if not keep:
        print("\n  Egyetlen valtozo sem szignifikans. Nincs mit modellezni.")
        return

    print(f"\n=== A BENTMARADO {len(keep)} VALTOZO ===")
    print(f"  {'#':>4} {'feature':<34}{'egyutthato':>12}{'t':>8}{'p':>11}")
    sub = res[res["feature"].isin(keep)].sort_values("p")
    for i, (_, r) in enumerate(sub.iterrows(), 1):
        star = " *" if r["p"] < bonf else ""
        print(f"  {i:>4} {r['feature']:<34}{r['coef']:>+12.5f}"
              f"{r['t']:>8.2f}{r['p']:>11.2e}{star}")
    print("\n  * = Bonferroni-korrekcio utan is szignifikans")

    # Refit on the survivors alone, then judge that model out of sample.
    x2_tr = sm.add_constant(tr[keep].to_numpy(dtype="float64"), has_constant="add")
    x2_va = sm.add_constant(va[keep].to_numpy(dtype="float64"), has_constant="add")
    final = sm.OLS(y_tr, x2_tr).fit(
        cov_type="cluster", cov_kwds={"groups": day_tr}, use_t=True
    )

    print(f"\n=== A {len(keep)} VALTOZOS MODELL — TRAIN vs VALID ===")
    _report_fit("TRAIN", final, x2_tr, y_tr)
    _report_fit("VALID", final, x2_va, va["y"].to_numpy())

    out = pathlib.Path(utils._repo_root()) / OUT_JSON
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "n_train": int(len(tr)), "n_valid": int(len(va)),
        "p_max": P_MAX, "kept": keep,
    }, indent=2), encoding="utf-8")
    print(f"\n  mentve: {out.name}")


def rolling() -> None:
    """Monthly walk-forward on the fixed six, refitting but never re-selecting.

    The point is stability, so the feature set stays exactly as the first window
    chose it. Re-picking the survivors each month would measure how well a
    selection procedure adapts, not whether the relationship holds.
    """
    kept = json.loads(
        (pathlib.Path(utils._repo_root()) / OUT_JSON).read_text(encoding="utf-8")
    )["kept"]
    logger.info("fix valtozok: %s", ", ".join(kept))

    df = load(kept, SEARCH_START, EVAL_END).dropna().reset_index(drop=True)
    ts = pd.to_datetime(df["open_time"])
    logger.info("panel: %s sor, %s .. %s", f"{len(df):,}", ts.min(), ts.max())

    rows = []
    for period in pd.period_range(EVAL_START, EVAL_END, freq="M"):
        m0, m1 = period.to_timestamp(), (period + 1).to_timestamp()
        tr = df[ts < m0 - pd.Timedelta(hours=PURGE_H)]
        te = df[(ts >= m0) & (ts < m1)]
        if len(tr) < 50_000 or len(te) < 500:
            continue

        x_tr = sm.add_constant(tr[kept].to_numpy(dtype="float64"), has_constant="add")
        x_te = sm.add_constant(te[kept].to_numpy(dtype="float64"), has_constant="add")
        y_tr, y_te = tr["y"].to_numpy(), te["y"].to_numpy()
        model = sm.OLS(y_tr, x_tr).fit()

        p_tr = np.asarray(model.predict(x_tr))
        p_te = np.asarray(model.predict(x_te))
        ic_tr = float(np.corrcoef(p_tr, y_tr)[0, 1])
        ic_te = float(np.corrcoef(p_te, y_te)[0, 1])

        # The threshold comes from the training scores and is applied to the
        # month unchanged, exactly as an entry rule would have to.
        thr_hi = float(np.quantile(p_tr, 0.99))
        thr_lo = float(np.quantile(p_tr, 0.01))
        s_hi, s_lo = p_te >= thr_hi, p_te <= thr_lo
        rows.append({
            "ho": str(period), "n": len(te),
            "IC_train": round(ic_tr, 4), "IC_valid": round(ic_te, 4),
            "n_hi": int(s_hi.sum()),
            "lift_hi": round((float((y_te[s_hi] == 1).mean())
                              - float((y_te == 1).mean())) * 100, 1)
            if s_hi.sum() >= 20 else np.nan,
            "n_lo": int(s_lo.sum()),
            "lift_lo": round((float((y_te[s_lo] == -1).mean())
                              - float((y_te == -1).mean())) * 100, 1)
            if s_lo.sum() >= 20 else np.nan,
        })

    res = pd.DataFrame(rows)
    print("\n" + "=" * 78)
    print("GORDULO VALIDACIO — fix 6 valtozo, havi ujratanitas")
    print("=" * 78)
    print(res.to_string(index=False))

    ic = res["IC_valid"].to_numpy()
    ic_tr = res["IC_train"].to_numpy()
    t = float(ic.mean() / (ic.std(ddof=1) / np.sqrt(len(ic))))
    print(f"\n  honapok                 {len(res):>8}")
    print(f"  atlagos IC (train)      {ic_tr.mean():>8.4f}")
    print(f"  atlagos IC (valid)      {ic.mean():>8.4f}")
    print(f"  IC szoras (valid)       {ic.std(ddof=1):>8.4f}")
    print(f"  t-statisztika           {t:>8.2f}")
    print(f"  pozitiv IC-ju honap     {int((ic > 0).sum()):>8} / {len(ic)}")

    for col, nev in (("lift_hi", "felso p99 (long)"), ("lift_lo", "also p01 (short)")):
        v = res[col].dropna().to_numpy()
        if v.size:
            tt = float(v.mean() / (v.std(ddof=1) / np.sqrt(len(v))))
            print(f"\n  {nev}: atlagos lift {v.mean():+6.2f}pp  "
                  f"szoras {v.std(ddof=1):5.2f}  t={tt:5.2f}  "
                  f"pozitiv {int((v > 0).sum())}/{len(v)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Linearis alapmodell")
    parser.add_argument("--fit", action="store_true")
    parser.add_argument("--rolling", action="store_true")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.fit:
        fit()
    if args.rolling:
        rolling()


if __name__ == "__main__":
    main()
