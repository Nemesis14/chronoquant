#!/usr/bin/env python3
"""Direction-only draft models (epic_064, irany-modellek).

Same coin, same 24-hour barrier label, same train/validation split as
everything before. Two things change.

**The candidate pool is signed.** Every pure-magnitude feature is dropped --
`natr`, `atr`, `hist_vol`, `parkinson_vol`, `bb_width`, `returns_std`, the
range and volume levels, ADX itself. They dominated mutual information in every
earlier run because they separate the timeout class, and they cannot say which
way price goes. What is left either carries a sign or is thrown out.

**Positioning is included.** The derivatives block adds funding, open interest,
the retail and top-trader long/short ratios, the taker split, and the crowding
combinations built from them. This is the only information in the panel that is
not another transform of the price series.

Four drafts, all judged on cross-validated IC inside the training half and each
scored once on the untouched validation period:

    A  OLS,   directional price features only
    B  OLS,   directional price + positioning
    C  WoE + Lasso, directional price + positioning
    D  WoE + Lasso, positioning only

D exists to answer the attribution question directly: does the derivatives data
carry direction on its own, or only as a modifier of price features?

Usage:
    python -m modeling.experiments.direction_models --symbol XRPUSDT
"""

import argparse
import logging
import os
import sys
import warnings

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm
from optbinning import OptimalBinning
from sklearn.linear_model import LassoCV

import utils
from modeling.experiments.baseline_features import (
    DB_REL,
    TABLE,
    exceeds_horizon,
    is_calendar,
)
from modeling.experiments.direction_features import TABLE as DIR_TABLE
from modeling.experiments.direction_features import is_directional
from modeling.experiments.forward_selection import (
    MAX_CANDIDATES,
    MAX_FEATURES,
    MIN_GAIN,
    N_FOLDS,
    SEED,
    mi_order,
    purged_folds,
)
from modeling.experiments.single_coin_regression import PURGE_H, TRAIN_FRAC
from modeling.experiments.woe_models import (
    MAX_BINS,
    MIN_BIN_FRAC,
    MONOTONIC_TREND,
)

logger = logging.getLogger(__name__)

START = "2021-12-01"
END = "2026-07-31"


def load(symbol: str) -> tuple[pd.DataFrame, list[str], list[str]]:
    """Directional price features, positioning features, and the label."""
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)
    try:
        price = [
            r[0] for r in conn.execute(f"DESCRIBE {TABLE}").fetchall()
            if r[0].startswith("feat_") and not is_calendar(r[0])
            and not exceeds_horizon(r[0]) and is_directional(r[0])
        ]
        pos = [
            r[0] for r in conn.execute(f"DESCRIBE {DIR_TABLE}").fetchall()
            if r[0].startswith("feat_")
        ]
        sel_p = ", ".join(f'f."{c}"' for c in price)
        sel_d = ", ".join(f'd."{c}"' for c in pos)
        df = conn.execute(f"""
            SELECT f.open_time, {sel_p}, {sel_d},
                   t.tb_label_1440_vol::DOUBLE AS y
            FROM {TABLE} f
            JOIN {DIR_TABLE} d USING (symbol, open_time)
            JOIN target_5m t   USING (symbol, open_time)
            WHERE f.symbol = '{symbol}' AND t.tb_label_1440_vol IS NOT NULL
              AND f.open_time >= '{START}' AND f.open_time <= '{END}'
            ORDER BY f.open_time
        """).df()
    finally:
        conn.close()
    return df.dropna().reset_index(drop=True), price, pos


def woe_transform(tr: pd.DataFrame, va: pd.DataFrame, cols: list[str]) -> tuple:
    """Unconstrained optimal binning, fitted on training rows only."""
    y = tr["y"].to_numpy()
    mask = y != 0
    y_bin = (y[mask] > 0).astype(int)
    a, b, kept = {}, {}, []
    for c in cols:
        x = tr[c].to_numpy(dtype="float64")
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                ob = OptimalBinning(name=c, dtype="numerical",
                                    max_n_bins=MAX_BINS, min_bin_size=MIN_BIN_FRAC,
                                    monotonic_trend=MONOTONIC_TREND)
                ob.fit(x[mask], y_bin)
            if ob.status != "OPTIMAL":
                continue
            t_a = ob.transform(x, metric="woe")
            t_b = ob.transform(va[c].to_numpy(dtype="float64"), metric="woe")
        except Exception:  # noqa: BLE001 - unbinnable features are dropped
            continue
        if not np.isfinite(t_a).all() or np.nanstd(t_a) < 1e-9:
            continue
        a[c], b[c] = t_a, t_b
        kept.append(c)
    return (tr[["open_time", "y"]].reset_index(drop=True).assign(**a),
            va[["open_time", "y"]].reset_index(drop=True).assign(**b), kept)


def _ols_cv(df: pd.DataFrame, feats: list[str], folds) -> float:
    ics = []
    for i, j in folds:
        a, b = df.iloc[i], df.iloc[j]
        xa = sm.add_constant(a[feats].to_numpy(dtype="float64"), has_constant="add")
        xb = sm.add_constant(b[feats].to_numpy(dtype="float64"), has_constant="add")
        p = np.asarray(sm.OLS(a["y"].to_numpy(), xa).fit().predict(xb))
        ics.append(0.0 if p.std() < 1e-12
                   else float(np.corrcoef(p, b["y"].to_numpy())[0, 1]))
    return float(np.mean(ics))


def _forward(df: pd.DataFrame, order: list[str], folds) -> tuple[list[str], float]:
    chosen: list[str] = []
    best = 0.0
    for _ in range(MAX_FEATURES):
        cands = [c for c in order if c not in chosen][:MAX_CANDIDATES]
        if not cands:
            break
        cand, ic = max(((c, _ols_cv(df, [*chosen, c], folds)) for c in cands),
                       key=lambda kv: kv[1])
        if ic - best < MIN_GAIN:
            break
        chosen.append(cand)
        best = ic
    return chosen, best


def _lasso_cv(df: pd.DataFrame, feats: list[str], folds) -> float:
    ics = []
    for i, j in folds:
        a, b = df.iloc[i], df.iloc[j]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            m = LassoCV(cv=3, random_state=SEED, n_alphas=30, max_iter=5000)
            m.fit(a[feats].to_numpy(dtype="float64"), a["y"].to_numpy())
        p = m.predict(b[feats].to_numpy(dtype="float64"))
        ics.append(0.0 if p.std() < 1e-12
                   else float(np.corrcoef(p, b["y"].to_numpy())[0, 1]))
    return float(np.mean(ics))


def _slices(pred: np.ndarray, y: np.ndarray) -> str:
    out = []
    for q, sign in ((0.95, 1), (0.05, -1)):
        thr = float(np.quantile(pred, q))
        sel = pred >= thr if sign == 1 else pred <= thr
        if sel.sum() < 30:
            continue
        lift = (float((y[sel] == sign).mean()) - float((y == sign).mean())) * 100
        out.append(f"{'p95' if sign == 1 else 'p05'} {lift:+5.1f}pp")
    return "  ".join(out)


def run(symbol: str) -> None:
    df, price, pos = load(symbol)
    ts = pd.to_datetime(df["open_time"])
    cut = ts.iloc[int(len(df) * TRAIN_FRAC)]
    tr = df[ts < cut - pd.Timedelta(hours=PURGE_H)].reset_index(drop=True)
    va = df[ts >= cut].reset_index(drop=True)
    folds = purged_folds(len(tr), N_FOLDS, PURGE_H)
    y_va = va["y"].to_numpy()

    print("\n" + "=" * 92)
    print(f"IRANY-MODELLEK — {symbol}")
    print("=" * 92)
    print(f"  TRAIN {len(tr):>7,} sor    VALID {len(va):>7,} sor    "
          f"{len(folds)} purge-olt CV fold")
    print(f"  irany-jellegu ar-feature : {len(price):>4}  "
          "(a magnitudo/volatilitas valtozok kiszurve)")
    print(f"  pozicionaltsagi feature  : {len(pos):>4}")

    tr_w, va_w, woe_all = woe_transform(tr, va, price + pos)
    woe_pos = [c for c in woe_all if c in pos]
    print(f"  WoE binning: {len(price) + len(pos)} -> {len(woe_all)} valtozo\n")

    results = []

    def add(name, cv, sel, pred, extra=""):
        results.append((name, len(sel) if sel is not None else extra,
                        cv, float(np.corrcoef(pred, y_va)[0, 1]), pred, sel))

    sel_a, cv_a = _forward(tr, mi_order(tr, price), folds)
    xa = sm.add_constant(tr[sel_a].to_numpy(dtype="float64"), has_constant="add")
    xb = sm.add_constant(va[sel_a].to_numpy(dtype="float64"), has_constant="add")
    add("A  OLS, csak ar", cv_a, sel_a,
        np.asarray(sm.OLS(tr["y"].to_numpy(), xa).fit().predict(xb)))

    both = price + pos
    sel_b, cv_b = _forward(tr, mi_order(tr, both), folds)
    xa = sm.add_constant(tr[sel_b].to_numpy(dtype="float64"), has_constant="add")
    xb = sm.add_constant(va[sel_b].to_numpy(dtype="float64"), has_constant="add")
    add("B  OLS, ar + pozicio", cv_b, sel_b,
        np.asarray(sm.OLS(tr["y"].to_numpy(), xa).fit().predict(xb)))

    cv_c = _lasso_cv(tr_w, woe_all, folds)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        lc = LassoCV(cv=3, random_state=SEED, n_alphas=30, max_iter=5000)
        lc.fit(tr_w[woe_all].to_numpy(dtype="float64"), tr_w["y"].to_numpy())
    add("C  WoE+Lasso, ar+pozicio", cv_c, None,
        lc.predict(va_w[woe_all].to_numpy(dtype="float64")),
        extra=int((lc.coef_ != 0).sum()))

    cv_d = _lasso_cv(tr_w, woe_pos, folds)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ld = LassoCV(cv=3, random_state=SEED, n_alphas=30, max_iter=5000)
        ld.fit(tr_w[woe_pos].to_numpy(dtype="float64"), tr_w["y"].to_numpy())
    add("D  WoE+Lasso, csak pozicio", cv_d, None,
        ld.predict(va_w[woe_pos].to_numpy(dtype="float64")),
        extra=int((ld.coef_ != 0).sum()))

    print(f"  {'draft':<28}{'valtozo':>9}{'CV IC':>10}{'IC valid':>11}   szelek")
    for name, n, cv, vic, pred, _ in results:
        print(f"  {name:<28}{n:>9}{cv:>+10.4f}{vic:>+11.4f}   {_slices(pred, y_va)}")

    best = max(results, key=lambda r: r[2])
    print(f"\n  CV IC alapjan a legjobb: {best[0]}   "
          f"(CV {best[2]:+.4f}, valid {best[3]:+.4f})")

    if best[5]:
        print("\n  a valasztott valtozok:")
        for i, c in enumerate(best[5], 1):
            tag = " [pozicio]" if c in pos else ""
            print(f"    {i:>3}. {c}{tag}")
    else:
        model = lc if best[0].startswith("C") else ld
        feats = woe_all if best[0].startswith("C") else woe_pos
        co = pd.Series(model.coef_, index=feats)
        co = co[co != 0].abs().sort_values(ascending=False)
        print(f"\n  a Lasso {len(co)} nem-nulla egyutthatoja kozul a 12 legnagyobb:")
        for i, (c, v) in enumerate(co.head(12).items(), 1):
            tag = " [pozicio]" if c in pos else ""
            print(f"    {i:>3}. {c:<34}|coef| {v:.4f}{tag}")

    rng = np.random.default_rng(SEED)
    block = 30 * 24
    nb = max(1, len(y_va) // block)
    boots = np.array([
        np.corrcoef(best[4][i], y_va[i])[0, 1]
        for i in (np.concatenate([np.arange(s, s + block)
                                  for s in rng.integers(0, len(y_va) - block, nb)])
                  for _ in range(2000))
    ])
    print("\n  blokk-bootstrap a valasztott draft valid IC-jere:")
    print(f"    IC {best[3]:+.4f}   szoras {boots.std(ddof=1):.4f}   "
          f"t = {best[3] / boots.std(ddof=1):.2f}   P(IC>0) = {(boots > 0).mean():.1%}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Irany-orientalt draft modellek")
    parser.add_argument("--symbol", default="XRPUSDT")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.symbol)


if __name__ == "__main__":
    main()
