#!/usr/bin/env python3
"""WoE-transformed linear models on one coin (epic_064, alapmodell-valtozatok).

The discretised barrier label has beaten every continuous target tried so far:
on XRP a three-variable OLS keeps 94% of its training IC out of sample
(+0.041 -> +0.038), while the same recipe on the raw 24h log return lands at
-0.047. Discretising the *target* removed the volatility component that kept
dragging the model off direction.

This asks whether discretising the *features* helps too. Weight of evidence
bins each feature and replaces it with the log-odds of its bin, which lets a
linear model express a non-monotone relationship -- a U-shape, or a signal that
only lives in the tails -- without any of the freedom that let the trees
memorise the window. Binning runs with no monotonicity constraint, precisely so
those U shapes survive.

Three variants, all on the same coin, split and target:

    A  OLS on the raw percentile features        (the current baseline)
    B  OLS on WoE features, forward selection    (same procedure, new inputs)
    C  Lasso on all WoE features                 (penalty instead of selection)

⚠️ **The winner is chosen on cross-validated IC inside the training half, never
on the validation period.** Picking the variant with the best validation IC
would fit the choice to the window meant to test it; each variant's validation
IC is printed once, for information, after the choice is already made.

WoE bins are fitted on training rows only. The binary used for binning is
"upper barrier first" against "lower barrier first", with timeouts left out of
the binning itself -- they carry no direction and would only blur the odds.

Usage:
    python -m modeling.experiments.woe_models --symbol XRPUSDT
"""

import argparse
import logging
import sys
import warnings

import numpy as np
import pandas as pd
import statsmodels.api as sm
from optbinning import OptimalBinning
from sklearn.linear_model import LassoCV

from modeling.experiments.forward_selection import (
    MAX_CANDIDATES,
    MAX_FEATURES,
    MIN_GAIN,
    N_FOLDS,
    SEED,
    mi_order,
    purged_folds,
)
from modeling.experiments.single_coin_regression import (
    PURGE_H,
    TRAIN_FRAC,
    load,
)

logger = logging.getLogger(__name__)

MAX_BINS = 6          # few bins: the effective sample is ~2,350 observations
MIN_BIN_FRAC = 0.05   # no bin may hold less than 5% of the rows
# No monotonicity constraint. The whole reason to bin is that some features
# relate to direction in a U shape -- extreme in either direction means one
# thing, the middle another -- and forcing an ascending or descending trend
# would flatten exactly the structure this transform exists to capture.
# optbinning still regularises through MAX_BINS and MIN_BIN_FRAC.
MONOTONIC_TREND = None


def fit_woe(tr: pd.DataFrame, va: pd.DataFrame, cols: list[str]) -> tuple:
    """Bin every feature on the training half and return WoE-coded frames.

    The binary target is +1 against -1; timeouts are excluded from the fitting
    because they say nothing about direction, but every row still gets a WoE
    value from the resulting bins.
    """
    y_tr = tr["y"].to_numpy()
    mask = y_tr != 0
    y_bin = (y_tr[mask] > 0).astype(int)

    out_tr, out_va, kept = {}, {}, []
    shapes: dict[str, int] = {}
    for c in cols:
        x = tr[c].to_numpy(dtype="float64")
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                ob = OptimalBinning(
                    name=c, dtype="numerical", max_n_bins=MAX_BINS,
                    min_bin_size=MIN_BIN_FRAC, monotonic_trend=MONOTONIC_TREND,
                )
                ob.fit(x[mask], y_bin)
            if ob.status != "OPTIMAL":
                continue
            t_tr = ob.transform(x, metric="woe")
            t_va = ob.transform(va[c].to_numpy(dtype="float64"), metric="woe")
        except Exception:  # noqa: BLE001 - a feature that will not bin is dropped
            continue
        if not np.isfinite(t_tr).all() or np.nanstd(t_tr) < 1e-9:
            continue
        out_tr[c], out_va[c] = t_tr, t_va
        kept.append(c)
        # Record whether the WoE actually turned around inside the feature:
        # a sign change means the relationship is not monotone.
        # The binning table ends with Special / Missing / Totals rows and comes
        # back as object dtype, so it needs coercing before any float maths.
        w = pd.to_numeric(
            ob.binning_table.build()["WoE"].iloc[:-3], errors="coerce"
        ).to_numpy(dtype="float64")
        w = w[np.isfinite(w)]
        if w.size >= 3:
            turns = int((np.diff(np.sign(np.diff(w))) != 0).sum())
            shapes[c] = turns

    a = tr[["open_time", "y"]].reset_index(drop=True).assign(**out_tr)
    b = va[["open_time", "y"]].reset_index(drop=True).assign(**out_va)
    return a, b, kept, shapes


def _ols_cv_ic(df: pd.DataFrame, feats: list[str], folds) -> float:
    ics = []
    for tr_idx, te_idx in folds:
        a, b = df.iloc[tr_idx], df.iloc[te_idx]
        x_a = sm.add_constant(a[feats].to_numpy(dtype="float64"), has_constant="add")
        x_b = sm.add_constant(b[feats].to_numpy(dtype="float64"), has_constant="add")
        pred = np.asarray(sm.OLS(a["y"].to_numpy(), x_a).fit().predict(x_b))
        ics.append(0.0 if pred.std() < 1e-12
                   else float(np.corrcoef(pred, b["y"].to_numpy())[0, 1]))
    return float(np.mean(ics))


def _forward(df: pd.DataFrame, order: list[str], folds) -> tuple[list[str], float]:
    chosen: list[str] = []
    best = 0.0
    for _ in range(MAX_FEATURES):
        cands = [c for c in order if c not in chosen][:MAX_CANDIDATES]
        if not cands:
            break
        scored = [(c, _ols_cv_ic(df, [*chosen, c], folds)) for c in cands]
        cand, ic = max(scored, key=lambda kv: kv[1])
        if ic - best < MIN_GAIN:
            break
        chosen.append(cand)
        best = ic
    return chosen, best


def _lasso_cv_ic(df: pd.DataFrame, feats: list[str], folds) -> float:
    ics = []
    for tr_idx, te_idx in folds:
        a, b = df.iloc[tr_idx], df.iloc[te_idx]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            m = LassoCV(cv=3, random_state=SEED, n_alphas=30, max_iter=5000)
            m.fit(a[feats].to_numpy(dtype="float64"), a["y"].to_numpy())
        pred = m.predict(b[feats].to_numpy(dtype="float64"))
        ics.append(0.0 if pred.std() < 1e-12
                   else float(np.corrcoef(pred, b["y"].to_numpy())[0, 1]))
    return float(np.mean(ics))


def _valid_ic(pred: np.ndarray, y: np.ndarray) -> float:
    return float(np.corrcoef(pred, y)[0, 1])


def _slice_report(pred: np.ndarray, y: np.ndarray) -> str:
    parts = []
    for q, sign in ((0.95, 1), (0.05, -1)):
        thr = float(np.quantile(pred, q))
        sel = pred >= thr if sign == 1 else pred <= thr
        if sel.sum() < 30:
            continue
        lift = (float((y[sel] == sign).mean()) - float((y == sign).mean())) * 100
        parts.append(f"{'p95' if sign == 1 else 'p05'} {lift:+.1f}pp")
    return "  ".join(parts)


def run(symbol: str) -> None:
    df, cols = load(symbol)
    ts = pd.to_datetime(df["open_time"])
    cut = ts.iloc[int(len(df) * TRAIN_FRAC)]
    tr = df[ts < cut - pd.Timedelta(hours=PURGE_H)].reset_index(drop=True)
    va = df[ts >= cut].reset_index(drop=True)
    folds = purged_folds(len(tr), N_FOLDS, PURGE_H)

    print("\n" + "=" * 84)
    print(f"WoE VALTOZATOK — {symbol}")
    print("=" * 84)
    print(f"  TRAIN {len(tr):>7,} sor    VALID {len(va):>7,} sor    "
          f"{len(folds)} purge-olt CV fold")

    logger.info("WoE binning %d valtozora (csak a trainen illesztve)...", len(cols))
    tr_w, va_w, woe_cols, shapes = fit_woe(tr, va, cols)
    n_nonmono = sum(1 for v in shapes.values() if v > 0)
    print(f"  WoE binning: {len(cols)} -> {len(woe_cols)} valtozo sikeresen binelve")
    print(f"  ebbol NEM monoton (a WoE iranyt valt a binek kozott): "
          f"{n_nonmono} ({n_nonmono / max(len(woe_cols), 1):.0%})")

    results = []

    # --- A: the existing baseline, raw percentile features -------------------
    order_raw = mi_order(tr, cols)
    sel_a, cv_a = _forward(tr, order_raw, folds)
    x_tr = sm.add_constant(tr[sel_a].to_numpy(dtype="float64"), has_constant="add")
    x_va = sm.add_constant(va[sel_a].to_numpy(dtype="float64"), has_constant="add")
    m_a = sm.OLS(tr["y"].to_numpy(), x_tr).fit()
    p_a = np.asarray(m_a.predict(x_va))
    results.append(("A  OLS, nyers feature", len(sel_a), cv_a,
                    _valid_ic(p_a, va["y"].to_numpy()), p_a, sel_a))

    # --- B: same procedure, WoE inputs ---------------------------------------
    order_woe = mi_order(tr_w, woe_cols)
    sel_b, cv_b = _forward(tr_w, order_woe, folds)
    x_tr = sm.add_constant(tr_w[sel_b].to_numpy(dtype="float64"), has_constant="add")
    x_va = sm.add_constant(va_w[sel_b].to_numpy(dtype="float64"), has_constant="add")
    m_b = sm.OLS(tr_w["y"].to_numpy(), x_tr).fit()
    p_b = np.asarray(m_b.predict(x_va))
    results.append(("B  OLS, WoE feature", len(sel_b), cv_b,
                    _valid_ic(p_b, va["y"].to_numpy()), p_b, sel_b))

    # --- C: Lasso over every WoE feature, penalty instead of selection -------
    cv_c = _lasso_cv_ic(tr_w, woe_cols, folds)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        lasso = LassoCV(cv=3, random_state=SEED, n_alphas=30, max_iter=5000)
        lasso.fit(tr_w[woe_cols].to_numpy(dtype="float64"), tr_w["y"].to_numpy())
    p_c = lasso.predict(va_w[woe_cols].to_numpy(dtype="float64"))
    n_nz = int((lasso.coef_ != 0).sum())
    results.append(("C  Lasso, WoE feature", n_nz, cv_c,
                    _valid_ic(p_c, va["y"].to_numpy()), p_c, None))

    print("\n=== A HAROM VALTOZAT ===")
    print(f"  {'valtozat':<24}{'valtozo':>9}{'CV IC':>10}{'IC valid':>11}   szelek")
    for name, n, cv, vic, pred, _ in results:
        print(f"  {name:<24}{n:>9}{cv:>+10.4f}{vic:>+11.4f}   "
              f"{_slice_report(pred, va['y'].to_numpy())}")

    best = max(results, key=lambda r: r[2])
    print(f"\n  A CV IC alapjan valasztott: {best[0]}   (CV {best[2]:+.4f})")
    print(f"  ennek a valid IC-je: {best[3]:+.4f}")

    if best[5]:
        print(f"\n  a {len(best[5])} valasztott valtozo:")
        for i, c in enumerate(best[5], 1):
            print(f"    {i:>3}. {c}")
    elif best[0].startswith("C"):
        coefs = pd.Series(lasso.coef_, index=woe_cols)
        coefs = coefs[coefs != 0].abs().sort_values(ascending=False)
        print(f"\n  a Lasso {n_nz} nem-nulla egyutthatoja kozul a 10 legnagyobb:")
        for i, (c, v) in enumerate(coefs.head(10).items(), 1):
            print(f"    {i:>3}. {c:<36}|coef| = {v:.4f}")

    rng = np.random.default_rng(SEED)
    y_va = va["y"].to_numpy()
    block = 30 * 24
    n_blocks = max(1, len(y_va) // block)
    boots = np.array([
        np.corrcoef(best[4][i], y_va[i])[0, 1]
        for i in (
            np.concatenate([np.arange(s, s + block)
                            for s in rng.integers(0, len(y_va) - block, n_blocks)])
            for _ in range(2000)
        )
    ])
    print("\n  blokk-bootstrap a valasztott valtozat valid IC-jere:")
    print(f"    IC {best[3]:+.4f}   szoras {boots.std(ddof=1):.4f}   "
          f"t = {best[3] / boots.std(ddof=1):.2f}   P(IC>0) = {(boots > 0).mean():.1%}")


def main() -> None:
    parser = argparse.ArgumentParser(description="WoE valtozatok egy coinon")
    parser.add_argument("--symbol", default="XRPUSDT")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.symbol)


if __name__ == "__main__":
    main()
