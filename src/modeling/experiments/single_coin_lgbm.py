#!/usr/bin/env python3
"""Regularised LightGBM on one coin (epic_064, coinonkenti fa-modell).

The linear baseline on XRP held its IC out of sample (0.0407 -> 0.0381) but is
too rigid to find anything non-linear. The first tree attempt went the other
way: 232 features and 95 leaves memorised the training window outright. This
sits between the two -- a tree, but sized for the sample that actually exists.

That sample is small. XRP has 32,576 hourly training rows, but the residuals
stay correlated for a full day, inflating the variance 13.9x, so the effective
sample is about 2,350 independent observations. A model tuned as though it had
32,000 will fit noise every time.

Three things follow, and all of them are decided before the real validation
period is touched:

**Selection happens on an inner split.** The training half is split again, and
both the hyperparameters and the feature pruning are judged on that inner
validation. The real validation period is scored exactly once, at the end.

**Regularisation is swept, not guessed.** A small grid over leaf count, minimum
leaf size and L2 penalty; the winner is the one with the best inner-validation
IC, with the train/inner gap reported alongside so over-fitting is visible
rather than inferred.

**Zero-importance features are cut, repeatedly.** A feature the trees never
split on contributes nothing but gives the next fit another chance to latch
onto noise. Pruning and refitting continues until the set stops shrinking.

Usage:
    python -m modeling.experiments.single_coin_lgbm --symbol XRPUSDT
"""

import argparse
import logging
import sys
import warnings

import lightgbm as lgb
import numpy as np
import pandas as pd

from modeling.experiments.single_coin_regression import (
    PURGE_H,
    TRAIN_FRAC,
    _mi_filter,
    load,
)

logger = logging.getLogger(__name__)

SEED = 42
INNER_FRAC = 0.80
FIXED: dict = {
    "objective": "regression",
    "n_estimators": 3000,
    "learning_rate": 0.01,
    "subsample": 0.7,
    "subsample_freq": 1,
    "colsample_bytree": 0.5,
    "verbosity": -1,
    "n_jobs": 4,
    "random_state": SEED,
}
# Deliberately small. With ~2,350 effective observations, anything past a
# handful of leaves is fitting the sample rather than the market.
GRID = [
    {"num_leaves": 3, "min_child_samples": 2000, "reg_lambda": 50.0},
    {"num_leaves": 3, "min_child_samples": 4000, "reg_lambda": 200.0},
    {"num_leaves": 4, "min_child_samples": 1000, "reg_lambda": 20.0},
    {"num_leaves": 4, "min_child_samples": 2000, "reg_lambda": 100.0},
    {"num_leaves": 4, "min_child_samples": 4000, "reg_lambda": 200.0},
    {"num_leaves": 8, "min_child_samples": 1000, "reg_lambda": 50.0},
    {"num_leaves": 8, "min_child_samples": 2000, "reg_lambda": 200.0},
    {"num_leaves": 16, "min_child_samples": 2000, "reg_lambda": 200.0},
]


def _ic(model, x, y) -> float:
    return float(np.corrcoef(np.asarray(model.predict(x)), y)[0, 1])


def _fit(params: dict, x_tr, y_tr, x_va, y_va) -> lgb.LGBMRegressor:
    model = lgb.LGBMRegressor(**{**FIXED, **params})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model.fit(x_tr, y_tr, eval_set=[(x_va, y_va)],
                  callbacks=[lgb.early_stopping(100, verbose=False),
                             lgb.log_evaluation(period=-1)])
    return model


def _purged_split(df: pd.DataFrame, frac: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    ts = pd.to_datetime(df["open_time"])
    cut = ts.iloc[int(len(df) * frac)]
    return df[ts < cut - pd.Timedelta(hours=PURGE_H)], df[ts >= cut]


def run(symbol: str) -> None:
    df, cols = load(symbol)
    tr, va = _purged_split(df, TRAIN_FRAC)
    inner_tr, inner_va = _purged_split(tr, INNER_FRAC)

    feats = _mi_filter(inner_tr, cols)
    print("\n" + "=" * 78)
    print(f"LIGHTGBM EGY COINON — {symbol}")
    print("=" * 78)
    print(f"  TRAIN        {len(tr):>7,} sor   "
          f"{pd.to_datetime(tr['open_time']).min().date()} .. "
          f"{pd.to_datetime(tr['open_time']).max().date()}")
    print(f"    belso train{len(inner_tr):>7,} sor")
    print(f"    belso valid{len(inner_va):>7,} sor   <- ezen valasztunk mindent")
    print(f"  VALID        {len(va):>7,} sor   "
          f"{pd.to_datetime(va['open_time']).min().date()} .. "
          f"{pd.to_datetime(va['open_time']).max().date()}   <- egyszer nezzuk meg")
    print(f"  MI-szures: {len(cols)} -> {len(feats)} valtozo")

    x_itr, y_itr = inner_tr[feats], inner_tr["y"].to_numpy()
    x_iva, y_iva = inner_va[feats], inner_va["y"].to_numpy()

    print("\n=== REGULARIZACIO — a belso validon valasztva ===")
    print(f"  {'levelek':>8}{'min_leaf':>10}{'lambda':>9}{'fak':>6}"
          f"{'IC belso train':>16}{'IC belso valid':>16}{'kulonbseg':>11}")
    best, best_ic = None, -np.inf
    for params in GRID:
        m = _fit(params, x_itr, y_itr, x_iva, y_iva)
        ic_t, ic_v = _ic(m, x_itr, y_itr), _ic(m, x_iva, y_iva)
        print(f"  {params['num_leaves']:>8}{params['min_child_samples']:>10}"
              f"{params['reg_lambda']:>9.0f}{m.best_iteration_:>6}"
              f"{ic_t:>+16.4f}{ic_v:>+16.4f}{ic_t - ic_v:>+11.4f}")
        if ic_v > best_ic:
            best, best_ic = params, ic_v
    assert best is not None
    print(f"\n  valasztott: {best}")

    print("\n=== VALTOZO-REDUKCIO — a nulla fontossagu oszlopok kivagasa ===")
    print(f"  {'kor':>4}{'valtozo':>9}{'hasznalt':>10}{'IC belso train':>16}"
          f"{'IC belso valid':>16}")
    cur = list(feats)
    for rnd in range(1, 8):
        m = _fit(best, inner_tr[cur], y_itr, inner_va[cur], y_iva)
        imp = pd.Series(m.booster_.feature_importance("split"), index=cur)
        used = [c for c in cur if imp[c] > 0]
        print(f"  {rnd:>4}{len(cur):>9}{len(used):>10}"
              f"{_ic(m, inner_tr[cur], y_itr):>+16.4f}"
              f"{_ic(m, inner_va[cur], y_iva):>+16.4f}")
        if len(used) == len(cur):
            break
        cur = used

    print(f"\n  vegleges valtozokeszlet: {len(cur)}")
    final_imp = pd.Series(
        _fit(best, inner_tr[cur], y_itr, inner_va[cur], y_iva)
        .booster_.feature_importance("gain"), index=cur)
    final_imp = (final_imp / final_imp.sum() * 100).sort_values(ascending=False)
    for i, (name, v) in enumerate(final_imp.head(15).items(), 1):
        print(f"    {i:>3}. {name:<36}{v:>6.2f}%")

    # One pass on the real validation, with the model refit on the whole
    # training half using the settings the inner split chose.
    model = _fit(best, inner_tr[cur], y_itr, inner_va[cur], y_iva)
    n_trees = model.best_iteration_
    final = lgb.LGBMRegressor(**{**FIXED, **best, "n_estimators": n_trees})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        final.fit(tr[cur], tr["y"].to_numpy())

    print("\n=== A VEGSO MODELL — TRAIN vs VALID ===")
    for nev, d in (("TRAIN", tr), ("VALID", va)):
        y = d["y"].to_numpy()
        pred = np.asarray(final.predict(d[cur]))
        ic = float(np.corrcoef(pred, y)[0, 1])
        print(f"  {nev:<7}{len(y):>8,} sor   IC {ic:>+7.4f}")
        for q, side, target in ((0.95, "top    p95", 1), (0.05, "also   p05", -1)):
            thr = float(np.quantile(pred, q))
            sel = pred >= thr if target == 1 else pred <= thr
            if sel.sum() < 30:
                continue
            base = float((y == target).mean())
            hit = float((y[sel] == target).mean())
            print(f"          {side}  n={int(sel.sum()):>5}  talalat {hit * 100:>5.1f}%"
                  f"   alap {base * 100:>5.1f}%   lift {(hit - base) * 100:>+6.1f}pp")

    # A block bootstrap makes no independence assumption, which matters when
    # the residuals stay correlated for a day.
    y_va = va["y"].to_numpy()
    pred = np.asarray(final.predict(va[cur]))
    rng = np.random.default_rng(SEED)
    block = 30 * 24
    n_blocks = max(1, len(y_va) // block)
    boots = np.array([
        np.corrcoef(pred[idx], y_va[idx])[0, 1]
        for idx in (
            np.concatenate([np.arange(s, s + block)
                            for s in rng.integers(0, len(y_va) - block, n_blocks)])
            for _ in range(2000)
        )
    ])
    ic = float(np.corrcoef(pred, y_va)[0, 1])
    print("\n  blokk-bootstrap a valid IC-re (30 napos blokkok):")
    print(f"    IC {ic:+.4f}   szoras {boots.std(ddof=1):.4f}   "
          f"t = {ic / boots.std(ddof=1):.2f}   P(IC>0) = {(boots > 0).mean():.1%}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Regularizalt LightGBM egy coinon")
    parser.add_argument("--symbol", default="XRPUSDT")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.symbol)


if __name__ == "__main__":
    main()
