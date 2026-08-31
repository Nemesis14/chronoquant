#!/usr/bin/env python3
"""What the 8-hour LightGBM earns once it has to pay for its trades (epic_064).

`sol_lgbm_8h` established that the model clears both the rotated-label null and
a random-feature baseline: validation IC +0.0240 against a baseline of +0.0134,
at t = 2.00. That is a statistical result about ranking, measured on every hour
including the timeouts. It says nothing about money, because the only rows we
would ever trade are the tail, and the tail has to pay a spread.

This module closes that gap. It replays the same monthly refit, keeps the
out-of-sample predictions, and applies the decile rule the IC implies: go long
when the score clears the training distribution's 90th percentile, short below
the 10th. The barrier is symmetric, so a trade returns +w, -w or zero, and the
maker cost measured in `execution_cost_gate.md` comes off every one.

⚠️ The thresholds come from the *training* prediction distribution, never from
the month being traded. Taking the top decile of the test month itself would be
peeking at the answer -- it guarantees exactly 10% of trades regardless of
whether the model saw anything worth trading that month.

Significance uses a 30-day block bootstrap. Trades overlap by eight hours, so
treating 1,187 of them as independent understates the error by roughly the
overlap factor and would turn a coin-flip into a discovery.

Usage:
    python -m modeling.experiments.sol_lgbm_8h_econ
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
from modeling.experiments.sol_lgbm_8h import EVAL_START, FIXED, HORIZON_MIN
from modeling.experiments.sol_short_horizon import (
    DB_REL,
    EVAL_END,
    TABLE,
    TRAIN_END,
    build_labels,
)

logger = logging.getLogger(__name__)

# Chosen by the purged CV inside the training window; see sol_lgbm_8h.
BEST: dict = {"num_leaves": 3, "min_child_samples": 4000, "reg_lambda": 200.0,
              "colsample_bytree": 0.3, "n_estimators": 200}
COST_BPS = 4.72        # maker entry + exit, execution_cost_gate.md
N_BOOT = 2000
BLOCK_DAYS = 30
SEED = 42


def collect_oos() -> tuple[pd.DataFrame, pd.Series]:
    """Monthly refit, keeping every out-of-sample prediction and its cut-offs."""
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
         .dropna(subset=[y, *cols]).reset_index(drop=True))
    dts = pd.to_datetime(d["open_time"])

    out, imps = [], []
    for period in pd.period_range(EVAL_START, EVAL_END, freq="M"):
        m0, m1 = period.to_timestamp(), (period + 1).to_timestamp()
        tr = d.loc[dts < m0 - pd.Timedelta(minutes=HORIZON_MIN)]
        te = d.loc[(dts >= m0) & (dts < m1)]
        if len(tr) < 8000 or len(te) < 200:
            continue
        model = lgb.LGBMRegressor(**{**FIXED, **BEST})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(tr[cols], tr[y].to_numpy())
        imps.append(pd.Series(model.feature_importances_, index=cols))
        in_sample = np.asarray(model.predict(tr[cols]))
        out.append(te[["open_time"]].assign(
            y=te[y].to_numpy(), w=te[w].to_numpy(),
            pred=np.asarray(model.predict(te[cols])),
            hi=float(np.quantile(in_sample, 0.90)),
            lo=float(np.quantile(in_sample, 0.10))))
    imp = pd.concat(imps, axis=1).mean(axis=1).sort_values(ascending=False)
    oos = pd.concat(out, ignore_index=True)
    oos["open_time"] = pd.to_datetime(oos["open_time"])
    return oos, imp


def _pnl(s: pd.DataFrame, side: int) -> np.ndarray:
    """Symmetric barrier: the trade wins w, loses w, or times out flat."""
    return np.where(s["y"] == side, s["w"],
                    np.where(s["y"] == -side, -s["w"], 0.0)) - COST_BPS


def _bootstrap(s: pd.DataFrame, side: int) -> tuple[float, float, float, float]:
    rng = np.random.default_rng(SEED)
    daily = (s.assign(pnl=_pnl(s, side), day=s["open_time"].dt.floor("D"))
             .groupby("day")["pnl"].agg(["sum", "count"]))
    blocks = [np.arange(i, min(i + BLOCK_DAYS, len(daily)))
              for i in range(0, len(daily), BLOCK_DAYS)]
    draws = np.empty(N_BOOT)
    for b in range(N_BOOT):
        pick = np.concatenate([blocks[i] for i in
                               rng.integers(0, len(blocks), len(blocks))])
        sub = daily.iloc[pick]
        draws[b] = sub["sum"].sum() / sub["count"].sum()
    obs = daily["sum"].sum() / daily["count"].sum()
    lo, hi = np.percentile(draws, [2.5, 97.5])
    return float(obs), float(lo), float(hi), float((draws <= 0).mean())


def run() -> None:
    o, imp = collect_oos()
    print("\n" + "=" * 86)
    print("A 8 ORAS LIGHTGBM GAZDASAGI MERLEGE — SOLUSDT")
    print("=" * 86)
    print(f"  {len(o):,} out-of-sample ora, "
          f"{o['open_time'].min()} .. {o['open_time'].max()}")
    print(f"  koltseg {COST_BPS:.2f} bps/kotes, median barrier "
          f"{o['w'].median():.0f} bps\n")

    print("=== MIT HASZNAL A MODELL (atlagos split-szam, 24 honap) ===")
    for k, v in imp.head(10).items():
        print(f"  {v:7.1f}  {k}")
    print(f"  ... {int((imp == 0).sum())} valtozo soha nem vagott\n")

    directional = o.loc[o["y"] != 0]
    print("=== KUSZOB-SZABALY ===")
    print(f"  {'szabaly':<22}{'kotes':>7}{'gyak.':>8}{'talalat':>9}"
          f"{'brutto':>9}{'netto':>8}{'95% CI':>20}{'p(<=0)':>9}")
    for name, sel, side in (("long  (pred >= p90)", o["pred"] >= o["hi"], +1),
                            ("short (pred <= p10)", o["pred"] <= o["lo"], -1)):
        s = o.loc[sel]
        dirn = s.loc[s["y"] != 0]
        if dirn.empty:
            continue
        gross = float(np.nanmean(_pnl(s, side))) + COST_BPS
        obs, lo, hi, p = _bootstrap(s, side)
        print(f"  {name:<22}{len(s):>7}{len(s) / len(o) * 100:>7.1f}%"
              f"{float((dirn['y'] == side).mean()) * 100:>8.1f}%{gross:>+9.2f}"
              f"{obs:>+8.2f}   [{lo:+6.2f}, {hi:+6.2f}]{p:>9.3f}")
    print(f"\n  referencia: {len(directional):,} iranyos ora, "
          f"ebbol +1 {float((directional['y'] == 1).mean()) * 100:.1f}%")

    print("\n=== HAVI KONZISZTENCIA (long szabaly) ===")
    s = o.loc[o["pred"] >= o["hi"]]
    mo = (s.assign(pnl=_pnl(s, 1),
                   m=s["open_time"].dt.to_period("M"))
          .groupby("m")["pnl"].agg(["mean", "count"]))
    t = mo["mean"].mean() / (mo["mean"].std(ddof=1) / np.sqrt(len(mo)))
    print(f"  pozitiv honap {int((mo['mean'] > 0).sum())}/{len(mo)}   "
          f"legrosszabb {mo['mean'].min():+.0f} bps   "
          f"legjobb {mo['mean'].max():+.0f} bps   havi atlagok t-je {t:.2f}")

    # How much more evidence would settle it, at the observed effect and spread.
    pnl = _pnl(o.loc[o["pred"] >= o["hi"]], 1)
    need = (2.0 * float(np.nanstd(pnl)) / max(float(np.nanmean(pnl)), 1e-9)) ** 2
    print(f"\n  t=2-hoz szukseges kotesszam a mert hatas mellett: ~{need:,.0f}"
          f"  (~{need / len(pnl) * 2:.0f} ev egy coinon, vagy "
          f"~{need / len(pnl):.0f} coin 2 ev alatt)")


def main() -> None:
    argparse.ArgumentParser(description="8h LightGBM gazdasagi merleg").parse_args()
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()


if __name__ == "__main__":
    main()
