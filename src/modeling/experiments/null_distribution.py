#!/usr/bin/env python3
"""A null distribution, not a single rotation (epic_064).

The 15-minute grid appeared to beat the hourly one on every metric, and then a
rotated-label run produced almost the same numbers -- IC +0.0275 against the
real +0.0312. That killed the result, and it exposed two flaws in how the null
was being tested everywhere else.

**One rotation is one draw.** The hourly grid's clean null (IC -0.0041) was a
single sample from a distribution nobody had looked at. A single draw cannot
say whether +0.0240 is unusual; it can only say that one particular rotation
happened to score below it. Every "passes the null test" claim made from a
single shift was weaker than it sounded.

**Re-searching the grid on the rotated label lets the null cheat.** The real run
picks its regularisation on real data; the null run was picking its own, and it
chose a different configuration -- so the null model was allowed to tune itself
to the noise it was supposed to fail on. The configuration is now held fixed at
whatever the real data chose, which is the only way the two are comparable.

So: many shifts, fixed configuration, and the real result read against the
resulting distribution as an empirical p-value.

⚠️ Rotation preserves each series' own autocorrelation, which is the point --
it destroys the pairing while keeping the block structure that makes spurious
agreement possible. That is why the null is not centred on zero for a grid with
heavy label overlap, and why comparing against zero was the wrong test.

Usage:
    python -m modeling.experiments.null_distribution --grid 15m
    python -m modeling.experiments.null_distribution --grid 1h
"""

import argparse
import logging
import os
import sys

import duckdb
import numpy as np
import pandas as pd

import utils
from modeling.experiments.baseline_features import DB_REL
from modeling.experiments.sol_lgbm_8h import _t
from modeling.experiments.sol_lgbm_8h_econ import BEST
from modeling.experiments.sol_short_horizon import TRAIN_END

logger = logging.getLogger(__name__)

# Spread across the sample so no single seasonal alignment drives the answer.
# The panel is roughly 40,000 hours, so shifts stay under half of that: beyond
# the midpoint a rotation mirrors one already taken.
N_SHIFTS = 24
SHIFT_LO, SHIFT_HI = 1500, 19500


def shifts(n: int) -> list[int]:
    """Evenly spaced rotations across the usable range, in hours."""
    return [int(round(x)) for x in np.linspace(SHIFT_LO, SHIFT_HI, n)]


def load(grid: str) -> tuple[pd.DataFrame, pd.Series, list[str], int]:
    """Features, labels and bar size for either grid, with one 'y' column."""
    if grid == "15m":
        from modeling.experiments.sol_15m_grid import (
            BAR_MIN,
            TABLE,
            build_labels,
        )
        labels = build_labels(TRAIN_END)
    else:
        from modeling.experiments.sol_short_horizon import TABLE, build_labels
        BAR_MIN = 60
        raw = build_labels(TRAIN_END)
        labels = pd.DataFrame({"open_time": raw["open_time"],
                               "y": raw["y480"], "w": raw["w480"]})

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
    d = (feats.merge(labels, on="open_time", how="inner")
         .dropna(subset=["y", *cols]).reset_index(drop=True))
    return d, pd.to_datetime(d["open_time"]), cols, BAR_MIN


def walk(d: pd.DataFrame, dts: pd.Series, cols: list[str],
         y: np.ndarray) -> dict:
    """Monthly refit on a supplied label vector, fixed configuration."""
    from modeling.experiments.sol_lgbm_8h import _fit, _metrics

    ics, aucs, lifts = [], [], []
    for period in pd.period_range("2024-08", "2026-07-31", freq="M"):
        m0, m1 = period.to_timestamp(), (period + 1).to_timestamp()
        itr = np.flatnonzero(dts < m0 - pd.Timedelta(minutes=480))
        ite = np.flatnonzero((dts >= m0) & (dts < m1))
        if len(itr) < 8000 or len(ite) < 200:
            continue
        m = _fit(BEST, d.iloc[itr][cols], y[itr])
        mm = _metrics(np.asarray(m.predict(d.iloc[ite][cols])), y[ite])
        ics.append(mm["ic"])
        aucs.append(mm["auc"])
        lifts.append(mm["lift"])
    return {"ic": float(np.mean(ics)), "auc": float(np.mean(aucs)),
            "lift": float(np.mean(lifts)), "ic_t": _t(np.array(ics)),
            "lift_t": _t(np.array(lifts))}


def run(grid: str, n_shifts: int = N_SHIFTS) -> None:
    d, dts, cols, bar = load(grid)
    per_hour = 60 // bar
    y = d["y"].to_numpy()

    print("\n" + "=" * 84)
    print(f"NULL-ELOSZLAS — {grid} racs, rogzitett konfiguracio")
    print("=" * 84)
    print(f"  {len(d):,} sor, {len(cols)} feature, {bar} perces bar")
    print("  konfiguracio (az eles adaton valasztva, MINDEN futasban azonos):")
    print(f"    {BEST}")
    sh = shifts(n_shifts)
    print(f"  {len(sh)} rotacio: {sh[0]}..{sh[-1]} ora\n")

    real = walk(d, dts, cols, y)
    print(f"  ELES:  IC {real['ic']:+.4f} (t {real['ic_t']:.2f})   "
          f"AUC {real['auc']:.4f}   p90 lift {real['lift']:+.2f}pp "
          f"(t {real['lift_t']:.2f})\n")

    print("=== ROTALT CIMKE ===")
    print(f"  {'eltolas':>10}{'IC':>10}{'AUC':>9}{'p90 lift':>11}")
    rows = []
    for h in sh:
        r = walk(d, dts, cols, np.roll(y, h * per_hour))
        rows.append(r)
        print(f"  {str(h) + ' ora':>10}{r['ic']:>+10.4f}{r['auc']:>9.4f}"
              f"{r['lift']:>+10.2f}pp")
    null = pd.DataFrame(rows)

    print("\n=== A NULL-ELOSZLAS ===")
    print(f"  {'':<10}{'atlag':>9}{'szoras':>9}{'p95':>9}{'max':>9}"
          f"{'ELES':>10}{'p-ertek':>10}")
    for key, fmt in (("ic", ">+9.4f"), ("auc", ">9.4f"), ("lift", ">+9.2f")):
        v = null[key].to_numpy()
        # (1 + count) / (1 + n): the real result is itself one draw under the
        # null, so a count of zero must not be reported as p = 0.
        p = (1.0 + float((v >= real[key]).sum())) / (1.0 + len(v))
        print(f"  {key:<10}{v.mean():{fmt}}{v.std(ddof=1):>9.4f}"
              f"{np.percentile(v, 95):{fmt}}{v.max():{fmt}}"
              f"{real[key]:{fmt}}{p:>10.3f}")
    print("\n  p = (1 + ahany rotacio eleri az eles eredmenyt) / (1 + rotaciok).")
    print("  Az eles futas maga is egy huzas a null alol, ezert nem lehet p = 0.")
    print(f"  ({len(sh)} rotacioval a legkisebb elerheto p-ertek "
          f"{1 / (1 + len(sh)):.3f}.)")


def main() -> None:
    parser = argparse.ArgumentParser(description="null-eloszlas rotaciokkal")
    parser.add_argument("--grid", choices=("1h", "15m"), default="15m")
    parser.add_argument("--n-shifts", type=int, default=N_SHIFTS)
    args = parser.parse_args()
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.grid, args.n_shifts)


if __name__ == "__main__":
    main()
