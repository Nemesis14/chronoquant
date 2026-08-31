#!/usr/bin/env python3
"""Which lookback lengths does the 15-minute model actually use? (epic_064)

The 15-minute grid beat the hourly one on every metric. There are two possible
reasons and they lead to opposite conclusions.

**The sub-hour features carry signal.** A 30-minute window did not exist on the
hourly grid at any setting, so if the model leans on those, the intra-hour
build-up hypothesis is confirmed and the next step is to look shorter still.

**Or the same eight-hour features simply got a cleaner estimate.** Computing an
8-hour indicator from 32 quarter-hour bars instead of 8 hourly ones is a less
noisy measurement of the same quantity. That would be a real improvement too,
but it says nothing about intra-hour signal, and shortening further would not
help.

The importance is grouped by the wall-clock span each feature's window covers,
so the two stories separate. A per-window-only rerun then checks the same thing
from the other side: a model given *only* the 30-minute features either finds
something or it does not.

⚠️ Split counts are not evidence of usefulness on their own -- a feature can be
split on often and contribute nothing. The per-window refit is the check that
matters; the importance table only says where the model looked.

Usage:
    python -m modeling.experiments.sol_15m_windows
"""

import argparse
import logging
import os
import re
import sys

import duckdb
import pandas as pd

import utils
from modeling.experiments.baseline_features import DB_REL
from modeling.experiments.sol_15m_grid import (
    BAR_MIN,
    TABLE,
    WINDOWS,
    _walk,
    build_labels,
)
from modeling.experiments.sol_lgbm_8h import _fit, _t
from modeling.experiments.sol_lgbm_8h_econ import BEST
from modeling.experiments.sol_short_horizon import TRAIN_END

logger = logging.getLogger(__name__)


def window_of(col: str) -> int | None:
    """The trailing _N of a feature name, in bars, if it has one."""
    m = re.findall(r"_(\d+)$", col)
    return int(m[0]) if m else None


def run() -> None:
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL),
                          read_only=True)
    try:
        cols = [r[0] for r in conn.execute(f"DESCRIBE {TABLE}").fetchall()
                if r[0].startswith("feat_")]
        sel = ", ".join(f'"{c}"' for c in cols)
        feats = conn.execute(
            f"SELECT open_time, {sel} FROM {TABLE} ORDER BY open_time").df()
    finally:
        conn.close()
    cols = [c for c in cols if feats[c].notna().mean() > 0.5]
    d = (feats.merge(build_labels(TRAIN_END), on="open_time", how="inner")
         .dropna(subset=["y", *cols]).reset_index(drop=True))
    dts = pd.to_datetime(d["open_time"])

    print("\n" + "=" * 84)
    print("MELYIK ABLAKHOSSZAKAT HASZNALJA A 15 PERCES MODELL?")
    print("=" * 84)

    # --- where the model looked ------------------------------------------
    imps = []
    for period in pd.period_range("2024-08", "2026-07-31", freq="M"):
        m0 = period.to_timestamp()
        tr = d.loc[dts < m0 - pd.Timedelta(minutes=480)]
        if len(tr) < 8000:
            continue
        m = _fit(BEST, tr[cols], tr["y"].to_numpy())
        imps.append(pd.Series(m.feature_importances_, index=cols))
    imp = pd.concat(imps, axis=1).mean(axis=1)

    print("\n=== 1. VAGASOK ABLAKHOSSZ SZERINT (24 havi atlag) ===")
    print(f"  {'ablak':<12}{'db feature':>12}{'ossz. vagas':>14}"
          f"{'atlag/feature':>16}{'reszarany':>12}")
    tot = float(imp.sum())
    wins = pd.Series([window_of(c) for c in cols], index=cols)
    for w in WINDOWS:
        g = imp.loc[wins == w]
        if len(g) == 0:
            continue
        print(f"  {str(w * BAR_MIN) + ' perc':<12}{len(g):>12}{g.sum():>14.0f}"
              f"{g.mean():>16.1f}{g.sum() / tot * 100:>11.1f}%")
    other = imp.loc[wins.isna()]
    print(f"  {'ablak nelkul':<12}{len(other):>12}{other.sum():>14.0f}"
          f"{other.mean():>16.1f}{other.sum() / tot * 100:>11.1f}%")

    print("\n=== 2. TOP 12 FEATURE ===")
    for k, v in imp.sort_values(ascending=False).head(12).items():
        w = window_of(str(k))
        tag = f"{w * BAR_MIN}p" if w else "—"
        print(f"  {v:7.1f}  {tag:>6}  {k}")

    # --- does each window earn its keep on its own? -----------------------
    print("\n=== 3. CSAK EGY ABLAKHOSSZ, KULON-KULON (gorduló, 24 honap) ===")
    print(f"  {'ablak':<12}{'feature':>9}{'IC valid':>11}{'t':>7}"
          f"{'AUC':>9}{'p90 lift':>11}{'t':>7}")
    for w in WINDOWS:
        sub = [c for c in cols if wins[c] == w]
        if len(sub) < 5:
            continue
        r = _walk(d, dts, sub, BEST)
        ic, auc, lf = (r["ic"].to_numpy(), r["auc"].to_numpy(),
                       r["lift"].to_numpy())
        print(f"  {str(w * BAR_MIN) + ' perc':<12}{len(sub):>9}{ic.mean():>+11.4f}"
              f"{_t(ic):>7.2f}{auc.mean():>9.4f}{lf.mean():>+10.2f}pp{_t(lf):>7.2f}")

    # --- and the sub-hour block against everything else -------------------
    print("\n=== 4. SUB-ORAS BLOKK vs A TOBBI ===")
    print(f"  {'blokk':<22}{'feature':>9}{'IC valid':>11}{'t':>7}"
          f"{'AUC':>9}{'p90 lift':>11}{'t':>7}")
    groups = {
        "csak 30 perces": [c for c in cols if wins[c] == 2],
        "1 ora es hosszabb": [c for c in cols if wins[c] is not None
                              and wins[c] >= 4],
        "minden (referencia)": cols,
    }
    for name, sub in groups.items():
        if len(sub) < 5:
            continue
        r = _walk(d, dts, sub, BEST)
        ic, auc, lf = (r["ic"].to_numpy(), r["auc"].to_numpy(),
                       r["lift"].to_numpy())
        print(f"  {name:<22}{len(sub):>9}{ic.mean():>+11.4f}{_t(ic):>7.2f}"
              f"{auc.mean():>9.4f}{lf.mean():>+10.2f}pp{_t(lf):>7.2f}")


def main() -> None:
    argparse.ArgumentParser(description="15 perces racs ablak-bontas").parse_args()
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()


if __name__ == "__main__":
    main()
