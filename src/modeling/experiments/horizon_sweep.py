#!/usr/bin/env python3
"""Does the signal decay with horizon? (epic_064, horizont-teszt)

The project moved from a 60-minute target to 4 hours and then to 24 hours, and
each move was made for the fee, not for the signal: a fixed 13 bps round trip is
25% of a typical 4-hour move but only 5% of a 24-hour one. Meanwhile the
measured directional lift went the other way -- +9.5pp at four hours, roughly
zero at twenty-four.

So the horizon may have been pushed past where the predictability lives. This
measures that directly: the same coin, the same features, the same split, the
same recipe, with only the label horizon changing.

The barrier is rebuilt for each horizon exactly as the pipeline builds it --
one shared k times the coin's own sigma scaled by the square root of the
horizon -- and k is re-normalised per horizon so the median barrier stays at
the same multiple of that horizon's typical move. Otherwise a short horizon
would simply get an unreachable barrier and look unpredictable for the wrong
reason.

⚠️ One confound is stated rather than fixed: the features were cut for a
24-hour target, with lookbacks of 4 to 24 hours. They are too slow for a
60-minute label. If the short horizons still win with handicapped inputs, that
is the stronger version of the result.

Usage:
    python -m modeling.experiments.horizon_sweep --symbol XRPUSDT
"""

import argparse
import logging
import os
import sys

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

import utils
from data_handling.sync_tables._triple_barrier import (
    horizon_barrier_bps,
    normalising_k,
    triple_barrier_labels,
)
from modeling.experiments.baseline_features import (
    DB_REL,
    TABLE,
    exceeds_horizon,
    is_calendar,
)
from modeling.experiments.single_coin_regression import (
    P_MAX,
    TRAIN_FRAC,
    _mi_filter,
)

logger = logging.getLogger(__name__)

HORIZONS_MIN = (60, 240, 480, 1440)
# Same relative width at every horizon: the median barrier is this multiple of
# the horizon's own sigma, so "+1" always means a comparable amount of movement.
K_MULTIPLE = 0.648
VOL_SPAN = 1440
MIN_BPS, MAX_BPS = 10.0, 2000.0
# A round trip at the best execution measured in epic_059.
FEE_BPS = 3.6


def build_labels(symbol: str) -> pd.DataFrame:
    """Hourly rows with a vol-scaled barrier label at each horizon."""
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)
    try:
        bars = conn.execute(
            "SELECT open_time, high, low, close FROM ohlcv_1m"
            " WHERE symbol = ? ORDER BY open_time", [symbol]).df()
    finally:
        conn.close()

    close = bars["close"].to_numpy(dtype="float64")
    high = bars["high"].to_numpy(dtype="float64")
    low = bars["low"].to_numpy(dtype="float64")

    out = {"open_time": bars["open_time"]}
    for h in HORIZONS_MIN:
        # normalising_k targets a median width; asking for K_MULTIPLE times the
        # unit width keeps every horizon at the same multiple of its own sigma.
        unit = horizon_barrier_bps(close, horizon=h, k=1.0, span=VOL_SPAN,
                                   min_bps=0.0, max_bps=np.inf)
        target_med = K_MULTIPLE * float(np.nanmedian(unit))
        k = normalising_k(close, target_med, h, VOL_SPAN)
        w = horizon_barrier_bps(close, horizon=h, k=k, span=VOL_SPAN,
                                min_bps=MIN_BPS, max_bps=MAX_BPS)
        out[f"y{h}"] = triple_barrier_labels(close, high, low, horizon=h,
                                             up_bps=w, dn_bps=w)
        out[f"w{h}"] = w
    frame = pd.DataFrame(out)
    ts = pd.to_datetime(frame["open_time"])
    return frame[ts.dt.minute == 0].reset_index(drop=True)


def load_features(symbol: str) -> tuple[pd.DataFrame, list[str]]:
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)
    try:
        cols = [
            r[0] for r in conn.execute(f"DESCRIBE {TABLE}").fetchall()
            if r[0].startswith("feat_")
            and not is_calendar(r[0]) and not exceeds_horizon(r[0])
        ]
        sel = ", ".join(f'"{c}"' for c in cols)
        df = conn.execute(
            f"SELECT open_time, {sel} FROM {TABLE} WHERE symbol = ? "
            "ORDER BY open_time", [symbol]).df()
    finally:
        conn.close()
    return df, cols


def run(symbol: str) -> None:
    labels = build_labels(symbol)
    feats_df, cols = load_features(symbol)
    df = feats_df.merge(labels, on="open_time", how="inner")

    print("\n" + "=" * 88)
    print(f"HORIZONT-TESZT — {symbol}")
    print("=" * 88)
    print(f"  {len(df):,} orai sor, {len(cols)} jelolt feature")
    print("  ugyanaz a recept minden horizonton: MI-szures -> OLS klaszterelt "
          "SE-vel -> p < 0.01\n")
    print(f"  {'horizont':>10}{'korlat med':>12}{'dij/korlat':>12}{'+1':>7}{'-1':>7}"
          f"{'timeout':>9}{'valt':>6}{'IC train':>10}{'IC valid':>10}{'p95 lift':>10}")

    for h in HORIZONS_MIN:
        d = df.dropna(subset=[f"y{h}", *cols]).reset_index(drop=True)
        ts = pd.to_datetime(d["open_time"])
        cut = ts.iloc[int(len(d) * TRAIN_FRAC)]
        # The purge is the label horizon, which now differs per run.
        tr = d[ts < cut - pd.Timedelta(minutes=h)].reset_index(drop=True)
        va = d[ts >= cut].reset_index(drop=True)

        y_col = f"y{h}"
        tr = tr.rename(columns={y_col: "y"})
        va = va.rename(columns={y_col: "y"})

        keep_mi = _mi_filter(tr.assign(y=tr["y"]), cols)
        if not keep_mi:
            continue
        y_tr = tr["y"].to_numpy()
        x_tr = sm.add_constant(tr[keep_mi].to_numpy(dtype="float64"),
                               has_constant="add")
        day = pd.to_datetime(tr["open_time"]).dt.normalize().to_numpy()
        full = sm.OLS(y_tr, x_tr).fit(
            cov_type="cluster", cov_kwds={"groups": day}, use_t=True)
        res = pd.DataFrame({"feature": ["const", *keep_mi],
                            "p": full.pvalues}).iloc[1:]
        keep = res[res["p"] < P_MAX]["feature"].tolist()

        med_w = float(np.nanmedian(tr[f"w{h}"]))
        v = tr["y"].value_counts(normalize=True)
        if not keep:
            print(f"  {h:>7} perc{med_w:>12.0f}{FEE_BPS / med_w:>11.1%}"
                  f"{v.get(1, 0) * 100:>7.1f}{v.get(-1, 0) * 100:>7.1f}"
                  f"{v.get(0, 0) * 100:>9.1f}{0:>6}{'-':>10}{'-':>10}{'-':>10}")
            continue

        x2_tr = sm.add_constant(tr[keep].to_numpy(dtype="float64"),
                                has_constant="add")
        x2_va = sm.add_constant(va[keep].to_numpy(dtype="float64"),
                                has_constant="add")
        m = sm.OLS(y_tr, x2_tr).fit()
        p_tr = np.asarray(m.predict(x2_tr))
        p_va = np.asarray(m.predict(x2_va))
        y_va = va["y"].to_numpy()
        ic_tr = float(np.corrcoef(p_tr, y_tr)[0, 1])
        ic_va = float(np.corrcoef(p_va, y_va)[0, 1])
        thr = float(np.quantile(p_va, 0.95))
        sel = p_va >= thr
        lift = ((float((y_va[sel] == 1).mean()) - float((y_va == 1).mean())) * 100
                if sel.sum() >= 30 else float("nan"))

        print(f"  {h:>7} perc{med_w:>12.0f}{FEE_BPS / med_w:>11.1%}"
              f"{v.get(1, 0) * 100:>7.1f}{v.get(-1, 0) * 100:>7.1f}"
              f"{v.get(0, 0) * 100:>9.1f}{len(keep):>6}"
              f"{ic_tr:>+10.4f}{ic_va:>+10.4f}{lift:>+10.1f}")

    print("\n  A 'dij/korlat' oszlop az, amiert a horizontot noveltuk: a fix "
          "koltseg\n  aranya a megcelzott mozgashoz. Minel kisebb, annal "
          "konnyebb fedezni.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Horizont-teszt egy coinon")
    parser.add_argument("--symbol", default="XRPUSDT")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.symbol)


if __name__ == "__main__":
    main()
