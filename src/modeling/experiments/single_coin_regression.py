#!/usr/bin/env python3
"""Per-coin linear baselines (epic_064, coinonkenti modellek).

The pooled 30-coin regression kept only 6% of its training IC out of sample
(0.0253 -> 0.0016). BTC alone, run through the identical recipe, kept 90%
(0.0649 -> 0.0585). That points at the pooling rather than the model: one set
of coefficients forced onto thirty coins describes none of them well.

So each coin gets its own everything -- its own mutual-information filter, its
own p-value filter, its own coefficients. Nothing is fixed in advance and
nothing is shared, which is the point: the question is how many coins are
individually modellable at all.

Selection runs on the training half only, so neither filter sees the validation
period.

⚠️ The binding constraint is not rows but independent observations. A 24-hour
label sampled hourly overlaps 23 of its 24 hours, so 40,872 hourly rows carry
about 1,700 independent windows, and roughly 1,360 of those land in training.
At that size a pure-noise IC has a standard deviation of 0.027, which is the
bar any result here has to clear.

Usage:
    python -m modeling.experiments.single_coin_regression --symbol BTCUSDT
    python -m modeling.experiments.single_coin_regression --all
"""

import argparse
import logging
import os
import sys

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.feature_selection import mutual_info_classif

import utils
from modeling.experiments.baseline_features import (
    CORR_MAX,
    DB_REL,
    MI_MIN,
    SEED,
    TABLE,
    exceeds_horizon,
    is_calendar,
)

logger = logging.getLogger(__name__)

START = "2021-12-01"
END = "2026-07-31"
TRAIN_FRAC = 0.80
PURGE_H = 24
P_MAX = 0.01
MI_SAMPLE = 40_000
# Standard deviation of a pure-noise IC at ~1360 independent training windows.
NOISE_IC = 0.0271


def load(symbol: str, table: str = TABLE) -> tuple[pd.DataFrame, list[str]]:
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)
    try:
        cols = [
            r[0] for r in conn.execute(f"DESCRIBE {table}").fetchall()
            if r[0].startswith("feat_")
            and not is_calendar(r[0]) and not exceeds_horizon(r[0])
        ]
        sel = ", ".join(f'f."{c}"' for c in cols)
        df = conn.execute(f"""
            SELECT f.open_time, {sel},
                   t.tb_label_1440_vol::DOUBLE   AS y,
                   t.fwd_ret_1440::DOUBLE        AS y_ret,
                   t.tb_label_1440_vol_bps::DOUBLE AS barrier
            FROM {table} f JOIN target_5m t USING (symbol, open_time)
            WHERE f.symbol = '{symbol}' AND t.tb_label_1440_vol IS NOT NULL
              AND t.fwd_ret_1440 IS NOT NULL
              AND f.open_time >= '{START}' AND f.open_time <= '{END}'
            ORDER BY f.open_time
        """).df()
    finally:
        conn.close()
    df = df.dropna().reset_index(drop=True)
    # Volatility-normalised return: the raw 24h move expressed in units of that
    # bar's own barrier, so one violent day cannot dominate the fit the way it
    # would in raw basis points.
    df["y_ret_norm"] = df["y_ret"] / df["barrier"]
    return df, cols


def _mi_filter(tr: pd.DataFrame, cols: list[str]) -> list[str]:
    """Mutual information then correlation pruning, on the training half only."""
    samp = tr.sample(min(MI_SAMPLE, len(tr)), random_state=SEED)
    mi = mutual_info_classif(
        samp[cols].to_numpy(dtype="float32"), samp["y"].to_numpy(),
        discrete_features="auto", random_state=SEED,
    )
    mi_d = dict(zip(cols, mi, strict=True))
    surv = sorted([c for c in cols if mi_d[c] > MI_MIN], key=lambda c: -mi_d[c])
    if not surv:
        return []
    corr = np.corrcoef(tr[surv].to_numpy(dtype="float64").T)
    keep: list[str] = []
    for j, c in enumerate(surv):
        if all(abs(corr[j, surv.index(k)]) < CORR_MAX for k in keep):
            keep.append(c)
    return keep


def _metrics(model, x, y) -> dict:
    pred = np.asarray(model.predict(x))
    out = {
        "ic": float(np.corrcoef(pred, y)[0, 1]),
        "r2": 1.0 - float(((y - pred) ** 2).sum()) / float(((y - y.mean()) ** 2).sum()),
    }
    for q, key, target in ((0.95, "hi", 1), (0.05, "lo", -1)):
        thr = float(np.quantile(pred, q))
        sel = pred >= thr if target == 1 else pred <= thr
        out[key] = ((float((y[sel] == target).mean())
                     - float((y == target).mean())) * 100
                    if sel.sum() >= 30 else float("nan"))
        out[f"n_{key}"] = int(sel.sum())
    return out


def run(symbol: str, table: str = TABLE, verbose: bool = True) -> dict:
    df, cols = load(symbol, table)
    ts = pd.to_datetime(df["open_time"])
    cut_at = ts.iloc[int(len(df) * TRAIN_FRAC)]
    tr = df[ts < cut_at - pd.Timedelta(hours=PURGE_H)]
    va = df[ts >= cut_at]

    keep_mi = _mi_filter(tr, cols)
    if not keep_mi:
        return {"symbol": symbol, "kept": [], "ic_train": float("nan"),
                "ic_valid": float("nan"), "n_train": len(tr), "n_valid": len(va),
                "years": len(df) / 8760, "lift_hi": float("nan"),
                "lift_lo": float("nan"), "n_mi": 0}

    y_tr = tr["y"].to_numpy()
    x_tr = sm.add_constant(tr[keep_mi].to_numpy(dtype="float64"), has_constant="add")
    day = pd.to_datetime(tr["open_time"]).dt.normalize().to_numpy()
    full = sm.OLS(y_tr, x_tr).fit(
        cov_type="cluster", cov_kwds={"groups": day}, use_t=True
    )
    res = pd.DataFrame({
        "feature": ["const", *keep_mi],
        "coef": full.params, "t": full.tvalues, "p": full.pvalues,
    }).iloc[1:]
    keep = res[res["p"] < P_MAX].sort_values("p")["feature"].tolist()

    out = {"symbol": symbol, "kept": keep, "n_train": len(tr), "n_valid": len(va),
           "years": len(df) / 8760, "n_mi": len(keep_mi),
           "coefs": {r["feature"]: float(r["coef"])
                     for _, r in res[res["feature"].isin(keep)].iterrows()}}
    if not keep:
        out |= {"ic_train": float("nan"), "ic_valid": float("nan"),
                "lift_hi": float("nan"), "lift_lo": float("nan")}
        if verbose:
            print(f"\n  {symbol}: egyetlen valtozo sem szignifikans.")
        return out

    x2_tr = sm.add_constant(tr[keep].to_numpy(dtype="float64"), has_constant="add")
    x2_va = sm.add_constant(va[keep].to_numpy(dtype="float64"), has_constant="add")
    final = sm.OLS(y_tr, x2_tr).fit()
    m_tr = _metrics(final, x2_tr, y_tr)
    m_va = _metrics(final, x2_va, va["y"].to_numpy())
    out |= {"ic_train": m_tr["ic"], "ic_valid": m_va["ic"],
            "lift_hi": m_va["hi"], "lift_lo": m_va["lo"]}

    if verbose:
        print("\n" + "=" * 78)
        print(f"EGY COIN — {symbol}")
        print("=" * 78)
        print(f"  TRAIN  {pd.to_datetime(tr['open_time']).min()}  ..  "
              f"{pd.to_datetime(tr['open_time']).max()}   {len(tr):>7,} sor")
        print(f"  VALID  {pd.to_datetime(va['open_time']).min()}  ..  "
              f"{pd.to_datetime(va['open_time']).max()}   {len(va):>7,} sor")
        print(f"  MI-szures (csak trainen): {len(cols)} -> {len(keep_mi)} valtozo")
        print(f"  p < {P_MAX}: {len(keep)} / {len(keep_mi)}")
        print(f"\n=== A BENTMARADO {len(keep)} VALTOZO ===")
        for i, (_, r) in enumerate(res[res["feature"].isin(keep)]
                                   .sort_values("p").iterrows(), 1):
            print(f"  {i:>3}. {r['feature']:<34}{r['coef']:>+11.5f}"
                  f"{r['t']:>8.2f}{r['p']:>11.2e}")
        print("\n=== TRAIN vs VALID ===")
        for nev, m, n in (("TRAIN", m_tr, len(tr)), ("VALID", m_va, len(va))):
            print(f"  {nev:<7}{n:>8,} sor   R^2 {m['r2']:>8.5f}   IC {m['ic']:>7.4f}")
            print(f"          top p95  n={m['n_hi']:>5}  lift {m['hi']:>+6.1f}pp"
                  f"      also p05  n={m['n_lo']:>5}  lift {m['lo']:>+6.1f}pp")
    return out


def sweep(table: str = TABLE) -> None:
    """The same recipe on every coin, then a summary of what held up."""
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)
    try:
        symbols = [r[0] for r in conn.execute(
            f"SELECT DISTINCT symbol FROM {table} ORDER BY 1").fetchall()]
    finally:
        conn.close()

    rows, chosen, signs = [], {}, {}
    for i, sym in enumerate(symbols, 1):
        r = run(sym, table=table, verbose=False)
        rows.append(r)
        for f in r["kept"]:
            chosen[f] = chosen.get(f, 0) + 1
            signs.setdefault(f, []).append(np.sign(r["coefs"][f]))
        logger.info("[%2d/%d] %-14s valtozo=%2d  IC train %+.4f  valid %+.4f",
                    i, len(symbols), sym, len(r["kept"]),
                    r["ic_train"], r["ic_valid"])

    df = pd.DataFrame(rows).sort_values("ic_valid", ascending=False)
    print("\n" + "=" * 96)
    print("COINONKENTI LINEARIS MODELL — kulon valtozo-szelekcio mindegyikre")
    print("=" * 96)
    print(f"  {'symbol':<14}{'ev':>5}{'train':>8}{'valid':>7}{'MI':>5}{'valt':>6}"
          f"{'IC train':>10}{'IC valid':>10}{'megtart':>9}{'lift hi':>9}{'lift lo':>9}")
    for _, r in df.iterrows():
        ratio = (r["ic_valid"] / r["ic_train"]
                 if r["ic_train"] and np.isfinite(r["ic_train"]) else np.nan)
        rs = f"{ratio:>8.0%}" if np.isfinite(ratio) else f"{'-':>8}"
        print(f"  {r['symbol']:<14}{r['years']:>5.1f}{r['n_train']:>8,}"
              f"{r['n_valid']:>7,}{r['n_mi']:>5}{len(r['kept']):>6}"
              f"{r['ic_train']:>+10.4f}{r['ic_valid']:>+10.4f}{rs}"
              f"{r['lift_hi']:>+9.1f}{r['lift_lo']:>+9.1f}")

    fit = df[df["kept"].apply(len) > 0]
    ok = fit[fit["ic_valid"] > 0]
    strong = fit[fit["ic_valid"] > NOISE_IC]
    print(f"\n  coin osszesen                    {len(df):>4}")
    print(f"  volt szignifikans valtozo        {len(fit):>4}")
    print(f"  pozitiv valid IC                 {len(ok):>4}  "
          f"({len(ok) / max(len(fit), 1):.0%} a modellezettekbol)")
    print(f"  valid IC > zajszoras ({NOISE_IC})  {len(strong):>4}  "
          f"({len(strong) / max(len(fit), 1):.0%})")
    print(f"  atlagos valid IC              {fit['ic_valid'].mean():>+7.4f}")
    print(f"  medias valtozoszam            {fit['kept'].apply(len).median():>7.1f}")

    print("\n=== MELY VALTOZOK JONNEK KI TOBB COINON? ===")
    print(f"  {'db':>4}  {'elojel':<12}{'valtozo'}")
    for f, n in sorted(chosen.items(), key=lambda kv: -kv[1]):
        if n < 2:
            continue
        s = signs[f]
        agree = max(s.count(1), s.count(-1)) / len(s)
        lbl = ("egysegesen +" if s.count(1) == len(s)
               else "egysegesen -" if s.count(-1) == len(s)
               else f"vegyes {agree:.0%}")
        print(f"  {n:>4}  {lbl:<12}{f}")
    print(f"\n  kulonbozo valtozo osszesen: {len(chosen)}")
    print(f"  csak 1 coinon szerepel:     {sum(1 for v in chosen.values() if v == 1)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Coinonkenti linearis alapmodell")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--table", default=TABLE,
                        help="Melyik feature-tablat hasznalja (normalizalt vs nyers)")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.all:
        sweep(args.table)
    else:
        run(args.symbol, args.table)


if __name__ == "__main__":
    main()
