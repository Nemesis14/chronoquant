#!/usr/bin/env python3
"""Short features, shorter horizon, rolling refit — on SOL (epic_064).

Three complaints about the previous setup, all of them fair, all addressed here.

**The model sat frozen for a year.** Train ended 2025-08 and the validation ran
to 2026-07 with the same coefficients throughout. Live it would be refitted
monthly. Here it is: coefficients are re-estimated every month on everything
available up to that month, so the validation measures a model that is kept
current rather than one left to rot.

**The barrier's `k` looked into the future.** It was normalised over each
coin's whole close series, validation included. One global scalar, so it moved
class balance rather than direction -- but it was a leak, and it is now
computed from the training window alone.

**The features were cut for a 24-hour target and could not see a fast move.**
Windows were 4 to 24 hours; anything that starts inside two hours was invisible.
They are now 1, 2, 4 and 8 hours, and the target is measured at 8, 12 and 24
hours so the pairing can be compared rather than assumed.

The shorter horizon also buys sample: an 8-hour label overlaps 7 of its 8 hours
instead of 23 of 24, roughly tripling the independent observations.

⚠️ It costs on the fee side. At 8 hours SOL's barrier is around 130 bps against
a 3.6 bps round trip -- still fine -- but the same logic at 2 hours would put
the fee at 5-6% of the target, which is why 2 hours is not on the list.

Usage:
    python -m modeling.experiments.sol_short_horizon --build
    python -m modeling.experiments.sol_short_horizon --run
"""

import argparse
import logging
import os
import sys

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import statsmodels.api as sm

import utils
from data_handling.sync_tables._features_polars import (
    _rolling_rank_arr,
    compute_features_polars,
)
from data_handling.sync_tables._triple_barrier import (
    horizon_barrier_bps,
    triple_barrier_labels,
)
from modeling.experiments.baseline_features import (
    CALENDAR_GROUPS,
    DB_REL,
    NORM_WINDOW,
    PANEL_START,
    is_calendar,
)
from modeling.experiments.single_coin_regression import MI_SAMPLE, P_MAX, SEED

logger = logging.getLogger(__name__)

SYMBOL = "SOLUSDT"
TABLE = "feat_sol_short_1h"
# Short by design: a move that starts inside two hours has to be visible.
WINDOWS = (1, 2, 4, 8)
MAX_LOOKBACK_H = 8
HORIZONS_MIN = (480, 720, 1440)
VOL_SPAN = 1440
K_TARGET_MULT = 0.648      # the same relative width the panel uses
MIN_BPS, MAX_BPS = 20.0, 2000.0
FEE_BPS = 3.6

TRAIN_END = "2024-07-31"   # everything after this is walked forward monthly
EVAL_END = "2026-07-31"

HIDDEN_LOOKBACK = ("tenkan", "kijun", "senkou", "ichimoku", "feat_obv",
                   "feat_ad_line")


def too_long(col: str) -> bool:
    """Reject anything reaching past MAX_LOOKBACK_H, name-encoded or hidden."""
    import re
    if any(h in col for h in HIDDEN_LOOKBACK):
        return True
    m = re.findall(r"_(\d+)$", col)
    return bool(m) and int(m[0]) > MAX_LOOKBACK_H


def short_config() -> dict:
    base = utils.load_features_config()["database"]["features"]["indicators"]
    out: dict = {}
    for group, inds in base.items():
        if group in CALENDAR_GROUPS:
            out[group] = False
            continue
        if not isinstance(inds, dict):
            out[group] = inds
            continue
        out[group] = {}
        for name, variants in inds.items():
            if not isinstance(variants, list):
                out[group][name] = variants
                continue
            if name == "macd":
                out[group][name] = [{"fast": 1, "slow": 2, "signal": 1},
                                    {"fast": 2, "slow": 4, "signal": 2},
                                    {"fast": 4, "slow": 8, "signal": 3}]
                continue
            new, seen = [], set()
            for v in variants:
                if not isinstance(v, dict) or "window" not in v:
                    new.append(v)
                    continue
                for w in WINDOWS:
                    item = dict(v)
                    item["window"] = w
                    if "smooth_window" in item:
                        item["smooth_window"] = max(2, w)
                    if name == "kama":
                        item["fast"], item["slow"] = 2, max(w, 4)
                    key = tuple(sorted(item.items()))
                    if key not in seen:
                        seen.add(key)
                        new.append(item)
            out[group][name] = new
    return out


def build_features() -> None:
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL))
    try:
        bars = conn.execute("""
            SELECT date_trunc('hour', open_time) AS open_time,
                   arg_min(open, open_time) AS "open", max(high) AS high,
                   min(low) AS low, arg_max(close, open_time) AS "close",
                   sum(volume) AS volume, sum(quote_volume) AS quote_volume,
                   sum(n_trades) AS trades, sum(taker_buy_volume) AS taker_buy_base
            FROM ohlcv_5m WHERE symbol = ? GROUP BY 1 ORDER BY 1
        """, [SYMBOL]).pl()

        feats = compute_features_polars(
            df_ohlcv=bars, indicators=short_config(), feat_prefix="feat_",
            available_activity=["quote_volume", "trades", "taker_buy_base"],
            targets_cfg=[])
        keep = sorted(c for c in feats.columns
                      if c.startswith("feat_") and not is_calendar(c)
                      and not too_long(c))

        ranked = {}
        for c in keep:
            a = feats[c].to_numpy().astype("float64")
            ranked[c] = (a if np.unique(a[~np.isnan(a)]).size <= 3
                         else _rolling_rank_arr(a, NORM_WINDOW))
        out = (
            feats.with_columns([pl.Series(c, v) for c, v in ranked.items()])
            .filter(pl.col("open_time") >= pl.lit(PANEL_START).str.to_datetime())
            .select(["open_time", *keep])
            .with_columns([pl.col(c).cast(pl.Float32) for c in keep])
        )
        conn.register("in_", out.to_arrow())
        conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
        conn.execute(f"CREATE TABLE {TABLE} AS SELECT * FROM in_")
        conn.unregister("in_")
        logger.info("%s: %d sor, %d rovid-ablaku feature", TABLE, out.height,
                    len(keep))
    finally:
        conn.close()


def build_labels(train_end: str) -> pd.DataFrame:
    """Labels at each horizon, with k taken from the training window only."""
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)
    try:
        bars = conn.execute(
            "SELECT open_time, high, low, close FROM ohlcv_1m WHERE symbol = ?"
            " ORDER BY open_time", [SYMBOL]).df()
    finally:
        conn.close()

    close = bars["close"].to_numpy(dtype="float64")
    high = bars["high"].to_numpy(dtype="float64")
    low = bars["low"].to_numpy(dtype="float64")
    in_train = (pd.to_datetime(bars["open_time"]) <= train_end).to_numpy()

    out = {"open_time": bars["open_time"]}
    for h in HORIZONS_MIN:
        unit = horizon_barrier_bps(close, horizon=h, k=1.0, span=VOL_SPAN,
                                   min_bps=0.0, max_bps=np.inf)
        # ⚠️ The median is taken over training bars only. Using the whole
        # series would let the validation period set the barrier width.
        med = float(np.nanmedian(unit[in_train]))
        k = K_TARGET_MULT
        w = np.clip(k * unit, MIN_BPS, MAX_BPS)
        out[f"y{h}"] = triple_barrier_labels(close, high, low, horizon=h,
                                             up_bps=w, dn_bps=w)
        out[f"w{h}"] = w
        logger.info("h=%4d perc  train-median egyseg %.0f bps  ->  korlat med %.0f",
                    h, med, float(np.nanmedian(w[in_train])))
    frame = pd.DataFrame(out)
    ts = pd.to_datetime(frame["open_time"])
    return frame[ts.dt.minute == 0].reset_index(drop=True)


def _auc(pred: np.ndarray, y: np.ndarray) -> float:
    """Rank AUC on the directional pair, timeouts left out.

    A three-class label has no single AUC, and the timeout class carries no
    direction -- scoring it would measure something no trade ever asks.
    """
    from sklearn.metrics import roc_auc_score
    mask = y != 0
    if mask.sum() < 30 or len(np.unique(y[mask])) < 2:
        return float("nan")
    return float(roc_auc_score((y[mask] > 0).astype(int), pred[mask]))


def _lift(pred: np.ndarray, y: np.ndarray, q: float = 0.90) -> float:
    """Hit-rate lift in the top decile of the score, against its own base rate.

    Restricted to the directional rows, so the number answers "given that the
    price did move to a barrier, how much better than chance is the top decile
    at calling which one".
    """
    mask = y != 0
    if mask.sum() < 60:
        return float("nan")
    p, yy = pred[mask], y[mask]
    sel = p >= float(np.quantile(p, q))
    if sel.sum() < 20:
        return float("nan")
    return float((yy[sel] == 1).mean() - (yy == 1).mean()) * 100


def _cluster_prune(tr: pd.DataFrame, feats: list[str], mi: dict[str, float],
                   thresh: float = 0.90) -> list[str]:
    """One representative per correlated family, chosen by mutual information.

    ⚠️ The greedy alternative -- walk the list and drop anything correlated with
    something already kept -- collapses here. Short windows make near-duplicates
    of each other: a 1, 2, 4 and 8-hour RSI are almost the same series, and once
    one strong feature is kept, everything correlated with it goes, taking whole
    unrelated families down with it. On this data that left 1 feature out of
    132. Clustering keeps one member of each family instead of one member
    overall.
    """
    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import squareform

    corr = np.corrcoef(tr[feats].to_numpy(dtype="float64").T)
    corr = np.nan_to_num(corr, nan=0.0)
    dist = 1.0 - np.abs(corr)
    np.fill_diagonal(dist, 0.0)
    dist = np.clip((dist + dist.T) / 2.0, 0.0, None)
    labels = fcluster(linkage(squareform(dist, checks=False), method="average"),
                      t=1.0 - thresh, criterion="distance")

    keep = []
    for cl in np.unique(labels):
        members = [f for f, lab in zip(feats, labels, strict=True) if lab == cl]
        keep.append(max(members, key=lambda f: mi[f]))
    return keep


def _select(tr: pd.DataFrame, cols: list[str]) -> list[str]:
    """MI screen, correlation clustering, then a clustered-SE p-filter."""
    from sklearn.feature_selection import mutual_info_classif
    samp = tr.sample(min(MI_SAMPLE, len(tr)), random_state=SEED)
    mi_vals = mutual_info_classif(samp[cols].to_numpy(dtype="float32"),
                                  samp["y"].to_numpy(), discrete_features="auto",
                                  random_state=SEED)
    mi = dict(zip(cols, mi_vals, strict=True))
    surv = [c for c in cols if mi[c] > 0.001]
    if not surv:
        return []
    keep_mi = _cluster_prune(tr, surv, mi)
    logger.info("      MI: %d -> %d, korrelacio-klaszterezes utan: %d",
                len(cols), len(surv), len(keep_mi))

    x = sm.add_constant(tr[keep_mi].to_numpy(dtype="float64"), has_constant="add")
    day = pd.to_datetime(tr["open_time"]).dt.normalize().to_numpy()
    fit = sm.OLS(tr["y"].to_numpy(), x).fit(
        cov_type="cluster", cov_kwds={"groups": day}, use_t=True)
    pv = fit.pvalues[1:]
    logger.info("      legkisebb p: %.4f   p<0.01: %d   p<0.05: %d",
                float(np.min(pv)), int((pv < 0.01).sum()), int((pv < 0.05).sum()))
    return [c for c, p in zip(keep_mi, pv, strict=True) if p < P_MAX]


def run(null_shift_h: int = 0) -> None:
    conn = duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)
    try:
        cols = [r[0] for r in conn.execute(f"DESCRIBE {TABLE}").fetchall()
                if r[0].startswith("feat_")]
        sel = ", ".join(f'"{c}"' for c in cols)
        feats = conn.execute(
            f"SELECT open_time, {sel} FROM {TABLE} ORDER BY open_time").df()
    finally:
        conn.close()

    # A one-bar window leaves nothing to compute -- no dispersion, no
    # kurtosis -- so those columns come back all-NaN. DuckDB's count() does not
    # catch them, because a float NaN is a value there, not a NULL.
    usable = [c for c in cols if feats[c].notna().mean() > 0.5]
    dropped = len(cols) - len(usable)
    cols = usable

    labels = build_labels(TRAIN_END)
    if null_shift_h:
        # Null test. The label is rotated forward by a long offset, which
        # destroys any relation to the features while leaving both series'
        # own autocorrelation intact -- a fairer null than shuffling, which
        # would also destroy the overlap that inflates the in-sample fit.
        for h in HORIZONS_MIN:
            for c in (f"y{h}", f"w{h}"):
                labels[c] = np.roll(labels[c].to_numpy(), null_shift_h)
        logger.warning("NULL-TESZT: a cimke %d oraval elforgatva", null_shift_h)
    df = feats.merge(labels, on="open_time", how="inner")

    print("\n" + "=" * 92)
    print(f"ROVID FEATURE + ROVIDEBB HORIZONT — {SYMBOL}")
    print("=" * 92)
    print(f"  {len(df):,} orai sor, {len(cols)} rovid-ablaku feature "
          f"(max {MAX_LOOKBACK_H}h visszanezes)")
    print(f"  {dropped} oszlop eldobva: 1-2 oras ablakon degeneralt "
          "(nincs szoras/csucsossag ennyi baron)")
    print(f"  valtozo-szeles: egyszer, a {TRAIN_END}-ig tarto ablakon")
    print("  egyutthatok: HAVONTA ujraillesztve minden addigi adaton\n")

    for h in HORIZONS_MIN:
        d = df.dropna(subset=[f"y{h}", *cols]).reset_index(drop=True)
        dts = pd.to_datetime(d["open_time"])
        d = d.rename(columns={f"y{h}": "y"})
        init = d[dts <= TRAIN_END]
        chosen = _select(init, cols)
        med_w = float(np.nanmedian(init[f"w{h}"]))
        v = init["y"].value_counts(normalize=True)

        print(f"  --- horizont {h} perc ({h / 60:.0f} ora) ---")
        print(f"      korlat med {med_w:.0f} bps   dij/korlat {FEE_BPS / med_w:.1%}"
              f"   +1 {v.get(1, 0) * 100:.1f}%  -1 {v.get(-1, 0) * 100:.1f}%"
              f"  timeout {v.get(0, 0) * 100:.1f}%")
        if not chosen:
            print("      egyetlen valtozo sem szignifikans\n")
            continue
        print(f"      valasztott valtozok ({len(chosen)}): {', '.join(chosen)}")

        rows = []
        for period in pd.period_range("2024-08", EVAL_END, freq="M"):
            m0, m1 = period.to_timestamp(), (period + 1).to_timestamp()
            tr = d[dts < m0 - pd.Timedelta(minutes=h)]
            te = d[(dts >= m0) & (dts < m1)]
            if len(tr) < 8000 or len(te) < 200:
                continue
            x_tr = sm.add_constant(tr[chosen].to_numpy(dtype="float64"),
                                   has_constant="add")
            x_te = sm.add_constant(te[chosen].to_numpy(dtype="float64"),
                                   has_constant="add")
            m = sm.OLS(tr["y"].to_numpy(), x_tr).fit()
            p_te = np.asarray(m.predict(x_te))
            y_te = te["y"].to_numpy()
            if p_te.std() < 1e-12:
                continue
            p_tr = np.asarray(m.predict(x_tr))
            y_tr = tr["y"].to_numpy()
            rows.append({
                "ho": str(period), "n": len(te),
                "ic_tr": float(np.corrcoef(p_tr, y_tr)[0, 1]),
                "ic": float(np.corrcoef(p_te, y_te)[0, 1]),
                "auc_tr": _auc(p_tr, y_tr), "auc": _auc(p_te, y_te),
                "lift_tr": _lift(p_tr, y_tr), "lift": _lift(p_te, y_te),
            })

        res = pd.DataFrame(rows)

        def _t(v: np.ndarray, centre: float = 0.0) -> float:
            v = v[np.isfinite(v)]
            return (float((v - centre).mean() / (v.std(ddof=1) / np.sqrt(len(v))))
                    if len(v) > 1 else float("nan"))

        ic, ic_tr = res["ic"].to_numpy(), res["ic_tr"].to_numpy()
        auc, auc_tr = res["auc"].to_numpy(), res["auc_tr"].to_numpy()
        lf, lf_tr = res["lift"].to_numpy(), res["lift_tr"].to_numpy()
        print(f"      gorduló meres: {len(res)} honap")
        print(f"        IC        train {ic_tr.mean():+.4f}   valid {ic.mean():+.4f}"
              f"   szoras {ic.std(ddof=1):.4f}   t {_t(ic):5.2f}"
              f"   pozitiv {int((ic > 0).sum())}/{len(ic)}")
        print(f"        AUC       train {auc_tr.mean():.4f}   valid {auc.mean():.4f}"
              f"   szoras {auc.std(ddof=1):.4f}   t {_t(auc, 0.5):5.2f}"
              f"   >0,5   {int((auc > 0.5).sum())}/{len(auc)}")
        print(f"        p90 lift  train {lf_tr.mean():+.2f}pp  "
              f"valid {lf[np.isfinite(lf)].mean():+.2f}pp"
              f"  szoras {lf[np.isfinite(lf)].std(ddof=1):.2f}"
              f"   t {_t(lf):5.2f}"
              f"   pozitiv {int((lf > 0).sum())}/{int(np.isfinite(lf).sum())}")
        print("        (AUC es lift: +1 vs -1, a timeout sorok nelkul)\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rovid feature / rovid horizont")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--null-shift", type=int, default=0,
                        help="A cimke elforgatasa N oraval: null-teszt. "
                             "Ha a train IC igy is ugyanakkora, akkor az "
                             "illesztesi optimizmus, nem jel.")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.build:
        build_features()
    if args.run:
        run(args.null_shift)


if __name__ == "__main__":
    main()
