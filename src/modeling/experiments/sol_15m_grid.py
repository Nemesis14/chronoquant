#!/usr/bin/env python3
"""The whole explanatory side on a 15-minute bar (epic_064).

The hourly grid smooths away everything that happens inside an hour. A move
that builds over twenty minutes and is spent by the close of the hour leaves no
trace in an hourly bar, and no feature computed on that bar can see it. Since
the target is an 8-hour barrier, an intra-hour build-up is exactly the kind of
early sign worth having.

So the bars themselves become 15 minutes, not just the sampling. Two things
follow, and they are different from each other:

**The lookbacks are rescaled to cover the same wall-clock spans.** A 4-bar
window is one hour here, 32 bars is eight. Without this the model would be
comparing a one-hour history against the hourly run's eight-hour one and the
comparison would mean nothing.

**Windows shorter than an hour become expressible for the first time.** A
2-bar window is 30 minutes -- there was no such feature on the hourly grid at
any setting. This, not the extra rows, is what could actually help.

⚠️ **Four times the rows is not four times the evidence.** An 8-hour label on an
hourly grid overlaps 7 of its 8 hours; here it overlaps 31 of its 32 quarters.
The count of independent 8-hour windows is unchanged, so significance is still
judged on the 24 monthly blocks, never on the row count. If the numbers improve
it is because the features got sharper, not because the sample got bigger.

Usage:
    python -m modeling.experiments.sol_15m_grid --build
    python -m modeling.experiments.sol_15m_grid --run
    python -m modeling.experiments.sol_15m_grid --run --null-shift 4380
"""

import argparse
import logging
import os
import re
import sys

import duckdb
import numpy as np
import pandas as pd
import polars as pl

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
    PANEL_START,
    is_calendar,
)
from modeling.experiments.sol_lgbm_8h import (
    GRID,
    N_RANDOM,
    SEED,
    _fit,
    _metrics,
    _t,
    purged_folds,
)
from modeling.experiments.sol_short_horizon import (
    HIDDEN_LOOKBACK,
    K_TARGET_MULT,
    MAX_BPS,
    MIN_BPS,
    SYMBOL,
    TRAIN_END,
    VOL_SPAN,
)

logger = logging.getLogger(__name__)

TABLE = "feat_sol_short_15m"
BAR_MIN = 15
HORIZON_MIN = 480
EVAL_START, EVAL_END = "2024-08", "2026-07-31"

# In bars: 30 min, 1h, 2h, 4h, 8h. The first is new -- the hourly grid could
# not express a sub-hour lookback at all.
WINDOWS = (2, 4, 8, 16, 32)
MAX_LOOKBACK_BARS = 32                    # still 8 hours, so the comparison holds
NORM_WINDOW = 30 * 24 * (60 // BAR_MIN)   # 30 days, matching the hourly 720
PURGE_BARS = HORIZON_MIN // BAR_MIN       # 32 bars = the label horizon

# Reference from the hourly run (sol_lgbm_8h), for side-by-side reading only.
HOURLY = {"ic": 0.0240, "auc": 0.5157, "lift": 2.22,
          "ic_base": 0.0134, "auc_base": 0.5084, "lift_base": 1.25}


def too_long(col: str) -> bool:
    """Reject anything reaching past MAX_LOOKBACK_BARS, named or hidden."""
    if any(h in col for h in HIDDEN_LOOKBACK):
        return True
    m = re.findall(r"_(\d+)$", col)
    return bool(m) and int(m[0]) > MAX_LOOKBACK_BARS


def config_15m() -> dict:
    """The short config with every window expressed in 15-minute bars."""
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
                # The hourly run used 1/2/1, 2/4/2, 4/8/3 bars; x4 keeps the
                # same wall-clock spans.
                out[group][name] = [{"fast": 4, "slow": 8, "signal": 4},
                                    {"fast": 8, "slow": 16, "signal": 8},
                                    {"fast": 16, "slow": 32, "signal": 12}]
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
                        item["smooth_window"] = max(2, w // 2)
                    if name == "kama":
                        item["fast"], item["slow"] = 4, max(w, 8)
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
            SELECT time_bucket(INTERVAL '15 minutes', open_time) AS open_time,
                   arg_min(open, open_time) AS "open", max(high) AS high,
                   min(low) AS low, arg_max(close, open_time) AS "close",
                   sum(volume) AS volume, sum(quote_volume) AS quote_volume,
                   sum(n_trades) AS trades, sum(taker_buy_volume) AS taker_buy_base
            FROM ohlcv_5m WHERE symbol = ? GROUP BY 1 ORDER BY 1
        """, [SYMBOL]).pl()
        logger.info("%d db 15 perces bar", bars.height)

        feats = compute_features_polars(
            df_ohlcv=bars, indicators=config_15m(), feat_prefix="feat_",
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
        logger.info("%s: %d sor, %d feature", TABLE, out.height, len(keep))
    finally:
        conn.close()


def build_labels(train_end: str) -> pd.DataFrame:
    """The same 1-minute barrier label, kept every 15 minutes instead of 60."""
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

    unit = horizon_barrier_bps(close, horizon=HORIZON_MIN, k=1.0, span=VOL_SPAN,
                               min_bps=0.0, max_bps=np.inf)
    w = np.clip(K_TARGET_MULT * unit, MIN_BPS, MAX_BPS)
    frame = pd.DataFrame({
        "open_time": bars["open_time"],
        "y": triple_barrier_labels(close, high, low, horizon=HORIZON_MIN,
                                   up_bps=w, dn_bps=w),
        "w": w,
    })
    logger.info("h=%d perc  train-median egyseg %.0f bps  ->  korlat med %.0f",
                HORIZON_MIN, float(np.nanmedian(unit[in_train])),
                float(np.nanmedian(w[in_train])))
    ts = pd.to_datetime(frame["open_time"])
    return frame.loc[ts.dt.minute % BAR_MIN == 0].reset_index(drop=True)


def _walk(d: pd.DataFrame, dts: pd.Series, feats: list[str],
          params: dict) -> pd.DataFrame:
    rows = []
    for period in pd.period_range(EVAL_START, EVAL_END, freq="M"):
        m0, m1 = period.to_timestamp(), (period + 1).to_timestamp()
        tr = d.loc[dts < m0 - pd.Timedelta(minutes=HORIZON_MIN)]
        te = d.loc[(dts >= m0) & (dts < m1)]
        if len(tr) < 8000 or len(te) < 200:
            continue
        ya, yb = tr["y"].to_numpy(), te["y"].to_numpy()
        m = _fit(params, tr[feats], ya)
        a = _metrics(np.asarray(m.predict(tr[feats])), ya)
        b = _metrics(np.asarray(m.predict(te[feats])), yb)
        rows.append({"ho": str(period),
                     **{f"{k}_tr": v for k, v in a.items()}, **b})
    return pd.DataFrame(rows)


def run(null_shift: int = 0) -> None:
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

    d = (feats.merge(build_labels(TRAIN_END), on="open_time", how="inner")
         .dropna(subset=["y", *cols]).reset_index(drop=True))
    dts = pd.to_datetime(d["open_time"])
    if null_shift:
        d["y"] = np.roll(d["y"].to_numpy(), null_shift * (60 // BAR_MIN))
    init = d.loc[dts <= TRAIN_END].reset_index(drop=True)

    print("\n" + "=" * 88)
    print(f"{BAR_MIN} PERCES RACS, 8 ORAS TARGET — SOLUSDT")
    print("=" * 88)
    print(f"  {len(d):,} sor ({BAR_MIN} percenkent), {len(cols)} feature")
    print(f"  ablakok: {', '.join(f'{w * BAR_MIN}p' for w in WINDOWS)} "
          f"(= {', '.join(f'{w * BAR_MIN / 60:g}h' for w in WINDOWS)})")
    print(f"  train ablak: {len(init):,} sor ({TRAIN_END}-ig)")
    print(f"  ⚠️  atfedes: a {HORIZON_MIN} perces cimke {PURGE_BARS} barbol "
          f"{PURGE_BARS - 1}-et megoszt a szomszedjaval")
    print("     (orasnal 8-bol 7) — a fuggetlen 8 oras ablakok szama VALTOZATLAN")
    if null_shift:
        print(f"  ⚠️  NULL TESZT: cimke {null_shift} oraval elforgatva")
    print()

    folds = purged_folds(len(init), 4, PURGE_BARS)
    print("=== REGULARIZACIO — purge-olt CV a train ablakon belul ===")
    print(f"  {'levelek':>8}{'min_leaf':>10}{'lambda':>8}{'colsamp':>9}{'fak':>6}"
          f"{'CV IC':>10}{'CV AUC':>9}")
    best, best_ic = None, -np.inf
    for params in GRID:
        ics, aucs = [], []
        for i, j in folds:
            m = _fit(params, init.iloc[i][cols], init.iloc[i]["y"].to_numpy())
            mm = _metrics(np.asarray(m.predict(init.iloc[j][cols])),
                          init.iloc[j]["y"].to_numpy())
            ics.append(mm["ic"])
            aucs.append(mm["auc"])
        ic = float(np.mean(ics))
        print(f"  {params['num_leaves']:>8}{params['min_child_samples']:>10}"
              f"{params['reg_lambda']:>8.0f}{params['colsample_bytree']:>9.1f}"
              f"{params['n_estimators']:>6}{ic:>+10.4f}{np.mean(aucs):>9.4f}")
        if ic > best_ic:
            best, best_ic = params, ic
    assert best is not None
    print(f"\n  valasztott: {best}   (CV IC {best_ic:+.4f})\n")

    print("=== GORDULO MERES ===")
    res = _walk(d, dts, cols, best)
    ic, auc, lf = (res["ic"].to_numpy(), res["auc"].to_numpy(),
                   res["lift"].to_numpy())
    print(f"  {BAR_MIN} PERCES RACS ({len(res)} honap)")
    print(f"    IC       train {res['ic_tr'].mean():+.4f}   valid {ic.mean():+.4f}"
          f"   szoras {ic.std(ddof=1):.4f}   t {_t(ic):5.2f}"
          f"   pozitiv {int((ic > 0).sum())}/{len(ic)}")
    print(f"    AUC      train {res['auc_tr'].mean():.4f}   valid {auc.mean():.4f}"
          f"   t {_t(auc, 0.5):5.2f}")
    print(f"    p90 lift train {res['lift_tr'].mean():+.2f}pp  "
          f"valid {lf.mean():+.2f}pp   t {_t(lf):5.2f}"
          f"   pozitiv {int((lf > 0).sum())}/{len(lf)}")

    rng = np.random.default_rng(SEED)
    acc = []
    for _ in range(N_RANDOM):
        pick = list(rng.choice(cols, size=5, replace=False))
        r = _walk(d, dts, pick, best)
        if not r.empty:
            acc.append(r[["ic", "auc", "lift"]].mean())
    base = pd.DataFrame(acc)
    print(f"\n  VELETLEN 5 feature, ugyanaz a modell ({len(base)} huzas)")
    print(f"    IC valid {base['ic'].mean():+.4f}   "
          f"AUC {base['auc'].mean():.4f}   p90 lift {base['lift'].mean():+.2f}pp")

    print("\n  --- 15 PERCES vs ORAS RACS (mindketto a sajat baseline-jahoz) ---")
    print(f"  {'':<10}{'15p modell':>12}{'15p base':>11}{'15p tobblet':>13}"
          f"{'oras tobblet':>14}")
    for nev, a, b, ha, hb in (
            ("IC", ic.mean(), base["ic"].mean(),
             HOURLY["ic"], HOURLY["ic_base"]),
            ("AUC-0,5", auc.mean() - 0.5, base["auc"].mean() - 0.5,
             HOURLY["auc"] - 0.5, HOURLY["auc_base"] - 0.5),
            ("p90 lift", lf.mean(), base["lift"].mean(),
             HOURLY["lift"], HOURLY["lift_base"])):
        print(f"  {nev:<10}{a:>+12.4f}{b:>+11.4f}{a - b:>+13.4f}"
              f"{ha - hb:>+14.4f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="15 perces racs, 8h target")
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--null-shift", type=int, default=0)
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
