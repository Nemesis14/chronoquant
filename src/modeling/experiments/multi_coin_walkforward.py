#!/usr/bin/env python3
"""Selective-entry walk-forward on the 30-coin panel (epic_064 / t856).

The question this measures, in one line: with thirty candidates instead of one,
can a threshold be set strict enough that what survives it pays the fee?

On a single coin p99 was the practical limit -- beyond it there were not enough
bars left to measure. Thirty coins buy roughly thirty times the draw, so the
same number of trades can come from p99.9 instead, a region SOLUSDT could never
sample. The epic_058 evidence says selectivity does lift the hit rate (p95 ~50%,
p99 61.8%); whether it keeps lifting, and whether it survives both regimes, is
what this run answers.

Protocol, fixed before the first run and unchanged from the earlier epics:

    search      ONCE, on the earliest window, cached to disk
    refit       monthly, on everything before that month minus the purge
    threshold   a score level taken from the training predictions and applied
                to the evaluation month UNCHANGED -- never re-picked on the
                data being measured

Entry is selective: if no coin clears the threshold in a given hour, no trade
happens. Both directions are live. Entry rests as a maker limit and its fill is
checked against the next 1-minute bar, the model measured in epic_059 (98.2%
fill, -0.35pp adverse selection).

⚠️ Every trade is priced at its own barrier, because the barrier is volatility-
scaled per coin and per bar: a win is worth what the barrier was when the trade
opened, while the fee is fixed. The break-even hit rate is therefore a per-trade
number, not a constant.

Usage:
    python -m modeling.experiments.multi_coin_walkforward --trials 60
    python -m modeling.experiments.multi_coin_walkforward --quantile 0.999
"""

import argparse
import json
import logging
import os
import pathlib
import sys
import warnings

import duckdb
import lightgbm as lgb
import numpy as np
import optuna
import pandas as pd

import utils

logger = logging.getLogger(__name__)

DB_REL = os.path.join("database", "_universe", "universe.duckdb")
LABEL = "tb_label_1440_vol"
BPS = "tb_label_1440_vol_bps"
FWD = "fwd_ret_1440"
HORIZON_H = 24

SEARCH_START = "2021-12-01"
SEARCH_END = "2023-06-30"
EVAL_START = "2023-07-01"
EVAL_END = "2026-07-31"
PURGE_H = HORIZON_H
SEED = 42

# Best execution measured in epic_059: maker entry and maker take-profit with
# the BNB fee discount; the stop stays a market order on purpose, because a
# stop-limit that does not fill leaves the position in the move it exists to
# escape.
MAKER_FEE_BPS = 1.8
TAKER_FEE_BPS = 4.5
SLIPPAGE_BPS = 1.5
ENTRY_BPS = MAKER_FEE_BPS
EXIT_MAKER_BPS = MAKER_FEE_BPS
EXIT_TAKER_BPS = TAKER_FEE_BPS + SLIPPAGE_BPS
# Three settlements in a 24h hold, on the 8h funding grid.
FUNDING_SETTLEMENTS = 3.0

FIXED_PARAMS: dict = {
    "objective": "multiclass",
    "num_class": 3,
    "boosting_type": "gbdt",
    "metric": "multi_logloss",
    "verbosity": -1,
    "n_jobs": 4,
    "force_col_wise": True,
    "random_state": SEED,
}


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(os.path.join(utils._repo_root(), DB_REL), read_only=True)


def load_dataset() -> tuple[pd.DataFrame, list[str]]:
    """The full hourly panel: features, label, economics and fill inputs."""
    conn = _conn()
    try:
        feats = [
            r[0] for r in conn.execute("DESCRIBE feat_1h").fetchall()
            if r[0].startswith("feat_")
        ]
        shares = [
            r[0] for r in conn.execute("DESCRIBE feat_share_1h").fetchall()
            if r[0].startswith("feat_")
        ]
        # Columns that are null for every row carry nothing and only slow the
        # fit; they are the spot and bookDepth features that were deferred.
        alive = conn.execute(
            "SELECT " + ", ".join(f'count("{c}")' for c in feats) + " FROM feat_1h"
        ).fetchone()
        feats = [c for c, n in zip(feats, alive or [], strict=False) if n]

        sel = ", ".join(f'f."{c}"' for c in feats)
        sel_s = ", ".join(f's."{c}"' for c in shares)
        frame = conn.execute(f"""
            SELECT f.symbol, f.open_time, {sel}, {sel_s},
                   t."{LABEL}" AS label,
                   t."{BPS}"   AS barrier_bps,
                   t."{FWD}"   AS fwd_bps,
                   o.close     AS signal_close,
                   n.low       AS next_low,
                   n.high      AS next_high,
                   q.quote_volume AS turnover
            FROM feat_1h f
            JOIN feat_share_1h s USING (symbol, open_time)
            JOIN target_5m t     USING (symbol, open_time)
            JOIN ohlcv_1m o      ON o.symbol = f.symbol AND o.open_time = f.open_time
            LEFT JOIN ohlcv_1m n ON n.symbol = f.symbol
                                AND n.open_time = f.open_time + INTERVAL 1 MINUTE
            JOIN ohlcv_5m q      ON q.symbol = f.symbol AND q.open_time = f.open_time
            WHERE t."{LABEL}" IS NOT NULL
              AND f.open_time >= '{SEARCH_START}' AND f.open_time <= '{EVAL_END}'
            ORDER BY f.open_time, f.symbol
        """).df()
    finally:
        conn.close()

    logger.info("betoltve: %s sor, %d feature (%d + %d reszarany)",
                f"{len(frame):,}", len(feats) + len(shares), len(feats), len(shares))
    return frame, feats + shares


def monthly_funding() -> pd.DataFrame:
    """Average settled funding per coin per month, for the holding cost."""
    conn = _conn()
    try:
        return conn.execute("""
            SELECT symbol, strftime(funding_ts, '%Y-%m') AS ho,
                   avg(funding_rate) AS rate
            FROM funding GROUP BY 1, 2
        """).df()
    finally:
        conn.close()


def _lift(params: dict, x_tr, y_tr, x_va, y_va) -> float:
    """Hit-rate lift in the top slice of each direction -- what entry consumes."""
    model = lgb.LGBMClassifier(**{**FIXED_PARAMS, **params})
    model.fit(
        x_tr, y_tr + 1,
        eval_set=[(x_va, y_va + 1)],
        callbacks=[lgb.early_stopping(60, verbose=False),
                   lgb.log_evaluation(period=-1)],
    )
    classes = list(model.classes_)
    proba = model.predict_proba(x_va)
    best = 0.0
    for idx, target in ((classes.index(2), 1), (classes.index(0), -1)):
        base = float((y_va == target).mean())
        thr = float(np.quantile(proba[:, idx], 0.995))
        sel = proba[:, idx] >= thr
        if sel.sum() >= 50:
            best = max(best, float((y_va[sel] == target).mean()) - base)
    return best


def search_params(frame: pd.DataFrame, feats: list[str], n_trials: int) -> dict:
    """Optuna on the earliest window only; it never sees an evaluation month."""
    ts = pd.to_datetime(frame["open_time"])
    block = frame[(ts >= SEARCH_START) & (ts <= SEARCH_END)]
    cut = int(len(block) * 0.80)
    x_tr, y_tr = block[feats].iloc[:cut], block["label"].to_numpy()[:cut]
    x_va, y_va = block[feats].iloc[cut:], block["label"].to_numpy()[cut:]
    logger.info("kereses: train=%s  belso valid=%s", f"{len(y_tr):,}", f"{len(y_va):,}")

    def objective(trial: optuna.Trial) -> float:
        return _lift({
            "n_estimators": 1500,
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.10, log=True),
            "num_leaves": trial.suggest_int("num_leaves", 8, 96, log=True),
            "min_child_samples": trial.suggest_int("min_child_samples", 100, 2000, log=True),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "subsample_freq": 1,
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.3, 1.0),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-2, 100.0, log=True),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-3, 10.0, log=True),
        }, x_tr, y_tr, x_va, y_va)

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=SEED)
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    logger.info("legjobb lift a belso validon: %.4f", study.best_value)

    best = dict(study.best_params)
    best.update({"n_estimators": 1500, "subsample_freq": 1})
    return best


def walk_forward(
    frame: pd.DataFrame, feats: list[str], params: dict,
    quantile: float, funding: pd.DataFrame,
) -> pd.DataFrame:
    """Refit monthly, apply a frozen threshold, and price what clears it."""
    ts = pd.to_datetime(frame["open_time"])
    fund = funding.set_index(["symbol", "ho"])["rate"].to_dict()
    rows = []

    for period in pd.period_range(EVAL_START, EVAL_END, freq="M"):
        m_start = period.to_timestamp()
        m_end = (period + 1).to_timestamp()
        train_hi = m_start - pd.Timedelta(hours=PURGE_H)

        tr = frame[(ts >= SEARCH_START) & (ts < train_hi)]
        te = frame[(ts >= m_start) & (ts < m_end)]
        if len(tr) < 50_000 or len(te) < 500:
            continue

        cut = int(len(tr) * 0.85)
        model = lgb.LGBMClassifier(**{**FIXED_PARAMS, **params})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(
                tr[feats].iloc[:cut], tr["label"].to_numpy()[:cut] + 1,
                eval_set=[(tr[feats].iloc[cut:], tr["label"].to_numpy()[cut:] + 1)],
                callbacks=[lgb.early_stopping(60, verbose=False),
                           lgb.log_evaluation(period=-1)],
            )

        classes = list(model.classes_)
        i_dn, i_up = classes.index(0), classes.index(2)
        proba_tr = model.predict_proba(tr[feats])
        proba_te = model.predict_proba(te[feats])

        y = te["label"].to_numpy()
        bar = te["barrier_bps"].to_numpy()
        fwd = te["fwd_bps"].to_numpy()
        sig = te["signal_close"].to_numpy()
        nlo = te["next_low"].to_numpy()
        nhi = te["next_high"].to_numpy()
        turn = te["turnover"].to_numpy()
        syms = te["symbol"].to_numpy()
        times = pd.to_datetime(te["open_time"]).to_numpy()

        for direction, idx, target in (("LONG", i_up, 1), ("SHORT", i_dn, -1)):
            # The threshold is a score level frozen on the training scores, not
            # a quantile of the month being measured. That distinction is what
            # makes "no signal, no trade" meaningful: a quiet month simply
            # produces fewer trades instead of the same number of weaker ones.
            thr = float(np.quantile(proba_tr[:, idx], quantile))
            sel = proba_te[:, idx] >= thr
            if not sel.any():
                rows.append({"ho": str(period), "irany": direction, "jel": 0,
                             "n": 0, "talalat_%": np.nan, "fedezeti_%": np.nan,
                             "netto_bps": 0.0, "bps_per_kotes": np.nan,
                             "napok": 0, "coinok": 0})
                continue

            # A resting limit at the signal close fills only if the next minute
            # trades through it -- a real filter, not a discount.
            filled = (nlo <= sig) if direction == "LONG" else (nhi >= sig)
            filled = filled & ~np.isnan(nlo) & ~np.isnan(nhi)
            take = sel & filled
            if not take.any():
                continue

            lab, b = y[take], bar[take]
            win = b - ENTRY_BPS - EXIT_MAKER_BPS
            loss = -b - ENTRY_BPS - EXIT_TAKER_BPS
            breakeven = -loss / (win - loss)

            month = str(period)
            rate = np.array([fund.get((s, month), 0.0) for s in syms[take]])
            fund_bps = rate * FUNDING_SETTLEMENTS * 10_000.0 * target

            net = np.where(
                lab == target, win,
                np.where(lab == -target, loss,
                         fwd[take] * target - ENTRY_BPS - EXIT_TAKER_BPS),
            ) - fund_bps

            hit = float((lab == target).mean())
            be = float(np.nanmean(breakeven))
            rows.append({
                "ho": month, "irany": direction,
                "jel": int(sel.sum()), "n": int(take.sum()),
                "talalat_%": round(hit * 100, 1),
                "fedezeti_%": round(be * 100, 1),
                "tobblet_pp": round((hit - be) * 100, 1),
                "netto_bps": round(float(np.nansum(net)), 1),
                "bps_per_kotes": round(float(np.nanmean(net)), 2),
                "napok": int(pd.Series(times[take]).dt.normalize().nunique()),
                "coinok": int(pd.unique(syms[take]).size),
                "med_turnover": float(np.nanmedian(turn[take])),
            })

    return pd.DataFrame(rows)


def summarise(res: pd.DataFrame, quantile: float) -> None:
    live = res[res["n"] > 0]
    if live.empty:
        print("nincs egyetlen kotes sem ezen a kuszobon")
        return

    print(f"\n=== HAVI EREDMENY (kuszob = train p{quantile * 100:g}) ===")
    cols = ["ho", "irany", "jel", "n", "talalat_%", "fedezeti_%", "tobblet_pp",
            "netto_bps", "bps_per_kotes", "napok", "coinok"]
    print(live[cols].to_string(index=False))

    print("\n=== OSSZESITES IRANYONKENT ===")
    for direction in ("LONG", "SHORT"):
        d = live[live["irany"] == direction]
        if d.empty:
            continue
        n = int(d["n"].sum())
        total = float(d["netto_bps"].sum())
        hit = float((d["talalat_%"] * d["n"]).sum() / n)
        be = float((d["fedezeti_%"] * d["n"]).sum() / n)
        fill = n / int(d["jel"].sum()) * 100 if d["jel"].sum() else float("nan")
        print(f"  {direction:6} n={n:5}  talalat {hit:5.1f}%  fedezeti {be:5.1f}%"
              f"  tobblet {hit - be:+5.1f}pp  netto {total:9.1f} bps"
              f"  ({total / n:+.2f} bps/kotes)  telesules {fill:4.1f}%"
              f"  pozitiv honap {int((d['netto_bps'] > 0).sum())}/{len(d)}")

    n_all = int(live["n"].sum())
    tot = float(live["netto_bps"].sum())
    print(f"\n  EGYUTT n={n_all}  netto {tot:.1f} bps  ({tot / n_all:+.2f} bps/kotes)")
    print(f"  1000 USD notionalon: {tot / 100:.0f} USD")

    print("\n=== REZSIM SZERINT ===")
    ts = pd.to_datetime(live["ho"] + "-01")
    for name, lo, hi in (("2023H2-2024", "2023-07", "2024-12"),
                         ("2025", "2025-01", "2025-12"),
                         ("2026", "2026-01", "2026-07")):
        sub = live[(ts >= f"{lo}-01") & (ts <= f"{hi}-01")]
        if sub.empty or sub["n"].sum() == 0:
            continue
        n = int(sub["n"].sum())
        t = float(sub["netto_bps"].sum())
        hit = float((sub["talalat_%"] * sub["n"]).sum() / n)
        be = float((sub["fedezeti_%"] * sub["n"]).sum() / n)
        print(f"  {name:14} n={n:5}  talalat {hit:5.1f}%  fedezeti {be:5.1f}%"
              f"  tobblet {hit - be:+5.1f}pp  {t / n:+.2f} bps/kotes")

    print("\n=== JEL-KLASZTEREZES ===")
    print("  Ha a jelek keves napra tomorulnek es egyszerre sok coinon szolalnak")
    print("  meg, akkor nem szelektivitast vettunk, hanem rezsim-detektort.")
    days = int(live["napok"].sum())
    print(f"    kulonbozo napok osszesen (honaponkent osszegezve): {days:,}")
    print(f"    atlagosan {live['n'].sum() / max(days, 1):.2f} kotes/nap")
    print(f"    atlagosan {live['coinok'].mean():.1f} kulonbozo coin havonta")


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-coin szelektiv walk-forward")
    parser.add_argument("--trials", type=int, default=60)
    parser.add_argument("--quantile", type=float, default=0.995)
    parser.add_argument("--params-cache", default=None)
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    frame, feats = load_dataset()
    funding = monthly_funding()

    cache = pathlib.Path(args.params_cache) if args.params_cache else None
    if cache is not None and cache.exists():
        params = json.loads(cache.read_text(encoding="utf-8"))
        logger.info("parameterek a cache-bol: %s", cache)
    else:
        params = search_params(frame, feats, args.trials)
        if cache is not None:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps(params, indent=2), encoding="utf-8")

    print("\nparameterek:")
    for k, v in sorted(params.items()):
        print(f"  {k:20} {v}")

    res = walk_forward(frame, feats, params, args.quantile, funding)
    summarise(res, args.quantile)


if __name__ == "__main__":
    main()
