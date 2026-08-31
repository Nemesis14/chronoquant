#!/usr/bin/env python3
"""Cross-sectional ranker walk-forward (epic_062 / t833).

The probe (t832) answered the first half: the gap between the best and worst
perp in a given hour is ~336 bps at the median, against ~19 bps of round-trip
cost for a two-legged book. There is plenty on the table.

It also answered how much of that a trivial signal reaches: **4.1 bps**, or
1.2%. Trailing-return rankers do not get at it, and every one of them loses to
the cost. So the open question is not "is there opportunity" but "is any of it
forecastable by something better than a one-line rule".

This measures that, with the same discipline as the directional work:

    features        computed per symbol from the hourly panel, all lagged so
                    the bar being predicted contributes nothing
    target          the forward 4h return **demeaned across symbols** -- the
                    market move is exactly what a neutral book does not earn,
                    so training on raw return would teach beta again
    refit           monthly, on everything before that month
    evaluation      long the predicted top decile, short the bottom, and read
                    off the realised demeaned return, minus cost

The cost is charged once per pair opened, not per hour: each hour starts a
book that is held for the full horizon and then closed. Running overlapping
books does not make it cheaper -- each one pays its own round trip. So the
only lever on the cost ratio is the horizon itself: the fee is fixed per pair
while the move being captured grows with holding time.

Usage:
    python -m modeling.experiments.cross_sectional_ranker
    python -m modeling.experiments.cross_sectional_ranker --decile 0.2
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

logger = logging.getLogger(__name__)

DB_REL = os.path.join("database", "_universe", "universe.duckdb")
HORIZON_H = 4  # overridden by --horizon
LEG_COST_BPS = 9.4
PAIR_COST_BPS = 2 * LEG_COST_BPS
TRAIN_START = "2024-01-01"
EVAL_START = "2024-10-01"   # nine months of history before the first refit
EVAL_END = "2026-07-31"
MIN_SYMBOLS = 15
SEED = 42

LGB_PARAMS: dict = {
    "objective": "regression",
    "n_estimators": 400,
    "learning_rate": 0.03,
    "num_leaves": 31,
    "min_child_samples": 200,
    "subsample": 0.8,
    "subsample_freq": 1,
    "colsample_bytree": 0.7,
    "reg_lambda": 10.0,
    "verbosity": -1,
    "n_jobs": 4,
    "random_state": SEED,
}


def load_panel() -> tuple[pd.DataFrame, pd.DataFrame]:
    db_path = os.path.join(utils._repo_root(), DB_REL)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"{DB_REL} hianyzik -- eloszor a build_universe")
    conn = duckdb.connect(db_path, read_only=True)
    try:
        panel = conn.execute("""
            SELECT symbol, open_time, high, low, close, quote_volume
            FROM ohlcv_1h ORDER BY symbol, open_time
        """).df()
        funding = conn.execute("""
            SELECT symbol, funding_ts, funding_rate FROM funding
            ORDER BY symbol, funding_ts
        """).df()
    finally:
        conn.close()
    return panel, funding


def build_features(
    panel: pd.DataFrame, funding: pd.DataFrame
) -> tuple[pd.DataFrame, list[str]]:
    """Per-symbol time-series features plus their cross-sectional ranks.

    Two families, and the second is the point. Raw features tell the model how
    one symbol behaved; the cross-sectional rank tells it how that compares to
    the other 29 in the same hour, which is the only thing a relative book can
    act on. A feature that is identical for every symbol in an hour carries no
    cross-sectional information at all, however predictive it looks alone.
    """
    # Funding timestamps land on the 8-hour grid; snapping them to the hour
    # lets the whole window sum become a cumulative-sum difference.
    funding = funding.copy()
    funding["hour"] = pd.to_datetime(funding["funding_ts"]).dt.floor("h")
    fund_map = {
        str(sym): g.groupby("hour")["funding_rate"].sum()
        for sym, g in funding.groupby("symbol")
    }

    frames = []
    for _symbol, g in panel.groupby("symbol", sort=False):
        g = g.sort_values("open_time").copy()
        close = g["close"]
        logret = np.log(close / close.shift(1))

        for lb in (1, 4, 12, 24, 72, 168):
            g[f"ret_{lb}"] = np.log(close / close.shift(lb))
        g["vol_24"] = logret.rolling(24).std()
        g["vol_168"] = logret.rolling(168).std()
        g["vol_ratio"] = g["vol_24"] / g["vol_168"]
        # Where price sits inside its recent range: 0 at the low, 1 at the high.
        hi = g["high"].rolling(168).max()
        lo = g["low"].rolling(168).min()
        g["range_pos"] = (close - lo) / (hi - lo)
        turn = g["quote_volume"]
        g["turn_z"] = (
            (turn - turn.rolling(168).mean()) / turn.rolling(168).std()
        )
        g["turn_log"] = np.log1p(turn)
        # Overnight-style gap: how much of the last move happened in one bar.
        g["ret1_over_vol"] = logret / g["vol_24"]

        g["fwd"] = np.log(close.shift(-HORIZON_H) / close) * 10_000.0

        # Funding settled inside the holding window, in bps. Positive means
        # longs pay: a long position loses it, a short position collects it.
        # On a 48h hold that is six settlements, which on alt perps is the same
        # order of magnitude as the entire edge -- so it is priced, not assumed
        # to cancel between the two legs.
        f = fund_map.get(str(g["symbol"].iloc[0]))
        if f is None:
            g["fund_bps"] = 0.0
        else:
            hourly = f.reindex(g["open_time"]).fillna(0.0).to_numpy() * 10_000.0
            cum = np.concatenate([[0.0], np.cumsum(hourly)])
            fwd_idx = np.minimum(np.arange(len(g)) + HORIZON_H, len(g))
            g["fund_bps"] = cum[fwd_idx] - cum[np.arange(len(g))]
        frames.append(g)

    df = pd.concat(frames, ignore_index=True)

    base = [c for c in df.columns
            if c.startswith(("ret_", "vol_", "turn_", "range_", "ret1_"))]
    # Everything is lagged one bar: the hour being predicted must not be in
    # its own features, and `ret_1` at time t already ends at t.
    df[base] = df.groupby("symbol", sort=False)[base].shift(1)

    # The market move is what a neutral book cannot earn, so it is removed from
    # the target rather than left for the model to rediscover as beta.
    df["fwd_demeaned"] = df["fwd"] - df.groupby("open_time")["fwd"].transform("mean")

    ranks = []
    for col in base:
        r = df.groupby("open_time")[col].rank(pct=True)
        name = f"xs_{col}"
        df[name] = r
        ranks.append(name)

    feats = base + ranks
    df = df.dropna(subset=[*feats, "fwd_demeaned"])
    return df, feats


def walk_forward(
    df: pd.DataFrame, feats: list[str], decile: float, top_liquid: int = 0
) -> pd.DataFrame:
    """Refit monthly, rank that month's hours, and price the resulting book."""
    ts = pd.to_datetime(df["open_time"])
    rows = []

    for period in pd.period_range(EVAL_START, EVAL_END, freq="M"):
        m_start = period.to_timestamp()
        m_end = (period + 1).to_timestamp()
        # The target reaches HORIZON_H hours forward, so training rows that
        # close inside the evaluation month have to go.
        train_hi = m_start - pd.Timedelta(hours=HORIZON_H)

        tr = df[(ts >= TRAIN_START) & (ts < train_hi)]
        te = df[(ts >= m_start) & (ts < m_end)]
        if len(tr) < 20_000 or len(te) < 500:
            continue

        model = lgb.LGBMRegressor(**LGB_PARAMS)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(tr[feats], tr["fwd_demeaned"])

        te = te.copy()
        te["pred"] = model.predict(te[feats])

        hourly: list[float] = []
        fundings: list[float] = []
        for _, hour in te.groupby("open_time"):
            if top_liquid:
                # The cost is calibrated on SOLUSDT. On a thin alt the spread
                # is wider, so a book that only trades the deepest names is the
                # honest version of the same cost assumption.
                hour = hour.nlargest(top_liquid, "quote_volume")
            if len(hour) < MIN_SYMBOLS:
                continue
            k = max(1, int(round(len(hour) * decile)))
            order = hour.sort_values("pred")
            longs = order["fwd_demeaned"].iloc[-k:].mean()
            shorts = order["fwd_demeaned"].iloc[:k].mean()
            # The long leg pays its funding, the short leg collects its own.
            fund = (-order["fund_bps"].iloc[-k:].mean()
                    + order["fund_bps"].iloc[:k].mean())
            hourly.append(longs - shorts)
            fundings.append(fund)

        if not hourly:
            continue
        arr = np.asarray(hourly)
        fnd = np.asarray(fundings)
        net = arr + fnd - PAIR_COST_BPS
        rows.append({
            "ho": str(period),
            "orak": len(arr),
            "brutto_bps": round(float(arr.mean()), 2),
            "funding_bps": round(float(fnd.mean()), 2),
            "netto_bps": round(float(net.mean()), 2),
            "median_bps": round(float(np.median(net)), 2),
            "pozitiv_%": round(float((net > 0).mean() * 100), 1),
        })

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cross-sectional rangsorolo")
    parser.add_argument("--decile", type=float, default=0.10)
    parser.add_argument("--top-liquid", type=int, default=0,
                        help="Csak a legforgalmasabb N par kereskedheto az adott "
                             "oraban (0 = mind). A koltseg SOLUSDT-re van "
                             "kalibralva, ezert a vekony parok kizarasa az "
                             "oszinte valtozata ugyanannak a feltevesnek.")
    parser.add_argument("--horizon", type=int, default=4,
                        help="Tartasi ido oraban. A dij par-onkent fix, a "
                             "megfogott mozgas viszont a horizonttal no -- "
                             "ez az egyetlen kar a koltseg-aranyra.")
    args = parser.parse_args()

    global HORIZON_H  # noqa: PLW0603
    HORIZON_H = args.horizon

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    panel, funding = load_panel()
    logger.info("panel: %d szimbolum, %s funding sor",
                panel["symbol"].nunique(), f"{len(funding):,}")

    df, feats = build_features(panel, funding)
    logger.info("feature-ok: %d, hasznalhato sor: %s", len(feats), f"{len(df):,}")

    res = walk_forward(df, feats, args.decile, args.top_liquid)
    if res.empty:
        print("nincs eredmeny")
        return

    print(f"\n=== HAVI EREDMENY (top/also {args.decile:.0%} decilis) ===")
    print(res.to_string(index=False))

    total = float((res["brutto_bps"] * res["orak"]).sum() / res["orak"].sum())
    fund_t = float((res["funding_bps"] * res["orak"]).sum() / res["orak"].sum())
    n_h = int(res["orak"].sum())
    ts = pd.to_datetime(res["ho"] + "-01")
    print(f"\n  par-koltseg {PAIR_COST_BPS:.1f} bps oranként (a legszigorubb "
          "feltevés: teljes forgás minden órában)")
    print(f"  EGYUTT {n_h:,} ora  brutto {total:+.2f} bps  "
          f"funding {fund_t:+.2f} bps  netto {total + fund_t - PAIR_COST_BPS:+.2f} bps")
    print(f"  pozitiv honap: {int((res['netto_bps'] > 0).sum())}/{len(res)} (netto)"
          f"   {int((res['brutto_bps'] > 0).sum())}/{len(res)} (brutto)")

    for name, lo, hi in (("emelkedo", "2024-10", "2025-07"),
                         ("eso", "2025-08", "2026-07")):
        sub = res[(ts >= f"{lo}-01") & (ts <= f"{hi}-01")]
        if sub.empty:
            continue
        t = float((sub["brutto_bps"] * sub["orak"]).sum() / sub["orak"].sum())
        fq = float((sub["funding_bps"] * sub["orak"]).sum() / sub["orak"].sum())
        print(f"  {name:9} brutto {t:+.2f}  funding {fq:+.2f}  "
              f"netto {t + fq - PAIR_COST_BPS:+.2f} bps")

    be_leg = (total + fund_t) / 2.0
    print(f"\n  fedezeti labankenti koltseg: {be_leg:.1f} bps "
          f"({be_leg / LEG_COST_BPS:.1f}x a feltetelezett {LEG_COST_BPS:.1f} bps-nek)"
          " -- ennyire kellene romlania a vegrehajtasnak, hogy nullara jojjon")

    print("\n  Osszevetes: a legjobb naiv rangsorolo (reverzal 72h) +4,1 bps "
          "bruttot fogott meg,")
    print("  a tokeletes elorelatas felso korlatja ~336 bps.")


if __name__ == "__main__":
    main()
