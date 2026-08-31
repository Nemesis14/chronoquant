#!/usr/bin/env python3
"""Does a run of champion signals call a multi-day move? (epic_064)

The live champion is a 60-minute model: it predicts the largest favourable
excursion over the next hour, and traded on that horizon the fee eats the
little it earns. But it is not traded that way. The observed practice is to
watch the signal stream and enter when long signals arrive *in a run*, then
hold for the swing -- days, sometimes a week.

That is a different claim from the one the model was fitted for, and it is
testable: if a run of K consecutive long signals says something about the next
H days, trades opened that way must beat what the same H days would have
returned anyway.

**That last clause is the whole test.** SOL rising over a period makes every
long rule look profitable. So a trade's return is never reported on its own --
it is measured against the mean return of *every* H-day window in the same
period. That difference, not the raw return, is the edge.

Three windows are reported separately and never pooled:

- **train (2021-01 … 2026-01)** -- the champion was fitted here. The signals
  are in-sample; a good result means nothing. It is shown as an upper bound.
- **valid (2026-02 … 2026-07)** -- out-of-sample for the fit, though the
  calibration used it. This is the honest test.
- **2026-08** -- the month the hypothesis came from. It is the *generating*
  observation, so it cannot also be the confirmation. Descriptive only.

⚠️ The percentile that defines a signal is computed from a trailing window, not
from the stored calibration lookup. The live lookup was fitted on a fixed
period, and applying it backwards would let the entry threshold know about data
the moment had not seen.

⚠️ **Trades are not independent observations.** A run of signals opens many
entries inside one episode -- at one setting, 32 of 75 trades fall on a single
day -- and they all resolve into the same move. Entries separated by less than
one holding period are therefore collapsed into a single episode, and the
headline statistic is the equally-weighted mean across episodes. A block
bootstrap over trades is not enough: it still weights a busy episode by its
trade count, which is how a t of 0.46 first read as p = 0.009.

Usage:
    python -m modeling.experiments.champion_streak_test
    python -m modeling.experiments.champion_streak_test --hold 3 --detail
"""

import argparse
import logging
import sys

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

DB = "database/solusdt/solusdt.duckdb"
TRAIN_END = pd.Timestamp("2026-01-31")
VALID_END = pd.Timestamp("2026-07-31")

PCT_WINDOW_D = 30          # trailing window for the causal percentile
THRESHOLDS = (0.80, 0.90, 0.95)
STREAKS = (1, 2, 3, 4, 6, 8, 12)     # consecutive signal hours
HOLDS_D = (1, 2, 3, 5, 7, 14)        # holding period in days
COST_BPS = 10.0            # maker in + taker out + slippage, config/strategies.json
SEED = 42


def load() -> pd.DataFrame:
    """Hourly champion scores with a causal percentile for each side."""
    conn = duckdb.connect(DB, read_only=True)
    try:
        # ⚠️ predictions.close is NULL for 98.5% of rows (only the live tail
        # carries it), so the price comes from ohlcv, which is complete.
        d = conn.execute("""
            WITH p AS (
                SELECT date_trunc('hour', open_time) AS ts,
                       arg_max(long_pred, open_time)  AS long_pred,
                       arg_max(short_pred, open_time) AS short_pred
                FROM predictions
                WHERE long_pred IS NOT NULL AND short_pred IS NOT NULL
                GROUP BY 1
            ), o AS (
                SELECT date_trunc('hour', open_time) AS ts,
                       arg_max(close, open_time) AS close
                FROM ohlcv GROUP BY 1
            )
            SELECT p.ts, o.close, p.long_pred, p.short_pred
            FROM p JOIN o USING (ts)
            WHERE o.close IS NOT NULL
            ORDER BY p.ts
        """).df()
    finally:
        conn.close()
    d["ts"] = pd.to_datetime(d["ts"])

    w = PCT_WINDOW_D * 24
    # Rank within the trailing window only -- the live UI uses a fitted lookup,
    # but that lookup knows the whole calibration period, so replaying it
    # backwards would leak. `closed="left"` keeps the current bar out of its
    # own reference distribution.
    # ⚠️ short_pred is the predicted *downside* excursion, so it is negative and
    # a stronger short signal is more negative. Negating it puts both sides on
    # the same convention: a higher percentile always means a stronger signal.
    d["_long_s"], d["_short_s"] = d["long_pred"], -d["short_pred"]
    for side in ("long", "short"):
        roll = d[f"_{side}_s"].rolling(w, min_periods=w // 4, closed="left")
        d[f"{side}_pct"] = roll.rank(pct=True)
    return d.dropna(subset=["long_pct", "short_pct"]).reset_index(drop=True)


def entries(sig: np.ndarray, k: int) -> np.ndarray:
    """Indices where a run of exactly k consecutive signals completes.

    Only the bar that completes the run counts. A run of twenty would
    otherwise open seventeen overlapping trades and the count would measure
    persistence rather than signal.
    """
    if k == 1:
        return np.flatnonzero(sig & ~np.r_[False, sig[:-1]])
    run = np.zeros(len(sig), dtype=int)
    for i in range(len(sig)):
        run[i] = run[i - 1] + 1 if sig[i] and i else int(sig[i])
    return np.flatnonzero(run == k)


def _trades(d: pd.DataFrame, idx: np.ndarray, hold_h: int,
            side: int) -> np.ndarray:
    """Net return in bps for each entry, exiting hold_h hours later."""
    ok = idx[idx + hold_h < len(d)]
    if len(ok) == 0:
        return np.array([])
    entry = d["close"].to_numpy()[ok]
    exit_ = d["close"].to_numpy()[ok + hold_h]
    return (side * (exit_ / entry - 1.0) * 1e4) - COST_BPS


def _baseline(d: pd.DataFrame, hold_h: int, side: int) -> np.ndarray:
    """What every H-hour window in this period returned, same side, same cost."""
    c = d["close"].to_numpy()
    if len(c) <= hold_h:
        return np.array([])
    return (side * (c[hold_h:] / c[:-hold_h] - 1.0) * 1e4) - COST_BPS


def episodes(idx: np.ndarray, pnl: np.ndarray, hold_h: int) -> np.ndarray:
    """Collapse entries into independent episodes, equally weighted.

    Entries closer together than one holding period resolve into the same
    move, so counting them separately inflates the sample without adding
    evidence.
    """
    if len(pnl) == 0:
        return np.array([])
    ids = np.cumsum(np.r_[True, np.diff(idx[:len(pnl)]) > hold_h])
    return np.array([pnl[ids == e].mean() for e in np.unique(ids)])


def scan(d: pd.DataFrame, name: str, hold_d: int, detail: bool) -> None:
    hold_h = hold_d * 24
    print(f"\n{'=' * 96}")
    print(f"{name}   —   {hold_d} napos tartas   ({d['ts'].min():%Y-%m-%d} .. "
          f"{d['ts'].max():%Y-%m-%d}, {len(d):,} ora)")
    print("=" * 96)

    for side, label, pcol in ((1, "LONG", "long_pct"), (-1, "SHORT", "short_pct")):
        base_all = _baseline(d, hold_h, side)
        if len(base_all) == 0:
            continue
        base = float(base_all.mean())
        print(f"\n  {label}   barmely {hold_d} napos ablak atlaga: {base:+.0f} bps"
              f"   (ez a merce, nem a nulla)")
        print(f"  {'kuszob':>7}{'sorozat':>9}{'kotes':>7}{'epizod':>8}"
              f"{'ep.atlag':>10}{'tobblet':>10}{'nyero%':>8}{'t':>7}")
        for thr in THRESHOLDS:
            sig = (d[pcol].to_numpy() >= thr)
            for k in STREAKS:
                idx = entries(sig, k)
                pnl = _trades(d, idx, hold_h, side)
                if len(pnl) < 5:
                    continue
                ep = episodes(idx, pnl, hold_h)
                t = ((ep.mean() - base) / (ep.std(ddof=1) / np.sqrt(len(ep)))
                     if len(ep) > 2 and ep.std(ddof=1) > 0 else float("nan"))
                mark = "  <<<" if (t == t and abs(t) >= 2.0) else ""
                print(f"  {thr:>7.2f}{k:>9}{len(pnl):>7}{len(ep):>8}"
                      f"{ep.mean():>+10.0f}{ep.mean() - base:>+10.0f}"
                      f"{(ep > 0).mean() * 100:>7.0f}%{t:>7.2f}{mark}")
        if detail:
            print(f"    (barmely-ablak szoras: {base_all.std():.0f} bps, "
                  f"n={len(base_all):,})")


def main() -> None:
    parser = argparse.ArgumentParser(description="champion jel-sorozat teszt")
    parser.add_argument("--hold", type=int, default=0,
                        help="csak ez a tartasi ido napokban (0 = mind)")
    parser.add_argument("--detail", action="store_true")
    args = parser.parse_args()
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    d = load()
    print(f"\n{len(d):,} orai sor, {d['ts'].min()} .. {d['ts'].max()}")
    print(f"koltseg {COST_BPS:.0f} bps/kotes, percentilis {PCT_WINDOW_D} napos "
          f"gorduló ablakbol (oksagi)")

    periods = [
        ("TRAIN — IN-SAMPLE, nem bizonyitek", d.loc[d["ts"] <= TRAIN_END]),
        ("VALID — ez az ervenyes teszt", d.loc[(d["ts"] > TRAIN_END)
                                               & (d["ts"] <= VALID_END)]),
        ("2026-08 — a HIPOTEZIST GENERALO megfigyeles", d.loc[d["ts"] > VALID_END]),
    ]
    holds = (args.hold,) if args.hold else HOLDS_D
    for name, sub in periods:
        if len(sub) < 24 * 30:
            print(f"\n[{name}: csak {len(sub)} ora, kihagyva]")
            continue
        for h in holds:
            scan(sub.reset_index(drop=True), name, h, args.detail)


if __name__ == "__main__":
    main()
