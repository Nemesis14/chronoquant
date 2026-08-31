#!/usr/bin/env python3
"""Cross-sectional decile spread probe (epic_062 / t832).

One descriptive question, asked before any model is built:

    Is the gap between the best and worst performing perp, hour by hour, wide
    enough to pay two legs of fees?

Why it is worth asking at all: this project has now measured three times that
the model ranks well (2.77x lift on its own top percentile, stable across
windows) and fails at absolute direction, because the winning side flips with
the trend. A long-top-decile / short-bottom-decile book uses only the ranking
and cancels the beta -- the exact dimension that kept killing the directional
version.

The catch is priced in from the start: two legs means twice the round-trip
cost. At the measured best execution (52.8% break-even, ~9.4 bps round trip
per leg) a market-neutral pair costs ~19 bps to open and close. The pre-
registered bar is a mean decile spread above 80 bps -- roughly four times the
cost, because the model historically captures about half the available edge
and half of 80 is still comfortably above 19.

⚠️ This measures the *opportunity*, not an edge. A wide spread only says there
is something to sort; it says nothing about whether we can sort it. If the
spread is narrow, no model can help and the direction closes here.

Usage:
    python -m modeling.experiments.cross_sectional_probe
    python -m modeling.experiments.cross_sectional_probe --min-symbols 20
"""

import argparse
import logging
import os
import sys

import duckdb
import numpy as np
import pandas as pd

import utils

logger = logging.getLogger(__name__)

DB_REL = os.path.join("database", "_universe", "universe.duckdb")
HORIZON_H = 4          # forward window, matching the directional work
DECILE = 0.10
# Round trip on one leg at the best execution measured in epic_059:
# maker entry 1.8 + maker exit 1.8, or taker exit 6.0 -- call it 9.4 bps for a
# pair of legs opened and closed, doubled because a neutral book has two.
LEG_COST_BPS = 9.4
PAIR_COST_BPS = 2 * LEG_COST_BPS

# The two regimes the directional work kept flipping between, so the spread can
# be reported the same way rather than as one blended average.
REGIMES = {
    "emelkedo": ("2024-07-01", "2025-07-31"),
    "eso": ("2025-08-01", "2026-07-31"),
}


def load_panel() -> pd.DataFrame:
    """Hourly close and turnover for every symbol, wide by symbol."""
    db_path = os.path.join(utils._repo_root(), DB_REL)
    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"{DB_REL} hianyzik -- eloszor: "
            "python -m data_handling.collectors.universe.build_universe --top 30"
        )
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute("""
            SELECT symbol, open_time, close, quote_volume
            FROM ohlcv_1h ORDER BY open_time, symbol
        """).df()
    finally:
        conn.close()


def forward_returns(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Log return over the next HORIZON_H hours, and turnover, both wide."""
    close = panel.pivot(index="open_time", columns="symbol", values="close")
    turnover = panel.pivot(index="open_time", columns="symbol",
                           values="quote_volume")
    fwd = np.log(close.shift(-HORIZON_H) / close)
    # A delisted pair stops producing bars; its last HORIZON_H rows have no
    # forward price and drop out on their own. Nothing else is removed --
    # dropping the dead pairs is how survivorship bias gets in.
    return fwd * 10_000.0, turnover


def decile_spread(
    fwd_bps: pd.DataFrame,
    turnover: pd.DataFrame,
    min_symbols: int,
    liquid_only: bool,
) -> pd.DataFrame:
    """Per-hour mean(top decile) - mean(bottom decile), in basis points."""
    rows = []
    for ts, row in fwd_bps.iterrows():
        vals = row.dropna()
        if liquid_only:
            # The widest spreads tend to come from the thinnest pairs, where
            # the slippage that would eat them is also largest. Cutting the
            # bottom third of turnover is the honest version of the question.
            turn = turnover.loc[ts, vals.index].dropna()
            if len(turn) < min_symbols:
                continue
            keep = turn[turn >= turn.quantile(1 / 3)].index
            vals = vals[vals.index.isin(keep)]
        if len(vals) < min_symbols:
            continue
        k = max(1, int(round(len(vals) * DECILE)))
        ordered = np.sort(vals.to_numpy())
        rows.append({
            "open_time": ts,
            "n": len(vals),
            "top": float(ordered[-k:].mean()),
            "bottom": float(ordered[:k].mean()),
            "spread": float(ordered[-k:].mean() - ordered[:k].mean()),
        })
    return pd.DataFrame(rows)


def average_correlation(panel: pd.DataFrame) -> float:
    """Mean pairwise correlation of hourly returns -- the real ceiling.

    If everything moves together, the cross-section has little to sort no
    matter how wide any single hour looks.
    """
    close = panel.pivot(index="open_time", columns="symbol", values="close")
    ret = np.log(close / close.shift(1))
    corr = ret.corr().to_numpy()
    off = corr[~np.eye(len(corr), dtype=bool)]
    return float(np.nanmean(off))


def naive_capture(
    panel: pd.DataFrame, fwd_bps: pd.DataFrame, min_symbols: int
) -> pd.DataFrame:
    """What a zero-intelligence ranker actually earns from that spread.

    The raw decile spread is the perfect-foresight ceiling: it sorts on the
    outcome. It says how much is on the table, not how much is reachable. So
    the same book is run on trivial signals -- rank by trailing return over
    various lookbacks, long the top decile, short the bottom -- and the
    realised forward spread is what those signals actually captured.

    A signal that captures nothing is informative: it means the wide spread is
    unforecastable noise and the whole direction closes. Anything that clears
    the pair cost from a one-line rule means a real model has room above it.
    """
    close = panel.pivot(index="open_time", columns="symbol", values="close")
    rows = []
    for lookback in (1, 4, 12, 24, 72, 168):
        signal = np.log(close / close.shift(lookback))
        for side, name in ((1, "momentum"), (-1, "reverzal")):
            caught = []
            for ts, sig_row in signal.iterrows():
                fwd_row = fwd_bps.loc[ts].dropna()
                sig = (sig_row.dropna() * side).reindex(fwd_row.index).dropna()
                if len(sig) < min_symbols:
                    continue
                k = max(1, int(round(len(sig) * DECILE)))
                order = sig.sort_values()
                # Long what the signal ranks highest, short what it ranks
                # lowest, and read off the forward return actually delivered.
                longs = fwd_row[order.index[-k:]].mean()
                shorts = fwd_row[order.index[:k]].mean()
                caught.append(longs - shorts)
            if not caught:
                continue
            arr = np.asarray(caught)
            rows.append({
                "jel": f"{name} {lookback}h",
                "orak": len(arr),
                "atlag_bps": round(float(arr.mean()), 1),
                "median_bps": round(float(np.median(arr)), 1),
                "netto_bps": round(float(arr.mean()) - PAIR_COST_BPS, 1),
                "pozitiv_%": round(float((arr > 0).mean() * 100), 1),
            })
    return pd.DataFrame(rows).sort_values("atlag_bps", ascending=False)


def report(spreads: pd.DataFrame, title: str) -> None:
    s = spreads["spread"]
    print(f"\n  {title}")
    print(f"    orak            {len(s):>8,}")
    print(f"    atlag spread    {s.mean():>8.1f} bps")
    print(f"    median spread   {s.median():>8.1f} bps")
    print(f"    p25 / p75       {s.quantile(0.25):>8.1f} / {s.quantile(0.75):.1f} bps")
    print(f"    par-koltseg     {PAIR_COST_BPS:>8.1f} bps")
    print(f"    fedezet (median/koltseg) {s.median() / PAIR_COST_BPS:>5.1f}x")


def main() -> None:
    parser = argparse.ArgumentParser(description="Cross-sectional elomeres")
    parser.add_argument("--min-symbols", type=int, default=15,
                        help="Ennyi elő szimbolum alatt az ora kimarad")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    panel = load_panel()
    n_sym = panel["symbol"].nunique()
    logger.info("panel: %d szimbolum, %s sor", n_sym, f"{len(panel):,}")

    fwd_bps, turnover = forward_returns(panel)
    rho = average_correlation(panel)

    print(f"\n=== CROSS-SECTIONAL ELOMERES ({HORIZON_H} oras elore hozam) ===")
    print(f"  szimbolumok           {n_sym}")
    print(f"  atlagos paronkenti korrelacio  {rho:.3f}")

    for liquid_only, label in ((False, "minden par"), (True, "likviditasi szuro")):
        spreads = decile_spread(fwd_bps, turnover, args.min_symbols, liquid_only)
        if spreads.empty:
            print(f"\n  {label}: nincs ertekelheto ora")
            continue
        report(spreads, f"{label} — teljes ablak")
        ts = pd.to_datetime(spreads["open_time"])
        for name, (lo, hi) in REGIMES.items():
            sub = spreads[(ts >= lo) & (ts <= hi)]
            if not sub.empty:
                report(sub, f"{label} — {name} rezsim")

    print("\n  ⚠️ A fenti a lehetoseget meri, nem az elt: a decilisek a KIMENET")
    print("     szerint vannak sorbarendezve, tehat ez a tokeletes elorelatas")
    print("     felso korlatja. Az alabbi azt meri, mennyi ebbol elerheto.")

    print("\n=== NAIV RANGSOROLOK — mennyit fognak meg belole ===")
    caught = naive_capture(panel, fwd_bps, args.min_symbols)
    print(caught.to_string(index=False))
    print(f"\n  par-koltseg: {PAIR_COST_BPS:.1f} bps  "
          "(a netto_bps oszlop ezt mar levonta)")


if __name__ == "__main__":
    main()
