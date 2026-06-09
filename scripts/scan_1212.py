#!/usr/bin/env python3
# =============================================================================
# SOLUSDT 1-2-1-2 bullish setup full-history scan
# =============================================================================
# Purpose:
#  - Load full SOLUSDT 1m OHLCV (2020-08-11 to present)
#  - Resample to 5m, 10m, 15m, 30m
#  - Run elliott_1212 detector on each timeframe x threshold combo
#  - Print a summary table of setup counts and basic stats
# =============================================================================

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from elliott_waves.elliott_1212 import detect_1212, load_ohlcv, resample_ohlcv

DB_PATH   = "D:/repos/chronoquant/database/solusdt_data_dev.db"
TABLE     = "solusdt_1m"

VARIANTS: list[tuple[str, str, float]] = [
    # (label, resample_freq, pivot_threshold)
    ("5m",  "5min",  0.008),
    ("5m",  "5min",  0.010),
    ("5m",  "5min",  0.012),
    ("10m", "10min", 0.010),
    ("10m", "10min", 0.012),
    ("10m", "10min", 0.015),
    ("15m", "15min", 0.010),
    ("15m", "15min", 0.012),
    ("15m", "15min", 0.015),
    ("30m", "30min", 0.012),
    ("30m", "30min", 0.015),
    ("30m", "30min", 0.018),
]


def main() -> None:
    print("Loading full SOLUSDT 1m OHLCV...")
    df_1m = load_ohlcv(DB_PATH, TABLE)
    print(f"  Rows: {len(df_1m):,}  |  {df_1m['open_time'].iloc[0]}  ->  {df_1m['open_time'].iloc[-1]}")

    cache: dict[str, pd.DataFrame] = {}
    rows  = []

    print(f"\n{'TF':<5} {'Thr':>6} {'Pivots':>7} {'Setups':>7} {'Score>=0.4':>10} {'R_big med':>10} {'R_sub med':>10}")
    print("-" * 65)

    for tf, freq, thr in VARIANTS:
        if freq not in cache:
            print(f"  Resampling to {tf}...", flush=True)
            cache[freq] = resample_ohlcv(df_1m, freq)

        df_tf   = cache[freq]
        setups  = detect_1212(df_tf, threshold=thr)

        if setups.empty:
            n_setups = 0
            n_high   = 0
            r_big_m  = "-"
            r_sub_m  = "-"
        else:
            n_setups = len(setups)
            n_high   = int((setups["score"] >= 0.4).sum())
            r_big_m  = f"{setups['r_big'].median():.3f}"
            r_sub_m  = f"{setups['r_sub'].median():.3f}"

        from elliott_waves.elliott_1212 import detect_pivots
        pivots = detect_pivots(df_tf, thr)

        print(
            f"{tf:<5} {thr:>6.3f} {len(pivots):>7,} {n_setups:>7,} {n_high:>10,}"
            f" {r_big_m:>10} {r_sub_m:>10}"
        )
        rows.append({
            "tf":        tf,
            "threshold": thr,
            "pivots":    len(pivots),
            "setups":    n_setups,
            "score_40":  n_high,
            "r_big_med": float(setups["r_big"].median()) if n_setups else None,
            "r_sub_med": float(setups["r_sub"].median()) if n_setups else None,
        })

    results = pd.DataFrame(rows)
    print("\n--- összesítő ---")
    total = results["setups"].sum()
    print(f"Összes setup (minden variáns): {total:,}")
    best = results.sort_values("setups", ascending=False).head(3)
    print("\nTop 3 variáns (legtöbb setup):")
    print(best[["tf", "threshold", "pivots", "setups", "score_40"]].to_string(index=False))


if __name__ == "__main__":
    main()
