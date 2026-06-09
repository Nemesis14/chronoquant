#!/usr/bin/env python3
# =============================================================================
# SOLUSDT 1-2-1-2 bullish setup scan — 1m gyertyákon
# =============================================================================
# Ugyanolyan logika mint scan_1212.py, de 1m-es nyers gyertyákon fut.
# Duration limitek 5×-ösek (ugyanannyi idő, de 1m barokban).
# Threshold kisebb: 0.003–0.006.
# =============================================================================

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import elliott_waves.elliott_1212 as ew

# --- Duration konstansok felülírása 1m skálára (5m × 5) ---
ew.D1_MIN   = 25;  ew.D1_MAX   = 250
ew.D2_MIN   = 15;  ew.D2_MAX   = 250
ew.D3_MIN   = 15;  ew.D3_MAX   = 150
ew.D4_MIN   = 10;  ew.D4_MAX   = 150
ew.TOTAL_MIN = 50; ew.TOTAL_MAX = 600

from elliott_waves.elliott_1212 import detect_1212, detect_pivots, load_ohlcv

DB_PATH = "D:/repos/chronoquant/database/solusdt_data_dev.db"
TABLE   = "solusdt_1m"

THRESHOLDS = [0.003, 0.004, 0.005, 0.006]


def main() -> None:
    print("Loading full SOLUSDT 1m OHLCV...")
    df = load_ohlcv(DB_PATH, TABLE)
    print(f"  Rows: {len(df):,}  |  {df['open_time'].iloc[0]}  ->  {df['open_time'].iloc[-1]}")

    rows = []
    print(f"\n{'TF':<4} {'Thr':>6} {'Pivots':>8} {'Setups':>8} {'Score>=0.4':>11} {'R_big med':>10} {'R_sub med':>10}")
    print("-" * 68)

    for thr in THRESHOLDS:
        setups = detect_1212(df, threshold=thr)
        pivots = detect_pivots(df, thr)

        if setups.empty:
            n_setups = 0; n_high = 0; r_big_m = "-"; r_sub_m = "-"
        else:
            n_setups = len(setups)
            n_high   = int((setups["score"] >= 0.4).sum())
            r_big_m  = f"{setups['r_big'].median():.3f}"
            r_sub_m  = f"{setups['r_sub'].median():.3f}"

        print(f"1m   {thr:>6.3f} {len(pivots):>8,} {n_setups:>8,} {n_high:>11,} {r_big_m:>10} {r_sub_m:>10}")
        rows.append({
            "tf": "1m", "threshold": thr, "pivots": len(pivots),
            "setups": n_setups, "score_40": n_high,
            "r_big_med": float(setups["r_big"].median()) if n_setups else None,
            "r_sub_med": float(setups["r_sub"].median()) if n_setups else None,
        })

    results = pd.DataFrame(rows)
    print("\n--- összesítő ---")
    print(f"Összes setup (minden variáns): {results['setups'].sum():,}")
    best = results.sort_values("setups", ascending=False).head(3)
    print("\nTop 3 variáns (legtöbb setup):")
    print(best[["tf", "threshold", "pivots", "setups", "score_40"]].to_string(index=False))

    # Legjobb variáns: legtöbb setup ahol score>=0.4 arány jó
    best_row = results.sort_values("score_40", ascending=False).iloc[0]
    print(f"\nLegjobb variáns (score>=0.4): threshold={best_row['threshold']:.3f}  setups={best_row['setups']:,}  score>=0.4={best_row['score_40']:,}")


if __name__ == "__main__":
    main()
