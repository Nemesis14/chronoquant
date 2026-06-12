# =============================================================================
# backtest_wave4.py — Event study: 1-2-3-4 confirmed → did Wave 5 follow?
# =============================================================================
# Purpose:
#  - Scan full OHLCV database for confirmed long 1-2-3-4 impulse structures
#  - For each setup: label success if price exceeds P3 (Wave 3 high) within N bars
#  - Label failure if price breaks below P4 (Wave 4 low) before reaching P3
#  - Report summary statistics per timeframe
#
# Logic:
#  - P4.confirmed_idx = the bar at which the setup is "known" (no lookahead)
#  - Target  = P3.price  (Wave 5 must exceed Wave 3 high)
#  - Stop    = P4.price  (Wave 4 low broken = setup invalidated)
#  - Window  = --window bars forward from P4.confirmed_idx
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

import utils
from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import PatternCandidate, PivotKind, load_ohlcv, resample_ohlcv
from elliott_waves.elliott.pivots.zigzag import detect_zigzag

_TF_FREQ = {
    "5m":  "5min",
    "15m": "15min",
    "1h":  "1h",
    "4h":  "4h",
}


# =============================================================================
# _scan_1234(pivots, direction) -> list[PatternCandidate]
# =============================================================================
# Scan all 5-pivot windows for valid 1-2-3-4 structures.
# Hard rules (long direction):
#   P0 < P1 > P2 < P3 > P4   (alternating high/low)
#   P2 > P0                   (wave 2 does not invalidate wave 1)
#   P3 > P1                   (wave 3 extends beyond wave 1 high)
#   P4 > P2                   (wave 4 does not invalidate wave 3)
#   P4 > P1                   (non-overlap: wave 4 low stays above wave 1 high)
# =============================================================================
def _scan_1234(
    pivots:    list,
    direction: int = 1,
) -> list[PatternCandidate]:
    d          = direction
    candidates = []

    want_p0 = PivotKind.LOW  if direction > 0 else PivotKind.HIGH
    want_p1 = PivotKind.HIGH if direction > 0 else PivotKind.LOW

    for i in range(4, len(pivots)):
        p0, p1, p2, p3, p4 = (
            pivots[i - 4], pivots[i - 3], pivots[i - 2],
            pivots[i - 1], pivots[i],
        )

        # Kind alternation
        if p0.kind != want_p0: continue
        if p1.kind != want_p1: continue
        if p2.kind != want_p0: continue
        if p3.kind != want_p1: continue
        if p4.kind != want_p0: continue

        # Hard structural rules
        if not (d * p1.price > d * p0.price): continue  # wave 1 goes in direction
        if not (d * p2.price < d * p1.price): continue  # wave 2 pulls back
        if not (d * p2.price > d * p0.price): continue  # wave 2 non-invalidation
        if not (d * p3.price > d * p1.price): continue  # wave 3 extends beyond wave 1
        if not (d * p4.price < d * p3.price): continue  # wave 4 pulls back
        if not (d * p4.price > d * p2.price): continue  # wave 4 non-invalidation of wave 3
        if not (d * p4.price > d * p1.price): continue  # non-overlap: wave 4 above wave 1 top

        candidates.append(PatternCandidate(
            pattern_type       = "WAVE4_COMPLETE",
            start_idx          = p0.idx,
            end_idx            = p4.idx,
            confirmed_idx      = p4.confirmed_idx,
            pivots             = [p0, p1, p2, p3, p4],
            direction          = direction,
            degree             = p0.degree,
            hard_pass          = True,
            score              = 0.0,
            invalidation_level = p4.price,
            target_zones       = {"wave5_trigger": p3.price},
            diagnostics        = {
                "p0": p0.price, "p1": p1.price,
                "p2": p2.price, "p3": p3.price, "p4": p4.price,
                "W1": abs(p1.price - p0.price),
                "W3": abs(p3.price - p2.price),
                "R2": abs(p2.price - p1.price) / abs(p1.price - p0.price),
            },
        ))

    return candidates


# =============================================================================
# _label(candidates, df, window) -> pd.DataFrame
# =============================================================================
# For each setup, scan forward `window` bars from P4.confirmed_idx:
#   - hit_target : high >= P3.price  (Wave 5 triggered)
#   - hit_stop   : low  <= P4.price  (setup invalidated)
# =============================================================================
def _label(
    candidates: list[PatternCandidate],
    df:         pd.DataFrame,
    window:     int,
) -> pd.DataFrame:
    highs  = df["high"].to_numpy(dtype=float)
    lows   = df["low"].to_numpy(dtype=float)
    times  = df["open_time"].astype(str).to_list()
    n      = len(highs)
    rows   = []

    for c in candidates:
        entry = c.confirmed_idx
        stop  = c.invalidation_level
        tgt   = c.target_zones["wave5_trigger"]
        diag  = c.diagnostics

        if entry >= n:
            continue

        hit_target     = False
        hit_stop       = False
        bars_to_target = None
        bars_to_stop   = None

        for bar in range(entry + 1, min(n, entry + window + 1)):
            lo = lows[bar]
            hi = highs[bar]
            if lo <= stop:
                hit_stop     = True
                bars_to_stop = bar - entry
                break
            if hi >= tgt:
                hit_target     = True
                bars_to_target = bar - entry
                break

        # resolution_bar: the bar at which this trade ended (for non-overlap filter)
        if hit_target:
            resolution_bar = entry + bars_to_target
        elif hit_stop:
            resolution_bar = entry + bars_to_stop
        else:
            resolution_bar = entry + window

        rows.append({
            "confirmed_idx":   entry,
            "confirmed_time":  times[entry],
            "resolution_bar":  resolution_bar,
            "p0": round(diag["p0"], 4),
            "p1": round(diag["p1"], 4),
            "p2": round(diag["p2"], 4),
            "p3": round(diag["p3"], 4),
            "p4": round(diag["p4"], 4),
            "W1": round(diag["W1"], 4),
            "W3": round(diag["W3"], 4),
            "R2": round(diag["R2"], 3),
            "target":          round(tgt,  4),
            "stop":            round(stop, 4),
            "hit_target":      hit_target,
            "hit_stop":        hit_stop,
            "bars_to_target":  bars_to_target,
            "bars_to_stop":    bars_to_stop,
        })

    return pd.DataFrame(rows)


# =============================================================================
# _filter_non_overlapping(labeled) -> pd.DataFrame
# =============================================================================
# "One trade at a time": once a setup fires, skip all setups that start
# before the current trade resolves (hit target, hit stop, or timeout).
# =============================================================================
def _filter_non_overlapping(labeled: pd.DataFrame) -> pd.DataFrame:
    labeled = labeled.sort_values("confirmed_idx").reset_index(drop=True)
    kept             = []
    next_allowed_bar = 0

    for _, row in labeled.iterrows():
        if row["confirmed_idx"] < next_allowed_bar:
            continue
        kept.append(row)
        next_allowed_bar = int(row["resolution_bar"])

    return pd.DataFrame(kept).reset_index(drop=True)


# =============================================================================
# _run(asset_id, tf, window) -> None
# =============================================================================
def _run(asset_id: str, tf: str, window: int) -> None:
    asset_cfg   = utils.load_asset_config(asset_id)
    db_cfg      = asset_cfg["database"]
    db_path     = db_cfg["db_path"]
    table_ohlcv = db_cfg["tables"]["ohlcv"]
    symbol      = db_cfg.get("symbol", asset_id.upper())

    print(f"\n{'='*60}")
    print(f"  {symbol}  TF={tf}  window={window} bars")
    print(f"{'='*60}")

    print(f"  Loading full OHLCV from {table_ohlcv} ...")
    df_1m = load_ohlcv(db_path, table_ohlcv)
    if df_1m.empty:
        print("  ERROR: No data found.")
        return

    if tf == "1m":
        df = df_1m.copy()
    else:
        freq = _TF_FREQ.get(tf)
        if not freq:
            print(f"  ERROR: Unknown TF '{tf}'")
            return
        df = resample_ohlcv(df_1m, freq)

    df = df.reset_index(drop=True)
    print(f"  {len(df)} bars  ({df['open_time'].iloc[0]} to {df['open_time'].iloc[-1]})")

    cfg    = ElliottConfig.for_timeframe(tf)
    pivots = detect_zigzag(df, cfg, degree=0)
    print(f"  {len(pivots)} pivots detected")

    setups = _scan_1234(pivots, direction=1)
    print(f"  {len(setups)} long 1-2-3-4 setups found")

    if not setups:
        print("  No setups — try a lower zigzag threshold or more data.")
        return

    labeled = _label(setups, df, window)
    if labeled.empty:
        print("  No labelable setups (all beyond end of data).")
        return

    non_overlap = _filter_non_overlapping(labeled)

    def _print_stats(df_: pd.DataFrame, label: str) -> None:
        total     = len(df_)
        n_target  = df_["hit_target"].sum()
        n_stop    = df_["hit_stop"].sum()
        n_timeout = total - n_target - n_stop
        win_rate  = n_target / total * 100 if total else 0
        avg_bars  = df_.loc[df_["hit_target"], "bars_to_target"].mean()
        avg_W3W1  = (df_["W3"] / df_["W1"]).mean()

        print(f"\n  {label}")
        print(f"    Total setups       : {total}")
        print(f"    Wave 5 triggered   : {n_target}  ({win_rate:.1f}%)")
        print(f"    Stopped out        : {n_stop}  ({n_stop/total*100:.1f}%)")
        print(f"    Timeout (neither)  : {n_timeout}  ({n_timeout/total*100:.1f}%)")
        if n_target > 0:
            print(f"    Avg bars to Wave5  : {avg_bars:.1f}")
        print(f"    Avg W3/W1 ratio    : {avg_W3W1:.2f}")

        df_ = df_.copy()
        df_["year"] = pd.to_datetime(df_["confirmed_time"]).dt.year
        print("    By year:")
        for year, grp in df_.groupby("year"):
            yr_total  = len(grp)
            yr_target = grp["hit_target"].sum()
            print(f"      {year}: {yr_total:>3} setups  ->  {yr_target:>3} Wave5  ({yr_target/yr_total*100:.0f}%)")

    _print_stats(labeled,     "ALL SETUPS (with overlaps)")
    _print_stats(non_overlap, "NON-OVERLAPPING (one trade at a time)")

    overlap_pct = (1 - len(non_overlap) / len(labeled)) * 100
    print(f"\n  Overlap filtered out: {len(labeled) - len(non_overlap)} setups ({overlap_pct:.0f}%)")

    return labeled


def main() -> None:
    parser = argparse.ArgumentParser(description="Wave 4 completion event study")
    parser.add_argument("--asset-id", default="solusdt_fw60", help="Asset config ID")
    parser.add_argument("--tf",       default=None,           help="Single TF (omit = run 1h + 15m)")
    parser.add_argument("--window",   type=int, default=0,    help="Forward bars (0 = auto per TF)")
    args = parser.parse_args()

    tf_list = [args.tf] if args.tf else ["1h", "15m"]

    # Default windows: ~5 trading days per TF
    default_windows = {"1m": 2400, "5m": 480, "15m": 480, "1h": 120, "4h": 30}

    for tf in tf_list:
        window = args.window if args.window > 0 else default_windows.get(tf, 120)
        _run(args.asset_id, tf, window)


if __name__ == "__main__":
    main()
