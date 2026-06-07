#!/usr/bin/env python3
# =============================================================================
# Strategy threshold sweep for a single model
# =============================================================================
# Usage:
#   python scripts/sweep_strategy.py \
#       --model-id lgbm_solusdt_l_fw60_q90_local_v2 \
#       --asset-id solusdt_fw60 \
#       --start 2024-01-01 --end 2025-12-31 \
#       --side long
#
# Sweeps entry_threshold, max_hold_minutes (and optionally take_profit_pct)
# and prints a ranked table of results.
# =============================================================================

from __future__ import annotations

import argparse
import sys
from itertools import product
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from evaluation.backtest import build_backtest_frame, simulate_long_probability_strategy


_ENTRY_THRESHOLDS   = [0.30, 0.35, 0.38, 0.40, 0.42, 0.45, 0.48, 0.50, 0.55, 0.60]
_MAX_HOLD_MINUTES   = [30, 45, 60, 90, 120]
_TAKE_PROFIT_PCTS   = [0.0, 0.010, 0.015, 0.020]
_REARM_THRESHOLD    = 0.18
_EXIT_THRESHOLD     = 0.10
_MIN_HOLD_MINUTES   = 5
_COOLDOWN_MINUTES   = 60
_FEE_BPS            = 10.0
_SLIPPAGE_BPS       = 2.0
_INITIAL_EQUITY     = 10000.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Strategy parameter sweep.")
    parser.add_argument("--model-id",  required=True)
    parser.add_argument("--asset-id",  default=None)
    parser.add_argument("--start",     required=True)
    parser.add_argument("--end",       required=True)
    parser.add_argument("--side",      choices=["long", "short"], default="long")
    parser.add_argument("--top-n",     type=int, default=20,
                        help="Show top N results (default: 20)")
    args = parser.parse_args()

    print(f"Loading prediction frame: {args.model_id}  {args.start} to {args.end}")
    frame = build_backtest_frame(
        model_id=args.model_id,
        start=args.start,
        end=args.end,
        asset_id=args.asset_id,
    )
    print(f"Frame shape: {frame.shape}  "
          f"prediction range: [{frame['prediction'].min():.3f}, {frame['prediction'].max():.3f}]")

    rows = []
    combos = list(product(_ENTRY_THRESHOLDS, _MAX_HOLD_MINUTES, _TAKE_PROFIT_PCTS))
    print(f"Sweeping {len(combos)} combinations …")

    for entry_thr, max_hold, tp_pct in combos:
        cfg = {
            "side":                    args.side,
            "entry_threshold":         entry_thr,
            "rearm_threshold":         _REARM_THRESHOLD,
            "exit_threshold":          _EXIT_THRESHOLD,
            "min_hold_minutes":        _MIN_HOLD_MINUTES,
            "max_hold_minutes":        max_hold,
            "take_profit_pct":         tp_pct,
            "stop_loss_pct":           0.0,
            "trailing_activation_pct": 0.0,
            "trailing_stop_pct":       0.0,
            "cooldown_minutes":        _COOLDOWN_MINUTES,
            "fee_bps_per_side":        _FEE_BPS,
            "slippage_bps_per_side":   _SLIPPAGE_BPS,
            "initial_equity":          _INITIAL_EQUITY,
        }
        try:
            _, _, summary = simulate_long_probability_strategy(frame, cfg)
        except Exception as e:
            print(f"  SKIP entry={entry_thr} hold={max_hold} tp={tp_pct}: {e}")
            continue

        trade_count = summary.get("trade_count", 0)
        if trade_count < 10:
            continue

        rows.append({
            "entry_thr":    entry_thr,
            "max_hold":     max_hold,
            "tp_pct":       tp_pct,
            "trades":       trade_count,
            "win_rate":     round(summary.get("win_rate", 0) or 0, 4),
            "total_return": round(summary.get("total_return", 0), 4),
            "profit_factor":round(summary.get("profit_factor") or 0, 3),
            "max_dd":       round(summary.get("max_drawdown", 0), 4),
            "avg_hold":     round(summary.get("avg_hold_minutes", 0) or 0, 1),
            "exposure_pct": round(summary.get("exposure_pct", 0), 3),
        })

    if not rows:
        print("No valid results.")
        return

    df = pd.DataFrame(rows)

    # Score: balance return vs drawdown; penalise tiny trade counts
    df["score"] = (
        df["total_return"]
        - 0.5 * df["max_dd"].abs()
        + 0.3 * (df["win_rate"] - 0.5).clip(lower=0)
    )
    df = df.sort_values("score", ascending=False).reset_index(drop=True)

    print()
    print("=" * 95)
    print(f"TOP {args.top_n} CONFIGURATIONS")
    print("=" * 95)
    print(df.head(args.top_n).to_string(index=True))
    print()

    # Best overall
    best = df.iloc[0]
    print("=" * 95)
    print("RECOMMENDED CONFIGURATION:")
    print(f"  entry_threshold:  {best['entry_thr']}")
    print(f"  max_hold_minutes: {int(best['max_hold'])}")
    print(f"  take_profit_pct:  {best['tp_pct']}")
    print(f"  rearm_threshold:  {_REARM_THRESHOLD}")
    print(f"  exit_threshold:   {_EXIT_THRESHOLD}")
    print(f"  trades:           {int(best['trades'])}")
    print(f"  win_rate:         {best['win_rate']:.2%}")
    print(f"  total_return:     {best['total_return']:.2%}")
    print(f"  profit_factor:    {best['profit_factor']:.3f}")
    print(f"  max_drawdown:     {best['max_dd']:.2%}")
    print()

    # Save to CSV
    out_path = Path(f"backtests/sweep_{args.model_id}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Full sweep results saved to: {out_path}")


if __name__ == "__main__":
    main()
