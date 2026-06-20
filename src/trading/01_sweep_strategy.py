"""Strategy threshold sweep for a single model.

Sweeps entry_threshold, max_hold_minutes, and take_profit_pct combinations
over the model's sample_oos.parquet and prints a ranked results table.
Saves sweep_results.csv to artifacts/<model_id>/strategy/.

Usage:
    uv run python src/trading/01_sweep_strategy.py --model lgbm_solusdt_l_fw60_2021
    uv run python src/trading/01_sweep_strategy.py --model lgbm_solusdt_l_fw60_2021 --top-n 20
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from itertools import product
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils
from trading.calibration.backtest import load_oos_frame, simulate_long_probability_strategy

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)

# %% Sweep grid

_ENTRY_THRESHOLDS = [0.30, 0.35, 0.38, 0.40, 0.42, 0.45, 0.48, 0.50, 0.55, 0.60]
_MAX_HOLD_MINUTES = [30, 45, 60, 90, 120]
_TAKE_PROFIT_PCTS = [0.0, 0.010, 0.015, 0.020]
_REARM_THRESHOLD  = 0.18
_EXIT_THRESHOLD   = 0.10
_MIN_HOLD_MINUTES = 5
_COOLDOWN_MINUTES = 60
_FEE_BPS          = 10.0
_SLIPPAGE_BPS     = 2.0
_INITIAL_EQUITY   = 10000.0


# %% CLI


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Namespace with model, start, end, top_n attributes.
    """
    parser = argparse.ArgumentParser(description="Strategy parameter sweep.")
    parser.add_argument("--model",  required=True, help="Model ID (artifact directory name)")
    parser.add_argument("--start",  default=None,  help="OOS start date override (YYYY-MM-DD)")
    parser.add_argument("--end",    default=None,  help="OOS end date override (YYYY-MM-DD)")
    parser.add_argument("--top-n",  type=int, default=20, dest="top_n",
                        help="Show top N results (default: 20)")
    return parser.parse_args()


def _infer_side(model_id: str) -> str:
    """Infer strategy side from model manifest.

    Args:
        model_id : Artifact directory name.

    Returns:
        ``"long"`` or ``"short"``.

    Raises:
        FileNotFoundError: If manifest.json is missing.
        ValueError: If target_name is unrecognised.
    """
    manifest_path = Path(utils._resolve_path(f"artifacts/{model_id}/manifest.json"))
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found: {manifest_path}")
    manifest    = json.loads(manifest_path.read_text(encoding="utf-8"))
    target_name = manifest.get("target_name", "")
    if target_name == "long_mfe_fw60":
        return "long"
    if target_name == "short_mfe_fw60":
        return "short"
    raise ValueError(f"Unknown target_name {target_name!r} in manifest for {model_id!r}")


def main() -> None:
    """Run the sweep and print ranked results to stdout."""
    args = parse_args()
    side = _infer_side(args.model)

    print(f"Loading OOS frame: {args.model}  side={side}")
    frame = load_oos_frame(args.model, start=args.start, end=args.end)
    print(
        f"Frame shape: {frame.shape}  "
        f"prediction range: [{frame['prediction'].min():.3f}, {frame['prediction'].max():.3f}]"
    )

    combos = list(product(_ENTRY_THRESHOLDS, _MAX_HOLD_MINUTES, _TAKE_PROFIT_PCTS))
    print(f"Sweeping {len(combos)} combinations ...")

    rows = []
    for entry_thr, max_hold, tp_pct in combos:
        cfg = {
            "side":                    side,
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
            "entry_thr":     entry_thr,
            "max_hold":      max_hold,
            "tp_pct":        tp_pct,
            "trades":        trade_count,
            "win_rate":      round(summary.get("win_rate", 0) or 0, 4),
            "total_return":  round(summary.get("total_return", 0), 4),
            "profit_factor": round(summary.get("profit_factor") or 0, 3),
            "max_dd":        round(summary.get("max_drawdown", 0), 4),
            "avg_hold":      round(summary.get("avg_hold_minutes", 0) or 0, 1),
            "exposure_pct":  round(summary.get("exposure_pct", 0), 3),
        })

    if not rows:
        print("No valid results.")
        return

    df = pd.DataFrame(rows)
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

    # Save sweep results
    out_dir = Path(utils._resolve_path(f"artifacts/{args.model}/strategy"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sweep_results.csv"
    df.to_csv(out_path, index=False)
    print(f"Full sweep results saved to: {out_path}")


if __name__ == "__main__":
    main()
