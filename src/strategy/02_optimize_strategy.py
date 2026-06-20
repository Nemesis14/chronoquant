"""CLI wrapper: optimize strategy entry/exit parameters with Optuna.

Usage:
    uv run python src/strategy/02_optimize_strategy.py \
      --session-id strategy_2101_2605_202605 \
      --long-model  lgbm_solusdt_l_fw60_2021 \
      --short-model lgbm_solusdt_s_fw60_2021 \
      --start 2026-01-01 --end 2026-05-31 \
      --n-trials 200
"""

# %% Imports

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import logging

from strategy.strategy.optimize import optimize_strategy
from strategy.strategy.session_naming import derive_strategy_session_id

# %% Logging setup

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)

# %% Main


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Optimize strategy entry/exit/cooldown parameters via Optuna."
    )
    parser.add_argument("--session-id",   help="Strategy session identifier (optional; auto-derived by default)")
    parser.add_argument("--long-model",   required=True,         help="Model ID for the long direction")
    parser.add_argument("--short-model",  required=True,         help="Model ID for the short direction")
    parser.add_argument("--start",        required=True,         help="Optimization start date YYYY-MM-DD")
    parser.add_argument("--end",          required=True,         help="Optimization end date YYYY-MM-DD")
    parser.add_argument("--n-trials",     type=int, default=200, help="Number of Optuna trials")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    session_id = args.session_id or derive_strategy_session_id(args.long_model, args.short_model)

    print(f"[optimize_strategy] session={session_id}")
    print(f"  long_model  : {args.long_model}")
    print(f"  short_model : {args.short_model}")
    print(f"  period      : {args.start} -> {args.end}")
    print(f"  n_trials    : {args.n_trials}")

    result = optimize_strategy(
        session_id     = session_id,
        long_model_id  = args.long_model,
        short_model_id = args.short_model,
        start          = args.start,
        end            = args.end,
        n_trials       = args.n_trials,
    )

    bp = result["best_params"]
    m  = result["metrics"]
    ob = result["optuna_best"]

    print("\n[RESULT]")
    print(f"  Best objective value : {ob['value']:.6f}  ({ob['n_trials']} trials)")
    print(f"  long_entry_pct       : {bp['long_entry_pct']:.4f}")
    print(f"  short_entry_pct      : {bp['short_entry_pct']:.4f}")
    print(f"  min_edge_gap         : {bp['min_edge_gap']:.4f}")
    print(f"  max_hold_minutes     : {bp['max_hold_minutes']}")
    print(f"  cooldown_minutes     : {bp['cooldown_minutes']}")
    print(f"  min_hold_minutes     : {bp['min_hold_minutes']}")
    print(f"  rearm_pct            : {bp['rearm_pct']:.4f}")
    print(f"  n_trades             : {m['n_trades']}")
    print(f"  total_return         : {m['total_return']}")
    print(f"  win_rate             : {m['win_rate']}")
    print(f"  sharpe               : {m['sharpe']}")
    print(f"  max_drawdown         : {m['max_drawdown']}")
    print(f"  sufficient_sample    : {m['sufficient_sample']}")
    print("\n[OK] strategy_artifact.json + sweep_results.csv saved")
