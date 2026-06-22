"""CLI: run a full strategy session — scored join -> calibrate -> optimize.

DuckDB-native flow (plan 5 step 7, 5.1, 6).  No parquet is moved between steps:
  1. build the scored table from snap ⋈ model_long.__pred ⋈ model_short.__pred,
  2. fit rank + isotonic calibration on the calibration window,
  3. run the Optuna sweep on the optimization window, writing strat.* tables and
     registering the session in reg.strategies + reg.artifacts.

Usage:
    uv run python src/strategy/00_run_strategy_session.py \
      --long-model  lgbm_solusdt_l_fw60_2101_2605 \
      --short-model lgbm_solusdt_s_fw60_2101_2605 \
      --calib-start 2025-10-01 --calib-end 2026-02-28 \
      --opt-start   2026-03-01 --opt-end   2026-05-31 \
      --n-trials 200
"""

# %% Imports

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import logging

from strategy.strategy.build_table import build_scored_table
from strategy.strategy.calibrate import fit_calibration
from strategy.strategy.optimize import optimize_strategy
from strategy.strategy.session_naming import derive_session_id

# %% Logging setup

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)

# %% Main


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a full strategy session (scored join -> calibrate -> optimize)."
    )
    parser.add_argument("--long-model",  required=True, help="Long-direction model id")
    parser.add_argument("--short-model", required=True, help="Short-direction model id")
    parser.add_argument("--session-id",  help="Session id (optional; auto-derived by default)")
    parser.add_argument("--asset-id",    help="Asset key (optional; resolved from model config)")
    parser.add_argument("--snapshot-id", help="Snapshot id (optional; resolved from reg/config)")
    parser.add_argument("--calib-start", required=True, help="Calibration start YYYY-MM-DD")
    parser.add_argument("--calib-end",   required=True, help="Calibration end YYYY-MM-DD")
    parser.add_argument("--opt-start",   required=True, help="Optimization start YYYY-MM-DD")
    parser.add_argument("--opt-end",     required=True, help="Optimization end YYYY-MM-DD")
    parser.add_argument("--n-trials",    type=int, default=200, help="Number of Optuna trials")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    session_id = args.session_id or derive_session_id(args.long_model, args.short_model)

    print(f"[run_strategy_session] session={session_id}")
    print(f"  long={args.long_model}  short={args.short_model}")
    print(f"  calib : {args.calib_start} -> {args.calib_end}")
    print(f"  opt   : {args.opt_start} -> {args.opt_end}")

    scored_df = build_scored_table(
        long_model_id  = args.long_model,
        short_model_id = args.short_model,
        asset_id       = args.asset_id,
        snapshot_id    = args.snapshot_id,
    )
    print(f"[OK] scored table: {len(scored_df)} rows")

    calibrated_df, _, _ = fit_calibration(
        session_id = session_id,
        scored_df  = scored_df,
        start      = args.calib_start,
        end        = args.calib_end,
    )
    print("[OK] calibration: rank_lookup_*.parquet + isotonic_*.pkl saved")

    result = optimize_strategy(
        session_id     = session_id,
        long_model_id  = args.long_model,
        short_model_id = args.short_model,
        calibrated_df  = calibrated_df,
        start          = args.opt_start,
        end            = args.opt_end,
        n_trials       = args.n_trials,
        asset_id       = args.asset_id,
    )

    bp = result["best_params"]
    m  = result["metrics"]
    print("\n[RESULT]")
    print(f"  best objective : {result['optuna_best']['value']:.6f}")
    print(f"  n_trades       : {m['n_trades']}  win_rate={m['win_rate']}  sharpe={m['sharpe']}")
    print(f"  long_entry_pct={bp['long_entry_pct']:.4f}  short_entry_pct={bp['short_entry_pct']:.4f}")
    print("\n[OK] strat.* tables + strategy_artifact.json written; reg.strategies registered")
    for kind, fqn in result["strat_tables"].items():
        print(f"     {kind:8s} -> {fqn}")
