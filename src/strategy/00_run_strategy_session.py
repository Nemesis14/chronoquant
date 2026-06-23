"""CLI: run a full strategy session — scored join -> calibrate -> grid search.

DuckDB-native flow (plan 5 step 7, 5.1, 6).  No parquet is moved between steps:
  1. build the scored table from snap ⋈ model_long.__pred ⋈ model_short.__pred,
  2. fit rank + isotonic calibration on the calibration window,
  3. run the grid search on the optimization window, writing strat.* tables and
     registering the session in reg.strategies + reg.artifacts.

Usage:
    uv run python src/strategy/00_run_strategy_session.py \\
      --long-model  lgbm_solusdt_l_fw60_2101_2605 \\
      --short-model lgbm_solusdt_s_fw60_2101_2605 \\
      --calib-start 2021-01-01 --calib-end 2026-05-31 \\
      --opt-start   2021-01-01 --opt-end   2026-05-31 \\
      --directions  long,short
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
from strategy.strategy.search import search_strategy
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
        description="Run a full strategy session (scored join -> calibrate -> grid search)."
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
    parser.add_argument(
        "--directions",
        default="long,short",
        help="Comma-separated directions to search (default: long,short)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    session_id = args.session_id or derive_session_id(args.long_model, args.short_model)
    directions = [d.strip() for d in args.directions.split(",")]

    print(f"[run_strategy_session] session={session_id}")
    print(f"  long={args.long_model}  short={args.short_model}")
    print(f"  calib : {args.calib_start} -> {args.calib_end}")
    print(f"  opt   : {args.opt_start} -> {args.opt_end}")
    print(f"  directions: {directions}")

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

    result = search_strategy(
        session_id     = session_id,
        long_model_id  = args.long_model,
        short_model_id = args.short_model,
        calibrated_df  = calibrated_df,
        start          = args.opt_start,
        end            = args.opt_end,
        directions     = directions,
        asset_id       = args.asset_id,
    )

    best   = result["best_setup"]
    m      = result["metrics"]
    grid   = result["grid_results"]

    print("\n[RESULT]")
    print(f"  best total_fact_log_return : {best['total_fact_log_return']:.6f}")
    print(f"  direction     : {best['direction']}")
    print(f"  entry_cutoff  : {best['entry_cutoff']}")
    print(f"  tp_spec       : {best['tp_spec']}")
    print(f"  sl_spec       : {best['sl_spec']}")
    print(f"  n_trades      : {m['n_trades']}  win_rate={m['win_rate']}  compounded_pct={m['compounded_return_pct']:.4f}")

    # Top-5 setups by total_fact_log_return
    top5 = sorted(grid, key=lambda r: r["total_fact_log_return"], reverse=True)[:5]
    print("\n[TOP-5 SETUPS]")
    for rank, s in enumerate(top5, start=1):
        print(
            f"  #{rank}: dir={s['direction']}  cutoff={s['entry_cutoff']}"
            f"  tp={s['tp_spec']}  sl={s['sl_spec']}"
            f"  total_lr={s['total_fact_log_return']:.6f}"
            f"  n_trades={s['n_trades']}"
        )

    print("\n[OK] strat.* tables + strategy_artifact.json + grid_results.csv written; reg.strategies registered")
    for kind, fqn in result["strat_tables"].items():
        print(f"     {kind:14s} -> {fqn}")
