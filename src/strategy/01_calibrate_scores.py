"""CLI wrapper: fit rank calibration and isotonic regression on the strategy table.

Usage:
    uv run python src/strategy/01_calibrate_scores.py \
      --session-id strategy_2101_2605_202605 \
      --start 2025-10-01 --end 2026-02-28
"""

# %% Imports

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import logging

from strategy.strategy.calibrate import fit_calibration
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
        description="Fit rank calibration and isotonic regression on the strategy table."
    )
    parser.add_argument("--session-id", help="Strategy session identifier (optional; auto-derived by default)")
    parser.add_argument("--long-model", help="Long model artifact ID (required if --session-id omitted)")
    parser.add_argument("--short-model", help="Short model artifact ID (required if --session-id omitted)")
    parser.add_argument("--start",      required=True, help="Calibration start date YYYY-MM-DD")
    parser.add_argument("--end",        required=True, help="Calibration end date YYYY-MM-DD")
    return parser.parse_args()


def _resolve_session_id(args: argparse.Namespace) -> str:
    if args.session_id:
        return args.session_id
    if not args.long_model or not args.short_model:
        raise SystemExit("--long-model and --short-model are required when --session-id is omitted")
    return derive_strategy_session_id(args.long_model, args.short_model)


if __name__ == "__main__":
    args = _parse_args()
    session_id = _resolve_session_id(args)

    print(f"[calibrate_scores] session={session_id}")
    print(f"  calibration range: {args.start} -> {args.end}")

    iso_long, iso_short = fit_calibration(
        session_id = session_id,
        start      = args.start,
        end        = args.end,
    )

    print("[OK] rank_lookup_long.parquet  + rank_lookup_short.parquet saved")
    print("[OK] isotonic_long.pkl         + isotonic_short.pkl saved")
    print("[OK] strategy_table.parquet updated with rank + isotonic columns")
    print("     (score_pct_long/short, bucket_long/short, bucket_mean_mfe_*, bucket_hit_rate_*,")
    print("      pred_long_cal, pred_short_cal)")
