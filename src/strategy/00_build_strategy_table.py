"""CLI wrapper: build strategy table from two LightGBM model predictions.

Usage:
    uv run python src/strategy/00_build_strategy_table.py \
      --long-model lgbm_solusdt_l_fw60_2101_2605 \
      --short-model lgbm_solusdt_s_fw60_2101_2605 \
      --start 2025-10-01 --end 2026-05-31 \
      --session-id strategy_2101_2605_202605
"""

# %% Imports

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import logging

from strategy.strategy.build_table import build_strategy_table
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
        description="Build strategy table from two LightGBM model predictions."
    )
    parser.add_argument("--long-model",  required=True, help="Long model artifact ID")
    parser.add_argument("--short-model", required=True, help="Short model artifact ID")
    parser.add_argument("--start",       required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",         required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--session-id", help="Strategy session identifier (optional; auto-derived by default)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    session_id = args.session_id or derive_strategy_session_id(args.long_model, args.short_model)

    print(f"[build_strategy_table] long={args.long_model}  short={args.short_model}")
    print(f"  range : {args.start} -> {args.end}")
    print(f"  session: {session_id}")

    out_path = build_strategy_table(
        long_model_id  = args.long_model,
        short_model_id = args.short_model,
        start          = args.start,
        end            = args.end,
        session_id     = session_id,
    )

    print(f"[OK] strategy_table.parquet -> {out_path}")
