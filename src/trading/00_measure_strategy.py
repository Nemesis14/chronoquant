#!/usr/bin/env python
# =============================================================================
# Run configured strategy backtest
# =============================================================================
# Purpose:
#  - Execute one strategy from config/strategies.json
#  - Print key summary metrics and save artifacts under the strategy output_dir
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from modeling.evaluation.backtest import run_configured_strategy


# =============================================================================
# parse_args() -> argparse.Namespace
# =============================================================================
# Purpose:
#  - Parse CLI args for one configured strategy backtest
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one configured strategy backtest.")
    parser.add_argument("strategy_id", help="Strategy id from config/strategies.json")
    return parser.parse_args()


# =============================================================================
# main() -> None
# =============================================================================
# Purpose:
#  - Run backtest and print compact result summary
# =============================================================================
def main() -> None:
    args = parse_args()
    summary = run_configured_strategy(args.strategy_id)
    print(f"strategy_id:  {summary['strategy_id']}")
    print(f"model_id:     {summary['model_id']}")
    print(f"trades:       {summary['trade_count']}")
    print(f"win_rate:     {_fmt_pct(summary.get('win_rate'))}")
    print(f"profit:       {summary.get('profit', 0.0):.2f}")
    print(f"total_return: {_fmt_pct(summary.get('total_return'))}")
    print(f"max_drawdown: {_fmt_pct(summary.get('max_drawdown'))}")
    print(f"output_dir:   {summary['output_dir']}")


# =============================================================================
# _fmt_pct(value: float | None) -> str
# =============================================================================
# Purpose:
#  - Format nullable ratios as percentages
# =============================================================================
def _fmt_pct(value) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


if __name__ == "__main__":
    main()
