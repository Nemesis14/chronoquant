"""Run single-pass strategy calibration for a trained model.

Loads the model's sample_oos.parquet, runs a default-parameter backtest,
and writes a strategy artifact to artifacts/<model_id>/strategy/.

Usage:
    uv run python src/trading/00_calibrate_strategy.py --model lgbm_solusdt_l_fw60_2021
    uv run python src/trading/00_calibrate_strategy.py --model lgbm_solusdt_l_fw60_2021 --start 2022-01-01 --end 2022-12-31
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from trading.calibration.calibrate import run_calibration

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)


# %% CLI


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Namespace with model, start, end attributes.
    """
    parser = argparse.ArgumentParser(description="Calibrate strategy for a trained model.")
    parser.add_argument(
        "--model", required=True,
        help="Model ID (artifact directory name), e.g. lgbm_solusdt_l_fw60_2021",
    )
    parser.add_argument(
        "--start", default=None,
        help="OOS start date override (YYYY-MM-DD). Default: full parquet range.",
    )
    parser.add_argument(
        "--end", default=None,
        help="OOS end date override (YYYY-MM-DD). Default: full parquet range.",
    )
    return parser.parse_args()


def main() -> None:
    """Run calibration and print the summary to stdout."""
    args   = parse_args()
    result = run_calibration(
        model_id = args.model,
        start    = args.start,
        end      = args.end,
    )

    metrics = result["metrics"]
    print()
    print("=" * 60)
    print(f"CALIBRATION COMPLETE: {result['model_id']}")
    print("=" * 60)
    print(f"side:          {result['side']}")
    print(f"oos_period:    {result['oos_period']['start']} to {result['oos_period']['end']}")
    print(f"trades:        {metrics.get('trade_count', 0)}")
    print(f"win_rate:      {_fmt_pct(metrics.get('win_rate'))}")
    print(f"total_return:  {_fmt_pct(metrics.get('total_return'))}")
    print(f"profit_factor: {metrics.get('profit_factor') or 'n/a'}")
    print(f"max_drawdown:  {_fmt_pct(metrics.get('max_drawdown'))}")
    print(f"exposure_pct:  {_fmt_pct(metrics.get('exposure_pct'))}")
    print()
    print(f"artifact_path: {result['artifact_path']}")
    print()


def _fmt_pct(value: float | None) -> str:
    """Format a ratio as a percentage string.

    Args:
        value : Ratio (0.0 to 1.0 scale) or None.

    Returns:
        Formatted string such as ``"78.50%"`` or ``"n/a"``.
    """
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


if __name__ == "__main__":
    main()
