"""CLI for generating a time-based CV sample definition.

Runs the full audit → splits → write pipeline and prints a summary to stdout.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from modeling.quantitative.sampling import SamplingConfig, create_sample, load_sample_definition


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for sample generation."""
    parser = argparse.ArgumentParser(
        description="Generate a ChronoQuant time-based CV sample definition.",
    )
    parser.add_argument("--sample-id",               required=True,               help="Unique sample identifier")
    parser.add_argument("--asset-id",                required=True,               help="Asset key from config/assets.json")
    parser.add_argument("--target-col",              required=True,               help="Target column (e.g. long_mfe_fw60)")
    parser.add_argument("--target-horizon-minutes",  required=True, type=int,     help="Forward-return horizon in minutes")
    parser.add_argument("--min-train-days",          default=730,   type=int,     help="Minimum training window (default: 730)")
    parser.add_argument("--valid-days",              default=180,   type=int,     help="Validation window in days (default: 180)")
    parser.add_argument("--step-days",               default=180,   type=int,     help="Fold step size in days (default: 180)")
    parser.add_argument("--test-days",               default=365,   type=int,     help="Final holdout window in days (default: 365)")
    parser.add_argument("--embargo-minutes",         default=None,  type=int,     help="Embargo gap in minutes (default: target-horizon-minutes)")
    parser.add_argument("--no-round-to-months",      action="store_true",          help="Disable whole-month rounding of data boundaries (default: enabled)")
    return parser.parse_args()


def main() -> None:
    """Run sample generation and print a summary."""
    args = parse_args()

    config = SamplingConfig(
        sample_id              = args.sample_id,
        asset_id               = args.asset_id,
        target_col             = args.target_col,
        target_horizon_minutes = args.target_horizon_minutes,
        min_train_days         = args.min_train_days,
        valid_days             = args.valid_days,
        step_days              = args.step_days,
        test_days              = args.test_days,
        embargo_minutes        = args.embargo_minutes,
        round_to_months        = not args.no_round_to_months,
    )

    create_sample(config)

    sample_dir = Path(f"database/{args.asset_id}/samples/{args.sample_id}")
    sample     = load_sample_definition(sample_dir)

    print(f"OK: Sample created at {sample_dir}")
    print(f"    n_folds        = {sample.get('n_folds')}")
    print(f"    data_start_safe= {sample['data']['start']}")
    print(f"    data_end_safe  = {sample['data']['end']}")


if __name__ == "__main__":
    main()
