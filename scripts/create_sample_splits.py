# =============================================================================
# Create reusable modeling sample splits
# =============================================================================
# Purpose:
#  - Generate shared train/validation/test fold definitions from features table
#  - Persist them under samples/<sample_id>/ for multiple models to reuse
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from modeling.sampling import (
    create_sample_definition_from_db,
    save_sample_definition,
    validate_sample_definition,
)


# =============================================================================
# parse_args() -> argparse.Namespace
# =============================================================================
# Purpose:
#  - Parse CLI args for sample split generation
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create shared modeling sample folds from the features table.",
    )
    parser.add_argument(
        "--sample-id",
        default = "base_fw240_dev",
        help    = "Sample identifier used under samples/<sample-id>",
    )
    parser.add_argument(
        "--target-horizon-minutes",
        type    = int,
        default = 240,
        help    = "Target horizon in minutes; also used as default embargo",
    )
    parser.add_argument(
        "--min-train-days",
        type    = int,
        default = 730,
        help    = "Minimum expanding train window before first validation fold",
    )
    parser.add_argument(
        "--valid-days",
        type    = int,
        default = 180,
        help    = "Validation window length per fold",
    )
    parser.add_argument(
        "--step-days",
        type    = int,
        default = 180,
        help    = "Step between validation fold starts",
    )
    parser.add_argument(
        "--test-days",
        type    = int,
        default = 365,
        help    = "Final holdout test window length",
    )
    parser.add_argument(
        "--output-root",
        default = "samples",
        help    = "Root directory for generated sample definitions",
    )
    parser.add_argument(
        "--asset-id",
        default = None,
        help    = "Asset ID from config/assets.json (e.g. solusdt_fw60); omit for BCH default",
    )
    return parser.parse_args()


# =============================================================================
# main() -> None
# =============================================================================
# Purpose:
#  - Generate, validate, and save a sample definition
# =============================================================================
def main() -> None:
    args = parse_args()
    sample = create_sample_definition_from_db(
        sample_id              = args.sample_id,
        asset_id               = args.asset_id,
        target_horizon_minutes = args.target_horizon_minutes,
        min_train_days         = args.min_train_days,
        valid_days             = args.valid_days,
        step_days              = args.step_days,
        test_days              = args.test_days,
    )
    validate_sample_definition(sample)

    output_dir = Path(args.output_root) / args.sample_id
    save_sample_definition(sample, output_dir)

    print(f"Sample saved: {output_dir}")
    print(f"Data range:   {sample['data']['start']} -> {sample['data']['end']}")
    print(f"Folds:        {len(sample['folds'])}")
    print(f"Test range:   {sample['test']['start']} -> {sample['test']['end']}")


if __name__ == "__main__":
    main()
