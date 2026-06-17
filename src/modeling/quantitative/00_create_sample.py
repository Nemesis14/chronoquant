"""CLI for generating a yearly random-hour sample.

Runs the full DB load → hourly select → segment assign → write pipeline
and prints a summary to stdout.

Usage:
    uv run python src/modeling/quantitative/00_create_sample.py --year 2021 --asset-id solusdt
    uv run python src/modeling/quantitative/00_create_sample.py --year 2022 --asset-id solusdt --seed 100
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from modeling.quantitative.sampling import (
    YearlySamplingConfig,
    create_yearly_sample,
    load_yearly_sample,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a ChronoQuant yearly random-hour sample.",
    )
    parser.add_argument("--year",     required=True, type=int, help="Calendar year to sample (e.g. 2021)")
    parser.add_argument("--asset-id", required=True,           help="Asset key from config/assets.json")
    parser.add_argument("--seed",     default=None,  type=int, help="Random seed (default: 42 + year)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    seed      = args.seed if args.seed is not None else (42 + args.year)
    sample_id = f"{args.asset_id}_fw60_yearly_{args.year}"

    config = YearlySamplingConfig(
        sample_id=sample_id,
        asset_id=args.asset_id,
        year=args.year,
        seed=seed,
    )

    create_yearly_sample(config)

    sample_dir = Path(f"database/{args.asset_id}/samples/{sample_id}")
    sample     = load_yearly_sample(sample_dir)
    counts     = sample.get("row_counts", {})
    n_weeks    = len(sample.get("selected_valid_weeks", []))

    print(f"OK: Sample created at {sample_dir}")
    print(f"    year           = {sample['year']}")
    print(f"    seed           = {sample['seed']}")
    print(f"    valid_weeks    = {n_weeks}")
    print(f"    total_rows     = {sum(counts.values())}")
    for seg in ("train", "valid", "purge"):
        print(f"      {seg:<6}       = {counts.get(seg, 0)}")


if __name__ == "__main__":
    main()
