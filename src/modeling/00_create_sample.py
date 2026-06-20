"""CLI for generating a yearly random-hour sample.

Runs the full DB load → hourly select → fold-assign → write pipeline
and prints a summary to stdout.

Usage:
    # Model-specific (recommended): sample written to artifact directory
    uv run python src/modeling/00_create_sample.py --model lgbm_solusdt_l_fw60_2021

    # Legacy (shared yearly sample): written to database/samples/
    uv run python src/modeling/00_create_sample.py --year 2021 --asset-id solusdt
    uv run python src/modeling/00_create_sample.py --year 2022 --asset-id solusdt --seed 100
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from modeling.sampling import (
    YearlySamplingConfig,
    create_model_sample,
    create_yearly_sample,
    load_yearly_sample,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a ChronoQuant yearly random-hour sample.",
    )
    parser.add_argument("--model",    default=None, help="Model key from config/models.json (recommended)")
    parser.add_argument("--year",     default=None, type=int, help="Calendar year (legacy mode)")
    parser.add_argument("--asset-id", default=None,           help="Asset key (legacy mode)")
    parser.add_argument("--seed",     default=None, type=int, help="Random seed (legacy mode)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.model:
        create_model_sample(args.model)

        import utils
        models_cfg = utils.load_models_config()
        meta       = models_cfg["models"][args.model]
        artifact_dir = Path(utils._resolve_path(meta["artifact_dir"]))
        sample = load_yearly_sample(artifact_dir)

        counts    = sample.get("fold_row_counts", {})
        n_folds   = sample.get("n_folds", 4)
        feat_cols = sample.get("feature_cols", [])

        print(f"OK: Sample created at {artifact_dir}")
        print(f"    model        = {args.model}")
        print(f"    year         = {sample['year']}")
        print(f"    seed         = {sample['seed']}")
        print(f"    n_folds      = {n_folds}")
        print(f"    feature_cols = {len(feat_cols)}")
        print(f"    total_rows   = {sum(counts.values())}")
        for fold_id in sorted(counts.keys()):
            print(f"      fold {fold_id}     = {counts[fold_id]}")
        return

    if not args.year or not args.asset_id:
        print("ERROR: provide --model OR both --year and --asset-id")
        sys.exit(1)

    seed      = args.seed if args.seed is not None else (42 + args.year)
    sample_id = f"{args.asset_id}_fw60_yearly_{args.year}"

    config = YearlySamplingConfig(
        sample_id = sample_id,
        asset_id  = args.asset_id,
        year      = args.year,
        seed      = seed,
    )

    create_yearly_sample(config)

    sample_dir = Path(f"database/{args.asset_id}/samples/{sample_id}")
    sample     = load_yearly_sample(sample_dir)
    counts     = sample.get("fold_row_counts", {})
    n_folds    = sample.get("n_folds", 4)
    feat_cols  = sample.get("feature_cols", [])

    print(f"OK: Sample created at {sample_dir}")
    print(f"    year         = {sample['year']}")
    print(f"    seed         = {sample['seed']}")
    print(f"    n_folds      = {n_folds}")
    print(f"    feature_cols = {len(feat_cols)}")
    print(f"    total_rows   = {sum(counts.values())}")
    for fold_id in sorted(counts.keys()):
        print(f"      fold {fold_id}     = {counts[fold_id]}")


if __name__ == "__main__":
    main()
