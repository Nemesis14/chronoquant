"""CLI for creating a model's snapshot-native sample table.

Builds ``model."<model_id>__sample"`` (open_time + target(s) + fold_id) from an
immutable ``snap."<snapshot_id>"`` table and registers the logical feature_set in
``reg.feature_sets`` (plan section 5, steps 2-3).

Usage:
    uv run python src/modeling/00_create_sample.py \
        --model lgbm_solusdt_l_fw60_2101_2605 \
        --snapshot solusdt_fw60_2101_2605__a1b2c3d4

If --snapshot is omitted, the model's sampling.snapshot_id from config/models.json
is used.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils
from modeling.sampling import create_model_sample


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a ChronoQuant model sample table from a snapshot.",
    )
    parser.add_argument("--model",    required=True, help="Model key from config/models.json")
    parser.add_argument("--snapshot", default=None,  help="Snapshot id (snap.\"<id>\"); "
                                                          "defaults to sampling.snapshot_id in config")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    snapshot_id = args.snapshot
    if not snapshot_id:
        meta        = utils.load_models_config()["models"][args.model]
        snapshot_id = meta.get("sampling", {}).get("snapshot_id")
        if not snapshot_id:
            print("ERROR: provide --snapshot or set sampling.snapshot_id in config/models.json")
            sys.exit(1)

    summary = create_model_sample(args.model, snapshot_id)

    print(f"OK: {summary['sample_table']}")
    print(f"    model          = {summary['model_id']}")
    print(f"    snapshot       = {summary['snapshot_id']}")
    print(f"    n_rows         = {summary['n_rows']}")
    print(f"    feature_set    = {summary['feature_set_id']}")
    print(f"    n_input        = {summary['n_input']}")
    print(f"    n_selected     = {summary['n_selected']}")
    for fold_id in sorted(summary["fold_row_counts"], key=int):
        print(f"      fold {fold_id}        = {summary['fold_row_counts'][fold_id]}")


if __name__ == "__main__":
    main()
