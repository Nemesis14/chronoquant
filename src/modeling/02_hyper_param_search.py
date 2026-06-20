#!/usr/bin/env python3
"""LightGBM hyperparameter search — standalone entry point.

Usage:
    uv run python src/modeling/02_hyper_param_search.py --model <model_id>
    uv run python src/modeling/02_hyper_param_search.py --model <model_id> --stage explore
    uv run python src/modeling/02_hyper_param_search.py --model <model_id> --stage explore --n-trials 60 --timeout-hours 6

The same search step can be run via the full pipeline:
    uv run python src/modeling/pipeline.py --model <model_id> --step search --stage smoke
"""

import argparse
import sys
from pathlib import Path

_ROOT = next(
    p for p in [Path(__file__).resolve().parent, *Path(__file__).resolve().parents]
    if (p / "pyproject.toml").exists()
)
sys.path.insert(0, str(_ROOT / "src"))

from modeling.search.lgbm_search import run_search  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="LightGBM hyperparameter search (yearly sample edition).",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Model ID from config/models.json (e.g. <model_id>)",
    )
    parser.add_argument(
        "--stage",
        choices=["smoke", "explore", "refine"],
        default="smoke",
        help="Search stage: smoke (5 trials / 2 folds), explore (60 / all), refine (30 / all). Default: smoke",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=60,
        help="Maximum number of trials (default: 60; stage cap applies)",
    )
    parser.add_argument(
        "--timeout-hours",
        type=float,
        default=None,
        help="Hard wall-clock time limit in hours (default: no limit)",
    )
    parser.add_argument(
        "--row-stride",
        type=int,
        default=None,
        help="Downsample every N-th row from the sample parquet (default: 1 = no downsampling)",
    )
    parser.add_argument(
        "--fold-limit",
        type=int,
        default=None,
        help="Limit to first N validation folds (default: 2 for smoke, all for other stages)",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-run previously failed trials (default: skip them)",
    )

    args = parser.parse_args()

    best = run_search(
        model_id      = args.model,
        stage         = args.stage,
        n_trials      = args.n_trials,
        timeout_hours = args.timeout_hours,
        row_stride    = args.row_stride,
        fold_limit    = args.fold_limit,
        retry_failed  = args.retry_failed,
    )

    if best:
        print("\n" + "=" * 60)
        print("BEST RESULT SUMMARY")
        print("=" * 60)
        print(f"  Trial:           #{best.get('trial_no')}")
        print(f"  Objective score: {best.get('objective_score', 'N/A'):.6f}")
        print(f"  Mean valid ll:   {best.get('mean_valid_ll', 'N/A'):.6f}")
        print(f"  Mean train ll:   {best.get('mean_train_ll', 'N/A'):.6f}")
        print(f"  Mean gap:        {best.get('mean_gap', 'N/A'):.6f}")
        print(f"  Std valid ll:    {best.get('std_valid_ll', 'N/A'):.6f}")
        if best.get("mean_valid_prauc") is not None:
            print(f"  Mean valid prauc:{best.get('mean_valid_prauc', 'N/A'):.4f}")
        print("=" * 60)
        sys.exit(0)
    else:
        print("No completed trials.")
        sys.exit(1)


if __name__ == "__main__":
    main()
