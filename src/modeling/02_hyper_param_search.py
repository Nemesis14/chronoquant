#!/usr/bin/env python3
# =============================================================================
# LightGBM distribution-based hyperparameter search
# =============================================================================
# Usage examples:
#   python scripts/search_lgbm.py --model-id lgbm_solusdt_l_fw60_q90_local_v2 --stage smoke
#   python scripts/search_lgbm.py --model-id lgbm_solusdt_l_fw60_q90_local_v2 --stage explore
#   python scripts/search_lgbm.py --model-id lgbm_solusdt_l_fw60_q90_local_v2 --stage explore --n-trials 60 --timeout-hours 6 --resume
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from modeling.search.lgbm_search import run_search


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Distribution-based LightGBM hyperparameter search.",
    )
    parser.add_argument(
        "--model-id",
        required=True,
        help="Model ID from config/models.json (e.g. lgbm_solusdt_l_fw60_q90_local_v2)",
    )
    parser.add_argument(
        "--stage",
        choices=["smoke", "explore", "refine"],
        default="smoke",
        help="Search stage: smoke (5 trials / 2 folds), explore (60 / all), refine (30 / narrow). Default: smoke",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=60,
        help="Maximum number of trials to run (default: 60; stage defaults override this if smaller)",
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
        help="Row stride for down-sampling the dataset (default: 60 for smoke/explore, 10 for refine)",
    )
    parser.add_argument(
        "--fold-limit",
        type=int,
        default=None,
        help="Limit to first N folds (default: 2 for smoke, all for other stages)",
    )
    parser.add_argument(
        "--asset-id",
        default=None,
        help="Asset ID override (default: taken from model config)",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-run previously failed trials (default: skip them)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Alias for default behavior (search always resumes automatically). "
             "Provided for clarity; has no additional effect.",
    )

    args = parser.parse_args()

    best = run_search(
        model_id      = args.model_id,
        n_trials      = args.n_trials,
        timeout_hours = args.timeout_hours,
        row_stride    = args.row_stride,
        fold_limit    = args.fold_limit,
        stage         = args.stage,
        retry_failed  = args.retry_failed,
        asset_id      = args.asset_id,
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
        print(f"  Mean valid prauc:{best.get('mean_valid_prauc', 'N/A'):.4f}" if best.get("mean_valid_prauc") else "  Mean valid prauc: N/A")
        print("=" * 60)
        sys.exit(0)
    else:
        print("No completed trials.")
        sys.exit(1)


if __name__ == "__main__":
    main()
