#!/usr/bin/env python3
"""LightGBM hyperparameter search — standalone entry point.

Standard usage (joint feature+param search, long direction):
    uv run python src/modeling/02_hyper_param_search.py --model lgbm_solusdt_l_fw60_2101_2605 --stage explore
    uv run python src/modeling/02_hyper_param_search.py --model lgbm_solusdt_s_fw60_2101_2605 --stage explore --direction short

The same step runs via the full pipeline:
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

from modeling.search.lgbm_search import run_gain_rank, run_search, run_prune  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="LightGBM joint feature+hyperparameter search.",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Model ID from config/models.json",
    )
    parser.add_argument(
        "--stage",
        choices=["smoke", "explore", "refine"],
        default="explore",
        help="Search stage: smoke (5 trials), explore (60), refine (30). Default: explore",
    )
    parser.add_argument(
        "--direction",
        choices=["long", "short"],
        default="long",
        help="Model direction — determines search objective metric. Default: long",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=60,
        help="Maximum number of trials (default: 60)",
    )
    parser.add_argument(
        "--timeout-hours",
        type=float,
        default=None,
        help="Hard wall-clock time limit in hours (default: no limit)",
    )
    parser.add_argument(
        "--feature-selection",
        choices=["joint", "fixed"],
        default="joint",
        help="'joint' (default): feature_k as Optuna param after gain_rank. 'fixed': use feature_key list.",
    )
    parser.add_argument(
        "--feature-key",
        default="selected",
        help="Feature set key in feature_set.json (only for --feature-selection fixed). Default: selected",
    )
    parser.add_argument(
        "--search-tag",
        default=None,
        help="Custom search directory tag (default: auto-derived from feature_selection)",
    )
    parser.add_argument(
        "--skip-gain-rank",
        action="store_true",
        help="Skip run_gain_rank step (use existing gain_ranked in feature_set.json)",
    )
    parser.add_argument(
        "--skip-prune",
        action="store_true",
        help="Skip run_prune step after search",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-run previously failed trials",
    )

    args = parser.parse_args()

    if args.feature_selection == "joint" and not args.skip_gain_rank:
        print(f"[1/3] run_gain_rank({args.model}) ...")
        gain_ranked = run_gain_rank(args.model)
        print(f"      Top-10 by gain: {gain_ranked[:10]}")
    else:
        print("[1/3] Skipping gain_rank (--skip-gain-rank or fixed selection)")

    print(f"\n[2/3] run_search({args.model}, stage={args.stage}, direction={args.direction}, "
          f"feature_selection={args.feature_selection}) ...")
    best = run_search(
        model_id          = args.model,
        stage             = args.stage,
        n_trials          = args.n_trials,
        timeout_hours     = args.timeout_hours,
        feature_key       = args.feature_key,
        feature_selection = args.feature_selection,
        search_tag        = args.search_tag,
        direction         = args.direction,
        retry_failed      = args.retry_failed,
    )

    if best:
        print("\n" + "=" * 60)
        print("BEST TRIAL")
        print("=" * 60)
        print(f"  trial_no         : #{best.get('trial_no')}")
        print(f"  objective_score  : {best.get('objective_score', float('inf')):.6f}")
        print(f"  valid_ratio_p925 : {best.get('valid_ratio_p925', 0.0):.6f}")
        print(f"  valid_ratio_p075 : {best.get('valid_ratio_p075', 0.0):.6f}")
        print(f"  train_valid_gap  : {best.get('train_valid_gap', 0.0):.6f}")
        fk = best.get("params", {}).get("feature_k")
        if fk is not None:
            print(f"  feature_k        : {fk}")
        print("=" * 60)
    else:
        print("No completed trials.")
        sys.exit(1)

    if not args.skip_prune:
        tag = args.search_tag or ("joint" if args.feature_selection == "joint" else args.feature_key)
        print(f"\n[3/3] run_prune({args.model}, search_tag={tag!r}) ...")
        prune_result = run_prune(args.model, search_tag=tag if tag != "selected" else None)
        print(f"      {prune_result}")
    else:
        print("\n[3/3] Skipping prune (--skip-prune)")


if __name__ == "__main__":
    main()
