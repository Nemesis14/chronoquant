# =============================================================================
# Train configured model
# =============================================================================
# Purpose:
#  - Thin CLI wrapper around modeling/training/train.py
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from modeling.training.train import train_model


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a configured ChronoQuant model.")
    parser.add_argument("--model", required=True, help="Model ID from config/models.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = train_model(args.model)
    print(f"Model trained : {result['model_id']}")
    print(f"n_estimators  : {result['n_estimators']}")
    print(f"n_features    : {result['n_features']}")
    print(f"oos_year      : {result.get('oos_year')}")
    print(f"Output        : {result['artifact_dir']}")


if __name__ == "__main__":
    main()
