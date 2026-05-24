# =============================================================================
# Train configured model
# =============================================================================
# Purpose:
#  - Thin CLI wrapper around src/modeling/train.py
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from modeling.train import train_model


# =============================================================================
# parse_args() -> argparse.Namespace
# =============================================================================
# Purpose:
#  - Parse model training CLI arguments
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a configured ChronoQuant model.",
    )
    parser.add_argument(
        "--model-id",
        required = True,
        help     = "Model id from config/models.json",
    )
    return parser.parse_args()


# =============================================================================
# main() -> None
# =============================================================================
# Purpose:
#  - Train the requested model
# =============================================================================
def main() -> None:
    args = parse_args()
    result = train_model(args.model_id)
    tuning_param = result.get("tuning_param", "alpha")
    best_value   = result.get("best_tuning_value", result.get(f"best_{tuning_param}"))
    print(f"Model trained: {result['model_id']}")
    print(f"Best {tuning_param}: {best_value}")
    print(f"Selected:      {result['n_features_selected']} / {result['n_features_input']}")
    print(f"Output:        {result['output_dir']}")


if __name__ == "__main__":
    main()
