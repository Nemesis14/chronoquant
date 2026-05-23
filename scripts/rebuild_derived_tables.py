# =============================================================================
# Rebuild derived database tables from the OHLCV base table
# =============================================================================
# Purpose:
#  - Drop and rebuild FEATURES and PREDICTIONS tables from existing OHLCV data
#  - Use the existing sync_features and sync_predictions code paths
#  - Support controlled interval tests before full rebuilds
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app import settings
from db.maintenance import rebuild_derived_tables


# =============================================================================
# parse_args() -> argparse.Namespace
# =============================================================================
# Purpose:
#  - Parse CLI arguments for interval rebuilds and full rebuilds
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild FEATURES and PREDICTIONS from existing OHLCV data.",
    )
    parser.add_argument(
        "--start",
        default = settings.INIT_START_DATE,
        help    = "Start time, format: YYYY-MM-DD HH:MM:SS",
    )
    parser.add_argument(
        "--end",
        default = None,
        help    = "Optional end time, format: YYYY-MM-DD HH:MM:SS",
    )
    parser.add_argument(
        "--drop",
        action = "store_true",
        help   = "Drop derived tables before rebuild",
    )
    parser.add_argument(
        "--features-only",
        action = "store_true",
        help   = "Rebuild only the FEATURES table",
    )
    parser.add_argument(
        "--predictions-only",
        action = "store_true",
        help   = "Rebuild only the PREDICTIONS table",
    )
    return parser.parse_args()


# =============================================================================
# main() -> None
# =============================================================================
# Purpose:
#  - Run the configured derived-table rebuild
# =============================================================================
def main() -> None:
    args = parse_args()
    rebuild_derived_tables(
        start            = args.start,
        end              = args.end,
        drop             = args.drop,
        features_only    = args.features_only,
        predictions_only = args.predictions_only,
    )


if __name__ == "__main__":
    main()



