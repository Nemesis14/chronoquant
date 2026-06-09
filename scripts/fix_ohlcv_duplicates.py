# =============================================================================
# Remove duplicate open_time rows from OHLCV and enforce UNIQUE index
# =============================================================================
# Purpose:
#  - One-time cleanup for existing duplicate rows (keeps newest rowid)
#  - Deletes stale features/predictions rows from earliest affected time
#  - Adds UNIQUE index to OHLCV, features, and predictions tables
#  - Rebuilds derived tables for the affected time range
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from db.maintenance import fix_ohlcv_duplicates


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fix duplicate open_time rows in OHLCV and add UNIQUE index.",
    )
    parser.add_argument(
        "--asset-id",
        default=None,
        help="Asset ID from config/assets.json (e.g. solusdt_fw60); omit for default",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fix_ohlcv_duplicates(asset_id=args.asset_id)


if __name__ == "__main__":
    main()
