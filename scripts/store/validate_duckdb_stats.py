"""Print non-blocking DuckDB statistics smoke metrics for validation.

Usage:
    uv run python scripts/validate_duckdb_stats.py
    uv run python scripts/validate_duckdb_stats.py --asset-id solusdt_fw60
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils
from store.duckdb_stats import collect_duckdb_stats_report, format_duckdb_stats_report


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="DuckDB statistics smoke report")
    parser.add_argument("--asset-id", default=None, help="Asset ID from config/assets.json")
    return parser.parse_args()


def main() -> None:
    """Print the informational DuckDB statistics report."""
    args     = _parse_args()
    asset_cfg = utils.load_asset_config(args.asset_id)
    data_dir  = asset_cfg["database"]["data_dir"]
    report    = collect_duckdb_stats_report(data_dir)
    print(format_duckdb_stats_report(report))


if __name__ == "__main__":
    main()
