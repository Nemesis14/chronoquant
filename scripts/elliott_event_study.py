#!/usr/bin/env python3
# =============================================================================
# Run SOLUSDT Elliott event study
# =============================================================================
# Purpose:
#  - Build the separate solusdt_elliott_events table from raw OHLCV
#  - Generate a markdown report for long 1-2 / nested 1-2-1-2 outcomes
# =============================================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from analysis.elliott_event_study import EVENT_TABLE, run_event_study


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run long-only SOLUSDT Elliott 1-2 / 1-2-1-2 event study.",
    )
    parser.add_argument(
        "--asset-id",
        default = "solusdt_fw60",
        help    = "Asset ID from config/assets.json",
    )
    parser.add_argument(
        "--start",
        default = None,
        help    = "Optional start time, format: YYYY-MM-DD HH:MM:SS",
    )
    parser.add_argument(
        "--end",
        default = None,
        help    = "Optional end time, format: YYYY-MM-DD HH:MM:SS",
    )
    parser.add_argument(
        "--event-table",
        default = EVENT_TABLE,
        help    = "Separate Elliott event table to replace",
    )
    parser.add_argument(
        "--report-path",
        default = "docs/analysis/solusdt_elliott_event_study_v1.md",
        help    = "Markdown report path",
    )
    parser.add_argument(
        "--raw-up-threshold",
        type    = float,
        default = 0.01,
        help    = "Forward max-up threshold over 60 minutes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    events, report_path = run_event_study(
        asset_id          = args.asset_id,
        start             = args.start,
        end               = args.end,
        event_table       = args.event_table,
        report_path       = args.report_path,
        raw_up_threshold  = args.raw_up_threshold,
    )
    print(f"OK: Wrote {len(events):,} Elliott events to '{args.event_table}'")
    print(f"OK: Wrote report to '{report_path}'")


if __name__ == "__main__":
    main()
