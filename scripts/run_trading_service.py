"""
Headless trading service CLI.

Usage:
    python scripts/run_trading_service.py [--mode dry_run|live]

Runs the TradingService loop in the foreground until Ctrl+C.
The UI can start the same service as a background thread via the
Live Trading button in the Streamlit sidebar.
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import utils
from trading.service import TradingService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("chronoquant.trading")


def main() -> None:
    parser = argparse.ArgumentParser(description="ChronoQuant trading service")
    parser.add_argument(
        "--mode",
        choices=["dry_run", "live"],
        default=None,
        help="Override mode from config/trading.json (dry_run or live)",
    )
    args = parser.parse_args()

    config = utils.load_trading_config()
    if args.mode:
        config["mode"] = args.mode

    mode = config["mode"]
    _logger.info("Starting trading service mode=%s", mode)

    if mode == "live":
        answer = input("Confirm LIVE trading (yes/no): ").strip().lower()
        if answer != "yes":
            _logger.info("Live mode not confirmed — exiting")
            sys.exit(0)

    service = TradingService(config)

    def _on_signal(sig, frame):
        _logger.info("Signal %s received — stopping service", sig)
        service.stop()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    service._startup()
    service._run()


if __name__ == "__main__":
    main()
