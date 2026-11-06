"""Output locations of the TV report (all under the gitignored ``data/tv_report/``)."""

from __future__ import annotations

from pathlib import Path

import utils

DEFAULT_SYMBOL = "BTCUSD"


def output_dir() -> Path:
    """Root of every generated file: ``<repo>/data/tv_report``."""
    return Path(utils.repo_root()) / "data" / "tv_report"


def latest_path() -> Path:
    """Cleaned report of the most recent fetch (overwritten on every run)."""
    return output_dir() / "latest.json"


def state_path() -> Path:
    """Dedup state: idea ids seen by the previous run."""
    return output_dir() / "state.json"


def runs_dir() -> Path:
    """Historised raw API pages (``runs/<run_ts>/``) and default report copy."""
    return output_dir() / "runs"


def backtest_dir() -> Path:
    """Author backtest candidates / report / raw author pages."""
    return runs_dir() / "author_backtest"


def ohlcv_cache_dir() -> Path:
    """Binance 1h kline cache used by the author backtest."""
    return runs_dir() / "ohlcv_cache"
