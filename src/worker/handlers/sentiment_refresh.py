# =============================================================================
# sentiment_refresh handler: fetch the latest TradingView ideas report
# =============================================================================
# Purpose:
#  - Run sentiment.tv_report.fetch.run_fetch (network: TradingView REST API,
#    4-8 s between pages) and return a header-only result
#  - Output files live under data/tv_report/ (latest.json is read by the
#    sentiment_latest MCP tool)
# =============================================================================

from __future__ import annotations

import logging
from typing import Any

from sentiment.tv_report import paths
from sentiment.tv_report.fetch import run_fetch

MAX_PAGES = 5
MAX_HOURS = 720.0
DEFAULT_PAGES = 3
DEFAULT_HOURS = 24.0


def run_sentiment_refresh(params: dict[str, Any], logger: logging.Logger) -> dict[str, Any]:
    pages = max(1, min(MAX_PAGES, int(params.get("pages", DEFAULT_PAGES))))
    hours = max(1.0, min(MAX_HOURS, float(params.get("hours", DEFAULT_HOURS))))
    symbol = str(params.get("symbol") or paths.DEFAULT_SYMBOL)

    logger.info("TV sentiment refresh: symbol=%s hours=%s pages=%d", symbol, hours, pages)
    report = run_fetch(symbol, pages, hours, log=logger)
    return {k: v for k, v in report.items() if k != "ideas"}
