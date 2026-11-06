"""Job handlers: `handler(params, logger) -> result dict`."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

Handler = Callable[[dict[str, Any], logging.Logger], dict[str, Any]]


def default_handlers() -> dict[str, Handler]:
    """Registry kind -> handler (imported lazily: pulls pandas/DuckDB code)."""
    from worker.handlers.sentiment_refresh import run_sentiment_refresh
    from worker.handlers.sync_full import run_sync_full
    from worker.handlers.sync_predictions import run_sync_predictions

    return {
        "sync_full": run_sync_full,
        "sync_predictions": run_sync_predictions,
        "sentiment_refresh": run_sentiment_refresh,
    }
