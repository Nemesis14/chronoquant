from __future__ import annotations

# =============================================================================
# Streamlit trading service runner
# =============================================================================
# Purpose:
#  - Manage the TradingService background thread as a module-level singleton
#    so it survives Streamlit rerenders and page refreshes.
#  - Provide read functions for the UI to display trading status from trading.db.
# =============================================================================
import logging
import threading
import traceback

_logger = logging.getLogger("chronoquant.trading")

# Module-level singleton — survives Streamlit session rerenders
_service_thread: threading.Thread | None = None
_service_instance = None  # TradingService instance
_last_error: str | None = None  # last startup error, shown in UI


# =============================================================================
# Start / Stop
# =============================================================================

def start_trading(mode: str = "dry_run") -> bool:
    """Start the trading service in a background thread. Returns True if started."""
    global _service_thread, _service_instance, _last_error
    _last_error = None

    if is_trading_running():
        _logger.warning("Trading service already running")
        return False

    try:
        import utils
        from trading.service import TradingService

        config = utils.load_trading_config()
        config["mode"] = mode

        _service_instance = TradingService(config)

        # Thread calls _run() which internally calls _startup() on first iteration.
        # We do NOT call _startup() here to avoid double initialisation.
        _service_thread = threading.Thread(
            target=_service_instance._run,
            name="chronoquant-trading",
            daemon=True,
        )
        _service_thread.start()

        _logger.info("Trading service thread started (mode=%s)", mode)
        return True

    except Exception as exc:
        tb = traceback.format_exc()
        _last_error = f"{type(exc).__name__}: {exc}\n{tb}"
        _logger.error("Failed to start trading service:\n%s", tb)
        _service_instance = None
        _service_thread = None
        return False


def stop_trading() -> None:
    """Signal the trading service to stop."""
    global _service_instance
    if _service_instance is not None:
        _service_instance.stop()
        _logger.info("Stop signal sent to trading service")


def is_trading_running() -> bool:
    """True if the background trading thread is alive."""
    return _service_thread is not None and _service_thread.is_alive()


def get_trading_mode() -> str | None:
    if _service_instance is not None:
        return _service_instance.mode
    return None


def get_last_error() -> str | None:
    return _last_error


# =============================================================================
# Status reads from trading.db (safe for Streamlit fragments)
# =============================================================================

def get_trading_status() -> dict | None:
    """Read current trading status from trading.db. Returns None if DB missing."""
    try:
        import os

        from trading.journal import get_current_run_status, trading_db_path
        db_path = trading_db_path()
        if not os.path.exists(db_path):
            return None
        status = get_current_run_status(db_path)
        if status:
            status["service_running"] = is_trading_running()
        return status
    except Exception:
        return None


def get_recent_signals(limit: int = 10) -> list[dict]:
    try:
        import os

        from trading.journal import get_recent_signals as _get
        from trading.journal import trading_db_path
        db_path = trading_db_path()
        if not os.path.exists(db_path):
            return []
        return _get(db_path, limit=limit)
    except Exception:
        return []


def get_recent_positions(limit: int = 20) -> list[dict]:
    try:
        import os

        from trading.journal import get_recent_positions as _get
        from trading.journal import trading_db_path
        db_path = trading_db_path()
        if not os.path.exists(db_path):
            return []
        return _get(db_path, limit=limit)
    except Exception:
        return []
