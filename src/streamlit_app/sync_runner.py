# =============================================================================
# Streamlit sync runner
# =============================================================================
# Purpose:
#  - Manage per-asset background sync threads and session state
#  - Provide asset-scoped state keys and locks so BCH and SOL sync independently
# =============================================================================

from __future__ import annotations

from datetime import datetime, timezone
import math
import threading
import time
from typing import Any

from streamlit_app.dashboard_logging import get_dashboard_logger
from streamlit_app.sync import SyncResult, get_sync_lock, run_database_sync


_STATE_KEY_PREFIX        = "database_sync_state"
AUTO_SYNC_INTERVAL_SECONDS = 30


# =============================================================================
# _state_key  — per-asset helper
# =============================================================================
def _state_key(asset_id: str | None) -> str:
    return f"{_STATE_KEY_PREFIX}_{asset_id or 'default'}"


# =============================================================================
# ensure_sync_state(session_state, asset_id) -> dict
# =============================================================================
# Purpose:
#  - Initialise and return the per-asset sync state dict from session_state
# =============================================================================
def ensure_sync_state(session_state, asset_id: str | None = None) -> dict[str, Any]:
    key = _state_key(asset_id)
    if key not in session_state:
        session_state[key] = {
            "thread":                    None,
            "running":                   False,
            "started_at":                None,
            "started_at_epoch":          None,
            "finished_at":               None,
            "finished_at_epoch":         None,
            "error":                     None,
            "result":                    None,
            "auto_sync_enabled":         False,
            "auto_sync_interval_seconds": AUTO_SYNC_INTERVAL_SECONDS,
        }

    state = session_state[key]
    state.setdefault("started_at_epoch", None)
    state.setdefault("finished_at_epoch", None)
    state.setdefault("auto_sync_enabled", False)
    state.setdefault("auto_sync_interval_seconds", AUTO_SYNC_INTERVAL_SECONDS)

    thread = state.get("thread")
    if thread is not None and not thread.is_alive() and state.get("running"):
        state["running"]       = False
        state["finished_at"]   = state.get("finished_at") or _now_label()
        state["finished_at_epoch"] = state.get("finished_at_epoch") or _now_epoch()
    return state


# =============================================================================
# is_sync_running(state, asset_id) -> bool
# =============================================================================
def is_sync_running(state: dict[str, Any], asset_id: str | None = None) -> bool:
    thread = state.get("thread")
    return bool(thread is not None and thread.is_alive()) or get_sync_lock(asset_id).locked()


# =============================================================================
# start_sync(state, asset_id) -> bool
# =============================================================================
# Purpose:
#  - Launch a background sync thread for the given asset
# =============================================================================
def start_sync(state: dict[str, Any], asset_id: str | None = None) -> bool:
    if is_sync_running(state, asset_id):
        return False

    now_epoch = _now_epoch()
    state.update(
        {
            "running":           True,
            "started_at":        _now_label(),
            "started_at_epoch":  now_epoch,
            "finished_at":       None,
            "finished_at_epoch": None,
            "error":             None,
            "result":            None,
        }
    )
    name   = f"chronoquant-db-sync-{asset_id or 'default'}"
    thread = threading.Thread(
        target=_sync_worker,
        args=(state, asset_id),
        name=name,
        daemon=True,
    )
    state["thread"] = thread
    thread.start()
    return True


# =============================================================================
# enable_auto_sync / disable_auto_sync
# =============================================================================
def enable_auto_sync(state: dict[str, Any]) -> None:
    state["auto_sync_enabled"]          = True
    state["auto_sync_interval_seconds"] = AUTO_SYNC_INTERVAL_SECONDS


def disable_auto_sync(state: dict[str, Any]) -> None:
    state["auto_sync_enabled"] = False


# =============================================================================
# auto_sync_due_seconds(state, asset_id) -> int | None
# =============================================================================
def auto_sync_due_seconds(state: dict[str, Any], asset_id: str | None = None) -> int | None:
    if not state.get("auto_sync_enabled") or is_sync_running(state, asset_id):
        return None

    interval = int(state.get("auto_sync_interval_seconds") or AUTO_SYNC_INTERVAL_SECONDS)
    base     = state.get("finished_at_epoch") or state.get("started_at_epoch")
    if base is None:
        return 0

    elapsed = _now_epoch() - float(base)
    return max(0, int(math.ceil(interval - elapsed)))


# =============================================================================
# _sync_worker — runs in background thread
# =============================================================================
def _sync_worker(state: dict[str, Any], asset_id: str | None) -> None:
    logger = get_dashboard_logger()
    try:
        result = run_database_sync(asset_id=asset_id)
        state["result"] = _result_payload(result)
    except Exception as exc:
        state["error"] = str(exc)
        logger.exception("Sync failed")
    finally:
        state["running"]             = False
        state["finished_at"]         = _now_label()
        state["finished_at_epoch"]   = _now_epoch()


def _result_payload(result: SyncResult) -> dict[str, Any]:
    return {
        "start_time":          result.start_time,
        "end_time":            result.end_time,
        "ohlcv_rows_before":   result.ohlcv_rows_before,
        "ohlcv_rows_after":    result.ohlcv_rows_after,
        "inserted_ohlcv_rows": result.inserted_ohlcv_rows,
    }


def _now_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _now_epoch() -> float:
    return time.time()
