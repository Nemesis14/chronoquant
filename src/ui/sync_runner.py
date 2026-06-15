# =============================================================================
# Streamlit sync runner
# =============================================================================
# Purpose:
#  - Manage per-asset background sync threads and session state
#  - Trigger sync once per closed minute, after a configurable delay
# =============================================================================

from __future__ import annotations

import math
import threading
import time
from datetime import UTC, datetime
from typing import Any

import utils
from ui.dashboard_logging import get_dashboard_logger
from ui.sync import SyncResult, get_sync_lock, run_database_sync

_STATE_KEY_PREFIX = "database_sync_state"
_DEFAULT_SYNC_DELAY_SECONDS = 3


def _load_sync_delay() -> int:
    """Return ohlcv_sync_delay_seconds from trading.json, default 3."""
    try:
        cfg = utils.load_trading_config()
        return int(cfg.get("ohlcv_sync_delay_seconds", _DEFAULT_SYNC_DELAY_SECONDS))
    except Exception:
        return _DEFAULT_SYNC_DELAY_SECONDS


# =============================================================================
# _state_key  — per-asset helper
# =============================================================================
def _state_key(asset_id: str | None) -> str:
    return f"{_STATE_KEY_PREFIX}_{asset_id or 'default'}"


# =============================================================================
# ensure_sync_state(session_state, asset_id) -> dict
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
            "last_synced_minute":        None,
        }

    state = session_state[key]
    state.setdefault("started_at_epoch", None)
    state.setdefault("finished_at_epoch", None)
    state.setdefault("auto_sync_enabled", False)
    state.setdefault("last_synced_minute", None)

    thread = state.get("thread")
    if thread is not None and not thread.is_alive() and state.get("running"):
        state["running"]             = False
        state["finished_at"]         = state.get("finished_at") or _now_label()
        state["finished_at_epoch"]   = state.get("finished_at_epoch") or _now_epoch()
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
    # Record which closed minute this sync covers to prevent double-firing.
    state["last_synced_minute"] = _current_closed_minute(now_epoch)

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
    state["auto_sync_enabled"] = True


def disable_auto_sync(state: dict[str, Any]) -> None:
    state["auto_sync_enabled"] = False


# =============================================================================
# auto_sync_due_seconds(state, asset_id) -> int | None
# =============================================================================
# Returns seconds until the next sync is due, or None if auto-sync is off/running.
#
# Sync timing rule:
#   target = closed_minute_boundary + ohlcv_sync_delay_seconds
#   Fire once per closed minute (guarded by last_synced_minute).
# =============================================================================
def auto_sync_due_seconds(state: dict[str, Any], asset_id: str | None = None) -> int | None:
    if not state.get("auto_sync_enabled") or is_sync_running(state, asset_id):
        return None

    delay        = _load_sync_delay()
    now          = _now_epoch()
    closed_min   = _current_closed_minute(now)
    last_synced  = state.get("last_synced_minute")

    if last_synced is not None and last_synced >= closed_min:
        # Already synced this minute — wait for the next one.
        next_target = closed_min + 60 + delay
        seconds_left = int(math.ceil(next_target - now))
        return max(0, seconds_left)

    # Closed minute has passed and we haven't synced it yet.
    # Wait until closed_minute + delay.
    target = closed_min + delay
    return max(0, int(math.ceil(target - now)))


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


def _current_closed_minute(epoch: float) -> int:
    """Return the Unix epoch seconds of the last completed minute boundary."""
    return int(epoch // 60) * 60


def _now_label() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _now_epoch() -> float:
    return time.time()
