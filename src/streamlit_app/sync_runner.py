from __future__ import annotations

from datetime import datetime, timezone
import math
import threading
import time
from typing import Any

from streamlit_app.dashboard_logging import get_dashboard_logger
from streamlit_app.sync import SyncResult, run_database_sync


STATE_KEY = "database_sync_state"
AUTO_SYNC_INTERVAL_SECONDS = 30
_SYNC_LOCK = threading.Lock()


def ensure_sync_state(session_state) -> dict[str, Any]:
    if STATE_KEY not in session_state:
        session_state[STATE_KEY] = {
            "thread": None,
            "running": False,
            "started_at": None,
            "started_at_epoch": None,
            "finished_at": None,
            "finished_at_epoch": None,
            "error": None,
            "result": None,
            "auto_sync_enabled": False,
            "auto_sync_interval_seconds": AUTO_SYNC_INTERVAL_SECONDS,
        }

    state = session_state[STATE_KEY]
    state.setdefault("started_at_epoch", None)
    state.setdefault("finished_at_epoch", None)
    state.setdefault("auto_sync_enabled", False)
    state.setdefault("auto_sync_interval_seconds", AUTO_SYNC_INTERVAL_SECONDS)

    thread = state.get("thread")
    if thread is not None and not thread.is_alive() and state.get("running"):
        state["running"] = False
        state["finished_at"] = state.get("finished_at") or _now_label()
        state["finished_at_epoch"] = state.get("finished_at_epoch") or _now_epoch()
    return state


def is_sync_running(state: dict[str, Any]) -> bool:
    thread = state.get("thread")
    return bool(thread is not None and thread.is_alive()) or _SYNC_LOCK.locked()


def start_sync(state: dict[str, Any]) -> bool:
    if is_sync_running(state):
        return False

    now_epoch = _now_epoch()
    state.update(
        {
            "running": True,
            "started_at": _now_label(),
            "started_at_epoch": now_epoch,
            "finished_at": None,
            "finished_at_epoch": None,
            "error": None,
            "result": None,
        }
    )
    thread = threading.Thread(target=_sync_worker, args=(state,), name="chronoquant-db-sync", daemon=True)
    state["thread"] = thread
    thread.start()
    return True


def enable_auto_sync(state: dict[str, Any]) -> None:
    state["auto_sync_enabled"] = True
    state["auto_sync_interval_seconds"] = AUTO_SYNC_INTERVAL_SECONDS


def disable_auto_sync(state: dict[str, Any]) -> None:
    state["auto_sync_enabled"] = False


def auto_sync_due_seconds(state: dict[str, Any]) -> int | None:
    if not state.get("auto_sync_enabled") or is_sync_running(state):
        return None

    interval = int(state.get("auto_sync_interval_seconds") or AUTO_SYNC_INTERVAL_SECONDS)
    base = state.get("finished_at_epoch") or state.get("started_at_epoch")
    if base is None:
        return 0

    elapsed = _now_epoch() - float(base)
    return max(0, int(math.ceil(interval - elapsed)))


def _sync_worker(state: dict[str, Any]) -> None:
    logger = get_dashboard_logger()
    with _SYNC_LOCK:
        try:
            result = run_database_sync()
            state["result"] = _result_payload(result)
        except Exception as exc:
            state["error"] = str(exc)
            logger.exception("Sync failed")
        finally:
            state["running"] = False
            state["finished_at"] = _now_label()
            state["finished_at_epoch"] = _now_epoch()


def _result_payload(result: SyncResult) -> dict[str, Any]:
    return {
        "start_time": result.start_time,
        "end_time": result.end_time,
        "ohlcv_rows_before": result.ohlcv_rows_before,
        "ohlcv_rows_after": result.ohlcv_rows_after,
        "inserted_ohlcv_rows": result.inserted_ohlcv_rows,
    }


def _now_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _now_epoch() -> float:
    return time.time()
