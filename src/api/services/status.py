"""``/api/status``: data freshness, live models, worker / sync state, DB lock state."""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import UTC, datetime
from typing import Any

from api import deps
from api.services import common, market

logger = logging.getLogger(__name__)

JOBS_DB_REL = "database/_jobs/jobs.sqlite"
HEARTBEAT_ALIVE_S = 15.0


def _beat_epoch(value: Any) -> float | None:
    """Heartbeat as epoch seconds (accepts epoch numbers or ISO strings)."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).timestamp()


def worker_and_sync() -> tuple[dict[str, Any], dict[str, Any]]:
    """Read the worker heartbeat and sync state from the job-queue SQLite (read-only).

    The queue is owned by the worker (blueprint §5.2); a missing or unreadable file means
    "worker down, no sync".  Column names follow the blueprint.
    """
    worker: dict[str, Any] = {"alive": False, "last_heartbeat": None}
    sync: dict[str, Any] = {
        "running": False, "job_id": None, "last_finished_at": None, "last_status": None,
        "auto_sync": False,
    }
    path = common.repo_path(JOBS_DB_REL)
    if not path.is_file():
        return worker, sync
    try:
        conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=1.0)
        conn.row_factory = sqlite3.Row
        try:
            beat = conn.execute("SELECT * FROM heartbeat ORDER BY last_beat DESC LIMIT 1").fetchone()
            if beat is not None:
                epoch = _beat_epoch(beat["last_beat"])
                worker["last_heartbeat"] = (
                    datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    if epoch is not None else None
                )
                worker["alive"] = epoch is not None and (time.time() - epoch) < HEARTBEAT_ALIVE_S
            running = conn.execute(
                "SELECT id FROM jobs WHERE kind IN ('sync_full', 'sync_predictions')"
                " AND status = 'running' ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if running is not None:
                sync["running"], sync["job_id"] = True, running["id"]
            last = conn.execute(
                "SELECT status, finished_at FROM jobs WHERE kind IN ('sync_full', 'sync_predictions')"
                " AND status IN ('succeeded', 'failed') ORDER BY finished_at DESC LIMIT 1"
            ).fetchone()
            if last is not None:
                sync["last_status"] = last["status"]
                sync["last_finished_at"] = common.iso_z(last["finished_at"])
            auto = conn.execute("SELECT value FROM settings WHERE key = 'auto_sync'").fetchone()
            if auto is not None:
                sync["auto_sync"] = str(auto["value"]).lower() in ("1", "true", "yes", "on")
        finally:
            conn.close()
    except sqlite3.Error:
        logger.debug("jobs.sqlite not readable", exc_info=True)
    return worker, sync


def _live_section(conn: Any, key: str) -> dict[str, Any]:
    name = common.live_table(key)
    if not common.table_exists(conn, "live", "main", name):
        return {"rows": 0, "latest_open_time": None}
    fq = f"live.{common.quote(name)}"
    return {
        "rows": int(common.scalar(conn, f"SELECT count(*) FROM {fq}") or 0),
        "latest_open_time": common.iso_z(common.scalar(conn, f"SELECT max(open_time) FROM {fq}")),
    }


def status() -> dict[str, Any]:
    cfg = market.config()
    live: dict[str, Any] | None = None
    models: dict[str, Any] = {"long": {"model_id": None}, "short": {"model_id": None}}
    try:
        with deps.ro_connection() as conn:
            live = {key: _live_section(conn, key) for key in ("ohlcv", "features", "predictions")}
            preds = common.live_table("predictions")
            if common.table_exists(conn, "live", "main", preds):
                row = conn.execute(
                    "SELECT long_model_id, short_model_id"
                    f" FROM live.{common.quote(preds)} ORDER BY open_time DESC LIMIT 1"
                ).fetchone()
                if row:
                    models = {"long": {"model_id": row[0]}, "short": {"model_id": row[1]}}
    except deps.DbBusyError:
        logger.info("status: database busy, returning partial status")
    worker, sync = worker_and_sync()
    return {
        "asset_id": cfg["asset_id"],
        "symbol": cfg["symbol"],
        "interval": cfg["interval"],
        "live": live,
        "models": models,
        "worker": worker,
        "sync": sync,
        "db": {"state": deps.db_state()},
    }
