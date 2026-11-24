# =============================================================================
# SQLite job store
# =============================================================================
# Purpose:
#  - Multi-process safe job queue (WAL): enqueue with dedup, FIFO claim,
#    queued-only cancel, results, settings (auto-sync) and worker heartbeat
#  - One short-lived connection per operation (API and worker share the file)
# =============================================================================

from __future__ import annotations

import json
import os
import secrets
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import utils
from worker.logs import job_log_path, log_size

JOB_KINDS: tuple[str, ...] = ("sync_full", "sync_predictions", "sentiment_refresh", "sync_scout")
#: Kinds that work on one asset: params carry ``asset_id`` and the dedup key includes it.
ASSET_KINDS: tuple[str, ...] = ("sync_full", "sync_predictions", "sync_scout")
ACTIVE_STATUSES: tuple[str, ...] = ("queued", "running")
TERMINAL_STATUSES: tuple[str, ...] = ("succeeded", "failed", "canceled")
HEARTBEAT_ALIVE_SECONDS = 15
RETENTION_JOBS = 200
RETENTION_DAYS = 30

CancelOutcome = Literal["canceled", "running", "finished", "not_found"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    status      TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT,
    error       TEXT,
    created_at  TEXT NOT NULL,
    started_at  TEXT,
    finished_at TEXT,
    log_path    TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, created_at);
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS heartbeat (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    pid            INTEGER,
    started_at     TEXT,
    last_beat      TEXT,
    current_job_id TEXT
);
"""


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def default_db_path() -> Path:
    return Path(utils.repo_root()) / "database" / "_jobs" / "jobs.sqlite"


def job_asset_id(params: dict[str, Any] | None) -> str:
    """Asset id of an asset-kind job's params (missing = default asset, as before multi-asset)."""
    return utils.resolve_asset_id((params or {}).get("asset_id"))


def new_job_id(now: datetime | None = None) -> str:
    return f"{(now or utc_now()).strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(3)}"


class JobQueue:
    def __init__(self, db_path: Path | str | None = None, log_dir: Path | None = None) -> None:
        self.db_path = Path(db_path) if db_path else default_db_path()
        self.log_dir = log_dir
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    # ------------------------------------------------------------------ infra
    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> dict[str, Any]:
        log_path = row["log_path"]
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "params": json.loads(row["params_json"] or "{}"),
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "error": row["error"],
            "log_size": log_size(Path(log_path)) if log_path else 0,
        }

    # ------------------------------------------------------------------- jobs
    def enqueue(self, kind: str, params: dict[str, Any] | None = None) -> tuple[dict, bool]:
        """Insert a queued job; return (job, deduplicated).

        If a queued/running job of the same kind (and, for asset kinds, the same asset) exists,
        it is returned instead.  Asset kinds get ``params["asset_id"]`` normalised (default asset
        when omitted), so the dedup key is always ``(kind, asset_id)``.
        """
        if kind not in JOB_KINDS:
            raise ValueError(f"unknown job kind: {kind}")
        params = dict(params or {})
        asset_id = job_asset_id(params) if kind in ASSET_KINDS else None
        if asset_id is not None:
            params["asset_id"] = asset_id
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                candidates = conn.execute(
                    "SELECT * FROM jobs WHERE kind = ? AND status IN ('queued', 'running') "
                    "ORDER BY rowid",
                    (kind,),
                ).fetchall()
                existing = next(
                    (
                        r for r in candidates
                        if asset_id is None
                        or job_asset_id(json.loads(r["params_json"] or "{}")) == asset_id
                    ),
                    None,
                )
                if existing is not None:
                    conn.execute("COMMIT")
                    return self._row_to_job(existing), True
                now = utc_now()
                job_id = new_job_id(now)
                conn.execute(
                    "INSERT INTO jobs (id, kind, status, params_json, created_at, log_path) "
                    "VALUES (?, ?, 'queued', ?, ?, ?)",
                    (
                        job_id,
                        kind,
                        json.dumps(params),
                        to_iso(now),
                        str(job_log_path(job_id, self.log_dir)),
                    ),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._row_to_job(row), False

    def get(self, job_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._row_to_job(row) if row else None

    def log_path(self, job_id: str) -> Path | None:
        with self._connect() as conn:
            row = conn.execute("SELECT log_path FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return Path(row["log_path"]) if row and row["log_path"] else None

    def list_jobs(
        self, status: str | None = None, kind: str | None = None, limit: int = 50
    ) -> list[dict]:
        clauses: list[str] = []
        args: list[Any] = []
        if status:
            clauses.append("status = ?")
            args.append(status)
        if kind:
            clauses.append("kind = ?")
            args.append(kind)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM jobs {where} ORDER BY rowid DESC LIMIT ?",
                (*args, limit),
            ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def cancel(self, job_id: str) -> CancelOutcome:
        """Cancel a queued job. Running jobs cannot be canceled."""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE jobs SET status = 'canceled', finished_at = ? "
                "WHERE id = ? AND status = 'queued'",
                (to_iso(utc_now()), job_id),
            )
            if cur.rowcount:
                return "canceled"
            row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return "not_found"
        return "running" if row["status"] == "running" else "finished"

    def claim_next(self) -> dict | None:
        """Atomically move the oldest queued job to running (only if none runs)."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                running = conn.execute(
                    "SELECT 1 FROM jobs WHERE status = 'running' LIMIT 1"
                ).fetchone()
                row = None
                if running is None:
                    row = conn.execute(
                        "SELECT * FROM jobs WHERE status = 'queued' "
                        "ORDER BY rowid LIMIT 1"
                    ).fetchone()
                    if row is not None:
                        conn.execute(
                            "UPDATE jobs SET status = 'running', started_at = ? WHERE id = ?",
                            (to_iso(utc_now()), row["id"]),
                        )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            if row is None:
                return None
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
        return self._row_to_job(row)

    def finish(self, job_id: str, result: dict[str, Any] | None) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET status = 'succeeded', result_json = ?, finished_at = ? "
                "WHERE id = ?",
                (json.dumps(result) if result is not None else None, to_iso(utc_now()), job_id),
            )

    def fail(self, job_id: str, error: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET status = 'failed', error = ?, finished_at = ? WHERE id = ?",
                (error, to_iso(utc_now()), job_id),
            )

    def recover_orphans(self) -> int:
        """Mark leftover running jobs (previous worker died) as failed."""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE jobs SET status = 'failed', error = 'worker_restart', finished_at = ? "
                "WHERE status = 'running'",
                (to_iso(utc_now()),),
            )
            return cur.rowcount

    def cleanup(self) -> list[str]:
        """Delete finished jobs beyond retention (200 newest / 30 days); return removed ids."""
        cutoff = to_iso(utc_now() - timedelta(days=RETENTION_DAYS))
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, created_at FROM jobs "
                "WHERE status IN ('succeeded', 'failed', 'canceled') "
                "ORDER BY rowid DESC"
            ).fetchall()
            drop = [
                r["id"]
                for i, r in enumerate(rows)
                if i >= RETENTION_JOBS or r["created_at"] < cutoff
            ]
            conn.executemany("DELETE FROM jobs WHERE id = ?", [(i,) for i in drop])
        return drop

    def kept_ids(self) -> set[str]:
        with self._connect() as conn:
            return {r["id"] for r in conn.execute("SELECT id FROM jobs").fetchall()}

    # --------------------------------------------------------------- settings
    def get_setting(self, key: str, default: str | None = None) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def auto_sync_enabled(self) -> bool:
        return self.get_setting("auto_sync", "0") == "1"

    def set_auto_sync(self, enabled: bool) -> None:
        self.set_setting("auto_sync", "1" if enabled else "0")

    # -------------------------------------------------------------- heartbeat
    def register_worker(self, pid: int | None = None) -> None:
        now = to_iso(utc_now())
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO heartbeat (id, pid, started_at, last_beat, current_job_id) "
                "VALUES (1, ?, ?, ?, NULL) ON CONFLICT(id) DO UPDATE SET "
                "pid = excluded.pid, started_at = excluded.started_at, "
                "last_beat = excluded.last_beat, current_job_id = NULL",
                (pid or os.getpid(), now, now),
            )

    def beat(self, current_job_id: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE heartbeat SET last_beat = ?, current_job_id = ? WHERE id = 1",
                (to_iso(utc_now()), current_job_id),
            )

    def worker_info(self, now: datetime | None = None) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM heartbeat WHERE id = 1").fetchone()
        if row is None:
            return {
                "alive": False,
                "pid": None,
                "started_at": None,
                "last_heartbeat": None,
                "current_job_id": None,
            }
        last = parse_iso(row["last_beat"])
        age = ((now or utc_now()) - last).total_seconds() if last else None
        return {
            "alive": age is not None and age < HEARTBEAT_ALIVE_SECONDS,
            "pid": row["pid"],
            "started_at": row["started_at"],
            "last_heartbeat": row["last_beat"],
            "current_job_id": row["current_job_id"],
        }
