# =============================================================================
# Worker runner
# =============================================================================
# Purpose:
#  - Main loop: claim one job at a time (FIFO), run its handler with the job log
#    attached, store result/error
#  - Side thread: heartbeat every 5 s + auto-sync scheduler tick every 1 s
# =============================================================================

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from worker import scheduler
from worker.handlers import Handler
from worker.logs import cleanup_logs, job_log_path, job_logging
from worker.queue import JobQueue

logger = logging.getLogger("chronoquant.worker")

POLL_SECONDS = 1.0
HEARTBEAT_SECONDS = 5.0


class WorkerRunner:
    def __init__(
        self,
        queue: JobQueue,
        handlers: dict[str, Handler],
        poll_seconds: float = POLL_SECONDS,
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
    ) -> None:
        self.queue = queue
        self.handlers = handlers
        self.poll_seconds = poll_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.stop_event = threading.Event()
        self._current_job_id: str | None = None

    # ------------------------------------------------------------------ setup
    def startup(self) -> None:
        orphans = self.queue.recover_orphans()
        if orphans:
            logger.warning("Marked %d orphaned running job(s) as failed", orphans)
        removed = self.queue.cleanup()
        cleanup_logs(self.queue.kept_ids(), self.queue.log_dir)
        if removed:
            logger.info("Retention cleanup removed %d job(s)", len(removed))
        self.queue.register_worker()

    # --------------------------------------------------------------- one job
    def run_one(self) -> bool:
        """Claim and execute one queued job. Return False if nothing was queued."""
        job = self.queue.claim_next()
        if job is None:
            return False
        self._current_job_id = job["id"]
        self.queue.beat(job["id"])
        try:
            self._execute(job)
        finally:
            self._current_job_id = None
            self.queue.beat(None)
        return True

    def _execute(self, job: dict[str, Any]) -> None:
        job_id, kind = job["id"], job["kind"]
        handler = self.handlers.get(kind)
        log_path = self.queue.log_path(job_id) or job_log_path(job_id, self.queue.log_dir)
        with job_logging(job_id, log_path) as job_logger:
            job_logger.info("Job %s (%s) started", job_id, kind)
            if handler is None:
                job_logger.error("No handler for kind %s", kind)
                self.queue.fail(job_id, f"no handler for kind {kind}")
                return
            try:
                result = handler(job["params"], job_logger)
            except Exception as exc:
                job_logger.exception("Job failed")
                self.queue.fail(job_id, f"{type(exc).__name__}: {exc}")
                return
            job_logger.info("Job %s finished", job_id)
        self.queue.finish(job_id, result)

    # ------------------------------------------------------------- side thread
    def _side_loop(self) -> None:
        last_beat = float("-inf")
        while not self.stop_event.is_set():
            try:
                now = time.monotonic()
                if now - last_beat >= self.heartbeat_seconds:
                    self.queue.beat(self._current_job_id)
                    last_beat = now
                scheduler.tick(self.queue)
            except Exception:
                logger.exception("Heartbeat/scheduler tick failed")
            self.stop_event.wait(1.0)

    # -------------------------------------------------------------------- loop
    def run_forever(self) -> None:
        self.startup()
        side = threading.Thread(target=self._side_loop, name="worker-side", daemon=True)
        side.start()
        logger.info("Worker started")
        try:
            while not self.stop_event.is_set():
                try:
                    ran = self.run_one()
                except Exception:
                    logger.exception("Worker loop error")
                    ran = False
                if not ran:
                    self.stop_event.wait(self.poll_seconds)
        finally:
            self.stop_event.set()
            logger.info("Worker stopped")
