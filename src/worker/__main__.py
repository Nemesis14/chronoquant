"""Entry point: `python -m worker` (run from repo root with src on the path)."""

from __future__ import annotations

import logging
import signal
from logging.handlers import RotatingFileHandler
from pathlib import Path

import utils
from worker.handlers import default_handlers
from worker.queue import JobQueue
from worker.runner import WorkerRunner


def _configure_logging() -> None:
    log_dir = Path(utils.repo_root()) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_handler = RotatingFileHandler(
        log_dir / "worker.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(stream)


def main() -> None:
    _configure_logging()
    runner = WorkerRunner(JobQueue(), default_handlers())

    def _stop(*_: object) -> None:
        runner.stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    runner.run_forever()


if __name__ == "__main__":
    main()
