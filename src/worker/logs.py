# =============================================================================
# Job log files
# =============================================================================
# Purpose:
#  - Per-job log file writer (logs/jobs/<job_id>.log), stdout redirect helper,
#    byte-offset reader used by the API log/SSE endpoints, retention cleanup
# =============================================================================

from __future__ import annotations

import contextlib
import io
import logging
import time
from collections.abc import Iterator
from contextlib import redirect_stdout
from pathlib import Path

import utils

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
JOB_LOGGER_PREFIX = "chronoquant.job"


def jobs_log_dir() -> Path:
    return Path(utils.repo_root()) / "logs" / "jobs"


def job_log_path(job_id: str, base_dir: Path | None = None) -> Path:
    return (base_dir or jobs_log_dir()) / f"{job_id}.log"


class _LoggerWriter(io.StringIO):
    """File-like object turning each written line into a logger.info call."""

    def __init__(self, logger: logging.Logger) -> None:
        super().__init__()
        self._logger = logger
        self._buffer = ""

    def write(self, value: str) -> int:
        self._buffer += value
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._log_line(line)
        return len(value)

    def flush(self) -> None:
        self._log_line(self._buffer)
        self._buffer = ""

    def _log_line(self, line: str) -> None:
        line = line.strip()
        if line:
            self._logger.info(line)


@contextlib.contextmanager
def job_logging(job_id: str, log_path: Path) -> Iterator[logging.Logger]:
    """Capture logger + stdout output of a job into its log file.

    A file handler is attached to the job logger and to the root logger (so
    library loggers are captured too); stdout is redirected into the job logger.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))

    logger = logging.getLogger(f"{JOB_LOGGER_PREFIX}.{job_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)

    root = logging.getLogger()
    root.addHandler(handler)
    writer = _LoggerWriter(logger)
    try:
        with redirect_stdout(writer):
            yield logger
    finally:
        writer.flush()
        root.removeHandler(handler)
        logger.removeHandler(handler)
        handler.close()


def read_log_entries(
    path: Path, offset: int = 0, final: bool = False
) -> list[tuple[str, int]]:
    """Read complete lines from `offset` as (line, end_byte_offset) pairs.

    A trailing partial line is left unread unless `final` is true.
    """
    if not path.exists():
        return []
    with path.open("rb") as f:
        f.seek(offset)
        data = f.read()
    if not data:
        return []
    if not final:
        cut = data.rfind(b"\n")
        if cut < 0:
            return []
        data = data[: cut + 1]
    entries: list[tuple[str, int]] = []
    pos = offset
    for raw in data.splitlines(keepends=True):
        pos += len(raw)
        text = raw.decode("utf-8", errors="replace").replace(chr(0), "").rstrip("\r\n")
        entries.append((text, pos))
    return entries


def read_log(path: Path, offset: int = 0, final: bool = False) -> tuple[list[str], int]:
    """Read complete lines from `offset`; return (lines, new_offset)."""
    entries = read_log_entries(path, offset, final)
    if not entries:
        return [], offset
    return [line for line, _ in entries], entries[-1][1]


def log_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def cleanup_logs(
    keep_ids: set[str],
    base_dir: Path | None = None,
    max_age_days: int = 30,
) -> int:
    """Delete log files whose job is no longer kept or is older than max_age_days."""
    directory = base_dir or jobs_log_dir()
    if not directory.exists():
        return 0
    cutoff = time.time() - max_age_days * 86400
    removed = 0
    for path in directory.glob("*.log"):
        try:
            if path.stem not in keep_ids or path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError:
            continue
    return removed
