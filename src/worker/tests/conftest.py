from __future__ import annotations

from pathlib import Path

import pytest

from worker.queue import JobQueue


@pytest.fixture
def queue(tmp_path: Path) -> JobQueue:
    return JobQueue(tmp_path / "jobs.sqlite", log_dir=tmp_path / "logs")
