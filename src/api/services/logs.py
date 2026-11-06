"""Tail of the API / worker log files (``logs/<source>*.log``)."""

from __future__ import annotations

import re
from collections import deque
from pathlib import Path
from typing import Any

from api.services import common

SOURCES = ("api", "worker")
_APP_FMT = re.compile(
    r"^(?P<ts>\d{4}-\d\d-\d\d[ T][\d:,.]+)\s*\|\s*(?P<level>[A-Z]+)\s*\|\s*(?:(?P<logger>[\w.\-]+)\s*\|\s*)?(?P<msg>.*)$"
)
_UVICORN_FMT = re.compile(r"^(?P<level>[A-Z]+):\s+(?P<msg>.*)$")


def log_file(source: str) -> Path | None:
    """Newest ``logs/<source>*.log`` file, or None."""
    files = sorted(common.repo_path("logs").glob(f"{source}*.log"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def parse_line(line: str) -> dict[str, Any]:
    m = _APP_FMT.match(line)
    if m:
        return {"ts": m["ts"], "level": m["level"], "logger": m["logger"], "msg": m["msg"]}
    m = _UVICORN_FMT.match(line)
    if m:
        return {"ts": None, "level": m["level"], "logger": None, "msg": m["msg"]}
    return {"ts": None, "level": None, "logger": None, "msg": line}


def tail(source: str, lines: int) -> list[dict[str, Any]]:
    path = log_file(source)
    if path is None:
        return []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        raw = [ln.replace("\x00", "").rstrip() for ln in deque(f, maxlen=lines)]
    return [parse_line(ln) for ln in raw if ln]
