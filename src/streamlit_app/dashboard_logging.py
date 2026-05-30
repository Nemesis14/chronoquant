from __future__ import annotations

from collections import deque
import logging
from pathlib import Path

import utils


LOGGER_NAME = "chronoquant.streamlit"
LOG_FILENAME = "streamlit_dashboard.log"


def log_path() -> Path:
    return Path(utils._repo_root()) / "logs" / LOG_FILENAME


def get_dashboard_logger() -> logging.Logger:
    path = log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M",
    )

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    resolved_path = path.resolve()
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler) and Path(handler.baseFilename).resolve() == resolved_path:
            handler.setFormatter(formatter)
            return logger

    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def read_recent_logs(max_lines: int = 200) -> list[str]:
    path = log_path()
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", errors="replace") as f:
        return [line.replace("\x00", "").rstrip() for line in deque(f, maxlen=max_lines)]


def clear_logs() -> None:
    path = log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")
