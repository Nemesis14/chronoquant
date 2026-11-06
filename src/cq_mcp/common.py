"""Shared helpers for the chronoquant MCP tools: output limits and JSON dumping."""

from __future__ import annotations

import json
from typing import Any

import api.services.common as svc_common

MAX_ROWS = 1000
MAX_CHARS = 50_000


def clamp(value: int, low: int, high: int) -> int:
    """Clamp ``value`` to ``[low, high]``."""
    return max(low, min(high, int(value)))


def dump(obj: Any, max_chars: int = MAX_CHARS) -> str:
    """JSON text of ``obj`` (NaN -> null, datetimes -> ISO-Z); cut at ``max_chars`` with a marker."""
    text = json.dumps(svc_common.json_safe(obj), ensure_ascii=False, default=str)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"... [TRUNCATED at {max_chars} chars; narrow the query / lower limit]"
