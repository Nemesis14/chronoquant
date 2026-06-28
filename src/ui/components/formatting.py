# =============================================================================
# Dashboard formatting helpers
# =============================================================================

from __future__ import annotations

# Dark theme palette — shared across dashboard components and charts
BG    = "#0b0e11"
PANEL = "#111418"
GRID  = "#2b3139"
TEXT  = "#eaecef"
MUTED = "#848e9c"
GREEN = "#0ecb81"
RED   = "#f6465d"
GOLD  = "#f0b90b"


def pct(value, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value) * 100:.{digits}f}%"
    except (TypeError, ValueError):
        return "n/a"


def money(value, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def number(value, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)
