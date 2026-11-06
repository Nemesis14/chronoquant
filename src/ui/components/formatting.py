# =============================================================================
# Dashboard formatting helpers
# =============================================================================

from __future__ import annotations

import os

# Page scale. Windows display scaling (e.g. 120%) makes Chrome's 100% zoom look "enlarged"
# (CSS viewport 1600 px on a 1920 px screen); the user compensated with 80% browser zoom.
# CSS `zoom` is NOT used: it breaks Plotly's size measurement. Instead the root font-size
# (all Streamlit rem sizes), the chart px sizes and the inline-px HTML cards are scaled.
# Override with CQ_UI_ZOOM (1 disables).
UI_ZOOM = float(os.environ.get("CQ_UI_ZOOM", "0.8"))


def scaled(px: float) -> int:
    """A design px value scaled by UI_ZOOM (for sizes Plotly / iframes take in raw px)."""
    return max(1, round(px * UI_ZOOM))

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
