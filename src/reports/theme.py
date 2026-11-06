"""Report palette (originally the former Quarto plot_utils colours) and a small Plotly layout helper.

The figure JSON written by the builder never embeds a template; the frontend merges its own
light/dark theme.  :func:`base_layout` only sets neutral, theme-independent layout defaults.
"""

from __future__ import annotations

from typing import Any

ACCENT = "#343a40"
LONG = "#198754"
SHORT = "#dc3545"
NEUTRAL = "#6c757d"
BLUE = "#0d6efd"
ORANGE = "#fd7e14"
AMBER = "#f0b90b"

SEQUENCE = [ACCENT, SHORT, LONG, NEUTRAL, BLUE, ORANGE]


def base_layout(
    *,
    x_title: str | None = None,
    y_title: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Return neutral layout kwargs (``fig.update_layout(**base_layout(...))``)."""
    layout: dict[str, Any] = {
        "margin": {"l": 56, "r": 16, "t": 24, "b": 48},
        "legend": {"orientation": "h", "y": 1.08, "x": 0},
        "hovermode": "x unified",
    }
    if x_title:
        layout["xaxis"] = {"title": {"text": x_title}}
    if y_title:
        layout["yaxis"] = {"title": {"text": y_title}}
    layout.update(extra)
    return layout
