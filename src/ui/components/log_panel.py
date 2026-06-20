from __future__ import annotations

import re
from html import escape

import streamlit as st

from ui.components.formatting import _BG, _GOLD, _MUTED, _RED, _TEXT
from ui.dashboard_logging import clear_logs, read_recent_logs

_LOG_HEADER_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}")

_LEVEL_COLORS = {
    "INFO":     _TEXT,
    "WARNING":  _GOLD,
    "ERROR":    _RED,
    "CRITICAL": _RED,
    "DEBUG":    _MUTED,
}


def _log_entries(lines: list[str]) -> list[dict]:
    entries: list[dict] = []
    current: dict | None = None
    for line in lines:
        if _LOG_HEADER_RE.match(line):
            current = {"header": line, "details": []}
            entries.append(current)
        elif current is not None:
            current["details"].append(line)
        elif line:
            entries.append({"header": line, "details": []})
    return entries


def _split_log_header(line: str) -> tuple[str, str, str]:
    parts = [p.strip() for p in line.split(" | ", 2)]
    if len(parts) == 3 and _LOG_HEADER_RE.match(parts[0]):
        return parts[0][:16], parts[1], parts[2]
    return "", "INFO", line


def _terminal_line_html(entry: dict) -> str:
    header  = str(entry["header"])
    details = [str(line) for line in entry.get("details", []) if str(line).strip()]
    ts, level, message = _split_log_header(header)
    color      = _LEVEL_COLORS.get(level.upper(), _TEXT)
    abbr       = level[:4].upper()
    detail_str = escape("\n" + "\n".join(details)) if details else ""
    return (
        f'<div style="padding:1px 0; white-space:pre-wrap; overflow-wrap:anywhere;">'
        f'<span style="color:#4b5563; margin-right:6px;">{escape(ts)}</span>'
        f'<span style="color:{color}; font-weight:600; margin-right:6px;">[{escape(abbr)}]</span>'
        f'<span style="color:{color};">{escape(message)}{detail_str}</span>'
        f'</div>\n'
    )


def _render_log_terminal(lines: list[str]) -> None:
    entries = _log_entries(lines)
    if entries:
        body = "".join(_terminal_line_html(e) for e in entries)
    else:
        body = f'<span style="color:{_MUTED};">No logs yet.</span>'

    st.iframe(
        f"""
        <style>
        body {{ margin:0; padding:0; background:{_BG}; }}
        #term {{
            box-sizing: border-box; width: 100%; height: 100vh;
            overflow-y: auto; padding: 10px 14px;
            background: {_BG}; color: {_TEXT};
            font-family: Consolas, "Courier New", monospace;
            font-size: 13px; line-height: 1.6;
        }}
        </style>
        <div id="term">{body}</div>
        <script>
        var el = document.getElementById('term');
        el.scrollTop = el.scrollHeight;
        </script>
        """,
        width="stretch",
        height=118,
    )


@st.fragment(run_every="2s")
def render_log_panel() -> None:
    st.divider()
    title_col, action_col = st.columns([1.0, 0.15])
    title_col.caption("Log")
    if action_col.button("Clear", width="stretch"):
        clear_logs()
        st.rerun(scope="fragment")
    _render_log_terminal(read_recent_logs(350))
