from __future__ import annotations

from html import escape
import re
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streamlit_app import data
from streamlit_app.components.charts import PLOTLY_CHART_CONFIG, prediction_price_figure
from streamlit_app.components.formatting import number
from streamlit_app.dashboard_logging import clear_logs, get_dashboard_logger, read_recent_logs
from streamlit_app.sync_runner import (
    auto_sync_due_seconds,
    disable_auto_sync,
    enable_auto_sync,
    ensure_sync_state,
    is_sync_running,
    start_sync,
)


st.set_page_config(
    page_title="ChronoQuant",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container { padding-top: 4.25rem; padding-bottom: 1rem; max-width: 100%; }
    [data-testid="stSidebar"] > div:first-child { padding-top: 4.25rem; }
    [data-testid="stMetricValue"] { font-size: 1.05rem; }
    textarea { font-family: Consolas, monospace; font-size: 0.78rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

if "dashboard_started_logged" not in st.session_state:
    get_dashboard_logger().info("Dashboard started")
    st.session_state.dashboard_started_logged = True

sync_state = ensure_sync_state(st.session_state)
cfg = data.load_dashboard_config()
CHART_LOOKBACK_HOURS = 4

@st.fragment(run_every="2s")
def render_sync_controls() -> None:
    state = ensure_sync_state(st.session_state)
    running = is_sync_running(state)
    logger = get_dashboard_logger()

    if state.get("auto_sync_enabled") and not running and auto_sync_due_seconds(state) == 0:
        if start_sync(state):
            logger.info("Auto sync requested from live polling")
        running = is_sync_running(state)

    st.divider()
    started = state.get("started_at") or "n/a"
    finished = state.get("finished_at") or "n/a"
    auto_enabled = bool(state.get("auto_sync_enabled"))
    due_seconds = auto_sync_due_seconds(state)

    if running:
        mode = "Live sync running" if auto_enabled else "Sync running"
        st.info(f"{mode}\n\nStarted: {started}")
    elif state.get("error"):
        st.error(f"Sync failed\n\n{state['error']}")
        if auto_enabled and due_seconds is not None:
            st.caption(f"Live retry in {due_seconds}s")
    elif state.get("result"):
        result = state["result"]
        st.success(f"Sync complete\n\nNew OHLCV rows: {result['inserted_ohlcv_rows']}")
        st.caption(f"Finished: {finished}")
        if auto_enabled and due_seconds is not None:
            st.caption(f"Next live sync in {due_seconds}s")
    elif auto_enabled and due_seconds is not None:
        st.info(f"Live sync enabled\n\nNext sync in {due_seconds}s")
    else:
        st.caption("Sync idle")

    if st.button("Sync", type="primary", width="stretch", disabled=running):
        enable_auto_sync(state)
        started_ok = start_sync(state)
        if started_ok:
            logger.info("Sync requested from UI; live polling enabled")
        st.rerun(scope="app")

    if auto_enabled and st.button("Stop live sync", width="stretch"):
        disable_auto_sync(state)
        logger.info("Live polling disabled from UI")
        st.rerun(scope="app")

    finished_at = state.get("finished_at")
    if finished_at and st.session_state.get("last_chart_refresh_sync") != finished_at:
        st.session_state.last_chart_refresh_sync = finished_at
        st.rerun(scope="app")


with st.sidebar:
    st.title("Menu")
    st.metric("Chart window", f"{CHART_LOOKBACK_HOURS}h")
    render_sync_controls()

sync_running = is_sync_running(sync_state)
if sync_running:
    latest = st.session_state.get("dashboard_latest")
    df = st.session_state.get("dashboard_prediction_history", pd.DataFrame())
else:
    try:
        latest = data.latest_prediction()
        df = data.prediction_history(lookback_hours=CHART_LOOKBACK_HOURS)
        st.session_state.dashboard_latest = latest
        st.session_state.dashboard_prediction_history = df
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc).lower():
            raise
        get_dashboard_logger().warning("Dashboard DB read skipped because database is locked")
        latest = st.session_state.get("dashboard_latest")
        df = st.session_state.get("dashboard_prediction_history", pd.DataFrame())

st.title("ChronoQuant")


_LOG_HEADER_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}")
_DB_KEYWORDS = (
    "sqlite",
    "database",
    " db",
    "table",
    "ohlcv",
    "feature",
    "prediction",
    "signal",
    "journal",
    "inserted",
    "synced",
    "rows",
)


def _render_log_html(lines: list[str]) -> None:
    entries = _log_entries(lines)
    body = "\n".join(_render_log_entry(entry) for entry in reversed(entries)) if entries else _empty_log_html()
    st.iframe(
        f"""
        <style>
        #cq-log-box {{
            box-sizing: border-box;
            height: 360px;
            width: 100%;
            overflow-y: auto;
            padding: 10px;
            border: 1px solid #cbd5e1;
            border-radius: 6px;
            background: #f8fafc;
            color: #0f172a;
            font-family: Consolas, "Courier New", monospace;
            font-size: 12px;
            line-height: 1.35;
        }}
        .log-row {{
            display: grid;
            grid-template-columns: 118px 76px 54px minmax(0, 1fr);
            gap: 8px;
            align-items: start;
            padding: 7px 9px;
            margin-bottom: 6px;
            border: 1px solid #e2e8f0;
            border-left: 4px solid #64748b;
            border-radius: 6px;
            background: #ffffff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05);
        }}
        .log-row.info {{ border-left-color: #2563eb; }}
        .log-row.warning {{ border-left-color: #f59e0b; background: #fffbeb; }}
        .log-row.error, .log-row.critical {{ border-left-color: #dc2626; background: #fef2f2; }}
        .log-row.debug {{ border-left-color: #64748b; }}
        .log-row.sql {{ background: #eef6ff; border-color: #bfdbfe; }}
        .time {{ color: #475569; white-space: nowrap; }}
        .level {{
            display: inline-block;
            min-width: 52px;
            padding: 2px 7px;
            border-radius: 999px;
            text-align: center;
            color: #ffffff;
            font-weight: 700;
            font-size: 11px;
        }}
        .level.info {{ background: #2563eb; }}
        .level.warning {{ background: #d97706; }}
        .level.error, .level.critical {{ background: #dc2626; }}
        .level.debug {{ background: #64748b; }}
        .tag {{
            display: inline-block;
            min-width: 28px;
            padding: 2px 7px;
            border-radius: 999px;
            text-align: center;
            background: #dbeafe;
            color: #1d4ed8;
            font-weight: 700;
            font-size: 11px;
        }}
        .tag.empty {{ visibility: hidden; }}
        .message {{
            min-width: 0;
            white-space: pre-wrap;
            overflow-wrap: anywhere;
        }}
        .details {{
            grid-column: 4;
            margin-top: 5px;
            padding: 7px 9px;
            border-radius: 5px;
            background: rgba(15, 23, 42, 0.06);
            color: #334155;
            white-space: pre-wrap;
            overflow-wrap: anywhere;
        }}
        .empty-log {{
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #64748b;
        }}
        </style>
        <div id="cq-log-box">{body}</div>
        """,
        width="stretch",
        height=372,
    )


def _log_entries(lines: list[str]) -> list[dict[str, list[str] | str]]:
    entries: list[dict[str, list[str] | str]] = []
    current: dict[str, list[str] | str] | None = None

    for line in lines:
        if _LOG_HEADER_RE.match(line):
            current = {"header": line, "details": []}
            entries.append(current)
        elif current is not None:
            current["details"].append(line)
        elif line:
            entries.append({"header": line, "details": []})

    return entries


def _render_log_entry(entry: dict[str, list[str] | str]) -> str:
    header = str(entry["header"])
    details = [str(line) for line in entry.get("details", []) if str(line).strip()]
    timestamp, level, message = _split_log_header(header)
    level_class = level.lower()
    detail_text = "\n".join(details)
    is_db = _is_db_log(message, detail_text)
    row_class = f"log-row {level_class} {'sql' if is_db else ''}".strip()
    tag_class = "tag" if is_db else "tag empty"
    tag = "DB" if is_db else "-"
    details_html = f'<div class="details">{escape(detail_text)}</div>' if detail_text else ""

    return f"""
        <div class="{row_class}">
            <div class="time">{escape(timestamp)}</div>
            <div><span class="level {level_class}">{escape(level)}</span></div>
            <div><span class="{tag_class}">{tag}</span></div>
            <div class="message">{escape(message)}</div>
            {details_html}
        </div>
    """


def _split_log_header(line: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in line.split(" | ", 2)]
    if len(parts) == 3 and _LOG_HEADER_RE.match(parts[0]):
        return parts[0][:16], parts[1], parts[2]
    return "", "INFO", line


def _is_db_log(message: str, details: str = "") -> bool:
    text = f"{message} {details}".lower()
    return any(keyword in text for keyword in _DB_KEYWORDS)


def _empty_log_html() -> str:
    return '<div class="empty-log">No logs yet.</div>'


metric_cols = st.columns(4)
metric_cols[0].metric("Symbol", cfg.get("symbol") or "n/a")
metric_cols[1].metric("Window", f"{CHART_LOOKBACK_HOURS}h")
metric_cols[2].metric("Open time", str(latest.get("open_time")) if latest else "n/a")
metric_cols[3].metric("Close", number(latest.get("close"), 2) if latest else "n/a")

fig = prediction_price_figure(df)
st.plotly_chart(fig, config=PLOTLY_CHART_CONFIG, width="stretch")


@st.fragment(run_every="2s")
def render_log_panel() -> None:
    st.divider()
    title_col, action_col = st.columns([1.0, 0.16])
    title_col.subheader("Log")
    if action_col.button("Clear log", width="stretch"):
        clear_logs()
        st.rerun(scope="fragment")
    _render_log_html(read_recent_logs(350))


render_log_panel()
