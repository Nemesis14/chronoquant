from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ui import data, trading_runner
from ui.components.charts import PLOTLY_CHART_CONFIG, prediction_price_figure
from ui.components.formatting import _GOLD, _GRID, _MUTED, _RED, _TEXT
from ui.components.log_panel import render_log_panel
from ui.components.trade_panel import render_trade_panel
from ui.dashboard_logging import get_dashboard_logger
from ui.sync_runner import (
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
    header[data-testid="stHeader"] { display: none; }
    html, body { font-size: 15px; }
    .stApp, .block-container { font-size: 15px; }
    p, span, div, label, .stCaption { font-size: 1rem; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; max-width: 100%; }
    [data-testid="stSidebar"] > div:first-child { padding-top: 1.5rem; }
    [data-testid="stPlotlyChart"] { border-radius: 6px; overflow: hidden; }
    [data-testid="stSidebar"] label, [data-testid="stSidebar"] span { font-size: 0.95rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

if "active_asset_id" not in st.session_state:
    st.session_state["active_asset_id"] = "solusdt"

if "dashboard_started_logged" not in st.session_state:
    _logger = get_dashboard_logger()
    _logger.info("Dashboard started")
    st.session_state.dashboard_started_logged = True

    _auto_state = ensure_sync_state(st.session_state, "solusdt")
    enable_auto_sync(_auto_state)
    if start_sync(_auto_state, "solusdt"):
        _logger.info("Auto-start sync (asset=solusdt)")


def _render_sync_controls(asset_id: str | None) -> None:
    state   = ensure_sync_state(st.session_state, asset_id)
    running = is_sync_running(state, asset_id)
    logger  = get_dashboard_logger()
    btn_key = asset_id or "sol"

    if state.get("auto_sync_enabled") and not running and auto_sync_due_seconds(state, asset_id) == 0:
        if start_sync(state, asset_id):
            logger.info("Auto sync requested from live polling")
        running = is_sync_running(state, asset_id)

    started      = state.get("started_at") or "n/a"
    finished     = state.get("finished_at") or "n/a"
    auto_enabled = bool(state.get("auto_sync_enabled"))
    due_seconds  = auto_sync_due_seconds(state, asset_id)

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

    if st.button("Sync", key=f"sync_{btn_key}", type="primary", width="stretch", disabled=running):
        enable_auto_sync(state)
        if start_sync(state, asset_id):
            logger.info("Sync requested from UI; live polling enabled")
        st.rerun(scope="app")

    if auto_enabled and st.button("Stop live sync", key=f"stop_{btn_key}", width="stretch"):
        disable_auto_sync(state)
        logger.info("Live polling disabled from UI")
        st.rerun(scope="app")

    finished_at = state.get("finished_at")
    cache_key = f"last_chart_refresh_{btn_key}"
    if finished_at and st.session_state.get(cache_key) != finished_at:
        st.session_state[cache_key] = finished_at
        st.rerun(scope="app")


@st.fragment(run_every="2s")
def _sync_panel_sol() -> None:
    _render_sync_controls(asset_id="solusdt")


_WINDOW_OPTIONS = {"24h": 24, "8h": 8, "4h": 4}
_WINDOW_DEFAULT = "24h"


def render_asset_chart(asset_id: str | None) -> None:
    long_strategy, short_strategy = data.load_long_short_strategies(asset_id=asset_id)

    sync_state   = ensure_sync_state(st.session_state, asset_id)
    sync_running = is_sync_running(sync_state, asset_id)

    win_key   = f"chart_window_{asset_id or 'default'}"
    win_label = st.radio(
        "Időtáv",
        options=list(_WINDOW_OPTIONS.keys()),
        index=list(_WINDOW_OPTIONS.keys()).index(_WINDOW_DEFAULT),
        horizontal=True,
        key=win_key,
        label_visibility="collapsed",
    )
    focus_hours    = _WINDOW_OPTIONS[win_label]
    lookback_hours = max(focus_hours, focus_hours + 2)

    cache_hist = f"chart_history_{asset_id or 'default'}_{win_label}"
    cache_pos  = f"chart_position_{asset_id or 'default'}"

    if sync_running:
        df       = st.session_state.get(cache_hist, pd.DataFrame())
        position = st.session_state.get(cache_pos)
    else:
        try:
            df       = data.prediction_history(lookback_hours=lookback_hours, asset_id=asset_id)
            position = data.active_position(asset_id=asset_id)
            st.session_state[cache_hist] = df
            st.session_state[cache_pos]  = position
        except duckdb.IOException as exc:
            if "locked" not in str(exc).lower():
                raise
            get_dashboard_logger().warning("Chart DB read skipped — database locked")
            df       = st.session_state.get(cache_hist, pd.DataFrame())
            position = st.session_state.get(cache_pos)

    active_trade = None
    if position:
        active_trade = {
            "entry_price": position.get("entry_price") or position.get("open_price"),
            "stop_loss":   position.get("stop_loss") or position.get("sl_price") or position.get("sl"),
            "take_profit": position.get("take_profit") or position.get("tp_price") or position.get("tp"),
        }

    fig = prediction_price_figure(
        df,
        entry_threshold       = long_strategy.get("entry_pct"),
        rearm_threshold       = long_strategy.get("rearm_pct"),
        exit_threshold        = None,
        short_entry_threshold = short_strategy.get("entry_pct"),
        short_rearm_threshold = short_strategy.get("rearm_pct"),
        short_exit_threshold  = None,
        active_trade          = active_trade,
        focus_hours           = focus_hours,
    )
    st.plotly_chart(fig, config=PLOTLY_CHART_CONFIG, width="stretch")


def _render_trading_controls() -> None:
    st.subheader("Live Trading")

    running      = trading_runner.is_trading_running()
    current_mode = trading_runner.get_trading_mode()

    if running:
        mode_label = current_mode or "dry_run"
        color = _GOLD if mode_label == "dry_run" else _RED
        st.markdown(
            f'<div style="color:{color}; font-size:13px; font-weight:600; margin-bottom:8px;">'
            f'● {mode_label.upper()} fut</div>',
            unsafe_allow_html=True,
        )
        if st.button("⏹ Leállítás", key="trading_stop", width="stretch"):
            trading_runner.stop_trading()
            get_dashboard_logger().info("Trading service stop requested from UI")
            st.rerun(scope="app")
    else:
        mode = st.selectbox(
            "Mód", ["dry_run", "live"], key="trading_mode_select",
            help="dry_run: nincs valós order | live: Binance Futures",
        )
        if st.button("▶ Kereskedés indítása", key="trading_start",
                     type="primary", width="stretch"):
            if mode == "live":
                st.warning("⚠ Live mód valós ordereket küld Binance Futures-ra!")
            ok = trading_runner.start_trading(mode=mode)
            if ok:
                get_dashboard_logger().info("Trading service started (mode=%s)", mode)
            else:
                err = trading_runner.get_last_error() or "ismeretlen hiba"
                get_dashboard_logger().error("Trading service failed to start: %s", err)
            st.rerun(scope="app")


with st.sidebar:
    st.title("ChronoQuant")
    st.divider()
    _sync_panel_sol()
    st.divider()
    _render_trading_controls()


active_asset_id = "solusdt"
asset_label     = "SOL / 1m"

st.markdown(
    f'<div style="padding:6px 0 12px 0; border-bottom:1px solid {_GRID}; margin-bottom:12px;">'
    f'<span style="color:{_TEXT}; font-size:20px; font-weight:700; letter-spacing:0.5px;">ChronoQuant</span>'
    f'<span style="color:{_MUTED}; font-size:14px; margin-left:14px;">{asset_label}</span>'
    f'</div>',
    unsafe_allow_html=True,
)

col_chart, col_trade = st.columns([3, 1])

with col_chart:
    render_asset_chart(active_asset_id)
    render_log_panel()

with col_trade:
    render_trade_panel(active_asset_id)
