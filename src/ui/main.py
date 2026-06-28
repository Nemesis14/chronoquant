from __future__ import annotations

import sys
from html import escape
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import utils
from ui import data, trading_runner
from ui.components.charts import PLOTLY_CHART_CONFIG, prediction_price_figure
from ui.components.formatting import GOLD as _GOLD
from ui.components.formatting import GREEN as _GREEN
from ui.components.formatting import GRID as _GRID
from ui.components.formatting import MUTED as _MUTED
from ui.components.formatting import PANEL as _PANEL
from ui.components.formatting import RED as _RED
from ui.components.formatting import TEXT as _TEXT
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
        entry_threshold       = long_strategy.get("entry_cutoff"),
        rearm_threshold       = None,
        exit_threshold        = None,
        short_entry_threshold = short_strategy.get("entry_cutoff"),
        short_rearm_threshold = None,
        short_exit_threshold  = None,
        active_trade          = active_trade,
        focus_hours           = focus_hours,
    )
    st.plotly_chart(fig, config=PLOTLY_CHART_CONFIG, width="stretch")


def _render_strategy_card() -> None:
    artifact = data.load_strategy_artifact()
    if not artifact:
        return

    session_id       = artifact.get("session_id", "—")
    fit_period       = artifact.get("fit_period", {})
    fit_start        = str(fit_period.get("start", ""))[:7] if fit_period.get("start") else "—"
    fit_end          = str(fit_period.get("end",   ""))[:7] if fit_period.get("end")   else "—"
    params           = artifact.get("decision_params", {})
    metrics          = artifact.get("metrics", {})

    entry_cutoff     = params.get("entry_cutoff")
    tp_spec          = params.get("tp_spec")          or "—"
    sl_spec          = params.get("sl_spec")          or "—"
    max_hold_minutes = params.get("max_hold_minutes")
    n_trades         = metrics.get("n_trades")
    win_rate         = metrics.get("win_rate")
    comp_return      = metrics.get("compounded_return_pct")
    avg_hold         = metrics.get("avg_hold_minutes")

    entry_lbl = f"{entry_cutoff * 100:.0f}%" if entry_cutoff     is not None else "—"
    hold_lbl  = f"{int(max_hold_minutes)} min" if max_hold_minutes is not None else "—"
    wr_lbl    = f"{win_rate * 100:.1f}%"       if win_rate        is not None else "—"
    ret_lbl   = f"+{comp_return:.1f}%"         if comp_return     is not None else "—"
    trade_lbl = f"{int(n_trades)}"             if n_trades        is not None else "—"
    avg_lbl   = f"{avg_hold:.1f} min"          if avg_hold        is not None else "—"

    _C   = (f"border:1px solid {_GRID}; border-radius:6px; "
            f"padding:10px 12px; background:{_PANEL}; margin-bottom:10px;")
    _LBL = f"color:{_MUTED}; font-size:11px;"
    _VAL = f"color:{_TEXT}; font-size:12px; font-weight:500;"

    header_text = f"{escape(session_id)} &nbsp;|&nbsp; {fit_start} → {fit_end}"

    long_row = (
        f'<div style="border-bottom:1px solid {_GRID}; padding:5px 0; font-size:12px;'
        f' display:grid; grid-template-columns:52px 58px 1fr; gap:4px; align-items:center;">'
        f'<span style="color:{_GREEN}; font-weight:700;">▲ LONG</span>'
        f'<span style="color:{_TEXT};">entry {entry_lbl}</span>'
        f'<span style="color:{_MUTED}; font-size:11px;">TP: {escape(tp_spec)}</span>'
        f'</div>'
    )
    short_row = (
        f'<div style="padding:5px 0; font-size:12px;'
        f' display:grid; grid-template-columns:52px 58px 1fr; gap:4px; align-items:center;">'
        f'<span style="color:{_RED}; font-weight:700;">▼ SHORT</span>'
        f'<span style="color:{_TEXT};">entry {entry_lbl}</span>'
        f'<span style="color:{_MUTED}; font-size:11px;">SL: {escape(sl_spec)} &nbsp; hold: {hold_lbl}</span>'
        f'</div>'
    )
    metrics_html = (
        f'<div style="border-top:1px solid {_GRID}; margin-top:6px; padding-top:6px;'
        f' display:grid; grid-template-columns:1fr 1fr; gap:3px 8px; font-size:12px;">'
        f'<div><span style="{_LBL}">Trades </span><span style="{_VAL}">{trade_lbl}</span></div>'
        f'<div><span style="{_LBL}">Win </span>'
        f'<span style="color:{_GREEN}; font-size:12px; font-weight:600;">{wr_lbl}</span></div>'
        f'<div><span style="{_LBL}">Return </span>'
        f'<span style="color:{_GREEN}; font-size:12px; font-weight:600;">{ret_lbl}</span></div>'
        f'<div><span style="{_LBL}">Avg hold </span><span style="{_VAL}">{avg_lbl}</span></div>'
        f'</div>'
    )

    st.markdown(
        f'<div style="{_C}">'
        f'<div style="color:{_TEXT}; font-size:12px; font-weight:700; margin-bottom:8px;">'
        f'Aktív Stratégia &nbsp;'
        f'<span style="color:{_MUTED}; font-size:11px; font-weight:400;">{header_text}</span>'
        f'</div>'
        f'{long_row}'
        f'{short_row}'
        f'{metrics_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


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


active_asset_id = utils.load_asset_config(None)["database"]["asset_id"]
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
