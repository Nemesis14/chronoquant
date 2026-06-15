# =============================================================================
# ChronoQuant Streamlit dashboard
# =============================================================================

from __future__ import annotations

import re
import sys
from html import escape
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ui import binance_data, data, trading_runner
from ui.components.charts import (
    PLOTLY_CHART_CONFIG,
    prediction_price_figure,
)
from ui.dashboard_logging import clear_logs, get_dashboard_logger, read_recent_logs
from ui.sync_runner import (
    auto_sync_due_seconds,
    disable_auto_sync,
    enable_auto_sync,
    ensure_sync_state,
    is_sync_running,
    start_sync,
)

# Dark theme palette (matches charts.py)
_BG    = "#0b0e11"
_PANEL = "#111418"
_GRID  = "#2b3139"
_TEXT  = "#eaecef"
_MUTED = "#848e9c"
_GREEN = "#0ecb81"
_RED   = "#f6465d"
_GOLD  = "#f0b90b"

st.set_page_config(
    page_title="ChronoQuant",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    /* Hide Streamlit's built-in sticky header bar */
    header[data-testid="stHeader"] { display: none; }
    /* Base font bump — Streamlit default is ~14px which feels small at wide layout */
    html, body { font-size: 15px; }
    .stApp, .block-container { font-size: 15px; }
    p, span, div, label, .stCaption { font-size: 1rem; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; max-width: 100%; }
    [data-testid="stSidebar"] > div:first-child { padding-top: 1.5rem; }
    [data-testid="stPlotlyChart"] { border-radius: 6px; overflow: hidden; }
    /* Sidebar labels */
    [data-testid="stSidebar"] label, [data-testid="stSidebar"] span { font-size: 0.95rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─── Session init ─────────────────────────────────────────────────────────────
if "active_asset_id" not in st.session_state:
    st.session_state["active_asset_id"] = "solusdt_fw60"

if "dashboard_started_logged" not in st.session_state:
    _logger = get_dashboard_logger()
    _logger.info("Dashboard started")
    st.session_state.dashboard_started_logged = True

    _auto_state = ensure_sync_state(st.session_state, "solusdt_fw60")
    enable_auto_sync(_auto_state)
    if start_sync(_auto_state, "solusdt_fw60"):
        _logger.info("Auto-start sync (asset=solusdt_fw60)")

    if not trading_runner.is_trading_running():
        if trading_runner.start_trading(mode="dry_run"):
            _logger.info("Auto-start trading (mode=dry_run)")


# =============================================================================
# Sync controls
# =============================================================================

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
    _render_sync_controls(asset_id="solusdt_fw60")


# =============================================================================
# Chart area (left 60%)
# =============================================================================

_WINDOW_OPTIONS = {"24h": 24, "8h": 8, "4h": 4}
_WINDOW_DEFAULT = "24h"


def render_asset_chart(asset_id: str | None) -> None:
    long_strategy, short_strategy = data.load_long_short_strategies(asset_id=asset_id)

    sync_state   = ensure_sync_state(st.session_state, asset_id)
    sync_running = is_sync_running(sync_state, asset_id)

    # Time-window selector
    win_key    = f"chart_window_{asset_id or 'default'}"
    win_label  = st.radio(
        "Időtáv",
        options=list(_WINDOW_OPTIONS.keys()),
        index=list(_WINDOW_OPTIONS.keys()).index(_WINDOW_DEFAULT),
        horizontal=True,
        key=win_key,
        label_visibility="collapsed",
    )
    focus_hours   = _WINDOW_OPTIONS[win_label]
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
        entry_threshold       = long_strategy.get("entry_threshold"),
        rearm_threshold       = long_strategy.get("rearm_threshold"),
        exit_threshold        = long_strategy.get("exit_threshold"),
        short_entry_threshold = short_strategy.get("entry_threshold"),
        short_rearm_threshold = short_strategy.get("rearm_threshold"),
        short_exit_threshold  = short_strategy.get("exit_threshold"),
        active_trade          = active_trade,
        focus_hours           = focus_hours,
    )
    st.plotly_chart(fig, config=PLOTLY_CHART_CONFIG, width="stretch")


# =============================================================================
# Trade panel helpers (right 40%)
# =============================================================================

_CARD = (
    f"border:1px solid {_GRID}; border-radius:6px; "
    f"padding:12px 14px; background:{_PANEL}; margin-bottom:10px;"
)
_LBL = f"color:{_MUTED}; font-size:13px;"
_VAL = f"color:{_TEXT}; font-size:14px; font-weight:500;"
_HDR = f"color:{_TEXT}; font-size:14px; font-weight:700; margin-bottom:10px;"


def _fmt(val, digits: int = 4) -> str:
    if val is None:
        return "—"
    try:
        return f"{float(val):,.{digits}f}"
    except (TypeError, ValueError):
        return str(val)


def _fmt_time(val) -> str:
    if val is None:
        return "—"
    s = str(val)
    return s[:16] if len(s) >= 16 else s


def _render_active_trade_card(position: dict | None) -> None:
    if not position:
        st.markdown(
            f'<div style="{_CARD}">'
            f'<div style="{_HDR}">Active Trade</div>'
            f'<div style="{_LBL}">No active trade</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        return

    side      = str(position.get("side") or position.get("direction") or "LONG").upper()
    entry     = position.get("entry_price") or position.get("open_price")
    sl        = position.get("stop_loss") or position.get("sl_price") or position.get("sl")
    tp        = position.get("take_profit") or position.get("tp_price") or position.get("tp")
    open_time = position.get("entry_time") or position.get("open_time") or position.get("created_at")

    side_color = _GREEN if "LONG" in side else _RED
    arrow      = "▲" if "LONG" in side else "▼"

    st.markdown(
        f"""
        <div style="{_CARD}">
            <div style="color:{side_color}; font-size:14px; font-weight:700; margin-bottom:10px;">
                {arrow} {side} &nbsp; Active Trade
            </div>
            <div style="display:grid; grid-template-columns:90px 1fr; row-gap:7px; font-size:13px;">
                <span style="{_LBL}">Entry</span>
                <span style="{_VAL}">{_fmt(entry)}</span>
                <span style="{_LBL}">Stop loss</span>
                <span style="color:{_RED}; font-size:14px; font-weight:500;">{_fmt(sl)}</span>
                <span style="{_LBL}">Take profit</span>
                <span style="color:{_GREEN}; font-size:14px; font-weight:500;">{_fmt(tp)}</span>
                <span style="{_LBL}">Opened</span>
                <span style="{_VAL}">{_fmt_time(open_time)}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _group_trades_by_minute(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["time_min"] = pd.to_datetime(df["time"]).dt.floor("min")
    rows = []
    for (time_min, side), grp in df.groupby(["time_min", "side"], sort=False):
        total_qty   = grp["qty"].sum()
        total_quote = grp["quote_qty"].sum()
        avg_price   = total_quote / total_qty if total_qty > 0 else grp["price"].mean()
        pnl_sum     = grp["pnl"].sum() if grp["pnl"].notna().any() else None
        comm_sum    = grp["commission"].sum() if grp["commission"].notna().any() else None
        rows.append({
            "time":       grp["time"].iloc[0],
            "side":       side,
            "price":      avg_price,
            "qty":        total_qty,
            "quote_qty":  total_quote,
            "pnl":        pnl_sum,
            "commission": comm_sum,
            "source":     grp["source"].iloc[0],
            "fills":      len(grp),
        })
    out = pd.DataFrame(rows)
    return out.sort_values("time", ascending=False).reset_index(drop=True)


def _binance_trade_row_html(row: dict) -> str:
    side   = str(row.get("side") or "?").upper()
    price  = _fmt(row.get("price"), 4)
    fills  = int(row.get("fills") or 1)
    qty    = _fmt(row.get("qty"), 4) + (f" <span style='color:{_MUTED};font-size:11px;'>×{fills}</span>" if fills > 1 else "")
    pnl    = row.get("pnl")
    ts     = row.get("time")
    source = str(row.get("source", ""))

    side_color = _GREEN if side == "BUY" else _RED if side == "SELL" else _MUTED
    try:
        pnl_val   = float(pnl) if pnl is not None else None
        pnl_str   = f"{pnl_val:+.4f}" if pnl_val is not None else "—"
        pnl_color = _GREEN if pnl_val and pnl_val > 0 else _RED if pnl_val and pnl_val < 0 else _MUTED
    except (TypeError, ValueError):
        pnl_str, pnl_color = "—", _MUTED

    time_str = ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else _fmt_time(ts)
    src_tag  = f' <span style="color:{_MUTED}; font-size:11px;">[{escape(source)}]</span>' if source else ""

    return (
        f'<div style="border-bottom:1px solid {_GRID}; padding:8px 0; font-size:13px;'
        f' display:grid; grid-template-columns:46px 90px 80px 80px auto; gap:6px; align-items:center;">'
        f'<span style="color:{side_color}; font-weight:600;">{side}</span>'
        f'<span style="color:{_TEXT};">{price}</span>'
        f'<span style="color:{_TEXT};">{qty}</span>'
        f'<span style="color:{pnl_color};">{pnl_str}</span>'
        f'<span style="color:{_TEXT}; font-size:12px; white-space:nowrap;">{time_str}{src_tag}</span>'
        f'</div>'
    )


def _render_recent_trades_panel(trades_df: pd.DataFrame, asset_id: str | None) -> None:
    is_binance = not trades_df.empty and "source" in trades_df.columns
    header     = "Recent Trades (Binance)" if is_binance else "Recent Trades"
    source_tag = ""
    if not trades_df.empty and is_binance:
        src = trades_df["source"].iloc[0] if "source" in trades_df.columns else ""
        source_tag = f' <span style="color:{_MUTED}; font-size:12px;">[{escape(str(src))}]</span>'

    if trades_df.empty:
        st.markdown(
            f'<div style="{_CARD}">'
            f'<div style="{_HDR}">{header}</div>'
            f'<div style="{_LBL}">No trade data available</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        return

    col_hdr = (
        f'<div style="display:grid; grid-template-columns:46px 90px 80px 80px auto; gap:6px;'
        f' font-size:12px; color:{_MUTED}; padding-bottom:6px; border-bottom:1px solid {_GRID};">'
        f'<span>Side</span><span>Price</span><span>Qty</span><span>PnL</span><span>Time</span>'
        f'</div>'
    )
    grouped   = _group_trades_by_minute(trades_df)
    rows_html = "".join(_binance_trade_row_html(r.to_dict()) for _, r in grouped.iterrows())
    st.markdown(
        f'<div style="{_CARD}">'
        f'<div style="{_HDR}">{header}{source_tag}</div>'
        f'<div style="max-height:400px; overflow-y:auto;">'
        f'{col_hdr}{rows_html}'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _load_sol_model_stats() -> dict[str, dict]:
    """Load model card data from models/<model_id>/model_card.json for active solusdt_fw60 models."""
    import json

    import utils as _utils

    model_cfg = _utils.load_models_config()
    result: dict[str, dict] = {}

    for model_id, meta in model_cfg.get("models", {}).items():
        if not meta.get("active"):
            continue
        if meta.get("asset_id") != "solusdt_fw60":
            continue
        model_dir = Path(_utils._resolve_path(meta["paths"]["model_dir"]))
        card_path = model_dir / "model_card.json"
        if not card_path.exists():
            continue
        card = json.loads(card_path.read_text(encoding="utf-8"))
        side = card.get("side")
        if side in ("long", "short"):
            result[side] = {
                "model_id":   card["model_id"],
                "train_auc":  card.get("train_prauc", 0),
                "test_auc":   card.get("valid_prauc", 0),
                "n_features": card.get("n_features", 0),
                "backtest": {
                    "period":       card["holdout"]["period"],
                    "trades":       card["holdout"]["trades"],
                    "wins":         card["holdout"]["wins"],
                    "losses":       card["holdout"]["losses"],
                    "win_rate":     card["holdout"]["win_rate"],
                    "total_return": card["holdout"]["total_return"],
                    "final_equity": card["holdout"]["final_equity"],
                    "max_dd":       card["holdout"]["max_dd"],
                },
            }
    return result


_SOL_MODEL_STATS = _load_sol_model_stats()


def _render_model_stats_panel(side: str, stats: dict) -> None:
    bt        = stats["backtest"]
    is_long   = side == "long"
    side_color = _GREEN if is_long else _RED
    side_label = "Long" if is_long else "Short"
    arrow      = "▲" if is_long else "▼"

    model_short = stats["model_id"].replace("lgbm_solusdt_", "").replace("_stable_v1", "")

    st.markdown(
        f"""
        <div style="{_CARD}">
            <div style="color:{side_color}; font-size:13px; font-weight:700; margin-bottom:8px;">
                {arrow} {side_label} Champion
            </div>
            <div style="color:{_MUTED}; font-size:11px; margin-bottom:8px; word-break:break-all;">
                {escape(stats["model_id"])}
            </div>
            <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:6px; margin-bottom:10px;">
                <div>
                    <div style="{_LBL}">Train AUC</div>
                    <div style="{_VAL}">{stats["train_auc"]:.3f}</div>
                </div>
                <div>
                    <div style="{_LBL}">Test AUC</div>
                    <div style="{_VAL}">{stats["test_auc"]:.3f}</div>
                </div>
                <div>
                    <div style="{_LBL}">Features</div>
                    <div style="{_VAL}">{stats["n_features"]}</div>
                </div>
            </div>
            <div style="border-top:1px solid {_GRID}; padding-top:8px;">
                <div style="color:{_MUTED}; font-size:11px; margin-bottom:6px;">
                    Backtest &nbsp; {escape(bt["period"])}
                </div>
                <div style="display:grid; grid-template-columns:1fr 1fr; gap:4px 10px; font-size:12px;">
                    <div>
                        <span style="{_LBL}">Trades </span>
                        <span style="{_VAL}">{bt["trades"]}</span>
                        <span style="color:{_MUTED}; font-size:11px;"> ({bt["wins"]}W / {bt["losses"]}L)</span>
                    </div>
                    <div>
                        <span style="{_LBL}">Win rate </span>
                        <span style="{_VAL}">{bt["win_rate"]}%</span>
                    </div>
                    <div>
                        <span style="{_LBL}">Return </span>
                        <span style="color:{_GREEN}; font-size:14px; font-weight:500;">{escape(bt["total_return"])}</span>
                    </div>
                    <div>
                        <span style="{_LBL}">Max DD </span>
                        <span style="color:{_RED}; font-size:14px; font-weight:500;">{escape(bt["max_dd"])}</span>
                    </div>
                    <div style="grid-column:1/-1;">
                        <span style="{_LBL}">Final equity </span>
                        <span style="{_VAL}">{escape(bt["final_equity"])}</span>
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_trading_status_card() -> None:
    status = trading_runner.get_trading_status()
    running = trading_runner.is_trading_running()

    if not running and status is None:
        return

    mode = (status or {}).get("mode", "—")
    mode_color = _GOLD if mode == "dry_run" else _RED if mode == "live" else _MUTED
    service_dot = f'<span style="color:{_GREEN};">●</span>' if running else f'<span style="color:{_MUTED};">○</span>'

    open_pos = (status or {}).get("open_position")
    last_sig = (status or {}).get("last_signal")

    pos_html = ""
    if open_pos:
        side = open_pos.get("side", "?")
        side_color = _GREEN if side == "LONG" else _RED
        arrow = "▲" if side == "LONG" else "▼"
        pos_html = (
            f'<div style="margin-top:8px; border-top:1px solid {_GRID}; padding-top:8px;">'
            f'<span style="color:{side_color}; font-weight:700;">{arrow} {side}</span>'
            f'&nbsp; entry <span style="color:{_TEXT};">{_fmt(open_pos.get("entry_price"))}</span>'
            f'&nbsp; qty <span style="color:{_TEXT};">{_fmt(open_pos.get("quantity"), 2)}</span>'
            f'</div>'
        )

    sig_html = ""
    if last_sig:
        dec = last_sig.get("decision", "")
        dec_color = _GREEN if "ENTER" in dec else _RED if "EXIT" in dec else _MUTED
        sig_html = (
            f'<div style="margin-top:6px; font-size:12px;">'
            f'<span style="{_LBL}">Last signal </span>'
            f'<span style="color:{dec_color}; font-weight:600;">{dec}</span>'
            f'<span style="color:{_MUTED}; font-size:11px;"> {_fmt_time(last_sig.get("processed_at"))}</span>'
            f'</div>'
            f'<div style="font-size:11px; color:{_MUTED};">{escape(last_sig.get("reason",""))}</div>'
        )

    started = _fmt_time((status or {}).get("started_at"))
    st.markdown(
        f'<div style="{_CARD}">'
        f'<div style="{_HDR}">{service_dot} Auto Trading'
        f'&nbsp;<span style="color:{mode_color}; font-size:12px; font-weight:400;">[{mode}]</span>'
        f'</div>'
        f'<div style="font-size:12px; color:{_MUTED};">Started: {started}</div>'
        f'{pos_html}{sig_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_trading_positions_card() -> None:
    positions = trading_runner.get_recent_positions(limit=10)
    if not positions:
        return

    rows_html = ""
    for p in positions:
        side = p.get("side", "?")
        side_color = _GREEN if side == "LONG" else _RED
        pnl = p.get("pnl_usdt")
        status = p.get("status", "")
        try:
            pnl_val = float(pnl) if pnl is not None else None
            pnl_str = f"{pnl_val:+.2f}" if pnl_val is not None else "open"
            pnl_color = _GREEN if pnl_val and pnl_val > 0 else _RED if pnl_val and pnl_val < 0 else _MUTED
        except (TypeError, ValueError):
            pnl_str, pnl_color = "—", _MUTED

        entry_t = _fmt_time(p.get("entry_time"))
        reason = p.get("exit_reason") or "open"
        rows_html += (
            f'<div style="border-bottom:1px solid {_GRID}; padding:5px 0; font-size:12px;'
            f' display:grid; grid-template-columns:46px 70px 70px auto; gap:4px; align-items:center;">'
            f'<span style="color:{side_color}; font-weight:600;">{side}</span>'
            f'<span style="color:{_TEXT};">{_fmt(p.get("entry_price"), 2)}</span>'
            f'<span style="color:{pnl_color}; font-weight:600;">{pnl_str}</span>'
            f'<span style="color:{_MUTED}; font-size:11px;">{entry_t} · {escape(reason)}</span>'
            f'</div>'
        )

    st.markdown(
        f'<div style="{_CARD}">'
        f'<div style="{_HDR}">Auto Trades</div>'
        f'<div style="display:grid; grid-template-columns:46px 70px 70px auto; gap:4px;'
        f' font-size:11px; color:{_MUTED}; padding-bottom:4px; border-bottom:1px solid {_GRID};">'
        f'<span>Side</span><span>Entry</span><span>PnL</span><span>Time · Reason</span>'
        f'</div>'
        f'{rows_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_signal_trigger_card(asset_id: str | None) -> None:
    long_cfg, short_cfg = data.load_long_short_strategies(asset_id=asset_id)
    if not long_cfg and not short_cfg:
        return

    latest = data.latest_prediction(asset_id=asset_id)
    long_pred  = None
    short_pred = None
    if latest:
        long_pred  = latest.get("long_prediction")
        short_pred = latest.get("short_prediction")
        if short_pred is None:
            # look for any short prediction column
            for k, v in latest.items():
                if "short" in k and v is not None:
                    try:
                        short_pred = float(v)
                    except (TypeError, ValueError):
                        pass
                    break

    long_entry  = long_cfg.get("entry_threshold")
    short_entry = short_cfg.get("entry_threshold")

    def _trigger_html(label: str, pred, threshold, side_color: str, arrow: str) -> str:
        if pred is None or threshold is None:
            return (
                f'<div style="display:grid; grid-template-columns:80px 1fr; gap:4px; font-size:13px; margin-bottom:6px;">'
                f'<span style="color:{_MUTED};">{label}</span>'
                f'<span style="color:{_MUTED};">—</span>'
                f'</div>'
            )
        try:
            pred_f = float(pred)
            thr_f  = float(threshold)
        except (TypeError, ValueError):
            return ""
        active     = pred_f >= thr_f
        bar_pct    = min(100, int(pred_f * 100))
        bar_color  = side_color if active else _MUTED
        tag_text   = "AKTÍV" if active else "VÁRAKOZIK"
        tag_color  = side_color if active else _MUTED
        return (
            f'<div style="margin-bottom:8px;">'
            f'<div style="display:flex; justify-content:space-between; font-size:12px; margin-bottom:3px;">'
            f'<span style="color:{side_color}; font-weight:600;">{arrow} {label}</span>'
            f'<span style="color:{tag_color}; font-weight:700; font-size:11px;">{tag_text}</span>'
            f'</div>'
            f'<div style="background:{_GRID}; border-radius:3px; height:6px; position:relative; margin-bottom:3px;">'
            f'<div style="background:{bar_color}; width:{bar_pct}%; height:6px; border-radius:3px;"></div>'
            f'<div style="position:absolute; left:{int(thr_f*100)}%; top:-3px; width:2px; height:12px; background:{_TEXT}; border-radius:1px;"></div>'
            f'</div>'
            f'<div style="display:flex; justify-content:space-between; font-size:11px; color:{_MUTED};">'
            f'<span>score {pred_f:.3f}</span><span>entry {thr_f:.3f}</span>'
            f'</div>'
            f'</div>'
        )

    long_html  = _trigger_html("Long",  long_pred,  long_entry,  _GREEN, "▲")
    short_html = _trigger_html("Short", short_pred, short_entry, _RED,   "▼")

    last_signal_html = ""
    signals = trading_runner.get_recent_signals(limit=1)
    if signals:
        sig       = signals[0]
        dec       = sig.get("decision", "")
        bar_ts    = str(sig.get("bar_open_time", ""))[:16]
        reason    = str(sig.get("reason", ""))[:120]
        dec_upper = dec.upper()
        if "ENTER" in dec_upper:
            dec_color = _GREEN if "LONG" in dec_upper else _RED
        elif "EXIT" in dec_upper:
            dec_color = _RED if "LONG" in dec_upper else _GREEN
        else:
            dec_color = _MUTED
        last_signal_html = (
            f'<div style="border-top:1px solid {_GRID}; padding-top:8px; margin-top:4px;">'
            f'<div style="display:flex; justify-content:space-between; font-size:12px; margin-bottom:3px;">'
            f'<span style="color:{_MUTED};">Legutóbbi döntés</span>'
            f'<span style="color:{_MUTED}; font-size:11px;">{escape(bar_ts)}</span>'
            f'</div>'
            f'<div style="font-size:13px; font-weight:700; color:{dec_color}; margin-bottom:2px;">{escape(dec)}</div>'
            f'<div style="font-size:11px; color:{_MUTED}; word-break:break-word;">{escape(reason)}</div>'
            f'</div>'
        )

    st.markdown(
        f'<div style="{_CARD}">'
        f'<div style="{_HDR}">Trigger állapot</div>'
        f'{long_html}{short_html}{last_signal_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_trade_panel(asset_id: str | None) -> None:
    sync_state   = ensure_sync_state(st.session_state, asset_id)
    sync_running = is_sync_running(sync_state, asset_id)

    cache_pos          = f"trade_position_{asset_id or 'default'}"
    cache_trades       = f"trade_binance_{asset_id or 'default'}"
    cache_sync_fin     = f"trade_last_sync_{asset_id or 'default'}"

    if sync_running:
        position  = st.session_state.get(cache_pos)
        trades_df = st.session_state.get(cache_trades, pd.DataFrame())
    else:
        try:
            position  = data.active_position(asset_id=asset_id)
            st.session_state[cache_pos] = position
        except duckdb.IOException as exc:
            if "locked" not in str(exc).lower():
                raise
            get_dashboard_logger().warning("Trade panel DB read skipped — database locked")
            position = st.session_state.get(cache_pos)

        finished_at = sync_state.get("finished_at")
        if finished_at and st.session_state.get(cache_sync_fin) != finished_at:
            st.session_state.pop(cache_trades, None)
            st.session_state[cache_sync_fin] = finished_at

        if cache_trades not in st.session_state:
            trades_df = binance_data.recent_trades(asset_id=asset_id, limit=50)
            st.session_state[cache_trades] = trades_df
        else:
            trades_df = st.session_state[cache_trades]

    if asset_id == "solusdt_fw60":
        _render_trading_status_card()
        _render_signal_trigger_card(asset_id)
        _render_trading_positions_card()

    _render_active_trade_card(position)
    _render_recent_trades_panel(trades_df, asset_id)

    if asset_id == "solusdt_fw60":
        for _side in ("long", "short"):
            if _side in _SOL_MODEL_STATS:
                _render_model_stats_panel(_side, _SOL_MODEL_STATS[_side])


# =============================================================================
# Log panel — terminal style
# =============================================================================

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
    details = [str(l) for l in entry.get("details", []) if str(l).strip()]
    ts, level, message = _split_log_header(header)
    color   = _LEVEL_COLORS.get(level.upper(), _TEXT)
    abbr    = level[:4].upper()
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
        # oldest at top, newest at bottom (no reversing)
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


# =============================================================================
# Sidebar
# =============================================================================

def _render_trading_controls() -> None:
    st.subheader("Live Trading")

    running = trading_runner.is_trading_running()
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


# =============================================================================
# Main body - 75/25 layout
# =============================================================================

active_asset_id = "solusdt_fw60"
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
