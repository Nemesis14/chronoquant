from __future__ import annotations

from html import escape

import pandas as pd
import streamlit as st

from ui import binance_data, data, trading_runner
from ui.components.formatting import (
    _GOLD,
    _GREEN,
    _GRID,
    _MUTED,
    _PANEL,
    _RED,
    _TEXT,
)
from ui.dashboard_logging import get_dashboard_logger
from ui.sync_runner import ensure_sync_state, is_sync_running

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
    for key, grp in df.groupby(["time_min", "side"], sort=False):  # type: ignore[assignment]
        _time_min, side = key  # type: ignore[misc]
        total_qty   = grp["qty"].sum()
        total_quote = grp["quote_qty"].sum()
        avg_price   = total_quote / total_qty if total_qty > 0 else grp["price"].mean()
        pnl_sum     = grp["pnl"].sum() if bool(grp["pnl"].notna().any()) else None
        comm_sum    = grp["commission"].sum() if bool(grp["commission"].notna().any()) else None
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

    time_str = ts.strftime("%m-%d %H:%M") if isinstance(ts, pd.Timestamp) else _fmt_time(ts)
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


def _render_trading_status_card() -> None:
    status  = trading_runner.get_trading_status()
    running = trading_runner.is_trading_running()

    if not running and status is None:
        return

    mode       = (status or {}).get("mode", "—")
    mode_color = _GOLD if mode == "dry_run" else _RED if mode == "live" else _MUTED
    service_dot = f'<span style="color:{_GREEN};">●</span>' if running else f'<span style="color:{_MUTED};">○</span>'

    open_pos = (status or {}).get("open_position")
    last_sig = (status or {}).get("last_signal")

    pos_html = ""
    if open_pos:
        side       = open_pos.get("side", "?")
        side_color = _GREEN if side == "LONG" else _RED
        arrow      = "▲" if side == "LONG" else "▼"
        pos_html = (
            f'<div style="margin-top:8px; border-top:1px solid {_GRID}; padding-top:8px;">'
            f'<span style="color:{side_color}; font-weight:700;">{arrow} {side}</span>'
            f'&nbsp; entry <span style="color:{_TEXT};">{_fmt(open_pos.get("entry_price"))}</span>'
            f'&nbsp; qty <span style="color:{_TEXT};">{_fmt(open_pos.get("quantity"), 2)}</span>'
            f'</div>'
        )

    sig_html = ""
    if last_sig:
        dec       = last_sig.get("decision", "")
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
        side       = p.get("side", "?")
        side_color = _GREEN if side == "LONG" else _RED
        pnl        = p.get("pnl_usdt")
        try:
            pnl_val   = float(pnl) if pnl is not None else None
            pnl_str   = f"{pnl_val:+.2f}" if pnl_val is not None else "open"
            pnl_color = _GREEN if pnl_val and pnl_val > 0 else _RED if pnl_val and pnl_val < 0 else _MUTED
        except (TypeError, ValueError):
            pnl_str, pnl_color = "—", _MUTED

        entry_t = _fmt_time(p.get("entry_time"))
        reason  = p.get("exit_reason") or "open"
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
    signals = trading_runner.get_recent_signals(limit=1)

    if not signals:
        st.markdown(
            f'<div style="{_CARD}">'
            f'<div style="{_HDR}">Trading State</div>'
            f'<div style="{_LBL}">Nincs aktív kereskedési service</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        return

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

    status    = trading_runner.get_trading_status()
    state_str = (status or {}).get("state", "FLAT")
    state_color = (
        _GREEN if state_str in ("LONG",)
        else _RED if state_str in ("SHORT",)
        else _GOLD if state_str == "COOLDOWN"
        else _MUTED
    )

    long_cfg, short_cfg = data.load_long_short_strategies(asset_id=asset_id)
    threshold_html = ""
    if long_cfg.get("entry_pct") is not None:
        threshold_html = (
            f'<div style="font-size:11px; color:{_MUTED}; margin-top:6px;">'
            f'entry küszöb: long {long_cfg["entry_pct"]:.1%}'
        )
        if short_cfg.get("entry_pct") is not None:
            threshold_html += f' / short {short_cfg["entry_pct"]:.1%}'
        threshold_html += '</div>'

    st.markdown(
        f'<div style="{_CARD}">'
        f'<div style="{_HDR}">Trading State</div>'
        f'<div style="font-size:15px; font-weight:700; color:{state_color}; margin-bottom:10px;">{escape(state_str)}</div>'
        f'<div style="border-top:1px solid {_GRID}; padding-top:8px;">'
        f'<div style="display:flex; justify-content:space-between; font-size:12px; margin-bottom:3px;">'
        f'<span style="color:{_MUTED};">Legutóbbi döntés</span>'
        f'<span style="color:{_MUTED}; font-size:11px;">{escape(bar_ts)}</span>'
        f'</div>'
        f'<div style="font-size:13px; font-weight:700; color:{dec_color}; margin-bottom:2px;">{escape(dec)}</div>'
        f'<div style="font-size:11px; color:{_MUTED}; word-break:break-word;">{escape(reason)}</div>'
        f'</div>'
        f'{threshold_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_trade_panel(asset_id: str | None) -> None:
    sync_state   = ensure_sync_state(st.session_state, asset_id)
    sync_running = is_sync_running(sync_state, asset_id)

    cache_pos      = f"trade_position_{asset_id or 'default'}"
    cache_trades   = f"trade_binance_{asset_id or 'default'}"
    cache_sync_fin = f"trade_last_sync_{asset_id or 'default'}"

    if sync_running:
        position  = st.session_state.get(cache_pos)
        trades_df = st.session_state.get(cache_trades, pd.DataFrame())
    else:
        import duckdb

        try:
            position = data.active_position(asset_id=asset_id)
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

