from __future__ import annotations

from html import escape

import pandas as pd
import streamlit as st

from ui import binance_data, data, trading_runner
from ui.components.formatting import (
    GOLD as _GOLD,
)
from ui.components.formatting import (
    GREEN as _GREEN,
)
from ui.components.formatting import (
    GRID as _GRID,
)
from ui.components.formatting import (
    MUTED as _MUTED,
)
from ui.components.formatting import (
    PANEL as _PANEL,
)
from ui.components.formatting import (
    RED as _RED,
)
from ui.components.formatting import (
    TEXT as _TEXT,
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

    side       = str(position.get("side") or position.get("direction") or "LONG").upper()
    entry      = position.get("entry_price") or position.get("open_price")
    sl         = position.get("stop_loss") or position.get("sl_price") or position.get("sl")
    tp         = position.get("take_profit") or position.get("tp_price") or position.get("tp")
    open_time  = position.get("entry_time") or position.get("open_time") or position.get("created_at")
    upnl       = position.get("unrealized_pnl")
    from_binance = position.get("_source") == "binance"

    side_color = _GREEN if "LONG" in side else _RED
    arrow      = "▲" if "LONG" in side else "▼"
    src_tag    = ' <span style="font-size:11px; font-weight:400; color:#aaa;">[Binance]</span>' if from_binance else ""

    upnl_row = ""
    if upnl is not None:
        try:
            upnl_val   = float(upnl)
            upnl_color = _GREEN if upnl_val > 0 else _RED if upnl_val < 0 else _MUTED
            upnl_row   = (
                f'<span style="{_LBL}">Unrealized PnL</span>'
                f'<span style="color:{upnl_color}; font-size:14px; font-weight:600;">'
                f'{upnl_val:+.4f} USDT</span>'
            )
        except (TypeError, ValueError):
            pass

    rows_html = (
        f'<span style="{_LBL}">Entry</span>'
        f'<span style="{_VAL}">{_fmt(entry)}</span>'
    )
    if sl is not None:
        rows_html += (
            f'<span style="{_LBL}">Stop loss</span>'
            f'<span style="color:{_RED}; font-size:14px; font-weight:500;">{_fmt(sl)}</span>'
        )
    if tp is not None:
        rows_html += (
            f'<span style="{_LBL}">Take profit</span>'
            f'<span style="color:{_GREEN}; font-size:14px; font-weight:500;">{_fmt(tp)}</span>'
        )
    rows_html += upnl_row
    if open_time:
        rows_html += (
            f'<span style="{_LBL}">Opened</span>'
            f'<span style="{_VAL}">{_fmt_time(open_time)}</span>'
        )

    st.markdown(
        f"""
        <div style="{_CARD}">
            <div style="color:{side_color}; font-size:14px; font-weight:700; margin-bottom:10px;">
                {arrow} {side} &nbsp; Active Trade{src_tag}
            </div>
            <div style="display:grid; grid-template-columns:120px 1fr; row-gap:7px; font-size:13px;">
                {rows_html}
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

    side_color = _GREEN if side in ("BUY", "LONG") else _RED if side in ("SELL", "SHORT") else _MUTED
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


def _render_strategy_card(cfg: dict, direction: str) -> str:
    """Return HTML for one strategy card (long or short)."""
    cutoff    = cfg.get("entry_cutoff")
    n_trades  = cfg.get("n_trades")
    win_rate  = cfg.get("win_rate")
    total_lr  = cfg.get("total_lr")
    compounded = cfg.get("compounded")

    color  = _GREEN if direction == "long" else _RED
    arrow  = "▲" if direction == "long" else "▼"
    label  = "Long Strategy" if direction == "long" else "Short Strategy"

    cutoff_str    = f"{cutoff:.1%}" if cutoff is not None else "—"
    n_str         = str(n_trades) if n_trades is not None else "—"
    win_str       = f"{win_rate:.1%}" if win_rate is not None else "—"
    lr_str        = f"{total_lr:+.4f}" if total_lr is not None else "—"
    compound_str  = f"{compounded:+.1f}%" if compounded is not None else "—"

    win_color     = _GREEN if (win_rate or 0) >= 0.6 else _RED if (win_rate or 0) < 0.5 else _GOLD
    lr_color      = _GREEN if (total_lr or 0) > 0 else _RED

    return (
        f'<div style="{_CARD}">'
        f'<div style="color:{color}; font-size:14px; font-weight:700; margin-bottom:10px;">'
        f'{arrow} {label}'
        f'</div>'
        f'<div style="display:grid; grid-template-columns:120px 1fr; row-gap:6px; font-size:13px;">'
        f'<span style="{_LBL}">Entry cutoff</span>'
        f'<span style="{_VAL}">{cutoff_str}</span>'
        f'<span style="{_LBL}">Trades</span>'
        f'<span style="{_VAL}">{n_str}</span>'
        f'<span style="{_LBL}">Win rate</span>'
        f'<span style="color:{win_color}; font-size:13px; font-weight:500;">{win_str}</span>'
        f'<span style="{_LBL}">Total log-ret</span>'
        f'<span style="color:{lr_color}; font-size:13px; font-weight:500;">{lr_str}</span>'
        f'<span style="{_LBL}">Compounded</span>'
        f'<span style="color:{lr_color}; font-size:13px; font-weight:500;">{compound_str}</span>'
        f'</div>'
        f'</div>'
    )


def _render_signal_trigger_card(asset_id: str | None) -> None:
    status  = trading_runner.get_trading_status()
    running = trading_runner.is_trading_running()
    signals = trading_runner.get_recent_signals(limit=1)

    # --- Trading state header ---
    if running:
        mode       = (status or {}).get("mode", "—")
        mode_color = _GOLD if mode == "dry_run" else _RED if mode == "live" else _MUTED
        started    = _fmt_time((status or {}).get("started_at"))
        dot        = f'<span style="color:{_GREEN};">●</span>'
        state_hdr  = (
            f'<div style="{_HDR}">{dot} Auto Trading'
            f'&nbsp;<span style="color:{mode_color}; font-size:12px; font-weight:400;">[{mode}]</span>'
            f'</div>'
            f'<div style="font-size:12px; color:{_MUTED}; margin-bottom:8px;">Started: {started}</div>'
        )
    else:
        state_hdr = f'<div style="{_HDR}">Trading State</div>'

    # --- Last signal row ---
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

        state_str = (status or {}).get("state", "FLAT")
        state_color = (
            _GREEN if state_str == "LONG"
            else _RED if state_str == "SHORT"
            else _GOLD if state_str == "COOLDOWN"
            else _MUTED
        )
        signal_html = (
            f'<div style="border-top:1px solid {_GRID}; padding-top:8px; margin-top:4px;">'
            f'<div style="font-size:15px; font-weight:700; color:{state_color}; margin-bottom:8px;">'
            f'{escape(state_str)}</div>'
            f'<div style="display:flex; justify-content:space-between; font-size:12px; margin-bottom:3px;">'
            f'<span style="color:{_MUTED};">Legutóbbi döntés</span>'
            f'<span style="color:{_MUTED}; font-size:11px;">{escape(bar_ts)}</span>'
            f'</div>'
            f'<div style="font-size:13px; font-weight:700; color:{dec_color}; margin-bottom:2px;">'
            f'{escape(dec)}</div>'
            f'<div style="font-size:11px; color:{_MUTED}; word-break:break-word;">'
            f'{escape(reason)}</div>'
            f'</div>'
        )
    else:
        signal_html = (
            f'<div style="{_LBL}; margin-top:4px;">Nincs aktív kereskedési service</div>'
        )

    st.markdown(
        f'<div style="{_CARD}">{state_hdr}{signal_html}</div>',
        unsafe_allow_html=True,
    )

    # --- Two strategy cards side by side ---
    long_cfg, short_cfg = data.load_long_short_strategies(asset_id=asset_id)
    if long_cfg or short_cfg:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(_render_strategy_card(long_cfg, "long"), unsafe_allow_html=True)
        with col2:
            st.markdown(_render_strategy_card(short_cfg, "short"), unsafe_allow_html=True)


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

    if asset_id in ("solusdt", "solusdt_fw60"):
        _render_signal_trigger_card(asset_id)
        _render_trading_positions_card()

    _render_active_trade_card(position)
    _render_recent_trades_panel(trades_df, asset_id)

