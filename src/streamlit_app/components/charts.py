# =============================================================================
# Dashboard chart helpers
# =============================================================================

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


CHART_LOAD_LOOKBACK_HOURS = 24
CHART_INITIAL_FOCUS_HOURS = 24

_BG    = "#0b0e11"
_PANEL = "#111418"
_GRID  = "#2b3139"
_TEXT  = "#eaecef"
_MUTED = "#848e9c"
_GREEN = "#0ecb81"
_RED   = "#f6465d"
_GOLD  = "#f0b90b"


def prediction_price_figure(
    df: pd.DataFrame,
    entry_threshold: float | None = None,
    rearm_threshold: float | None = None,
    exit_threshold: float | None = None,
    short_entry_threshold: float | None = None,
    short_rearm_threshold: float | None = None,
    short_exit_threshold: float | None = None,
    active_trade: dict | None = None,
    focus_hours: int | None = None,
):
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.20, 0.60, 0.20],
        vertical_spacing=0.03,
    )

    if df.empty:
        _ann(fig, "No prediction data", 0.90)
        _ann(fig, "No price data",      0.50)
        _ann(fig, "No prediction data", 0.10)
        return _style_figure(fig)

    plot_df = df.copy()
    plot_df["open_time"] = pd.to_datetime(plot_df["open_time"], errors="coerce")
    for col in ("open", "high", "low", "close"):
        if col in plot_df.columns:
            plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")
    for col in ("long_prediction", "short_prediction"):
        if col in plot_df.columns:
            plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")
    plot_df = (
        plot_df.dropna(subset=["open_time"])
        .sort_values("open_time")
        .drop_duplicates(subset=["open_time"])
        .reset_index(drop=True)
    )
    if plot_df.empty:
        _ann(fig, "No valid timestamps", 0.50)
        return _style_figure(fig)

    latest_ts  = plot_df["open_time"].max()
    x_end      = latest_ts + pd.Timedelta(minutes=2)
    _focus     = focus_hours if focus_hours is not None else CHART_INITIAL_FOCUS_HOURS
    x_start    = latest_ts - pd.Timedelta(hours=_focus)
    focus_df   = plot_df[plot_df["open_time"] >= x_start]
    if _has_ohlc(focus_df) and not focus_df.empty:
        _yl, _yh = focus_df["low"].min(), focus_df["high"].max()
    elif "close" in focus_df.columns and not focus_df.empty:
        _yl, _yh = focus_df["close"].min(), focus_df["close"].max()
    else:
        _yl, _yh = None, None
    _pad    = (_yh - _yl) * 0.06 if (_yl is not None and _yh != _yl) else None
    y2_range = [_yl - _pad, _yh + _pad] if _pad is not None else None

    # --- Row 1: Long prediction ---
    if "long_prediction" in plot_df.columns and not plot_df["long_prediction"].dropna().empty:
        fig.add_trace(
            go.Scatter(
                x=plot_df["open_time"],
                y=plot_df["long_prediction"],
                mode="lines",
                name="long prediction",
                line={"color": "#5b8af5", "width": 1.9},
                hovertemplate="long pred<br>%{x}<br>%{y:.4f}<extra></extra>",
                showlegend=False,
            ),
            row=1, col=1,
        )
    else:
        _ann(fig, "No long prediction data", 0.90)

    _add_threshold_trace(fig, plot_df, entry_threshold,  "entry", _GREEN, row=1)
    _add_threshold_trace(fig, plot_df, rearm_threshold, "rearm", _MUTED, row=1)
    _add_threshold_trace(fig, plot_df, exit_threshold,  "exit",  _RED,   row=1)
    _threshold_legend(
        fig,
        [(entry_threshold, "entry", _GREEN), (rearm_threshold, "rearm", _MUTED), (exit_threshold, "exit", _RED)],
        "y domain",
    )

    # --- Row 2: Candlestick / price ---
    if _has_ohlc(plot_df):
        cdf = plot_df.dropna(subset=["open", "high", "low", "close"]).copy()
        _add_price_candles(fig, cdf, focus_hours=_focus)
    elif "close" in plot_df.columns:
        fig.add_trace(
            go.Scatter(
                x=plot_df["open_time"],
                y=plot_df["close"],
                mode="lines",
                name="close",
                line={"color": _TEXT, "width": 1.8},
                showlegend=False,
            ),
            row=2, col=1,
        )

    if active_trade:
        _add_trade_overlay(fig, plot_df, active_trade)

    _long_signal_markers(fig, plot_df, entry_threshold)
    _short_signal_markers(fig, plot_df, short_entry_threshold)

    # --- Row 3: Short prediction ---
    if "short_prediction" in plot_df.columns and not plot_df["short_prediction"].dropna().empty:
        fig.add_trace(
            go.Scatter(
                x=plot_df["open_time"],
                y=plot_df["short_prediction"],
                mode="lines",
                name="short prediction",
                line={"color": _RED, "width": 1.9},
                hovertemplate="short pred<br>%{x}<br>%{y:.4f}<extra></extra>",
                showlegend=False,
            ),
            row=3, col=1,
        )
    else:
        _ann(fig, "No short prediction data", 0.10)

    _add_threshold_trace(fig, plot_df, short_entry_threshold, "entry", _GREEN, row=3)
    _add_threshold_trace(fig, plot_df, short_rearm_threshold, "rearm", _MUTED, row=3)
    _add_threshold_trace(fig, plot_df, short_exit_threshold,  "exit",  _RED,   row=3)
    _threshold_legend(
        fig,
        [(short_entry_threshold, "entry", _GREEN), (short_rearm_threshold, "rearm", _MUTED), (short_exit_threshold, "exit", _RED)],
        "y3 domain",
    )

    if all(v is None for v in (short_entry_threshold, short_rearm_threshold, short_exit_threshold)):
        fig.add_annotation(
            x=0.01, y=0.15,
            xref="paper", yref="y3 domain",
            text="no strategy thresholds configured",
            showarrow=False, xanchor="left", yanchor="bottom",
            font={"color": _MUTED, "size": 10},
        )

    return _style_figure(fig, x_start=x_start, x_end=x_end, y2_range=y2_range)


def _ann(fig, text: str, y: float) -> None:
    fig.add_annotation(
        text=text, x=0.5, y=y, xref="paper", yref="paper",
        showarrow=False, font={"color": _MUTED, "size": 13},
    )


def _add_trade_overlay(fig, df: pd.DataFrame, trade: dict) -> None:
    x0 = df["open_time"].min()
    x1 = df["open_time"].max() + pd.Timedelta(minutes=30)

    def _hline(val, color: str, label: str) -> None:
        if val is None:
            return
        fig.add_trace(
            go.Scatter(
                x=[x0, x1], y=[float(val), float(val)],
                mode="lines", name=label,
                line={"color": color, "width": 1.5, "dash": "dash"},
                hovertemplate=f"{label} %{{y:.4f}}<extra></extra>",
                showlegend=False,
            ),
            row=2, col=1,
        )

    _hline(trade.get("entry_price"), _GOLD,  "Entry")
    _hline(trade.get("stop_loss"),   _RED,   "SL")
    _hline(trade.get("take_profit"), _GREEN, "TP")


def _has_ohlc(df: pd.DataFrame) -> bool:
    required = {"open", "high", "low", "close"}
    return required.issubset(df.columns) and not df[list(required)].dropna(how="any").empty


def _resample_ohlcv(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    return (
        df.set_index("open_time")
        .resample(freq)
        .agg(open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"))
        .dropna()
        .reset_index()
    )


def _add_price_candles(fig, df: pd.DataFrame, focus_hours: int = 24) -> None:
    if focus_hours > 8:
        plot_df = _resample_ohlcv(df, "5min")   # 24h → ~288 candles @ ~3.9px
    elif focus_hours > 4:
        plot_df = _resample_ohlcv(df, "2min")   # 8h  → ~240 candles @ ~3.75px
    else:
        plot_df = df                             # 4h  → 240 candles @ ~3.75px
    fig.add_trace(
        go.Candlestick(
            x=plot_df["open_time"],
            open=plot_df["open"],
            high=plot_df["high"],
            low=plot_df["low"],
            close=plot_df["close"],
            increasing_line_color=_GREEN,
            increasing_fillcolor=_GREEN,
            decreasing_line_color=_RED,
            decreasing_fillcolor=_RED,
            line_width=1,
            name="OHLC",
            showlegend=False,
        ),
        row=2, col=1,
    )


def _add_threshold_trace(
    fig,
    df: pd.DataFrame,
    value: float | None,
    label: str,
    color: str,
    row: int = 2,
) -> None:
    if value is None or df.empty:
        return
    t = float(value)
    fig.add_trace(
        go.Scatter(
            x=[df["open_time"].min(), df["open_time"].max()],
            y=[t, t],
            mode="lines", name=label,
            line={"color": color, "width": 1.2, "dash": "dash"},
            hovertemplate=f"{label}<br>%{{y:.4f}}<extra></extra>",
            showlegend=False,
        ),
        row=row, col=1,
    )


def _threshold_legend(
    fig,
    items: list[tuple[float | None, str, str]],
    y_domain_ref: str = "y domain",
) -> None:
    valid = [(float(v), l, c) for v, l, c in items if v is not None]
    if not valid:
        return
    for x_pos, (value, label, color) in zip([0.01, 0.22, 0.43], valid):
        fig.add_annotation(
            x=x_pos, y=0.97,
            xref="paper", yref=y_domain_ref,
            text=f"{label} {value:.3f}",
            showarrow=False, xanchor="left", yanchor="top",
            font={"color": color, "size": 11},
            bgcolor="rgba(17, 20, 24, 0.88)",
            bordercolor=color, borderwidth=1, borderpad=3,
        )


def _long_signal_markers(
    fig,
    df: pd.DataFrame,
    entry_threshold: float | None,
) -> None:
    if entry_threshold is None or "long_prediction" not in df.columns:
        return
    mask = df["long_prediction"].notna() & (df["long_prediction"] >= float(entry_threshold))
    sig = df[mask]
    if sig.empty:
        return
    for ts, pred in zip(sig["open_time"], sig["prediction"]):
        fig.add_annotation(
            x=ts, y=0.99,
            xref="x", yref="y2 domain",
            text="▲",
            showarrow=False,
            font={"color": _GREEN, "size": 13},
            xanchor="center", yanchor="top",
        )


def _short_signal_markers(
    fig,
    df: pd.DataFrame,
    short_entry_threshold: float | None,
) -> None:
    if short_entry_threshold is None or "short_prediction" not in df.columns:
        return
    mask = df["short_prediction"].notna() & (df["short_prediction"] >= float(short_entry_threshold))
    sig = df[mask]
    if sig.empty:
        return
    for ts, pred in zip(sig["open_time"], sig["short_prediction"]):
        fig.add_annotation(
            x=ts, y=0.01,
            xref="x", yref="y2 domain",
            text="▼",
            showarrow=False,
            font={"color": _RED, "size": 13},
            xanchor="center", yanchor="bottom",
        )


def _style_figure(fig, x_start=None, x_end=None, y2_range=None):
    xkw: dict = dict(
        title_text="",
        tickfont={"color": _MUTED, "size": 11},
        linecolor=_GRID, linewidth=1, mirror=False,
        showgrid=True, gridcolor=_GRID,
        rangeslider={"visible": False},
        nticks=9, tickformat="%H:%M<br>%m-%d",
        showspikes=True, spikemode="across",
        spikesnap="cursor", spikedash="dot", spikecolor=_MUTED,
    )
    if x_start is not None and x_end is not None:
        xkw["range"] = [x_start.isoformat(), x_end.isoformat()]

    fig.update_layout(
        template="plotly_dark",
        height=820,
        margin={"l": 0, "r": 72, "t": 12, "b": 40},
        paper_bgcolor=_BG,
        plot_bgcolor=_PANEL,
        hovermode="x unified",
        dragmode="pan",
        uirevision="chronoquant-price-prediction-chart",
        showlegend=False,
        font={"color": _TEXT},
        newshape={
            "line": {"color": _GOLD, "width": 2},
            "fillcolor": "rgba(240,185,11,0.10)",
            "opacity": 0.85,
        },
    )
    fig.update_xaxes(**xkw)
    fig.update_yaxes(
        title_text="Long",
        title_font={"color": "#5b8af5", "size": 11},
        tickfont={"color": _MUTED, "size": 11},
        linecolor=_GRID, linewidth=1,
        mirror=False, side="right",
        showgrid=True, gridcolor=_GRID,
        zeroline=False, range=[-0.02, 1.02],
        tickformat=".2f", fixedrange=False,
        row=1, col=1,
    )
    y2_kw: dict = dict(
        title_text="",
        tickfont={"color": _MUTED, "size": 12},
        linecolor=_GRID, linewidth=1,
        mirror=False, side="right",
        showgrid=True, gridcolor=_GRID,
        zeroline=False, tickformat=",.2f",
        fixedrange=False,
    )
    if y2_range is not None:
        y2_kw["range"] = y2_range
    fig.update_yaxes(**y2_kw, row=2, col=1)
    fig.update_yaxes(
        title_text="Short",
        title_font={"color": "#e05252", "size": 11},
        tickfont={"color": _MUTED, "size": 11},
        linecolor=_GRID, linewidth=1,
        mirror=False, side="right",
        showgrid=True, gridcolor=_GRID,
        zeroline=False, range=[-0.02, 1.02],
        tickformat=".2f", fixedrange=False,
        row=3, col=1,
    )
    return fig


PLOTLY_CHART_CONFIG = {
    "scrollZoom": True,
    "displaylogo": False,
    "modeBarButtonsToAdd": [
        "drawline",
        "drawopenpath",
        "drawclosedpath",
        "drawcircle",
        "drawrect",
        "eraseshape",
    ],
    "toImageButtonOptions": {
        "format": "png",
        "filename": "chronoquant_chart",
        "height": 900,
        "width": 1500,
        "scale": 2,
    },
}


def equity_figure(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(12, 4))
    if df.empty:
        ax.text(0.5, 0.5, "No equity data", ha="center", va="center")
        fig.tight_layout()
        return fig

    time_col   = "open_time" if "open_time" in df.columns else df.columns[0]
    equity_col = "equity" if "equity" in df.columns else df.select_dtypes("number").columns[-1]
    ax.plot(pd.to_datetime(df[time_col]), pd.to_numeric(df[equity_col], errors="coerce"), color="#0f766e")
    ax.set_title("Equity curve")
    ax.set_ylabel("Equity")
    ax.grid(alpha=0.25)
    fig.autofmt_xdate(rotation=25, ha="right")
    fig.tight_layout()
    return fig
