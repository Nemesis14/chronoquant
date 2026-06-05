# =============================================================================
# Dashboard chart helpers
# =============================================================================

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def prediction_price_figure(
    df: pd.DataFrame,
    entry_threshold: float | None = None,
    rearm_threshold: float | None = None,
    exit_threshold: float | None = None,
    short_entry_threshold: float | None = None,
    short_rearm_threshold: float | None = None,
    short_exit_threshold: float | None = None,
):
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.20, 0.60, 0.20],
        vertical_spacing=0.03,
    )

    if df.empty:
        fig.add_annotation(text="No prediction data", x=0.5, y=0.90, xref="paper", yref="paper", showarrow=False)
        fig.add_annotation(text="No price data",      x=0.5, y=0.50, xref="paper", yref="paper", showarrow=False)
        fig.add_annotation(text="No prediction data", x=0.5, y=0.10, xref="paper", yref="paper", showarrow=False)
        return _style_price_figure(fig)

    plot_df = df.copy()
    plot_df["open_time"] = pd.to_datetime(plot_df["open_time"], errors="coerce")
    for col in ["open", "high", "low", "close"]:
        if col in plot_df.columns:
            plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")
    for col in ["prediction", "short_prediction"]:
        if col in plot_df.columns:
            plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")
    plot_df = plot_df.dropna(subset=["open_time"]).reset_index(drop=True)
    if plot_df.empty:
        fig.add_annotation(text="No valid timestamps", x=0.5, y=0.50, xref="paper", yref="paper", showarrow=False)
        return _style_price_figure(fig)

    # --- Row 1: Long prediction panel ---
    if "prediction" in plot_df.columns and not plot_df["prediction"].dropna().empty:
        fig.add_trace(
            go.Scatter(
                x=plot_df["open_time"],
                y=plot_df["prediction"],
                mode="lines",
                name="long prediction",
                line={"color": "#2563eb", "width": 1.9},
                hovertemplate="long pred<br>%{x}<br>%{y:.4f}<extra></extra>",
                showlegend=False,
            ),
            row=1, col=1,
        )
    else:
        fig.add_annotation(text="No long prediction data", x=0.5, y=0.90, xref="paper", yref="paper", showarrow=False)

    _add_threshold_trace(fig, plot_df, entry_threshold, "entry", "#16a34a", row=1)
    _add_threshold_trace(fig, plot_df, rearm_threshold,  "rearm",  "#64748b", row=1)
    _add_threshold_trace(fig, plot_df, exit_threshold,   "exit",   "#dc2626", row=1)
    _add_threshold_panel_legend(
        fig,
        [(entry_threshold, "entry", "#16a34a"), (rearm_threshold, "rearm", "#64748b"), (exit_threshold, "exit", "#dc2626")],
        y_domain_ref="y domain",
    )

    # --- Row 2: Candlestick / price ---
    has_ohlc = _has_ohlc(plot_df)
    if has_ohlc:
        candle_df = plot_df.dropna(subset=["open", "high", "low", "close"]).copy()
        fig.add_trace(
            go.Candlestick(
                x=candle_df["open_time"],
                open=candle_df["open"],
                high=candle_df["high"],
                low=candle_df["low"],
                close=candle_df["close"],
                name="OHLC",
                increasing_line_color="#0ECB81",
                increasing_fillcolor="#0ECB81",
                decreasing_line_color="#F6465D",
                decreasing_fillcolor="#F6465D",
                whiskerwidth=0.45,
                showlegend=False,
            ),
            row=2, col=1,
        )
    elif "close" in plot_df.columns:
        fig.add_trace(
            go.Scatter(
                x=plot_df["open_time"],
                y=plot_df["close"],
                mode="lines",
                name="close",
                line={"color": "#111827", "width": 1.8},
                showlegend=False,
            ),
            row=2, col=1,
        )

    # Entry signal triangles on the candle chart
    high_col = plot_df["high"] if "high" in plot_df.columns else plot_df.get("close")
    if high_col is not None:
        _add_long_signal_markers(fig, plot_df, high_col, entry_threshold)
        _add_short_signal_markers(fig, plot_df, high_col, short_entry_threshold)

    # --- Row 3: Short prediction panel ---
    if "short_prediction" in plot_df.columns and not plot_df["short_prediction"].dropna().empty:
        fig.add_trace(
            go.Scatter(
                x=plot_df["open_time"],
                y=plot_df["short_prediction"],
                mode="lines",
                name="short prediction",
                line={"color": "#dc2626", "width": 1.9},
                hovertemplate="short pred<br>%{x}<br>%{y:.4f}<extra></extra>",
                showlegend=False,
            ),
            row=3, col=1,
        )
    else:
        fig.add_annotation(text="No short prediction data", x=0.5, y=0.10, xref="paper", yref="paper", showarrow=False)

    _add_threshold_trace(fig, plot_df, short_entry_threshold, "entry", "#16a34a", row=3)
    _add_threshold_trace(fig, plot_df, short_rearm_threshold,  "rearm",  "#64748b", row=3)
    _add_threshold_trace(fig, plot_df, short_exit_threshold,   "exit",   "#dc2626", row=3)
    _add_threshold_panel_legend(
        fig,
        [(short_entry_threshold, "entry", "#16a34a"), (short_rearm_threshold, "rearm", "#64748b"), (short_exit_threshold, "exit", "#dc2626")],
        y_domain_ref="y3 domain",
    )

    return _style_price_figure(fig)


def _has_ohlc(df: pd.DataFrame) -> bool:
    required = {"open", "high", "low", "close"}
    return required.issubset(df.columns) and not df[list(required)].dropna(how="any").empty


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
    threshold = float(value)
    fig.add_trace(
        go.Scatter(
            x=[df["open_time"].min(), df["open_time"].max()],
            y=[threshold, threshold],
            mode="lines",
            name=label,
            line={"color": color, "width": 1.2, "dash": "dash"},
            hovertemplate=f"{label}<br>%{{y:.4f}}<extra></extra>",
            showlegend=False,
        ),
        row=row, col=1,
    )


def _add_threshold_panel_legend(
    fig,
    items: list[tuple[float | None, str, str]],
    y_domain_ref: str = "y domain",
) -> None:
    valid_items = [(float(v), label, color) for v, label, color in items if v is not None]
    if not valid_items:
        return
    x_positions = [0.01, 0.22, 0.43]
    for x_pos, (value, label, color) in zip(x_positions, valid_items):
        fig.add_annotation(
            x=x_pos,
            y=0.97,
            xref="paper",
            yref=y_domain_ref,
            text=f"{label} {value:.3f}",
            showarrow=False,
            xanchor="left",
            yanchor="top",
            align="left",
            font={"color": color, "size": 11},
            bgcolor="rgba(255, 255, 255, 0.86)",
            bordercolor=color,
            borderwidth=1,
            borderpad=3,
        )


def _add_long_signal_markers(
    fig,
    df: pd.DataFrame,
    high_col: pd.Series,
    entry_threshold: float | None,
) -> None:
    if entry_threshold is None or "prediction" not in df.columns:
        return
    mask = df["prediction"].notna() & (df["prediction"] >= float(entry_threshold))
    signals = df[mask]
    if signals.empty:
        return
    fig.add_trace(
        go.Scatter(
            x=signals["open_time"],
            y=high_col[mask] * 1.0015,
            mode="markers",
            name="long signal",
            marker={
                "symbol": "triangle-up",
                "color": "#16a34a",
                "size": 9,
                "line": {"width": 0},
            },
            hovertemplate="long signal<br>p=%{customdata:.3f}<extra></extra>",
            customdata=signals["prediction"].values,
            showlegend=False,
        ),
        row=2, col=1,
    )


def _add_short_signal_markers(
    fig,
    df: pd.DataFrame,
    high_col: pd.Series,
    short_entry_threshold: float | None,
) -> None:
    if short_entry_threshold is None or "short_prediction" not in df.columns:
        return
    mask = df["short_prediction"].notna() & (df["short_prediction"] >= float(short_entry_threshold))
    signals = df[mask]
    if signals.empty:
        return
    fig.add_trace(
        go.Scatter(
            x=signals["open_time"],
            y=high_col[mask] * 1.0015,
            mode="markers",
            name="short signal",
            marker={
                "symbol": "triangle-down",
                "color": "#dc2626",
                "size": 9,
                "line": {"width": 0},
            },
            hovertemplate="short signal<br>p=%{customdata:.3f}<extra></extra>",
            customdata=signals["short_prediction"].values,
            showlegend=False,
        ),
        row=2, col=1,
    )


def _style_price_figure(fig):
    fig.update_layout(
        template="plotly_white",
        height=760,
        margin={"l": 24, "r": 72, "t": 24, "b": 62},
        paper_bgcolor="#ffffff",
        plot_bgcolor="#fbfdff",
        hovermode="x unified",
        dragmode="pan",
        uirevision="chronoquant-price-prediction-chart",
        showlegend=False,
        newshape={
            "line": {"color": "#7c3aed", "width": 2},
            "fillcolor": "rgba(124, 58, 237, 0.10)",
            "opacity": 0.85,
        },
    )
    fig.update_xaxes(
        title_text="",
        tickfont={"color": "#111827", "size": 12},
        linecolor="#111827",
        linewidth=1.2,
        mirror=True,
        showgrid=True,
        gridcolor="#e5eaf2",
        rangeslider={"visible": False},
        nticks=9,
        tickformat="%H:%M<br>%m-%d",
        showspikes=True,
        spikemode="across",
        spikesnap="cursor",
        spikedash="dot",
        spikecolor="#64748b",
    )
    # Row 1 — long prediction y-axis
    fig.update_yaxes(
        title_text="Long",
        title_font={"color": "#2563eb", "size": 12},
        tickfont={"color": "#111827", "size": 11},
        linecolor="#111827",
        linewidth=1.2,
        mirror=True,
        side="right",
        showgrid=True,
        gridcolor="#e5eaf2",
        zeroline=False,
        range=[-0.02, 1.02],
        tickformat=".2f",
        fixedrange=False,
        row=1, col=1,
    )
    # Row 2 — price y-axis
    fig.update_yaxes(
        title_text="",
        tickfont={"color": "#111827", "size": 12},
        linecolor="#111827",
        linewidth=1.2,
        mirror=True,
        side="right",
        showgrid=True,
        gridcolor="#e5eaf2",
        zeroline=False,
        tickformat=",.2f",
        fixedrange=False,
        row=2, col=1,
    )
    # Row 3 — short prediction y-axis
    fig.update_yaxes(
        title_text="Short",
        title_font={"color": "#dc2626", "size": 12},
        tickfont={"color": "#111827", "size": 11},
        linecolor="#111827",
        linewidth=1.2,
        mirror=True,
        side="right",
        showgrid=True,
        gridcolor="#e5eaf2",
        zeroline=False,
        range=[-0.02, 1.02],
        tickformat=".2f",
        fixedrange=False,
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

    time_col = "open_time" if "open_time" in df.columns else df.columns[0]
    equity_col = "equity" if "equity" in df.columns else df.select_dtypes("number").columns[-1]
    ax.plot(pd.to_datetime(df[time_col]), pd.to_numeric(df[equity_col], errors="coerce"), color="#0f766e")
    ax.set_title("Equity curve")
    ax.set_ylabel("Equity")
    ax.grid(alpha=0.25)
    fig.autofmt_xdate(rotation=25, ha="right")
    fig.tight_layout()
    return fig
