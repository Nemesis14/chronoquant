# =============================================================================
# Dashboard chart helpers
# =============================================================================

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go


def prediction_price_figure(
    df: pd.DataFrame,
    entry_threshold: float | None = None,
    rearm_threshold: float | None = None,
    exit_threshold: float | None = None,
):
    fig = go.Figure()

    if df.empty:
        fig.add_annotation(text="No price data", x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False)
        return _style_price_figure(fig)

    plot_df = df.copy()
    plot_df["open_time"] = pd.to_datetime(plot_df["open_time"], errors="coerce")
    for col in ["open", "high", "low", "close"]:
        if col in plot_df.columns:
            plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")
    plot_df = plot_df.dropna(subset=["open_time"]).reset_index(drop=True)
    if plot_df.empty:
        fig.add_annotation(text="No valid timestamps", x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False)
        return _style_price_figure(fig)

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
            ),
        )
    elif "close" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=plot_df["open_time"],
                y=plot_df["close"],
                mode="lines",
                name="close",
                line={"color": "#111827", "width": 1.8},
            ),
        )

    return _style_price_figure(fig)


def _has_ohlc(df: pd.DataFrame) -> bool:
    required = {"open", "high", "low", "close"}
    return required.issubset(df.columns) and not df[list(required)].dropna(how="any").empty


def _style_price_figure(fig):
    fig.update_layout(
        template="plotly_white",
        height=620,
        margin={"l": 24, "r": 72, "t": 28, "b": 64},
        paper_bgcolor="#ffffff",
        plot_bgcolor="#fbfdff",
        hovermode="x unified",
        dragmode="pan",
        uirevision="chronoquant-price-chart",
        showlegend=False,
        newshape={
            "line": {"color": "#7c3aed", "width": 2},
            "fillcolor": "rgba(124, 58, 237, 0.10)",
            "opacity": 0.85,
        },
    )
    fig.update_xaxes(
        title_text="Ido",
        title_font={"color": "#111827", "size": 14},
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
    fig.update_yaxes(
        title_text="Arfolyam",
        title_font={"color": "#111827", "size": 14},
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
