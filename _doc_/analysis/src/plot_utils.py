"""Shared plotting and display utilities for _doc_/analysis/ notebooks.

Import at the top of every Setup cell:
    import plot_utils as pu
    pu.apply_theme()
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from IPython.display import HTML
from IPython.display import display as _ipy_display

# ── Project palette ────────────────────────────────────────────────────────────
ACCENT        = "#343a40"   # dark (neutral primary)
LONG_COLOR    = "#198754"   # Bootstrap green — long signals, positive
SHORT_COLOR   = "#dc3545"   # Bootstrap red  — short signals, negative
NEUTRAL_COLOR = "#6c757d"   # Bootstrap secondary gray

_PALETTE = [ACCENT, SHORT_COLOR, LONG_COLOR, NEUTRAL_COLOR, "#0d6efd", "#fd7e14"]

# ── Column type helpers ────────────────────────────────────────────────────────
_PCT_PATTERNS = ("_pct", "pct_", "_rate", "rate_")


def _is_pct_col(name: str) -> bool:
    n = name.lower()
    return any(p in n for p in _PCT_PATTERNS)


def _is_int_valued(s: pd.Series) -> bool:
    """True if a float64 column has no fractional part."""
    if not pd.api.types.is_float_dtype(s):
        return False
    notna = s.dropna()
    if len(notna) == 0:
        return False
    return bool((notna == notna.astype("int64").astype("float64")).all())


# ── Theme ─────────────────────────────────────────────────────────────────────

def apply_theme() -> None:
    """Set project-wide seaborn + matplotlib defaults.

    Call once in the Setup cell before any plotting.
    """
    sns.set_theme(
        style="whitegrid",
        palette=_PALETTE,
        font_scale=1.0,
        rc={
            "figure.dpi":        110,
            "figure.figsize":    (14, 5),
            "axes.spines.right": False,
            "axes.spines.top":   False,
            "grid.alpha":        0.6,
            "grid.linestyle":    "--",
            "grid.linewidth":    0.9,
            "grid.color":        "#9ba5b0",
            "axes.facecolor":    "#fafafa",
            "figure.facecolor":  "#ffffff",
            "axes.titlesize":    13,
            "axes.titleweight":  "bold",
            "axes.titlepad":     10,
            "axes.labelsize":    13,
            "axes.labelweight":  "bold",
            "axes.labelcolor":   "#212529",
            "font.weight":       "bold",
            "xtick.labelsize":   11,
            "ytick.labelsize":   11,
            "xtick.color":       "#495057",
            "ytick.color":       "#495057",
            "xtick.major.pad":   6,
            "ytick.major.pad":   6,
            "legend.fontsize":   9,
            "legend.framealpha": 0.88,
            "legend.edgecolor":  "#dee2e6",
        },
    )


# ── DataFrame display ─────────────────────────────────────────────────────────

def style_df(
    df: pd.DataFrame,
    max_rows: int = 30,
    precision: int = 4,
) -> "pd.io.formats.style.Styler":
    """Return a styled pandas DataFrame for Quarto notebook display.

    Auto-format rules:
    - int64 columns              → {:,}   (thousands separator)
    - float cols with no fraction → {:,.0f}
    - columns named *_pct/*_rate  → {:.2f}%
    - other float columns         → {:,.{precision}f}
    - text / bool                 → left-aligned
    - headers                     → centered, dark bg (#212529)

    Never call df.style.set_caption() — use display_table(caption=) or #| tbl-cap: instead
    (set_caption triggers a Quarto 1.4 Lua filter crash).
    """
    num_cols     = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    str_cols     = [c for c in df.columns if c not in num_cols]
    int_cols     = [c for c in num_cols if pd.api.types.is_integer_dtype(df[c])]
    int_val_cols = [c for c in num_cols if c not in int_cols and _is_int_valued(df[c])]
    pct_cols     = [c for c in num_cols if _is_pct_col(c) and c not in int_cols and c not in int_val_cols]
    float_cols   = [c for c in num_cols if c not in int_cols and c not in int_val_cols and c not in pct_cols]

    styler = df.head(max_rows).style

    styler = styler.set_table_styles([
        {"selector": "th",
         "props": [("text-align", "center"), ("font-weight", "600"),
                   ("background-color", "#212529"), ("color", "#ffffff"),
                   ("padding", "8px 14px"), ("font-size", "0.82rem"),
                   ("white-space", "nowrap"), ("border-right", "1px solid #495057")]},
        {"selector": "td",
         "props": [("padding", "6px 14px"),
                   ("border-bottom", "1px solid #dee2e6"),
                   ("border-right", "1px solid #adb5bd")]},
        {"selector": "tr:nth-child(even) td",
         "props": [("background-color", "#f8f9fa")]},
        {"selector": "tr:nth-child(odd) td",
         "props": [("background-color", "#ffffff")]},
        {"selector": "tr:hover td",
         "props": [("background-color", "#e9ecef")]},
    ])

    if num_cols:
        styler = styler.set_properties(
            subset=num_cols,
            **{"text-align": "right", "font-variant-numeric": "tabular-nums"},
        )
    if str_cols:
        styler = styler.set_properties(subset=str_cols, **{"text-align": "left"})

    fmt: dict[str, str | object] = {}
    for c in int_cols:      fmt[c] = "{:,}"
    for c in int_val_cols:  fmt[c] = "{:,.0f}"
    for c in pct_cols:      fmt[c] = "{:.2f}%"
    for c in float_cols:    fmt[c] = f"{{:,.{precision}f}}"

    if fmt:
        styler = styler.format(fmt, na_rep="—")

    return styler


def display_table(
    df: pd.DataFrame,
    caption: str = "",
    max_rows: int = 30,
    precision: int = 4,
) -> None:
    """Display a styled DataFrame with an optional caption.

    Caption is rendered as a <p> tag (not <caption>) to avoid a Quarto 1.4 crash.
    When using #| tbl-cap: in the cell, omit the caption= argument.
    """
    if caption:
        _ipy_display(HTML(
            f'<p style="font-weight:700;font-size:0.88rem;color:#212529;'
            f'margin:0.8rem 0 0.3rem 0;">{caption}</p>'
        ))
    _ipy_display(style_df(df, max_rows=max_rows, precision=precision))


# ── Plot Templates ─────────────────────────────────────────────────────────────
#
# Every template returns (fig, ax) — or (fig, ax1, ax2) for dual_axis.
# The caller adds any extra customisation, then calls:
#     plt.tight_layout()
#     plt.show()


def timeseries(
    df: pd.DataFrame,
    x: str,
    y: str,
    *,
    fill_lo: str | None = None,
    fill_hi: str | None = None,
    ylabel: str = "",
    title: str = "",
    color: str | None = None,
    figsize: tuple[int, int] = (14, 4),
) -> tuple[plt.Figure, plt.Axes]:
    """Line plot with optional min/max fill band.

    Example (from analysis_ohlcv — monthly close with range):
        fig, ax = pu.timeseries(monthly, "month", "avg_close",
                                fill_lo="min_close", fill_hi="max_close",
                                ylabel="Close (USDT)", title="SOL/USDT havi close")
        plt.tight_layout(); plt.show()
    """
    fig, ax = plt.subplots(figsize=figsize)
    c = color or ACCENT
    ax.plot(df[x], df[y], lw=1.2, color=c)
    if fill_lo and fill_hi:
        ax.fill_between(df[x], df[fill_lo], df[fill_hi], alpha=0.15, color=c)
    if ylabel:
        ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    return fig, ax


def bar_monthly(
    df: pd.DataFrame,
    x: str,
    y: str,
    *,
    ylabel: str = "",
    title: str = "",
    color: str = "steelblue",
    figsize: tuple[int, int] = (14, 4),
) -> tuple[plt.Figure, plt.Axes]:
    """Monthly bar chart (width=20 calendar days, alpha=0.7).

    Example (from analysis_ohlcv — monthly volume):
        fig, ax = pu.bar_monthly(monthly, "month", "avg_volume",
                                 ylabel="Átlag volume", title="Havi volume")
        plt.tight_layout(); plt.show()
    """
    fig, ax = plt.subplots(figsize=figsize)
    ax.bar(df[x], df[y], width=20, color=color, alpha=0.7)
    if ylabel:
        ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    return fig, ax


def kde_by_group(
    df: pd.DataFrame,
    value_col: str,
    group_col: str,
    *,
    log1p: bool = False,
    ylabel: str = "",
    xlabel: str = "",
    title: str = "",
    figsize: tuple[int, int] = (10, 5),
) -> tuple[plt.Figure, plt.Axes]:
    """KDE per group using tab10 palette. Groups sorted ascending.

    Example (from analysis_ohlcv — volume KDE by year):
        fig, ax = pu.kde_by_group(df_v, "volume", "year",
                                  log1p=True, xlabel="log(volume+1)")
        plt.tight_layout(); plt.show()
    """
    groups = sorted(df[group_col].unique())
    palette = sns.color_palette("tab10", len(groups))
    fig, ax = plt.subplots(figsize=figsize)
    for i, g in enumerate(groups):
        vals = df[df[group_col] == g][value_col].dropna()
        if log1p:
            vals = np.log1p(vals)
        sns.kdeplot(vals, ax=ax, label=str(g), color=palette[i], lw=1.5)
    ax.legend(fontsize=8)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    return fig, ax


def quarterly_boxplot(
    df: pd.DataFrame,
    x: str,
    y: str,
    *,
    ylabel: str = "",
    title: str = "",
    figsize: tuple[int, int] = (18, 5),
) -> tuple[plt.Figure, plt.Axes]:
    """Boxplot by quarter column. Shows year label only on Q1 ticks.

    The x column must contain strings like "2022-Q1", "2022-Q2", etc.

    Example (from analysis_ohlcv — quarterly close distribution):
        fig, ax = pu.quarterly_boxplot(df_q, "quarter", "close",
                                       ylabel="Close (USDT)")
        plt.tight_layout(); plt.show()
    """
    order = sorted(df[x].unique())
    fig, ax = plt.subplots(figsize=figsize)
    sns.boxplot(data=df, x=x, y=y, order=order,
                fliersize=1, linewidth=0.8, ax=ax)
    tick_labels = [t.get_text() for t in ax.get_xticklabels()]
    year_labels  = [t[:4] if t.endswith("Q1") else "" for t in tick_labels]
    ax.set_xticklabels(year_labels, rotation=0, ha="center", fontsize=14)
    ax.set_xlabel("")
    if ylabel:
        ax.set_ylabel(ylabel)
    if title:
        ax.set_title(title)
    return fig, ax


def dual_axis(
    df: pd.DataFrame,
    x: str,
    y_line: str,
    y_fill: str,
    *,
    line_label: str = "",
    line_color: str | None = None,
    fill_color: str | None = None,
    y1_label: str = "",
    y2_label: str = "",
    hline: float | None = None,
    title: str = "",
    figsize: tuple[int, int] = (14, 4),
) -> tuple[plt.Figure, plt.Axes, plt.Axes]:
    """Line on left y-axis + fill_between on right y-axis.

    Returns (fig, ax1, ax2). Caller can add ax1.legend(), ax1.set_ylim(), etc.

    Example (from analysis_ohlcv — taker buy ratio + quote volume):
        fig, ax1, ax2 = pu.dual_axis(
            taker, "month", "taker_ratio", "avg_quote_vol",
            line_label="Taker buy arány", y1_label="Taker buy arány",
            y2_label="Átlag quote volume", hline=0.5,
            title="Havi taker buy arány és quote volume",
        )
        ax1.set_ylim(0.4, 0.6)
        plt.tight_layout(); plt.show()
    """
    lc = line_color or LONG_COLOR
    fc = fill_color or SHORT_COLOR
    fig, ax1 = plt.subplots(figsize=figsize)
    ax2 = ax1.twinx()
    ax1.plot(df[x], df[y_line], color=lc, lw=1.5, label=line_label or y_line)
    if hline is not None:
        ax1.axhline(hline, color="grey", ls="--", lw=0.8)
    ax2.fill_between(df[x], df[y_fill], alpha=0.15, color=fc)
    if y1_label:
        ax1.set_ylabel(y1_label)
    if y2_label:
        ax2.set_ylabel(y2_label)
    if title:
        ax1.set_title(title)
    if line_label:
        ax1.legend(loc="upper left")
    return fig, ax1, ax2


def stacked_panels(
    n: int,
    *,
    panel_height: int = 4,
    width: int = 14,
    sharex: bool = True,
) -> tuple[plt.Figure, list[plt.Axes]]:
    """Create n vertically stacked panels sharing the x-axis.

    Returns (fig, [ax0, ax1, ...]). Each panel is panel_height inches tall.

    Example:
        fig, axes = pu.stacked_panels(3)
        axes[0].plot(monthly["month"], monthly["avg_close"])
        axes[1].bar(monthly["month"], monthly["avg_volume"], width=20)
        axes[2].bar(monthly["month"], monthly["candles"], width=20, color="salmon")
        plt.tight_layout(); plt.show()
    """
    fig, axes = plt.subplots(n, 1, figsize=(width, panel_height * n), sharex=sharex)
    if n == 1:
        axes = [axes]
    return fig, list(axes)


def hbar_features(
    df: pd.DataFrame,
    y: str,
    x: str,
    *,
    color: str = "salmon",
    title: str = "",
    figsize: tuple[int, int] | None = None,
) -> tuple[plt.Figure, plt.Axes]:
    """Horizontal bar chart auto-sized by row count. Inverts y-axis (largest first).

    Example:
        fig, ax = pu.hbar_features(feat_nulls.head(40), "column", "null_pct",
                                   title="Top 40 feature NA arány")
        ax.set_xlabel("NA %")
        plt.tight_layout(); plt.show()
    """
    h = figsize[1] if figsize else max(3, 0.35 * len(df))
    w = figsize[0] if figsize else 12
    fig, ax = plt.subplots(figsize=(w, h))
    ax.barh(df[y], df[x], color=color, alpha=0.8)
    ax.invert_yaxis()
    if title:
        ax.set_title(title)
    return fig, ax
