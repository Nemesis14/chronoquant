# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false
"""General report ``wave_event_dataset``: wave-start event dataset — visual checkpoint (epic_072 / t918).

Source: live DB ``ohlcv`` (SOLUSDT 1m, read-only) -> :mod:`reports.builders.wave_events` (definitions of
``_doc_/methodology_doc/5200_wave_start_image_model.md``). Writes the dataset to the LAB DB
(``research."wave_events_v1"``, unless ``store=False``) and the report to ``_doc_/reports/wave_event_dataset/``.

OOS hygiene (epic_071 lesson): every example, chart, distribution and label rate is TRAIN-only; the OOS and
holdout-2 events are built, stored and labelled in the table, but the report shows them only as counts.

Regenerate::

    uv run python -m reports.builders.wave_event_dataset            (cwd: repo root; --no-store: report only)
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from reports import ReportBuilder, theme
from reports.builders import wave_events as we
from reports.builders._common import fmt, source_info, themed
from reports.builders.sr_detection import load_ohlcv_1m

logger = logging.getLogger(__name__)

SLUG = "wave_event_dataset"
GENERATOR = "reports.builders.wave_event_dataset"
MIN_TRAIN = 500
MIN_OOS = 150
OVERVIEW_DAYS = 75
EXAMPLES_PER_CLASS = 3
EXAMPLE_CONTEXT_FRAC = 1.0      # extra history before the prefix, in units of W
CLASS_COLOR = {"impulse": theme.LONG, "t0_below": theme.SHORT, "false_start": theme.SHORT, "sideways": theme.AMBER}
CLASS_FILL = {"impulse": "rgba(25,135,84,0.13)", "t0_below": "rgba(220,53,69,0.13)", "false_start": "rgba(220,53,69,0.13)",
              "sideways": "rgba(240,185,11,0.13)"}
CANDLE_UP = "#8b949e"
CANDLE_DOWN = "#5c6670"
CELL_HU = {c: c.replace("W", "W=").replace("_H", ", H=") for c, _, _ in we.CELLS}


def _ts(value: Any) -> str:
    return "-" if value is None else pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")


def _cell_title(cell_id: str) -> str:
    return f"({CELL_HU.get(cell_id, cell_id)})"


# --------------------------------------------------------------------------- tables


def _summary(ds: we.EventDataset) -> pd.DataFrame:
    ev = ds.events
    rows = []
    train_days = (pd.Timestamp(ds.params.oos_start) - pd.Timestamp(ds.params.train_start)).days
    for cell_id, w, h in ds.params.cells:
        e = ev.filter(pl.col("cell_id") == cell_id)
        tr = e.filter(pl.col("split") == "train")
        n_tr = tr.height
        cnt = {s: int((e["split"] == s).sum()) for s in ("train", "oos", "holdout2", "purged")}

        def rate(cls: str, tr: pl.DataFrame = tr) -> float | None:
            return float((tr["label_class"] == cls).mean()) if tr.height else None  # type: ignore[arg-type]

        gaps = tr["t0"].diff().dt.total_seconds().drop_nulls() / 86400 if n_tr > 1 else pl.Series([], dtype=pl.Float64)
        rows.append({
            "cell": cell_id, "w_h": f"{w // 60}h / {h // 60}h",
            "train": cnt["train"], "oos": cnt["oos"], "holdout2": cnt["holdout2"], "purged": cnt["purged"],
            "impulse_rate": rate("impulse"), "t0_below_rate": rate("t0_below"), "sideways_rate": rate("sideways"),
            "n_impulse": int((tr["label_class"] == "impulse").sum()),
            "per_month": n_tr / (train_days / 30.44),
            "gap_days_median": float(gaps.median()) if gaps.len() else None,  # type: ignore[arg-type]
            "min_train_ok": "igen" if n_tr >= MIN_TRAIN else "NEM",
            "min_oos_ok": "igen (szám)" if cnt["oos"] >= MIN_OOS else "NEM",
        })
    return pd.DataFrame(rows)


def _yearly(ds: we.EventDataset) -> pd.DataFrame:
    tr = ds.events.filter(pl.col("split") == "train").with_columns(pl.col("t0").dt.year().alias("year"))
    if tr.height == 0:
        return pd.DataFrame({"cell": []})
    g = (tr.group_by("cell_id", "year").agg(
        pl.len().alias("n"),
        (pl.col("label_class") == "impulse").mean().alias("impulse_rate"),
        (pl.col("label_class") == "t0_below").mean().alias("t0_below_rate"),
        (pl.col("label_class") == "sideways").mean().alias("sideways_rate"),
        pl.col("a_sigma").median().alias("a_sigma_median"),
        ((pl.col("a_log").exp() - 1) * 100).median().alias("a_pct_median"),
    ))
    order = {c[0]: k for k, c in enumerate(ds.params.cells)}
    g = g.with_columns(pl.col("cell_id").replace_strict(order, return_dtype=pl.Int32).alias("_o")).sort(["_o", "year"]).drop("_o")
    return g.rename({"cell_id": "cell"}).with_columns(pl.col("year").cast(pl.Utf8)).to_pandas()


def _year_range(tr: pl.DataFrame, cells: Any) -> list[tuple[str, int, int]]:
    """(cell, min, max) of the yearly train counts over the full calendar years (the partial last year excluded)."""
    out = []
    for c in cells:
        y = (tr.filter(pl.col("cell_id") == c).group_by(pl.col("t0").dt.year().alias("y")).len()
             .filter(pl.col("y") < pl.col("y").max()))
        if y.height:
            out.append((str(c), int(y["len"].min()), int(y["len"].max())))  # type: ignore[arg-type]
    return out


def _quintiles(ds: we.EventDataset) -> pd.DataFrame:
    """Raw train label rates by A/sigma_W quintile (within the cell) — the scalar baseline's raw signal."""
    rows = []
    for cell_id, _, _ in ds.params.cells:
        tr = ds.events.filter((pl.col("cell_id") == cell_id) & (pl.col("split") == "train"))
        if tr.height < 10:
            continue
        x = tr["a_sigma"].to_numpy()
        edges = np.quantile(x, [0.2, 0.4, 0.6, 0.8])
        q = np.searchsorted(edges, x, side="right")
        cls = tr["label_class"].to_numpy()
        for k in range(5):
            sel = q == k
            rows.append({
                "cell": cell_id, "quintile": f"Q{k + 1}", "a_sigma_lo": float(x[sel].min()), "a_sigma_hi": float(x[sel].max()),
                "n": int(sel.sum()), "impulse_rate": float((cls[sel] == "impulse").mean()),
                "t0_below_rate": float((cls[sel] == "t0_below").mean()), "sideways_rate": float((cls[sel] == "sideways").mean()),
            })
    return pd.DataFrame(rows)


def _prefix_len(ds: we.EventDataset) -> pd.DataFrame:
    tr = ds.events.filter(pl.col("split") == "train")
    rows = []
    for cell_id, w, _ in ds.params.cells:
        n = tr.filter(pl.col("cell_id") == cell_id)["n_prefix_15m"].to_numpy()
        vals, cnts = np.unique(n, return_counts=True)
        for v, c in zip(vals, cnts, strict=True):
            rows.append({"cell": cell_id, "nominal": 2 * w // 15, "n_prefix_15m": int(v), "n": int(c),
                         "share_pct": float(c / n.size)})
    return pd.DataFrame(rows)


def _sensitivity(df_1m: pl.DataFrame, base: we.EventDataset) -> pd.DataFrame:
    """Counts (train / OOS) and train impulse rate under alternative definitions — NOT used for decisions."""
    variants = [
        ("v1 (alap): A ≥ 1,5·σ_W, nem-átfedés a teljes [t0−B, t_c+H]-ra", base),
        ("nem-átfedés csak a címke-ablakra (5200 szöveg: t0' ≥ t_c+H)",
         we.build_events(df_1m, replace(base.params, overlap_rule="label_window"))),
        ("A ≥ 1,0·σ_W, teljes nem-átfedés", we.build_events(df_1m, replace(base.params, min_a_sigma=1.0))),
        ("A ≥ 1,0·σ_W, címke-ablak nem-átfedés",
         we.build_events(df_1m, replace(base.params, min_a_sigma=1.0, overlap_rule="label_window"))),
        ("nincs nem-átfedés (minden megerősített jelölt, átfedő)",
         we.build_events(df_1m, replace(base.params, overlap_rule="none"))),
    ]
    rows = []
    for name, ds in variants:
        row: dict[str, Any] = {"variant": name}
        for cell_id, _, _ in base.params.cells:
            e = ds.events.filter(pl.col("cell_id") == cell_id)
            tr = e.filter(pl.col("split") == "train")
            row[f"{cell_id}_train"] = tr.height
            row[f"{cell_id}_oos"] = int((e["split"] == "oos").sum())
            row[f"{cell_id}_imp_rate"] = float(tr["label_impulse"].mean()) if tr.height else None  # type: ignore[arg-type]
        rows.append(row)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- distributions


def _hist_fig(ds: we.EventDataset, expr: pl.Expr, edges: np.ndarray, x_title: str) -> go.Figure:
    colors = [theme.BLUE, theme.ORANGE, theme.LONG, theme.SHORT]
    fig = go.Figure()
    mids = (edges[:-1] + edges[1:]) / 2
    for k, (cell_id, _, _) in enumerate(ds.params.cells):
        tr = ds.events.filter((pl.col("cell_id") == cell_id) & (pl.col("split") == "train"))
        if tr.height == 0:
            continue
        x = np.clip(tr.select(expr).to_series().to_numpy(), edges[0], edges[-1] - 1e-9)
        cnt, _ = np.histogram(x, bins=edges)
        fig.add_trace(go.Scatter(x=mids.tolist(), y=(cnt / cnt.sum()).tolist(), mode="lines", line_shape="hvh",
                                 name=_cell_title(cell_id), line={"color": colors[k % 4], "width": 1.8},
                                 hovertemplate="%{x:.2f}: %{y:.1%}<extra>" + cell_id + "</extra>"))
    themed(fig, height=340, hovermode="closest", x_title=x_title)
    fig.update_yaxes(title="Arány (train)", tickformat=".0%")
    return fig


# --------------------------------------------------------------------------- price charts


def _agg(df_1m: pl.DataFrame, lo: datetime, hi: datetime, every: str) -> pl.DataFrame:
    return (df_1m.filter((pl.col("open_time") >= lo) & (pl.col("open_time") < hi)).sort("open_time")
            .group_by_dynamic("open_time", every=every, closed="left", label="left")
            .agg(pl.col("open").first(), pl.col("high").max(), pl.col("low").min(), pl.col("close").last(),
                 pl.col("volume").sum(), pl.len().alias("n1m")))


def _candles(bars: pl.DataFrame, name: str, *, up: str = CANDLE_UP, down: str = CANDLE_DOWN,
             up_fill: str = "rgba(139,148,158,0.15)", opacity: float = 1.0, legend: bool = False) -> go.Candlestick:
    return go.Candlestick(
        x=bars["open_time"].to_list(), open=bars["open"].to_list(), high=bars["high"].to_list(),
        low=bars["low"].to_list(), close=bars["close"].to_list(), name=name, showlegend=legend, opacity=opacity,
        increasing={"line": {"color": up, "width": 1}, "fillcolor": up_fill},
        decreasing={"line": {"color": down, "width": 1}, "fillcolor": down},
    )


def _hover(r: dict[str, Any], hu: dict[str, str] | None = None) -> str:
    sub = (hu or we.CLASS_HU).get(r["label_class"] or "", "-")
    return (f"<b>{r['cell_id']}</b> · {sub}<br>t0 {_ts(r['t0'])} · t_c {_ts(r['t_c'])}<br>"
            f"low[t0] {r['low_t0']:.3f} · close[t_c] {r['close_tc']:.3f} · P* {r['p_star']:.3f}<br>"
            f"A {100 * (np.exp(r['a_log']) - 1):.2f}% · A/σ_W {r['a_sigma']:.2f} · σ_W {100 * r['sigma_w']:.2f}%<br>"
            f"sebesség 1h {r['velocity_1h']:+.2f} · trend 3n {r['pretrend_3d']:+.2f} · log σ_1m {r['log_sigma_1m']:.2f}<br>"
            f"esés-mélység {100 * r['drop_depth']:.2f}% ({r['drop_depth_sigma']:.2f} σ_W) · volumen-arány {r['volume_ratio']:.2f}<br>"
            f"ATR14(15m) {r['atr14_15m']:.3f} · prefix {r['n_prefix_15m']} × 15m<br>"
            f"jövő: MFE {100 * r['fwd_mfe_log']:+.2f}% · MAE {100 * r['fwd_mae_log']:+.2f}% · első: {r['first_hit']}")


def _overview(df_1m: pl.DataFrame, ds: we.EventDataset) -> tuple[go.Figure, dict[str, int], datetime, datetime]:
    hi = datetime.fromisoformat(ds.params.oos_start)
    lo = hi - timedelta(days=OVERVIEW_DAYS)
    bars = _agg(df_1m, lo, hi, "1h")
    fig = go.Figure([_candles(bars, "SOL 1h")])
    counts: dict[str, int] = {}
    for k, (cell_id, _, _) in enumerate(ds.params.cells):
        e = ds.events.filter((pl.col("cell_id") == cell_id) & (pl.col("split") == "train")
                             & (pl.col("event_start") >= lo) & (pl.col("label_end") <= hi))
        counts[cell_id] = e.height
        vis: Any = True if k == 0 else "legendonly"
        group = cell_id
        for cls in we.CLASS_ORDER:
            g = e.filter(pl.col("label_class") == cls)
            if g.height == 0:
                continue
            bx: list[Any] = []
            by: list[Any] = []
            for r in g.iter_rows(named=True):
                bx += [r["t_c"], r["label_end"], r["label_end"], r["t_c"], r["t_c"], None]
                by += [r["low_t0"], r["low_t0"], r["p_star"], r["p_star"], r["low_t0"], None]
            fig.add_trace(go.Scatter(x=bx, y=by, mode="lines", fill="toself", fillcolor=CLASS_FILL[cls],
                                     line={"color": CLASS_COLOR[cls], "width": 0.8}, hoverinfo="skip",
                                     name=f"{cell_id} H-sáv: {we.CLASS_HU[cls]}", legendgroup=group,
                                     showlegend=True, visible=vis))
        rx: list[Any] = []
        ry: list[Any] = []
        rc: list[str] = []
        rs: list[str] = []
        rt: list[Any] = []
        for r in e.iter_rows(named=True):
            h = _hover(r)
            rx += [r["t0"], r["t_c"], None]
            ry += [r["low_t0"], r["close_tc"], None]
            col = CLASS_COLOR.get(r["label_class"], theme.NEUTRAL)
            rc += [col, col, col]
            rs += ["triangle-up", "circle", "circle"]
            rt += [h, h, None]
        if rx:
            fig.add_trace(go.Scatter(x=rx, y=ry, mode="lines+markers", name=f"{cell_id} t0 ▲ → t_c ●",
                                     legendgroup=group, visible=vis, hovertext=rt, hoverinfo="text",
                                     line={"color": theme.BLUE, "width": 1.6},
                                     marker={"color": rc, "symbol": rs, "size": 11, "line": {"width": 1, "color": "#ffffff"}}))
    ylo, yhi = float(bars["low"].min()), float(bars["high"].max())  # type: ignore[arg-type]
    pad = (yhi - ylo) * 0.04
    themed(fig, height=640, hovermode="closest", showlegend=True,
           legend={"orientation": "h", "y": -0.1, "x": 0, "yanchor": "top"}, margin={"l": 56, "r": 16, "t": 28, "b": 96})
    fig.update_xaxes(rangeslider={"visible": False}, type="date")
    fig.update_yaxes(range=[ylo - pad, yhi + pad], title="SOLUSDT")
    return fig, counts, lo, hi


def select_examples(events: pl.DataFrame, cell_id: str, per_class: int = EXAMPLES_PER_CLASS,
                    classes: tuple[str, ...] = we.CLASS_ORDER) -> list[dict[str, Any]]:
    """Deterministic train examples: per class, evenly spaced over time (positions (k + 0.5) / per_class)."""
    out: list[dict[str, Any]] = []
    tr = events.filter((pl.col("cell_id") == cell_id) & (pl.col("split") == "train")).sort("t0")
    for cls in classes:
        g = tr.filter(pl.col("label_class") == cls)
        if g.height == 0:
            continue
        idx = sorted({min(int((k + 0.5) / per_class * g.height), g.height - 1) for k in range(min(per_class, g.height))})
        out.extend(g.row(i, named=True) for i in idx)
    return out


def _example_fig(df_1m: pl.DataFrame, r: dict[str, Any], hu: dict[str, str] | None = None,
                 hi_cap: datetime | None = None) -> go.Figure:
    w = timedelta(minutes=r["w_min"])
    lo = r["prefix_start"] - EXAMPLE_CONTEXT_FRAC * w
    hi = r["label_end"] + timedelta(minutes=30)
    if hi_cap is not None:
        hi = min(hi, hi_cap)
    bars = _agg(df_1m, lo, hi, "15m").with_columns((pl.col("open_time") + timedelta(minutes=15)).alias("close_time"))
    hist = bars.filter(pl.col("open_time") < r["prefix_start"])
    pref = bars.filter((pl.col("open_time") >= r["prefix_start"]) & (pl.col("close_time") <= r["prefix_end"]))
    fut = bars.filter(pl.col("close_time") > r["prefix_end"])
    fig = go.Figure()
    if hist.height:
        fig.add_trace(_candles(hist, "előzmény (nem bemenet)", opacity=0.8, legend=True))
    fig.add_trace(_candles(pref, "prefix = modell-bemenet (15m, zár ≤ t_c)", up=theme.BLUE, down=theme.BLUE,
                           up_fill="rgba(13,110,253,0.12)", legend=True))
    if fut.height:
        fig.add_trace(_candles(fut, "jövő (H, címke-ablak)", opacity=0.35, legend=True))
    fig.add_vrect(x0=r["prefix_start"], x1=r["prefix_end"], fillcolor=theme.BLUE, opacity=0.06, line_width=0,
                  annotation_text="prefix-ablak (2W)", annotation_position="top left",
                  annotation_font={"size": 10, "color": theme.NEUTRAL})
    fig.add_vrect(x0=r["t_c"], x1=r["label_end"], fillcolor=CLASS_COLOR[r["label_class"]], opacity=0.05, line_width=0,
                  annotation_text=f"H = {r['h_min'] // 60}h", annotation_position="top right",
                  annotation_font={"size": 10, "color": theme.NEUTRAL})
    fig.add_vline(x=r["t_c"], line={"color": theme.ACCENT, "width": 1.4, "dash": "dash"})
    fig.add_annotation(x=r["t_c"], y=1.0, yref="paper", text="t_c", showarrow=False, yanchor="bottom",
                       font={"size": 11, "color": theme.NEUTRAL})
    fig.add_trace(go.Scatter(x=[r["t_c"], r["label_end"]], y=[r["p_star"], r["p_star"]], mode="lines",
                             name="P* = close[t_c]·exp(A) (impulzus-küszöb)", hoverinfo="skip",
                             line={"color": theme.LONG, "width": 1.6, "dash": "dash"}))
    fig.add_trace(go.Scatter(x=[r["t0"], r["label_end"]], y=[r["low_t0"], r["low_t0"]], mode="lines",
                             name="low[t0] (t0 alá esés szintje)", hoverinfo="skip",
                             line={"color": theme.SHORT, "width": 1.2, "dash": "dot"}))
    hv = _hover(r, hu)
    fig.add_trace(go.Scatter(x=[r["t0"], r["t_c"]], y=[r["low_t0"], r["close_tc"]], mode="lines+markers",
                             name="t0 ▲ → t_c ● (A)", hovertext=[hv, hv], hoverinfo="text",
                             line={"color": theme.BLUE, "width": 1.4, "dash": "dot"},
                             marker={"symbol": ["triangle-up", "circle"], "size": 13,
                                     "color": CLASS_COLOR[r["label_class"]], "line": {"width": 1, "color": "#ffffff"}}))
    for key, label, col, sym in (("mins_to_target", "P* első átlépése", theme.LONG, "star"),
                                 ("mins_to_t0_break", "low[t0] első törése", theme.SHORT, "x")):
        if r[key] is not None:
            t = r["t_c"] + timedelta(minutes=int(r[key]) - 1)
            y = r["p_star"] if key == "mins_to_target" else r["low_t0"]
            fig.add_trace(go.Scatter(x=[t], y=[y], mode="markers", name=label, hovertext=[f"{label}: {_ts(t)}"],
                                     hoverinfo="text", marker={"symbol": sym, "size": 13, "color": col}))
    ys = [float(bars["low"].min()), float(bars["high"].max()), r["p_star"]]  # type: ignore[arg-type]
    pad = (max(ys) - min(ys)) * 0.05
    themed(fig, height=480, hovermode="closest", showlegend=True,
           legend={"orientation": "h", "y": -0.12, "x": 0, "yanchor": "top"}, margin={"l": 56, "r": 16, "t": 28, "b": 96})
    fig.update_xaxes(rangeslider={"visible": False}, type="date", range=[lo, hi])
    fig.update_yaxes(range=[min(ys) - pad, max(ys) + pad], title="SOLUSDT")
    return fig


def _input_panel(b15: we.Bars15, picks: list[dict[str, Any]], cell_id: str, hu: dict[str, str] | None = None) -> go.Figure:
    """One example per class: ATR-normalised 15m prefix (relative to low[t0]) + relative volume — the t919 input."""
    n = len(picks)
    titles = [f"{(hu or we.CLASS_HU)[r['label_class']]} · t_c {_ts(r['t_c'])}" for r in picks]
    fig = make_subplots(rows=2, cols=n, shared_xaxes="columns", row_heights=[0.72, 0.28], vertical_spacing=0.05,
                        horizontal_spacing=0.05, subplot_titles=titles)
    for c, r in enumerate(picks, start=1):
        img = we.prefix_image(b15, r)
        x = list(range(img["close"].size))
        fig.add_trace(go.Candlestick(x=x, open=img["open"].tolist(), high=img["high"].tolist(), low=img["low"].tolist(),
                                     close=img["close"].tolist(), showlegend=False, name="prefix",
                                     increasing={"line": {"color": theme.BLUE, "width": 1}, "fillcolor": "rgba(13,110,253,0.12)"},
                                     decreasing={"line": {"color": theme.BLUE, "width": 1}, "fillcolor": theme.BLUE}),
                      row=1, col=c)
        fig.add_trace(go.Bar(x=x, y=img["volume"].tolist(), showlegend=False, name="volumen / 7n átlag",
                             marker={"color": theme.NEUTRAL}), row=2, col=c)
        fig.update_xaxes(rangeslider={"visible": False}, row=1, col=c)
        fig.update_xaxes(title="15m bar # (prefix)", row=2, col=c)
    fig.update_yaxes(title="(ár − low[t0]) / ATR14", row=1, col=1)
    fig.update_yaxes(title="vol / 7n", row=2, col=1)
    themed(fig, height=460, hovermode="closest", showlegend=False, margin={"l": 56, "r": 16, "t": 48, "b": 48})
    return fig


# --------------------------------------------------------------------------- report


def _params_table(p: we.WaveParams) -> pd.DataFrame:
    rows = [
        ("(W, H) rács", ", ".join(f"({w // 60}h, {h // 60}h)" for _, w, h in p.cells)),
        ("B (bal oldali ablak)", "B = W"),
        ("Megerősítés", "t_c = t0 + W (a jobb oldali utolsó 1m bar záróideje)"),
        ("Min. visszapattanás", f"A = log(close[t_c]/low[t0]) ≥ {p.min_a_sigma:g}·σ_W, σ_W = σ_1m·√(W/1m)"),
        ("σ_1m", f"1m log-hozam szórás, {p.sigma_window} bar (7 nap), csak zár ≤ t0 (min. {p.sigma_min_frac:.0%} kitöltés)"),
        ("Nem-átfedés", "greedy időrend; elfogadás, ha t0 − B ≥ t_c(előző) + H (teljes intervallum, epic-döntés)"),
        ("Címke", f"impulzus: max high (t_c, t_c+H] > P* = close[t_c]·exp({p.impulse_mult:g}·A); "
                  "különben t0 alá (min low < low[t0]) / oldalazott"),
        ("Prefix-kép", "15m (óra-igazított), nyitás ≥ t0 − B, zárás ≤ t_c; ATR14(15m) SMA a t_c-ig"),
        ("Split + purge", f"train {p.train_start} → {p.oos_start} (excl.), OOS → {p.holdout2_start} (excl.), holdout-2 → adat vége; "
                          "egy esemény csak akkor tartozik egy splitbe, ha a teljes [t0 − B, t_c + H] benne van"),
    ]
    return pd.DataFrame(rows, columns=["param", "value"])


# --------------------------------------------------------------------------- v2 (epic_072 / t925)

MIN_BLOCK = 40            # 5200 v2: usable test events per CV block
MIN_POOLED = 100          # 5200 v2: pooled impulse and non-impulse
MIN_OOS_V2 = 100          # 5200 v2: OOS events (unique t0 across the two cells is the conservative reading)
SAME_BAR_MATERIAL = 0.02  # same-bar double touch is material above 2% of the events
CLASSES_V2 = we.CLASS_ORDER_V2
HU2 = we.CLASS_HU_V2


def _v2_cells(ds: we.EventDataset) -> list[str]:
    return [c[0] for c in ds.params.cells]


def _v2_summary(ds: we.EventDataset) -> pd.DataFrame:
    rows = []
    train_days = (pd.Timestamp(ds.params.oos_start) - pd.Timestamp(ds.params.train_start)).days
    for cell_id in _v2_cells(ds):
        e = ds.events.filter(pl.col("cell_id") == cell_id)
        tr = e.filter(pl.col("split") == "train")
        n = tr.height
        cnt = {s: int((e["split"] == s).sum()) for s in ("train", "oos", "holdout2", "purged")}
        row: dict[str, Any] = {"cell": cell_id, **cnt, "per_month": n / (train_days / 30.44),
                               "min_train_ok": "igen" if n >= MIN_TRAIN else "NEM"}
        for cls in CLASSES_V2:
            row[f"n_{cls}"] = int((tr["label_class"] == cls).sum())
            row[f"{cls}_rate"] = row[f"n_{cls}"] / n if n else None
        rows.append(row)
    return pd.DataFrame(rows)


def _v2_oos(ds: we.EventDataset) -> tuple[pd.DataFrame, dict[str, Any]]:
    """OOS / holdout-2: COUNTS only (no labels, no examples). Per cell, the per-cell sum and the unique t0 count."""
    ev = ds.events
    cells = _v2_cells(ds)
    rows = []
    for split in ("oos", "holdout2"):
        g = ev.filter(pl.col("split") == split)
        per = {c: int((g["cell_id"] == c).sum()) for c in cells}
        uniq = int(g["t0"].n_unique())
        rows.append({"split": "OOS" if split == "oos" else "Holdout-2", **{f"n_{c}": v for c, v in per.items()},
                     "sum_cells": sum(per.values()), "unique_t0": uniq,
                     "ge100_sum": "igen" if sum(per.values()) >= MIN_OOS_V2 else "NEM",
                     "ge100_unique": "igen" if uniq >= MIN_OOS_V2 else "NEM"})
    oos = rows[0]
    return pd.DataFrame(rows), {"sum": oos["sum_cells"], "unique": oos["unique_t0"], "ok_unique": oos["unique_t0"] >= MIN_OOS_V2}


def _v2_same_bar(ds: we.EventDataset) -> pd.DataFrame:
    """Train-only same-bar double-touch frequency + class shares under the primary and the opposite tie-break."""
    rows = []
    primary = ds.params.tie_break
    for cell_id in _v2_cells(ds):
        tr = ds.events.filter((pl.col("cell_id") == cell_id) & (pl.col("split") == "train"))
        n = tr.height
        sb = int(tr["same_bar"].sum())
        c = {cls: int((tr["label_class"] == cls).sum()) for cls in CLASSES_V2}
        alt = dict(c)
        if primary == "false_start":      # opposite tie-break: the same-bar events become impulses
            alt["impulse"] += sb
            alt["false_start"] -= sb
        else:
            alt["impulse"] -= sb
            alt["false_start"] += sb
        rows.append({
            "cell": cell_id, "n": n, "n_same_bar": sb, "same_bar_rate": sb / n if n else None,
            "material": "igen" if n and sb / n > SAME_BAR_MATERIAL else "nem",
            **{f"{cls}_rate": c[cls] / n for cls in CLASSES_V2},
            **{f"alt_{cls}_rate": alt[cls] / n for cls in CLASSES_V2},
        })
    return pd.DataFrame(rows)


def _v2_quintiles(ds: we.EventDataset) -> pd.DataFrame:
    rows = []
    for cell_id in _v2_cells(ds):
        tr = ds.events.filter((pl.col("cell_id") == cell_id) & (pl.col("split") == "train"))
        if tr.height < 10:
            continue
        x = tr["a_sigma"].to_numpy()
        q = np.searchsorted(np.quantile(x, [0.2, 0.4, 0.6, 0.8]), x, side="right")
        cls = tr["label_class"].to_numpy()
        for k in range(5):
            sel = q == k
            rows.append({"cell": cell_id, "quintile": f"Q{k + 1}", "a_sigma_lo": float(x[sel].min()),
                         "a_sigma_hi": float(x[sel].max()), "a_sigma_med": float(np.median(x[sel])), "n": int(sel.sum()),
                         **{f"{c}_rate": float((cls[sel] == c).mean()) for c in CLASSES_V2}})
    return pd.DataFrame(rows)


def _v2_cv_tables(ds: we.EventDataset) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, dict[str, Any]], list[Any]]:
    bounds = we.cv_block_bounds(ds.params)
    blocks, checks_rows, checks = [], [], {}
    for cell_id, _, h in ds.params.cells:
        e = ds.events.filter(pl.col("cell_id") == cell_id)
        df, ck = we.cv_summary(e, bounds, min_block=MIN_BLOCK, min_pooled=MIN_POOLED)
        blocks.append(df.with_columns(pl.lit(cell_id).alias("cell")))
        # fold sanity: purge / embargo effect on the training set of every fold
        n_tr = [we.cv_fold(e, k, bounds, h) for k in range(len(bounds) - 1)]
        ck = {**ck, "train_min": min(f.train.height for f in n_tr), "train_max": max(f.train.height for f in n_tr),
              "embargoed_total": sum(f.n_embargoed for f in n_tr), "purged_total": sum(f.n_purged for f in n_tr)}
        checks[cell_id] = ck
        checks_rows.append({"cell": cell_id, "min_block": ck["min_block_events"],
                            "block_ok": "igen" if ck["block_ok"] else "NEM", "pooled_impulse": ck["pooled_impulse"],
                            "pooled_non_impulse": ck["pooled_non_impulse"], "pooled_ok": "igen" if ck["pooled_ok"] else "NEM",
                            "result": "PASS" if ck["pass"] else "FAIL", "train_min": ck["train_min"],
                            "train_max": ck["train_max"], "embargoed_total": ck["embargoed_total"]})
    return pd.concat([b.to_pandas() for b in blocks], ignore_index=True), pd.DataFrame(checks_rows), checks, bounds


def _v2_block_fig(blocks: pd.DataFrame) -> go.Figure:
    cells = list(dict.fromkeys(blocks["cell"]))
    fig = make_subplots(rows=1, cols=len(cells), subplot_titles=[_cell_title(c) for c in cells], shared_yaxes=True)
    col = {"impulse": theme.LONG, "false_start": theme.SHORT, "sideways": theme.AMBER}
    for j, c in enumerate(cells, start=1):
        g = blocks[blocks["cell"] == c]
        for cls in CLASSES_V2:
            fig.add_trace(go.Bar(x=[f"B{b}" for b in g["block"]], y=(g[f"n_{cls}"] / g["n_test"]).tolist(), name=HU2[cls],
                                 marker={"color": col[cls]}, showlegend=(j == 1), legendgroup=cls,
                                 hovertemplate="%{x}: %{y:.1%}<extra>" + HU2[cls] + "</extra>"), row=1, col=j)
    themed(fig, height=380, hovermode="closest", margin={"l": 56, "r": 16, "t": 40, "b": 48})
    fig.update_layout(barmode="stack")
    fig.update_yaxes(title="Osztályarány (train blokk)", tickformat=".0%", row=1, col=1)
    return fig


def _v2_quint_fig(q: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for k, c in enumerate(dict.fromkeys(q["cell"])):
        g = q[q["cell"] == c]
        fig.add_trace(go.Scatter(x=g["a_sigma_med"].tolist(), y=g["impulse_rate"].tolist(), mode="lines+markers",
                                 name=f"{_cell_title(c)} impulzus", line={"color": [theme.BLUE, theme.ORANGE][k % 2], "width": 2},
                                 hovertemplate="A/σ_W med %{x:.2f}: %{y:.1%}<extra>" + c + "</extra>"))
    themed(fig, height=340, hovermode="closest", x_title="A / σ_W (kvintilis-medián)")
    fig.update_yaxes(title="Impulzus-arány (train)", tickformat=".0%")
    return fig


def build_v2(root: Path | None = None, *, df_1m: pl.DataFrame, params: we.WaveParams | None = None,
             asset_id: str | None = None, store: bool = True, lab_path: str | None = None) -> Path | None:
    """5200 v2 dataset (+ lab DB ``research."wave_events_v2"`` when ``store``) and the train-only v2 report."""
    ds = we.build_events(df_1m, params or we.params_v2())
    p = ds.params
    meta = we.persist(ds, asset_id, lab_path=lab_path) if store else None
    b15 = we.to_bars15(we.to_minute_grid(df_1m), p)
    oos0 = datetime.fromisoformat(p.oos_start)
    summary = _v2_summary(ds)
    oos_tbl, oos_info = _v2_oos(ds)
    sb = _v2_same_bar(ds)
    quint = _v2_quintiles(ds)
    blocks, cv_checks, ck, bounds = _v2_cv_tables(ds)
    s = summary.set_index("cell")
    cells = _v2_cells(ds)

    rb = ReportBuilder(
        scope="general", slug=SLUG, order=30, root=root,
        title="Hullámkezdet-események v2 — adatkészlet, first-touch címke, CV-blokkok",
        summary="5200 v2: megerősített lokális mélypontok SOLUSDT 1m-en a (4h,4h) és (4h,8h) cellában, háromosztályos "
                "first-touch címke (impulzus / hamis indulás / oldalazás), CV-blokkonkénti eloszlás, same-bar gyakoriság, "
                "A/σ_W vs impulzus-arány. Csak train; OOS / holdout-2 csak darabszám. Ellenőrzőpont a t919 előtt.",
        tags=["wave", "epic_072", "events", "dataset", "checkpoint", "v2"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    store_txt = (f"`{meta['table']}` a lab DB-ben ({ds.events.height} sor, content hash `{meta['content_hash']}`)"
                 if meta else "nem íródott a lab DB-be (`--no-store`)")
    rb.add_markdown(
        "## Cél\n\n"
        "**Ellenőrzőpont (epic_072 / t925, v2):** a t924 módszertan (`_doc_/methodology_doc/5200_wave_start_image_model.md`, v2) "
        "szerint újraépített eseményadatkészlet. A t919 (kép-embedding, klaszterezés) előtt számokkal és képekkel ellenőrizzük: "
        "elég esemény van-e cellánként és CV-blokkonként, mekkora a három címke-osztály aránya, mennyire számít a same-bar "
        "kétértelműség, és hogyan függ az impulzus-arány az `A/σ_W`-től.\n\n"
        f"**Adat:** SOLUSDT `ohlcv` 1m, {_ts(ds.data_first)[:10]} → adat vége, {ds.n_rows:,} sor, read-only. Adatkészlet: {store_txt}. "
        "Újragenerálás: `uv run python -m reports.builders.wave_event_dataset` (v1: `--version v1`).\n\n"
        "> ⚠️ **OOS-tisztaság:** minden példa, kép, eloszlás és címke-arány **kizárólag a train** időszakból (2021-01-01 → "
        "2026-01-31) származik. Az OOS és a holdout-2 itt kizárólag **darabszámként** szerepel (nincs példa, nincs "
        "címke-arány).")
    pt = _params_table(p)
    pt.loc[pt["param"] == "Címke", "value"] = (
        "háromosztályos first-touch: az 1m bar-ok időrendjében (t_c, t_c+H] az első bar, amelyen high > P* = close[t_c]·exp(A) → "
        "impulzus; low < low[t0] → hamis indulás; egyik sem → oldalazás; ugyanazon a baron mindkettő → hamis indulás (konzervatív)")
    pt.loc[pt["param"] == "Split + purge", "value"] = (
        "train 2021-01-01 → 2026-01-31; az OOS és a holdout-2 időszak a 5200 szerint, a riportban csak darabszám; egy esemény "
        "csak akkor tartozik egy splitbe, ha a teljes [t0 − B, t_c + H] benne van")
    pt.loc[len(pt)] = ("Fő cél", "label_impulse = impulzus vs (hamis indulás + oldalazás)")
    pt.loc[len(pt)] = ("CV-blokkok", f"{p.cv_blocks} összefüggő időblokk a train időszakon (egész napos határok, közel egyenlő hossz); "
                                     "blokk = a t0 blokkja; a blokkhatárt átlógó esemény egy foldba sem kerül; "
                                     "purge a teljes [t0 − B, t_c + H]-ra + embargo H a teszt-blokk szélein (fold-időben)")
    rb.add_table("params", pt, title="Definíciók (5200 v2, fagyasztva)", columns={"param": "Paraméter", "value": "Érték"})

    # ---- 1. counts + class shares
    rb.add_markdown(
        "## 1. Eseményszám és osztályarányok cellánként\n\n"
        "Rács: csak `W = 4h`, `H ∈ {4h, 8h}`. A (4h,4h) és (4h,8h) esemény-halmaz nagyrészt közös `t0`-kból áll, de a "
        "nem-átfedő greedy cellánként külön fut, ezért a két halmaz nem azonos. Az OOS / holdout-2 oszlop csak darabszám.")
    rb.add_table("summary", summary, title="Train-eseményszám és címke-osztályok cellánként (OOS / holdout-2: db)", columns={
        "cell": "Cella", "train": "Train", "oos": "OOS (db)", "holdout2": "Holdout-2 (db)", "purged": "Purge",
        "n_impulse": "Impulzus (db)", "n_false_start": "Hamis indulás (db)", "n_sideways": "Oldalazás (db)",
        "impulse_rate": {"label": "Impulzus", "type": "pct", "digits": 1},
        "false_start_rate": {"label": "Hamis indulás", "type": "pct", "digits": 1},
        "sideways_rate": {"label": "Oldalazás", "type": "pct", "digits": 1},
        "per_month": {"label": "Esemény / hó", "digits": 1}, "min_train_ok": f"Train ≥ {MIN_TRAIN}"})
    rb.add_markdown("### Tölcsér: jelölt → elfogadott\n\nJelölt = a `[t0 − B, t_c]` ablak minimuma; A-szűrő = `A ≥ 1,5·σ_W`; "
                    "elfogadott = a teljes-intervallumos nem-átfedő greedy után (az OOS / holdout-2 oszlop csak darabszám).")
    rb.add_table("funnel", ds.funnel.to_pandas(), title="Detektálási tölcsér cellánként", columns={
        "cell_id": "Cella", "candidates": "Jelölt mélypont", "passing_a": "A ≥ 1,5·σ_W", "accepted": "Nem-átfedő",
        "train": "Train", "oos": "OOS", "holdout2": "Holdout-2", "purged": "Purge"})

    # ---- 2. same-bar
    rb.add_markdown(
        "## 2. Same-bar kettős érintés (train)\n\n"
        "Ha ugyanazon az 1 perces baron egyszerre `high > P*` és `low < low[t0]`, a bar belső sorrendje ismeretlen; a szabály "
        "szerint az esemény **hamis indulás** (konzervatív). Az alábbi tábla a gyakoriságot és — érzékenységként — az "
        f"osztályarányokat az ellenkező tie-break (same-bar = impulzus) mellett mutatja; lényeges, ha > {SAME_BAR_MATERIAL:.0%}.")
    rb.add_table("same_bar", sb, title="Same-bar gyakoriság és osztályarány az ellenkező tie-break mellett (train)", columns={
        "cell": "Cella", "n": "Train", "n_same_bar": "Same-bar (db)",
        "same_bar_rate": {"label": "Same-bar arány", "type": "pct", "digits": 2}, "material": f"> {SAME_BAR_MATERIAL:.0%}",
        "impulse_rate": {"label": "Impulzus (szabály)", "type": "pct", "digits": 1},
        "false_start_rate": {"label": "Hamis ind. (szabály)", "type": "pct", "digits": 1},
        "sideways_rate": {"label": "Oldalazás (szabály)", "type": "pct", "digits": 1},
        "alt_impulse_rate": {"label": "Impulzus (ellenkező)", "type": "pct", "digits": 1},
        "alt_false_start_rate": {"label": "Hamis ind. (ellenkező)", "type": "pct", "digits": 1},
        "alt_sideways_rate": {"label": "Oldalazás (ellenkező)", "type": "pct", "digits": 1}})

    # ---- 3. CV blocks
    rb.add_markdown(
        "## 3. CV-blokkok (train, 6 időblokk)\n\n"
        "A blokk-határok egész napok, közel egyenlő hosszal: " + ", ".join(b.date().isoformat() for b in bounds[:-1]) +
        f" (az utolsó blokk a train-határig, {_ts(bounds[-1] - timedelta(days=1))[:10]}-ig tart). Egy esemény blokkja a `t0` "
        "blokkja; ha a teljes `[t0 − B, t_c + H]` intervalluma kilóg a blokkjából, **átlógó** (egy foldba sem kerül). Az "
        "alábbi darabszámok a használható teszt-eseményeket mutatják. A purge (átfedő intervallum) és az embargo (`H` a "
        "teszt-blokk szélein) fold-időben, a `cv_fold` segédfüggvénnyel alkalmazandó (tiszta függvény, tesztelve).")
    rb.add_table("cv_blocks", blocks, title="Esemény- és osztály-darabszám CV-blokkonként (train)", columns={
        "cell": "Cella", "block": "Blokk", "start": {"label": "Kezdet", "type": "str"}, "end": {"label": "Utolsó nap", "type": "str"},
        "n_events": "Esemény", "n_straddle": "Átlógó", "n_test": "Használható teszt", "n_impulse": "Impulzus",
        "n_false_start": "Hamis indulás", "n_sideways": "Oldalazás"})
    rb.add_chart("cv_block_shares", _v2_block_fig(blocks), title="Osztályarányok CV-blokkonként (train)", height=380)
    rb.add_markdown(
        f"### 5200 minimumok (CV): blokkonként ≥ {MIN_BLOCK} teszt-esemény; összevontan ≥ {MIN_POOLED} impulzus és ≥ {MIN_POOLED} "
        "nem-impulzus\n\nA „Train min/max” a fold-tanítóhalmaz mérete purge + embargo és az átlógók kizárása után.")
    rb.add_table("cv_checks", cv_checks, title="Minimum-ellenőrzés cellánként", columns={
        "cell": "Cella", "min_block": "Min. blokk-esemény", "block_ok": f"Blokk ≥ {MIN_BLOCK}",
        "pooled_impulse": "Összevont impulzus", "pooled_non_impulse": "Összevont nem-impulzus",
        "pooled_ok": f"Összevont ≥ {MIN_POOLED}", "result": "Eredmény", "train_min": "Train min (fold)",
        "train_max": "Train max (fold)", "embargoed_total": "Embargo-zott (összes fold)"})

    # ---- 4. A/sigma vs impulse
    qtxt = "; ".join(
        f"{c}: Q1 {fmt(100 * g['impulse_rate'].iloc[0], 1)}% → Q5 {fmt(100 * g['impulse_rate'].iloc[-1], 1)}%"
        for c, g in quint.groupby("cell", sort=False)) if len(quint) else "-"
    rb.add_markdown(
        "## 4. A/σ_W és impulzus-arány (train)\n\n"
        "Mivel a `P* = close[t_c]·exp(A)` az `A`-val együtt távolodik, nagy visszapattanás után nehezebb az impulzus. Ez a "
        "skalár alap (S0) legerősebb nyers tagja, a kép-modellnek ezt kell felülmúlnia. Az impulzus-arány kvintilisenként: "
        f"{qtxt}. Nyers leírás (nincs placebo / CV), nem teszt.")
    rb.add_chart("quint_impulse", _v2_quint_fig(quint), title="Impulzus-arány A/σ_W kvintilisenként (train)", height=340)
    rb.add_table("quintiles", quint, title="Osztályarányok A/σ_W kvintilisenként (train)", columns={
        "cell": "Cella", "quintile": "Kvintilis", "a_sigma_lo": {"label": "A/σ_W tól", "digits": 2},
        "a_sigma_hi": {"label": "A/σ_W ig", "digits": 2}, "a_sigma_med": {"label": "A/σ_W medián", "digits": 2}, "n": "Esemény",
        "impulse_rate": {"label": "Impulzus", "type": "pct", "digits": 1},
        "false_start_rate": {"label": "Hamis indulás", "type": "pct", "digits": 1},
        "sideways_rate": {"label": "Oldalazás", "type": "pct", "digits": 1}})
    rb.add_chart("dist_a_sigma", _hist_fig(ds, pl.col("a_sigma"), np.arange(1.5, 6.01, 0.25), "A / σ_W (6 = 6 vagy több)"),
                 title="A/σ_W eloszlás cellánként (train)", height=340)

    # ---- 5. OOS / holdout counts
    rb.add_markdown(
        "## 5. OOS és holdout-2: csak darabszám\n\n"
        f"Az OOS-ellenőrzés feltétele (5200 v2): ≥ {MIN_OOS_V2} esemény a két 4h-s cellában együtt. A két cella `t0`-i nagyrészt "
        "közösek, ezért a **cella-összeg duplán számol**; a konzervatív mérték az **egyedi `t0`-k száma**. Címke-arány vagy példa "
        "az OOS-ból nem szerepel.")
    rb.add_table("oos_counts", oos_tbl, title="OOS / holdout-2 darabszám (cella-összeg és egyedi t0)", columns={
        "split": "Időszak", **{f"n_{c}": f"{c} (db)" for c in cells}, "sum_cells": "Cella-összeg",
        "unique_t0": "Egyedi t0", "ge100_sum": f"Összeg ≥ {MIN_OOS_V2}", "ge100_unique": f"Egyedi ≥ {MIN_OOS_V2}"})

    # ---- 6. examples (train only)
    rb.add_markdown(
        "## 6. Példák: a modell-bemenet (prefix-kép) osztályonként (train)\n\n"
        f"**Kiválasztás (determinisztikus):** cellánként és osztályonként a train események időrendben, a (k + 0,5)/"
        f"{EXAMPLES_PER_CLASS} pozíciókban. A prefix-képek 15m OHLC-k a `low[t0]`-hoz viszonyítva, ATR14(15m)-mel normálva, alattuk a "
        "relatív volumen; a modell nem lát naptári időt, árszintet vagy `P*`-ot. Alatta osztályonként egy kinagyított, "
        "címkézett példa (kék = prefix, halvány = a H címke-ablak; ★ = P* első átlépése, ✕ = low[t0] első törése).")
    ex_rows: list[dict[str, Any]] = []
    for cell_id in cells:
        picks = select_examples(ds.events, cell_id, classes=CLASSES_V2)
        rb.add_markdown(f"### Cella {_cell_title(cell_id)}")
        for cls in CLASSES_V2:
            grp = [r for r in picks if r["label_class"] == cls]
            if not grp:
                continue
            rb.add_chart(f"input_{cell_id.lower()}_{cls}", _input_panel(b15, grp, cell_id, HU2),
                         title=f"Modell-bemenet {_cell_title(cell_id)} — {HU2[cls]} (ATR-normált prefix + relatív volumen)", height=460)
            for k, r in enumerate(grp, start=1):
                ex_rows.append({"cell": cell_id, "label": HU2[cls], "example": k, "t0": r["t0"], "t_c": r["t_c"],
                                "a_pct": 100 * (np.exp(r["a_log"]) - 1), "a_sigma": r["a_sigma"],
                                "mfe_pct": 100 * r["fwd_mfe_log"], "mae_pct": 100 * r["fwd_mae_log"],
                                "same_bar": "igen" if r["same_bar"] else "nem"})
        for cls in CLASSES_V2:
            mid = [r for r in select_examples(ds.events, cell_id, per_class=1, classes=CLASSES_V2) if r["label_class"] == cls]
            if mid:
                r = mid[0]
                rb.add_markdown(f"**{cell_id} — {HU2[cls]}:** t0 {_ts(r['t0'])}, t_c {_ts(r['t_c'])} UTC; A = "
                                f"{100 * (np.exp(r['a_log']) - 1):.2f}% ({r['a_sigma']:.2f} σ_W), MFE {100 * r['fwd_mfe_log']:+.2f}%, "
                                f"MAE {100 * r['fwd_mae_log']:+.2f}%.")
                rb.add_chart(f"ex_{cell_id.lower()}_{cls}", _example_fig(df_1m, r, HU2, hi_cap=oos0),
                             title=f"{cell_id}: {HU2[cls]} — t_c {_ts(r['t_c'])}", height=480)
    rb.add_table("examples", pd.DataFrame(ex_rows), title="A kiválasztott prefix-példák (train)", columns={
        "cell": "Cella", "label": "Címke", "example": "#", "t0": "t0", "t_c": "t_c",
        "a_pct": {"label": "A (%)", "digits": 2}, "a_sigma": {"label": "A/σ_W", "digits": 2},
        "mfe_pct": {"label": "MFE (%)", "digits": 2}, "mae_pct": {"label": "MAE (%)", "digits": 2}, "same_bar": "Same-bar"})

    # ---- 7. interpretation
    cv_txt = "; ".join(
        f"{c}: " + ("PASS" if ck[c]["pass"] else "FAIL") + f" (min. blokk {ck[c]['min_block_events']}, impulzus {ck[c]['pooled_impulse']}, "
        f"nem-impulzus {ck[c]['pooled_non_impulse']})" for c in cells)
    rate_txt = "; ".join(
        f"{c}: {int(s.loc[c, 'train'])} train, impulzus {fmt(100 * s.loc[c, 'impulse_rate'], 1)}% / hamis indulás "
        f"{fmt(100 * s.loc[c, 'false_start_rate'], 1)}% / oldalazás {fmt(100 * s.loc[c, 'sideways_rate'], 1)}%" for c in cells)
    sb_txt = "; ".join(f"{r['cell']} {fmt(100 * r['same_bar_rate'], 2)}%" for _, r in sb.iterrows())
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"**Osztályarányok (train):** {rate_txt}. **Same-bar:** {sb_txt} (lényegességi küszöb {SAME_BAR_MATERIAL:.0%}).\n\n"
        f"**CV-minimumok:** {cv_txt}.\n\n"
        f"**OOS:** {oos_info['sum']} esemény a két cellában összesen (cella-összeg), **{oos_info['unique']} egyedi `t0`**; a "
        f"≥ {MIN_OOS_V2} feltétel az egyedi számon " + ("teljesül" if oos_info["ok_unique"] else "NEM teljesül") +
        ". A holdout-2 érintetlen (csak darabszám).\n\n"
        "**Állásfoglalás:** az ellenőrzőpont a t919 előtt user döntés: a CV-minimumok és az OOS-szám fent közölt "
        "eredménye adja meg, mely cella értékelhető. A minimum alatti cella a 5200 szerint NO-GO (nem küszöb-csökkentés).")
    return rb.write()


def build(root: Path | None = None, *, df_1m: pl.DataFrame | None = None, params: we.WaveParams | None = None,
          asset_id: str | None = None, store: bool = True, sensitivity: bool = True,
          lab_path: str | None = None) -> Path | None:
    """Build the dataset (+ lab DB write when ``store``) and write the report; ``None`` without OHLCV.

    ``params.version == "v2"`` (see :func:`reports.builders.wave_events.params_v2`) writes the v2 report
    (:func:`build_v2`); the default parameters keep the v1 behaviour.
    """
    if df_1m is None:
        df_1m = load_ohlcv_1m(asset_id)
    if df_1m is None or df_1m.height == 0:
        logger.warning("%s skipped: no OHLCV data", SLUG)
        return None
    if params is not None and params.version != "v1":
        return build_v2(root, df_1m=df_1m, params=params, asset_id=asset_id, store=store, lab_path=lab_path)
    ds = we.build_events(df_1m, params)
    p = ds.params
    meta = we.persist(ds, asset_id, lab_path=lab_path) if store else None
    b15 = we.to_bars15(we.to_minute_grid(df_1m), p)
    summary = _summary(ds)
    tr_all = ds.events.filter(pl.col("split") == "train")

    rb = ReportBuilder(
        scope="general", slug=SLUG, order=30, root=root,
        title="Hullámkezdet-események — adatkészlet és vizuális ellenőrzés",
        summary="Megerősített lokális mélypontok SOLUSDT 1m-en a (W,H) rács 4 cellájában (5200 v1): darabszámok, "
                "címke-arányok, eloszlások, áttekintő és kinagyított példák, a modell-bemenet képe. Csak train; "
                "ellenőrzőpont a t919 előtt.",
        tags=["wave", "epic_072", "events", "dataset", "checkpoint"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    store_txt = (f"`{meta['table']}` a lab DB-ben ({ds.events.height} sor, content hash `{meta['content_hash']}`)"
                 if meta else "nem íródott a lab DB-be (`--no-store`)")
    rb.add_markdown(
        "## Cél\n\n"
        "**Ellenőrzőpont (epic_072 / t918):** mielőtt a t919 kép-embeddinget és klaszterezést futtat, vizuálisan és számokkal "
        "ellenőrizzük, hogy az eseménydefiníció *jó mélypontokat* talál-e, ésszerű darabszámmal, és hogy a bináris címke "
        "(impulzus vs korrekció) egyezik-e a szemmel látható ítélettel.\n\n"
        "**Kapcsolódó dokumentáció:** módszertan `_doc_/methodology_doc/5200_wave_start_image_model.md` (definíciók, "
        "rács, címke, split, go/no-go), kód `src/reports/builders/wave_events.py` (adatkészlet) és "
        "`src/reports/builders/wave_event_dataset.py` (ez a riport).\n\n"
        f"**Adat:** SOLUSDT `ohlcv` 1m, {_ts(ds.data_first)} → {_ts(ds.data_last)} UTC ({ds.n_rows:,} sor), read-only. "
        f"Adatkészlet: {store_txt}. Újragenerálás: `uv run python -m reports.builders.wave_event_dataset`.\n\n"
        "> ⚠️ **OOS-tisztaság:** minden példa, kép, eloszlás és címke-arány **kizárólag a train** időszakból származik. "
        "Az OOS és a holdout-2 eseményei felépültek és címkézve a táblában vannak, de itt csak darabszámként szerepelnek."
    )
    rb.add_table("params", _params_table(p), title="Definíciók (5200 v1, fagyasztva)", columns={"param": "Paraméter", "value": "Érték"})

    # ---- 1. summary
    rb.add_markdown(
        "## 1. Összefoglaló\n\n"
        f"Min. eseményszám (5200, feltételezés): train ≥ {MIN_TRAIN} / cella; OOS ≥ {MIN_OOS} (és ≥ 50 pozitív / negatív — ezt a "
        "t920 nézi, itt csak a darabszám-feltétel). A címke-arányok a train eseményekre vonatkoznak. „Purge” = a split-határt "
        "átlógó, a 2021 előtti vagy a címke-ablak nélküli (adat vége) esemény."
    )
    rb.add_table("summary", summary, title="Eseményszám és címke-arányok cellánként", columns={
        "cell": "Cella", "w_h": "W / H", "train": "Train", "oos": "OOS (db)", "holdout2": "Holdout-2 (db)", "purged": "Purge",
        "impulse_rate": {"label": "Impulzus", "type": "pct", "digits": 1},
        "t0_below_rate": {"label": "Korr. / t0 alá", "type": "pct", "digits": 1},
        "sideways_rate": {"label": "Korr. / oldalazott", "type": "pct", "digits": 1},
        "n_impulse": "Impulzus (db)", "per_month": {"label": "Esemény / hó", "digits": 1},
        "gap_days_median": {"label": "Medián távolság (nap)", "digits": 2},
        "min_train_ok": f"Train ≥ {MIN_TRAIN}", "min_oos_ok": f"OOS ≥ {MIN_OOS}",
    })
    rb.add_markdown("### Tölcsér: jelölt → elfogadott\n\n"
                    "Jelölt = a `[t0 − B, t_c]` ablak minimuma; A-szűrő = `A ≥ 1,5·σ_W`; elfogadott = a nem-átfedő greedy után "
                    "(az OOS / holdout-2 oszlop itt is csak darabszám).")
    rb.add_table("funnel", ds.funnel.to_pandas(), title="Detektálási tölcsér cellánként", columns={
        "cell_id": "Cella", "candidates": "Jelölt mélypont", "passing_a": "A ≥ 1,5·σ_W", "accepted": "Nem-átfedő",
        "train": "Train", "oos": "OOS", "holdout2": "Holdout-2", "purged": "Purge"})
    rb.add_markdown("### Évenként (train)")
    rb.add_table("yearly", _yearly(ds), title="Train események és címke-arányok évenként", columns={
        "cell": "Cella", "year": {"label": "Év", "type": "str"}, "n": "Esemény",
        "impulse_rate": {"label": "Impulzus", "type": "pct", "digits": 1},
        "t0_below_rate": {"label": "t0 alá", "type": "pct", "digits": 1},
        "sideways_rate": {"label": "Oldalazott", "type": "pct", "digits": 1},
        "a_sigma_median": {"label": "A/σ_W medián", "digits": 2}, "a_pct_median": {"label": "A medián (%)", "digits": 2}})
    sens: pd.DataFrame | None = None
    if sensitivity:
        rb.add_markdown(
            "### Érzékenység — csak darabszám, NEM döntési alap\n\n"
            "Mivel több cella a minimum alatt van, megnéztük, mennyit változik a darabszám alternatív definíciókkal. "
            "**A v1 definíció fagyasztva marad**; egy másik változat csak a 5200 új verziójaként (v2) jöhet, a user döntése "
            "után. Az OOS oszlop itt is csak darabszám; az impulzus-arány train.")
        sens = _sensitivity(df_1m, ds)
        cols: dict[str, Any] = {"variant": "Változat"}
        for cell_id, _, _ in p.cells:
            cols[f"{cell_id}_train"] = f"{cell_id} train"
            cols[f"{cell_id}_oos"] = f"{cell_id} OOS"
            cols[f"{cell_id}_imp_rate"] = {"label": f"{cell_id} impulzus", "type": "pct", "digits": 1}
        rb.add_table("sensitivity", sens, title="Darabszám alternatív definíciókkal (train / OOS db, train impulzus-arány)", columns=cols)

    # ---- 2. distributions
    rb.add_markdown("## 2. Eloszlások (train)\n\n"
                    "Az `A/σ_W` a visszapattanás mérete a W-re skálázott volatilitásban (≥ 1,5 a szűrő miatt); az `A` ugyanez "
                    "százalékban. A cellák eloszlása normált (arány), így a különböző darabszámok összevethetők.")
    rb.add_chart("dist_a_sigma", _hist_fig(ds, pl.col("a_sigma"), np.arange(1.5, 6.01, 0.25), "A / σ_W (6 = 6 vagy több)"),
                 title="A/σ_W eloszlás cellánként (train)", height=340)
    rb.add_chart("dist_a_pct", _hist_fig(ds, (pl.col("a_log").exp() - 1) * 100, np.arange(0, 20.01, 0.5), "A (%) (20 = 20 vagy több)"),
                 title="A (%) eloszlás cellánként (train)", height=340)
    rb.add_markdown("**Prefix-hossz.** A prefix 15m bar-okból áll, nyitás ≥ t0 − B, zárás ≤ t_c. Névleges hossza 2W/15; "
                    "ha a t0 nem esik 15 perces határra, egy bar-ral rövidebb (a bal szélen a csonka és a t_c-t átfedő bar kimarad).")
    rb.add_table("prefix_len", _prefix_len(ds), title="Prefix-hossz (15m bar) eloszlás (train)", columns={
        "cell": "Cella", "nominal": "Névleges (2W/15)", "n_prefix_15m": "Prefix bar", "n": "Esemény",
        "share_pct": {"label": "Arány", "type": "pct", "digits": 1}})
    quint = _quintiles(ds)
    qtxt = "; ".join(
        f"{c}: Q1 {fmt(100 * g['impulse_rate'].iloc[0], 1)}% → Q5 {fmt(100 * g['impulse_rate'].iloc[-1], 1)}%"
        for c, g in quint.groupby("cell", sort=False)) if len(quint) else "-"
    rb.add_markdown("**Nyers címke-arány A/σ_W kvintilisenként (train).** Ez a skalár alap (S0) legfontosabb tagjának nyers "
                    "kapcsolata a címkével; a kép-modellnek ezt kell felülmúlnia. Mivel a `P*` küszöb az A-val együtt nő, egy "
                    f"nagy visszapattanás után nehezebb az impulzus — impulzus-arány: {qtxt}. "
                    "Nem teszt (nincs placebo / CV); csak tájékozódás.")
    rb.add_table("quintiles", quint, title="Címke-arányok A/σ_W kvintilisenként (train, nyers)", columns={
        "cell": "Cella", "quintile": "Kvintilis", "a_sigma_lo": {"label": "A/σ_W tól", "digits": 2},
        "a_sigma_hi": {"label": "A/σ_W ig", "digits": 2}, "n": "Esemény",
        "impulse_rate": {"label": "Impulzus", "type": "pct", "digits": 1},
        "t0_below_rate": {"label": "t0 alá", "type": "pct", "digits": 1},
        "sideways_rate": {"label": "Oldalazott", "type": "pct", "digits": 1}})

    # ---- 3. overview
    fig_ov, ov_counts, ov_lo, ov_hi = _overview(df_1m, ds)
    rb.add_markdown(
        f"## 3. Áttekintő kép — a train utolsó {OVERVIEW_DAYS} napja ({_ts(ov_lo)[:10]} → {_ts(ov_hi)[:10]}), 1h gyertya\n\n"
        "**Jelölés:** ▲ = t0 (a mélypont, `low[t0]`), ● = t_c (megerősítés, `close[t_c]`), a kettőt összekötő kék szakasz a "
        "visszapattanás (A). A t_c-től induló **H-sáv** alja a `low[t0]`, teteje a `P*` impulzus-küszöb: **impulzus**, ha a "
        "jövőbeli ár a sáv tetejét átlépi (zöld), **t0 alá**, ha a sáv alját töri (piros), **oldalazott**, ha a sávban marad "
        "(sárga). A jelmagyarázattal cellánként ki-/bekapcsolható; alapból a (4h, 4h) cella látszik. Hover a markereken: "
        "minden jellemző és a jövőbeli MFE/MAE. Csak train-események (az ablak a train-határon zárul; a határt átlógó "
        "események purge-ölve vannak).\n\n"
        "Látható események: " + ", ".join(f"{c} {n}" for c, n in ov_counts.items()) + ".")
    rb.add_chart("overview", fig_ov, title=f"SOL 1h, train utolsó {OVERVIEW_DAYS} nap — detektált mélypontok címkével", height=640)

    # ---- 4. examples
    rb.add_markdown(
        "## 4. Kinagyított példák cellánként (train)\n\n"
        f"**Kiválasztás (determinisztikus, nem kézi):** cellánként és címke-osztályonként a train események időrendben, a "
        f"(k + 0,5)/{EXAMPLES_PER_CLASS} pozíciókban (k = 0…{EXAMPLES_PER_CLASS - 1}) — így a példák a teljes train időszakot "
        "lefedik és nem a szélsőséges eseteket mutatják. Osztályonként 3 → cellánként 9 példa.\n\n"
        "**Jelölés:** 15m gyertyák. Szürke = előzmény (nem bemenet), **kék = a prefix-ablak** (2W, zárás ≤ t_c — ez lesz a "
        "modell bemenete), halvány = a jövő (H, a címke-ablak). Szaggatott függőleges = t_c; zöld szaggatott = `P*`; piros "
        "pontozott = `low[t0]`; ★ = a `P*` első átlépése, ✕ = a `low[t0]` első törése (1m pontossággal). Hover a ▲/● "
        "markereken: a jellemzők (S0 + C) és a jövőbeli kimenet."
    )
    ex_rows: list[dict[str, Any]] = []
    panels: dict[str, list[dict[str, Any]]] = {}
    for cell_id, _, _ in p.cells:
        picks = select_examples(ds.events, cell_id)
        if not picks:
            continue
        rb.add_markdown(f"### Cella {_cell_title(cell_id)}")
        firsts: dict[str, dict[str, Any]] = {}
        for k, r in enumerate(picks, start=1):
            firsts.setdefault(r["label_class"], r)
            cid = f"ex_{cell_id.lower()}_{k:02d}"
            rb.add_markdown(
                f"**{cell_id} #{k}** — {we.CLASS_HU[r['label_class']]}; t0 {_ts(r['t0'])}, t_c {_ts(r['t_c'])} UTC; "
                f"A = {100 * (np.exp(r['a_log']) - 1):.2f}% ({r['a_sigma']:.2f} σ_W), MFE {100 * r['fwd_mfe_log']:+.2f}%, "
                f"MAE {100 * r['fwd_mae_log']:+.2f}%.")
            rb.add_chart(cid, _example_fig(df_1m, r), title=f"{cell_id} #{k}: {we.CLASS_HU[r['label_class']]} — t_c {_ts(r['t_c'])}",
                         height=480)
            ex_rows.append({"cell": cell_id, "example": k, "label": we.CLASS_HU[r["label_class"]], "t0": r["t0"], "t_c": r["t_c"],
                            "a_pct": 100 * (np.exp(r["a_log"]) - 1), "a_sigma": r["a_sigma"],
                            "mfe_pct": 100 * r["fwd_mfe_log"], "mae_pct": 100 * r["fwd_mae_log"], "chart": cid})
        panels[cell_id] = [firsts[c] for c in we.CLASS_ORDER if c in firsts]
    rb.add_table("examples", pd.DataFrame(ex_rows), title="A kiválasztott példák (train)", columns={
        "cell": "Cella", "example": "#", "label": "Címke", "t0": "t0", "t_c": "t_c",
        "a_pct": {"label": "A (%)", "digits": 2}, "a_sigma": {"label": "A/σ_W", "digits": 2},
        "mfe_pct": {"label": "MFE (%)", "digits": 2}, "mae_pct": {"label": "MAE (%)", "digits": 2}, "chart": "Chart"})

    # ---- 5. model input
    rb.add_markdown(
        "## 5. Így néz ki a modell bemenete (t919)\n\n"
        "Cellánként osztályonként az első fenti példa prefixe, ahogy a t919 kapja: 15m OHLC a `low[t0]`-hoz viszonyítva, "
        "**ATR14(15m)**-mel normálva (a t_c-ig ismert ATR), alatta a 15m volumen a t0 előtti 7 napos átlagos 15m volumenhez "
        "viszonyítva. Az x-tengely bar-sorszám — a modell nem lát naptári időt, árszintet, és nem látja a `P*`-ot. "
        "(Az A-normált diagnosztikai változat — `low[t0]` → 0, `close[t_c]` → 1 — célközeli információt hordoz, ezért itt nem szerepel.)")
    for cell_id, picks in panels.items():
        rb.add_chart(f"input_{cell_id.lower()}", _input_panel(b15, picks, cell_id),
                     title=f"Modell-bemenet {_cell_title(cell_id)} — ATR-normált prefix + relatív volumen", height=460)

    # ---- 6. checklist + interpretation
    s = summary.set_index("cell")
    below = [c for c in s.index if s.loc[c, "train"] < MIN_TRAIN]
    oos_below = [c for c in s.index if s.loc[c, "oos"] < MIN_OOS]
    rb.add_markdown(
        "## 6. Mire figyelj az ellenőrzésnél\n\n"
        "1. **Jó mélypontokat talál?** Az áttekintő képen és a példákon a ▲ tényleg egy szemmel is látható lokális mélypont "
        "(egy esés vége), nem egy oldalazás véletlen minimuma? A t_c-ig tartó visszapattanás látványos-e?\n"
        "2. **Túl sok / túl kevés esemény?** A (4h, 4h) cellában havonta ~"
        f"{fmt(s.loc['W4h_H4h', 'per_month'], 0) if 'W4h_H4h' in s.index else '-'} esemény van. Kimarad-e nyilvánvaló, "
        "nagy visszapattanás (pl. mert az előző esemény H-sávja még tart, vagy mert az A < 1,5·σ_W)?\n"
        "3. **A címke egyezik a szemeddel?** Az „impulzus” példák tényleg erős folytatást mutatnak, a „korrekció” példák "
        "visszaesést / oldalazást? Zavaró-e, hogy az impulzus-küszöb (1:1 vetítés) nagy A-nál nagyon messze van?\n"
        "4. **A prefix a jó szakasz?** A kék ablak az esést és a visszapattanást is tartalmazza (B = W), és a t_c-nél ér véget — "
        "ez lesz a kép. Elég-e ennyi előzmény, vagy hiányzik a nagyobb trend?\n"
        "5. **Modell-bemenet:** az ATR-normált képek összevethetők-e egymással (hasonló skála a cellák és az évek között)?\n\n"
        "## Nyitott kérdések (user döntés, a t919 előtt)\n\n"
        f"- **Min. eseményszám:** train < {MIN_TRAIN}: {', '.join(below) if below else 'nincs'}; OOS < {MIN_OOS}: "
        f"{', '.join(oos_below) if oos_below else 'nincs'}. A 5200 szabálya szerint a minimum alatti cella **NO-GO** (nem "
        "küszöb-csökkentés). Ha minden cella OOS-a a minimum alatt van, a v1 rács a t920-ban nem értékelhető — ehhez "
        "**v2 döntés** kell (pl. a 5200 szöveg szerinti, csak címke-ablakos nem-átfedés; 1,0·σ_W küszöb; rövidebb W; vagy "
        "a 150-es OOS-küszöb felülvizsgálata a holdout-2 bevonása nélkül) — lásd az érzékenységi táblát.\n"
        "- **Osztály-egyensúly:** az impulzus a train-ben ritka (cellánként "
        + ", ".join(f"{c} {fmt(100 * s.loc[c, 'impulse_rate'], 1)}%" for c in s.index) +
        "); az oldalazott osztály dominál. A bináris címke (impulzus vs minden más) így erősen kiegyensúlyozatlan; a 3-osztályos "
        "változat (t0 alá / oldalazott külön) informatívabb lehet.\n"
        + (("- **Nem-átfedés:** a teljes intervallumos szabály (epic-döntés) a 5200 szövege szerinti (csak címke-ablakos) "
            "szabályhoz képest ennyi train eseményt vesz el: " + ", ".join(
                f"{c} {fmt(100 * (1 - sens.iloc[0][f'{c}_train'] / sens.iloc[1][f'{c}_train']), 0)}%" for c in s.index)
            + "; a kettő közötti választás a független minták és a darabszám közti kompromisszum. Az OOS ≥ "
            f"{MIN_OOS} egyik vizsgált változatban sem teljesül (legjobb: "
            f"{int(sens[[f'{c}_oos' for c in s.index]].to_numpy().max())}, a leglazább változattal): a 6 hónapos OOS-ablak "
            "ehhez a küszöbhöz túl rövid, tehát a küszöböt vagy az OOS-ablakot kell újragondolni (v2 döntés).\n")
           if sens is not None and len(sens) > 1 else "") +
        "- **Impulzus-szorzó 1,0:** nagy A mellett a P* messze van, ezért az impulzus még ritkább; egy kisebb szorzó (pl. 0,618) "
        "Elliott-szerűbb lenne, de ez is v2."
    )
    rate_txt = "; ".join(
        f"{c}: {int(s.loc[c, 'train'])} train esemény, impulzus {fmt(100 * s.loc[c, 'impulse_rate'], 1)}% / t0 alá "
        f"{fmt(100 * s.loc[c, 't0_below_rate'], 1)}% / oldalazott {fmt(100 * s.loc[c, 'sideways_rate'], 1)}%" for c in s.index)
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"Az adatkészlet a 5200 v1 definíciói szerint felépült és look-ahead-mentes (egységteszt: csonkolt vs teljes futás). "
        f"Cellánként: {rate_txt}. A medián A/σ_W cellánként "
        + ", ".join(f"{fmt(tr_all.filter(pl.col('cell_id') == c)['a_sigma'].median(), 2)}" for c in s.index)
        + " (a szűrő 1,5-nél vág). Teljes train-évek (2021–2025) eseményszáma cellánként min–max: "
        + ", ".join(f"{c} {lo}–{hi}" for c, lo, hi in _year_range(tr_all, s.index))
        + " — ha a tartomány szűk, a detektor nem egyetlen rezsimből él.\n\n"
        "**Állásfoglalás:** a detektor strukturálisan rendben van, de a v1 rács **darabszám-korlátos**: a 8h / 12h cellák a train "
        f"minimum alatt vannak, és az OOS-ban egyik cella sem éri el a {MIN_OOS} eseményt. Az impulzus osztály ritka "
        f"(train-ben összesen {int(tr_all.filter(pl.col('label_class') == 'impulse').height)} esemény a 4 cellában együtt). "
        "A t919 (embedding + klaszterezés) a vizuális jóváhagyás után indítható a 4h-s cellákon, de a t920 gate a v1 "
        "darabszámokkal várhatóan nem lesz értékelhető — erről a t919 előtt célszerű dönteni (v2 vagy tudatos "
        "„csak feltáró” t919)."
    )
    return rb.write()


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or SLUG).split("\n")[0])
    ap.add_argument("--asset-id", default=None)
    ap.add_argument("--no-store", action="store_true", help="do not write research.wave_events_<version> into the lab DB")
    ap.add_argument("--no-sensitivity", action="store_true")
    ap.add_argument("--version", choices=("v1", "v2"), default="v2", help="5200 version (v1 = frozen original)")
    args = ap.parse_args()
    out = build(asset_id=args.asset_id, store=not args.no_store, sensitivity=not args.no_sensitivity,
                params=we.params_v2() if args.version == "v2" else None)
    print(out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
