# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false
"""General report ``sr_trendline_detection``: visual check of the S/R + trendline detector (epic_071 / t911).

Source: live DB ``ohlcv`` (1m, read-only) -> ``data_handling.sync_tables._levels_polars`` on every scale
(s = 1h / 7d, m = 4h / 30d, l = 1d / 180d). Scope ``general`` -> ``_doc_/reports/sr_trendline_detection/``.

The report is a user checkpoint before the detector output becomes a feature: summary tables per scale,
overview candle charts with the detected objects drawn only over their life (``confirmed_at`` ->
expiry / break) and 8-12 deterministically selected zoomed examples. The forward outcome rates are RAW
(no placebo) and are explicitly labelled as such.

Regenerate::

    uv run python -m reports.builders.sr_detection        (cwd: repo root, PYTHONPATH=src via uv/pytest config)
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl

from data_handling.sync_tables import _levels_polars as det
from reports import ReportBuilder, theme
from reports.builders._common import fmt, source_info, themed

logger = logging.getLogger(__name__)

SLUG = "sr_trendline_detection"
GENERATOR = "reports.builders.sr_detection"
SCALES = ("s", "m", "l")
BAR_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400, "1d": 86400}

OVERVIEW_M_L_DAYS = 90      # 4h candles with m + l levels
OVERVIEW_S_DAYS = 30        # 1h candles with s levels + trendlines
EXAMPLE_LOOKBACK_DAYS = 365  # examples are chosen from the last 12 months
EXAMPLE_PRE_BARS = 90        # zoomed window: at most this many scale bars before the key event
EXAMPLES_PER_CATEGORY = 3
EXAMPLE_MIN_GAP = timedelta(days=7)  # key events of two examples are at least this far apart

# visual encoding -------------------------------------------------------------------------------
ROLE_COLOR = {"support": theme.LONG, "resistance": theme.SHORT}
ROLE_HU = {"support": "támasz", "resistance": "ellenállás"}
CANDLE_UP = "#8b949e"
CANDLE_DOWN = "#5c6670"
PIVOT_COLOR = theme.AMBER
#: (label, line width, opacity) per strength tier; levels: as-of strength, lines: as-of touch count
TIERS = (("gyenge", 1.4, 0.6), ("közepes", 2.4, 0.8), ("erős", 3.6, 1.0))
LEVEL_TIER_EDGES = (0.5, 1.2)   # strength = sum exp(-age/tau) of the touches known at t (~50/35/15 % of drawn time)
LINE_TIER_EDGES = (4, 5)        # touches known at t: 3 -> weak, 4 -> medium, >= 5 -> strong
EVENT_SYMBOL = {"touch": "circle-open", "break": "x", "retest": "diamond"}
EVENT_HU = {"touch": "érintés", "break": "kitörés", "retest": "visszateszt"}

CATEGORIES = {
    "bounce": "Erős, többszörösen érintett szint visszapattanással",
    "role_flip": "Szerepcsere (áttört szint visszatesztje az új oldalról, tart)",
    "line_break_retest": "Trendvonal-kitörés + sikeres visszateszt",
    "false_break": "Hamis kitörés (záróár visszatér ε-on belülre)",
}


# --------------------------------------------------------------------------- data access


def load_ohlcv_1m(asset_id: str | None = None, *, tries: int = 10, delay: float = 3.0) -> pl.DataFrame | None:
    """Full 1m OHLCV of the asset from the live DB, strictly read-only (retries while a writer holds the lock)."""
    from data_handling.store.readonly_query import open_readonly_connection

    sql = "SELECT open_time, open, high, low, close, volume FROM live.ohlcv ORDER BY open_time"
    for attempt in range(tries):
        try:
            con = open_readonly_connection(asset_id)
        except duckdb.Error as exc:
            logger.warning("live db locked/unavailable (attempt %d/%d): %s", attempt + 1, tries, exc)
            if attempt == tries - 1:
                return None
            time.sleep(delay)
            continue
        try:
            return con.execute(sql).pl()
        finally:
            con.close()
    return None


# --------------------------------------------------------------------------- small helpers


def _scale_meta(params: dict, scale: str) -> tuple[str, int, int]:
    """(bar, bar seconds, lookback days) of a scale."""
    sc = params["scales"][scale]
    return sc["bar"], BAR_SECONDS[sc["bar"]], int(float(sc["lookback_days"]))


def _q(series: pl.Series, q: float) -> float | None:
    s = series.drop_nulls()
    return float(s.quantile(q, interpolation="linear")) if s.len() else None  # type: ignore[arg-type]


def _tier(value: float, edges: Sequence[float]) -> int:
    return int(np.searchsorted(np.asarray(edges, dtype=float), value, side="right"))


def _ts(value: Any) -> str:
    if value is None:
        return "-"
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")


def _data_end(res: det.ScaleResult) -> datetime:
    return res.bars["close_time"][-1]


def _touches_as_of_event(touches: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """events + ``n_touches_at`` = touches of the object confirmed <= event_time (no look-ahead)."""
    if events.height == 0:
        return events.with_columns(pl.lit(0, pl.Int64).alias("n_touches_at"))
    t = touches.select(pl.col("obj_id"), pl.col("confirmed_at").alias("_tc"))
    joined = (events.with_row_index("_r").join(t, on="obj_id", how="left")
              .with_columns((pl.col("_tc") <= pl.col("event_time")).fill_null(False).alias("_known"))
              .group_by("_r").agg(pl.col("_known").sum().cast(pl.Int64).alias("n_touches_at")))
    return events.with_row_index("_r").join(joined, on="_r", how="left").drop("_r")


# --------------------------------------------------------------------------- summary tables


def _initial_roles(res: det.ScaleResult) -> pl.Series:
    """Level role at its confirmation (close of the confirming bar vs level price)."""
    closes = res.bars["close"].to_numpy()
    idx = res.levels["confirm_idx"].to_numpy()
    prices = res.levels["price"].to_numpy()
    return pl.Series("role0", np.where(closes[idx] >= prices, "support", "resistance"))


def _scale_summary(results: dict[str, det.ScaleResult], params: dict) -> pd.DataFrame:
    rows = []
    for s, res in results.items():
        bar, _, lb = _scale_meta(params, s)
        lv, pv = res.levels, res.pivots
        end = _data_end(res)
        life = (lv.select((pl.coalesce("expired_at", pl.lit(end)) - pl.col("confirmed_at"))
                          .dt.total_seconds() / 86400).to_series()) if lv.height else pl.Series([], dtype=pl.Float64)
        role0 = _initial_roles(res) if lv.height else pl.Series([], dtype=pl.Utf8)
        rows.append({
            "scale": s, "bar": bar, "lookback_days": lb, "bars": res.bars.height,
            "first_bar": res.bars["open_time"][0] if res.bars.height else None,
            "pivots": pv.height,
            "pivots_high": int((pv["kind"] == "high").sum()), "pivots_low": int((pv["kind"] == "low").sum()),
            "levels": lv.height,
            "levels_support": int((role0 == "support").sum()), "levels_resistance": int((role0 == "resistance").sum()),
            "touches_median": _q(lv["n_touches"].cast(pl.Float64), 0.5) if lv.height else None,
            "touches_p90": _q(lv["n_touches"].cast(pl.Float64), 0.9) if lv.height else None,
            "touches_max": int(lv["n_touches"].max()) if lv.height else None,  # type: ignore[arg-type]
            "life_days_median": _q(life, 0.5), "life_days_p90": _q(life, 0.9),
            "round10_pct": float(lv["is_round"].mean()) if lv.height else None,  # type: ignore[arg-type]
            "round5_pct": float(lv["is_round_secondary"].mean()) if lv.height else None,  # type: ignore[arg-type]
            "role_flip_pct": float((lv["n_role_flips"] > 0).mean()) if lv.height else None,  # type: ignore[arg-type]
            "levels_per_month": lv.height / max(res.bars.height * BAR_SECONDS[bar] / (30 * 86400), 1e-9),
        })
    return pd.DataFrame(rows)


def _touch_distribution(results: dict[str, det.ScaleResult], obj: str) -> pd.DataFrame:
    buckets = ["2", "3", "4", "5", "6+"] if obj == "level" else ["3", "4", "5", "6+"]
    rows = []
    for b in buckets:
        row: dict[str, Any] = {"n_touches": b}
        for s, res in results.items():
            frame = res.levels if obj == "level" else res.trendlines
            n = frame["n_touches"].to_numpy() if frame.height else np.array([], dtype=int)
            cnt = int((n >= 6).sum()) if b == "6+" else int((n == int(b)).sum())
            row[f"{s}_n"] = cnt
            row[f"{s}_pct"] = cnt / n.size if n.size else None
        rows.append(row)
    return pd.DataFrame(rows)


def _line_summary(results: dict[str, det.ScaleResult], params: dict) -> pd.DataFrame:
    rows = []
    for s, res in results.items():
        _, secs, _ = _scale_meta(params, s)
        tl = res.trendlines
        end = _data_end(res)
        for kind in ("support", "resistance"):
            g = tl.filter(pl.col("kind") == kind)
            if g.height == 0:
                rows.append({"scale": s, "kind": ROLE_HU[kind], "lines": 0})
                continue
            span = ((g["last_touch"] - g["anchor1_time"]).dt.total_seconds() / secs).cast(pl.Float64)
            life = ((pl.select(pl.coalesce(g["expired_at"], pl.lit(end))).to_series() - g["confirmed_at"])
                    .dt.total_seconds() / 86400)
            slope = g["slope_per_bar_atr"]
            rows.append({
                "scale": s, "kind": ROLE_HU[kind], "lines": g.height,
                "touches_median": _q(g["n_touches"].cast(pl.Float64), 0.5),
                "touches_max": int(g["n_touches"].max()),  # type: ignore[arg-type]
                "span_bars_median": _q(span, 0.5), "span_bars_p90": _q(span, 0.9),
                "life_days_median": _q(life, 0.5),
                "slope_atr_p10": _q(slope, 0.1), "slope_atr_median": _q(slope, 0.5), "slope_atr_p90": _q(slope, 0.9),
                "broken_pct": float((g["expiry_reason"] == "break").fill_null(False).mean()),  # type: ignore[arg-type]
            })
    return pd.DataFrame(rows)


def _event_counts(results: dict[str, det.ScaleResult]) -> pd.DataFrame:
    rows = []
    for s, res in results.items():
        for obj in ("level", "line"):
            ev = res.events.filter(pl.col("obj_type") == obj)
            row: dict[str, Any] = {"scale": s, "obj_type": "szint" if obj == "level" else "trendvonal"}
            for et in ("touch", "break", "retest"):
                row[et] = int((ev["event_type"] == et).sum())
            row["total"] = ev.height
            rows.append(row)
    return pd.DataFrame(rows)


def _event_yearly(results: dict[str, det.ScaleResult]) -> pd.DataFrame:
    frames = [res.events.select(pl.col("event_time").dt.year().alias("year"), pl.lit(s).alias("scale"), "event_type")
              for s, res in results.items() if res.events.height]
    if not frames:
        return pd.DataFrame({"year": []})
    ev = pl.concat(frames)
    wide = (ev.with_columns(pl.concat_str(["scale", "event_type"], separator="_").alias("k"))
            .group_by("year", "k").len().pivot(on="k", index="year", values="len").sort("year").fill_null(0))
    cols = ["year"] + [f"{s}_{e}" for s in results for e in ("touch", "break", "retest") if f"{s}_{e}" in wide.columns]
    return wide.select(cols).to_pandas()


POS_LABEL = {"touch": "bounce", "break": "follow_through", "retest": "hold"}


def _forward_summary(fwd: dict[str, pl.DataFrame]) -> pd.DataFrame:
    rows = []
    for s, ev in fwd.items():
        for obj in ("level", "line"):
            for et in ("touch", "break", "retest"):
                g = ev.filter((pl.col("obj_type") == obj) & (pl.col("event_type") == et))
                lab = g.filter(pl.col("forward_label").is_not_null())
                pos = POS_LABEL[et]

                def rate(frame: pl.DataFrame, pos: str = pos) -> float | None:
                    return float((frame["forward_label"] == pos).mean()) if frame.height else None  # type: ignore[arg-type]

                rows.append({
                    "scale": s, "obj_type": "szint" if obj == "level" else "trendvonal", "event_type": et,
                    "positive_label": pos, "events": g.height, "labelled": lab.height,
                    "raw_positive_rate": rate(lab),
                    "raw_positive_support_rate": rate(lab.filter(pl.col("side") == "support")),
                    "raw_positive_resistance_rate": rate(lab.filter(pl.col("side") == "resistance")),
                    "timeout_rate": float(lab["forward_timeout"].mean()) if lab.height else None,  # type: ignore[arg-type]
                    "mfe_atr_median": _q(lab["forward_mfe_atr"], 0.5) if lab.height else None,
                })
    return pd.DataFrame(rows)


def _monthly_events_fig(results: dict[str, det.ScaleResult]) -> go.Figure:
    colors = {"s": theme.BLUE, "m": theme.ORANGE, "l": theme.ACCENT}
    fig = go.Figure()
    for s, res in results.items():
        if res.events.height == 0:
            continue
        m = (res.events.group_by(pl.col("event_time").dt.truncate("1mo").alias("month")).len().sort("month"))
        fig.add_trace(go.Scatter(x=m["month"].to_list(), y=m["len"].to_list(), name=f"{s} lépték", mode="lines",
                                 line={"color": colors.get(s, theme.NEUTRAL), "width": 1.6}))
    fig.update_yaxes(title="Események / hó (log)", type="log")
    return fig


# --------------------------------------------------------------------------- object geometry


@dataclass
class _Obj:
    """One level / trendline prepared for drawing (times are naive UTC datetimes)."""

    obj_type: str          # 'level' / 'line'
    obj_id: str
    scale: str
    start: datetime        # confirmed_at
    end: datetime          # expiry / break (data end if alive)
    price_fn: Callable[[datetime], float]
    role0: str             # role at confirmation
    flips: list[datetime]  # break event times (role flips; lines: only the first)
    touch_conf: np.ndarray  # touch confirmed_at (datetime64[us])
    touch_time: np.ndarray  # touch pivot_time
    tau_s: float           # strength decay (levels)
    info: dict[str, Any] = field(default_factory=dict)

    def touches_at(self, t: datetime) -> int:
        return int((self.touch_conf <= np.datetime64(t, "us")).sum())

    def strength_at(self, t: datetime) -> float:
        known = self.touch_conf <= np.datetime64(t, "us")
        age = (np.datetime64(t, "us") - self.touch_time[known]).astype("timedelta64[s]").astype(float)
        return float(np.exp(-age / self.tau_s).sum())

    def role_at(self, t: datetime) -> str:
        n = sum(1 for f in self.flips if f <= t)
        if n % 2 == 0:
            return self.role0
        return "resistance" if self.role0 == "support" else "support"

    def tier_at(self, t: datetime) -> int:
        if self.obj_type == "level":
            return _tier(self.strength_at(t), LEVEL_TIER_EDGES)
        return _tier(self.touches_at(t), LINE_TIER_EDGES)


def _touch_arrays(touches: pl.DataFrame, obj_id: str) -> tuple[np.ndarray, np.ndarray]:
    t = touches.filter(pl.col("obj_id") == obj_id)
    return (t["confirmed_at"].to_numpy().astype("datetime64[us]"), t["pivot_time"].to_numpy().astype("datetime64[us]"))


def _level_objs(res: det.ScaleResult, params: dict, lo: datetime, hi: datetime) -> list[_Obj]:
    """Levels whose life [confirmed_at, expiry) overlaps [lo, hi)."""
    _, _, lb = _scale_meta(params, res.scale)
    tau = lb * 86400 * float(params["strength_tau_frac"])
    end = _data_end(res)
    lv = res.levels.with_columns(pl.coalesce("expired_at", pl.lit(end)).alias("_end"))
    if lv.height:
        lv = lv.with_columns(_initial_roles(res))
    lv = lv.filter((pl.col("confirmed_at") < hi) & (pl.col("_end") > lo))
    brk = res.events.filter((pl.col("obj_type") == "level") & (pl.col("event_type") == "break"))
    out = []
    for r in lv.iter_rows(named=True):
        conf, tt = _touch_arrays(res.level_touches, r["level_id"])
        price = float(r["price"])
        flips = brk.filter(pl.col("obj_id") == r["level_id"])["event_time"].to_list()
        out.append(_Obj("level", r["level_id"], res.scale, r["confirmed_at"], r["_end"],
                        lambda _t, p=price: p, r["role0"], flips, conf, tt, tau, info=r))
    return out


def _line_objs(res: det.ScaleResult, params: dict, lo: datetime, hi: datetime) -> list[_Obj]:
    _, secs, _ = _scale_meta(params, res.scale)
    end = _data_end(res)
    tl = res.trendlines.with_columns(
        pl.min_horizontal(pl.coalesce("broken_at", "expired_at", pl.lit(end)),
                          pl.coalesce("expired_at", pl.lit(end))).alias("_end"))
    tl = tl.filter((pl.col("confirmed_at") < hi) & (pl.col("_end") > lo))
    out = []
    for r in tl.iter_rows(named=True):
        conf, tt = _touch_arrays(res.line_touches, r["line_id"])
        a_t, a_p, slope = r["anchor1_time"], float(r["anchor1_price"]), float(r["slope_per_bar"])

        def fn(t: datetime, a_t: datetime = a_t, a_p: float = a_p, slope: float = slope) -> float:
            return a_p + slope * (t - a_t).total_seconds() / secs

        flips = [r["broken_at"]] if r["broken_at"] is not None else []
        out.append(_Obj("line", r["line_id"], res.scale, r["confirmed_at"], r["_end"], fn, r["kind"], flips,
                        conf, tt, 1.0, info=r))
    return out


def _obj_hover(o: _Obj, t: datetime) -> str:
    i = o.info
    head = f"<b>{o.obj_id}</b> ({o.scale} {'szint' if o.obj_type == 'level' else 'trendvonal'}) · {ROLE_HU[o.role_at(t)]}"
    if o.obj_type == "level":
        body = (f"ár {i['price']:.3f} ± {i['tolerance']:.3f} (0.25·ATR)<br>érintés eddig {o.touches_at(t)} "
                f"(össz. {i['n_touches']}) · erő {o.strength_at(t):.2f}<br>kerek: {'10' if i['is_round'] else ('5' if i['is_round_secondary'] else '-')}"
                f" · szerepcsere {i['n_role_flips']}")
    else:
        body = (f"ár itt {o.price_fn(t):.3f} · meredekség {i['slope_per_bar_atr']:+.3f} ATR/bar<br>"
                f"érintés eddig {o.touches_at(t)} (össz. {i['n_touches']})")
    tail = f"<br>megerősítve {_ts(o.start)} · vége {_ts(i.get('expired_at'))} · törés {_ts(i.get('broken_at'))}"
    return head + "<br>" + body + tail


@dataclass
class _Trace:
    x: list[Any] = field(default_factory=list)
    y: list[Any] = field(default_factory=list)
    text: list[Any] = field(default_factory=list)


def _add_objects(fig: go.Figure, objs: list[_Obj], grid: list[datetime], lo: datetime, hi: datetime,
                 *, dash: str, label: str, legend_rank: int) -> int:
    """Draw objects as life-limited segments, split where role / strength tier changes; returns segment count."""
    bucket: dict[tuple[str, int], _Trace] = {}
    grid_np = np.asarray(grid, dtype="datetime64[us]")
    n_seg = 0
    for o in objs:
        s0, s1 = max(o.start, lo), min(o.end, hi)
        if s1 <= s0:
            continue
        inner = [g.astype(datetime) for g in grid_np[(grid_np > np.datetime64(s0, "us")) & (grid_np < np.datetime64(s1, "us"))]]
        cuts = [s0, *inner, s1]
        runs: list[tuple[datetime, datetime, str, int]] = []
        for a, b in zip(cuts[:-1], cuts[1:], strict=True):
            key = (o.role_at(a), o.tier_at(a))
            if runs and (runs[-1][2], runs[-1][3]) == key:
                runs[-1] = (runs[-1][0], b, *key)
            else:
                runs.append((a, b, *key))
        for a, b, role, tier in runs:
            tr = bucket.setdefault((role, tier), _Trace())
            mid = a + (b - a) / 2
            for t in (a, mid, b):
                tr.x.append(t)
                tr.y.append(o.price_fn(t))
                tr.text.append(_obj_hover(o, t))
            tr.x.append(None)
            tr.y.append(None)
            tr.text.append(None)
            n_seg += 1
    shown: set[str] = set()
    for role in ("support", "resistance"):
        for tier in (2, 1, 0):
            tr = bucket.get((role, tier))
            if tr is None:
                continue
            name, width, opacity = TIERS[tier]
            group = f"{label}-{role}"
            fig.add_trace(go.Scatter(
                x=tr.x, y=tr.y, mode="lines", hovertext=tr.text, hoverinfo="text",
                name=f"{label} – {ROLE_HU[role]}", legendgroup=group, showlegend=group not in shown,
                legendrank=legend_rank + (0 if role == "support" else 1),
                line={"color": ROLE_COLOR[role], "width": width, "dash": dash}, opacity=opacity,
                connectgaps=False,
            ))
            shown.add(group)
    return n_seg


def _candles(bars: pl.DataFrame, name: str) -> go.Candlestick:
    return go.Candlestick(
        x=bars["open_time"].to_list(), open=bars["open"].to_list(), high=bars["high"].to_list(),
        low=bars["low"].to_list(), close=bars["close"].to_list(), name=name, showlegend=False,
        increasing={"line": {"color": CANDLE_UP, "width": 1}, "fillcolor": "rgba(139,148,158,0.15)"},
        decreasing={"line": {"color": CANDLE_DOWN, "width": 1}, "fillcolor": CANDLE_DOWN},
    )


def _pivot_trace(pivots: pl.DataFrame, name: str, *, size: int = 7, visible: Any = True) -> go.Scatter:
    hi = pivots["kind"] == "high"
    return go.Scatter(
        x=pivots["pivot_time"].to_list(), y=pivots["price"].to_list(), mode="markers", name=name,
        marker={"symbol": np.where(hi, "triangle-down", "triangle-up").tolist(), "size": size,
                "color": PIVOT_COLOR, "line": {"width": 0}},
        hovertext=[f"pivot {r['pivot_id']} ({r['kind']})<br>ár {r['price']:.3f} · prominencia {r['prominence_atr']:.2f} ATR"
                   f"<br>megerősítve {_ts(r['confirmed_at'])}" for r in pivots.iter_rows(named=True)],
        hoverinfo="text", visible=visible, legendrank=900,
    )


def _price_layout(fig: go.Figure, bars: pl.DataFrame, *, height: int) -> go.Figure:
    lo, hi = float(bars["low"].min()), float(bars["high"].max())  # type: ignore[arg-type]
    pad = (hi - lo) * 0.04
    themed(fig, height=height, hovermode="closest",
           legend={"orientation": "h", "y": -0.1, "x": 0, "yanchor": "top"},
           margin={"l": 56, "r": 16, "t": 28, "b": 72}, showlegend=True)
    fig.update_xaxes(rangeslider={"visible": False}, type="date")
    fig.update_yaxes(range=[lo - pad, hi + pad], title="SOLUSDT")
    return fig


def _in_range(o: _Obj, lo_px: float, hi_px: float, lo: datetime, hi: datetime) -> bool:
    a, b = max(o.start, lo), min(o.end, hi)
    ys = (o.price_fn(a), o.price_fn(b))
    return max(ys) >= lo_px and min(ys) <= hi_px


def _overview(results: dict[str, det.ScaleResult], params: dict, *, base: str, days: int,
              layers: list[tuple[str, str, str]]) -> tuple[go.Figure, dict[str, int]]:
    """Candles of scale ``base`` over the last ``days`` with (scale, obj_type, dash) layers."""
    bres = results[base]
    hi = _data_end(bres)
    lo = hi - timedelta(days=days)
    bars = bres.bars.filter(pl.col("open_time") >= lo)
    lo = bars["open_time"][0]
    grid = bars["open_time"].to_list()
    lo_px, hi_px = float(bars["low"].min()), float(bars["high"].max())  # type: ignore[arg-type]
    pad = (hi_px - lo_px) * 0.04
    fig = go.Figure([_candles(bars, f"SOL {params['scales'][base]['bar']}")])
    counts: dict[str, int] = {}
    for rank, (s, obj, dash) in enumerate(layers):
        res = results[s]
        objs = _level_objs(res, params, lo, hi) if obj == "level" else _line_objs(res, params, lo, hi)
        vis = [o for o in objs if _in_range(o, lo_px - pad, hi_px + pad, lo, hi)]
        label = f"{s} {'szint' if obj == 'level' else 'trendvonal'}"
        _add_objects(fig, vis, grid, lo, hi, dash=dash, label=label, legend_rank=100 + 10 * rank)
        counts[f"{s}_{obj}"] = len(vis)
        counts[f"{s}_{obj}_offscreen"] = len(objs) - len(vis)
    piv = bres.pivots.filter((pl.col("pivot_time") >= lo) & (pl.col("confirmed_at") <= hi))
    if piv.height:
        fig.add_trace(_pivot_trace(piv, f"{base} pivotok", size=6))
    return _price_layout(fig, bars, height=640), counts


# --------------------------------------------------------------------------- examples


@dataclass
class Example:
    category: str
    scale: str
    obj_type: str
    obj_id: str
    key_time: datetime              # event_time of the key event (break or touch)
    key_bar: int                    # bar_idx of the key event
    end_bar: int                    # last bar of interest (retest / forward horizon)
    n_touches_at: int
    label: str
    note: str
    events: list[int]               # bar_idx of the highlighted events (key first)


def _candidates(fwd: dict[str, pl.DataFrame], results: dict[str, det.ScaleResult], params: dict) -> dict[str, dict[str, list[Example]]]:
    """Ranked candidates per category and scale (deterministic: touches desc, magnitude desc, time desc)."""
    H = int(params["forward_bars"])
    out: dict[str, dict[str, list[Example]]] = {c: {} for c in CATEGORIES}
    for s in SCALES:
        if s not in fwd:
            continue
        res = results[s]
        n_bars = res.bars.height
        since = _data_end(res) - timedelta(days=EXAMPLE_LOOKBACK_DAYS)
        touches = pl.concat([res.level_touches, res.line_touches])
        ev = _touches_as_of_event(touches, fwd[s]).filter(
            (pl.col("event_time") >= since) & (pl.col("bar_idx") + H < n_bars))

        # A) strong level + bounce
        a = (ev.filter((pl.col("obj_type") == "level") & (pl.col("event_type") == "touch")
                       & (pl.col("forward_label") == "bounce"))
             .sort(["n_touches_at", "forward_mfe_atr", "event_time"], descending=True))
        out["bounce"][s] = [Example("bounce", s, "level", r["obj_id"], r["event_time"], r["bar_idx"],
                                    r["bar_idx"] + H, r["n_touches_at"], "bounce",
                                    f"{ROLE_HU[r['side']]}-érintés, +{r['forward_mfe_atr']:.1f} ATR max. elmozdulás",
                                    [r["bar_idx"]]) for r in a.head(10).iter_rows(named=True)]

        # B/C) break followed by a held retest of the same object (B: levels, C: lines)
        brk = ev.filter(pl.col("event_type") == "break").select(
            "obj_id", "obj_type", pl.col("event_time").alias("b_time"), pl.col("bar_idx").alias("b_bar"),
            pl.col("side").alias("b_side"), pl.col("n_touches_at"), pl.col("forward_label").alias("b_label"))
        ret = fwd[s].filter((pl.col("event_type") == "retest") & (pl.col("forward_label") == "hold")).select(
            "obj_id", pl.col("event_time").alias("r_time"), pl.col("bar_idx").alias("r_bar"),
            pl.col("forward_mfe_atr").alias("r_mfe"), pl.col("side").alias("r_side"))
        pairs = (brk.filter(pl.col("b_label") == "follow_through").join(ret, on="obj_id")
                 .filter((pl.col("r_bar") > pl.col("b_bar")) & (pl.col("r_bar") <= pl.col("b_bar") + int(params["retest_bars"]))
                         & (pl.col("r_bar") + H < n_bars))
                 .sort(["n_touches_at", "r_mfe", "b_time"], descending=True))
        for cat, obj in (("role_flip", "level"), ("line_break_retest", "line")):
            p = pairs.filter(pl.col("obj_type") == obj)
            out[cat][s] = [Example(cat, s, obj, r["obj_id"], r["b_time"], r["b_bar"], r["r_bar"] + H,
                                   r["n_touches_at"], f"break={r['b_label']} → retest=hold",
                                   f"{ROLE_HU[r['b_side']]} áttörve, visszateszt {ROLE_HU[r['r_side']]}ként, "
                                   f"+{r['r_mfe']:.1f} ATR", [r["b_bar"], r["r_bar"]])
                           for r in p.head(10).iter_rows(named=True)]

        # D) false break (close back within eps, not a timeout)
        d = (ev.filter((pl.col("event_type") == "break") & (pl.col("forward_label") == "false_break")
                       & (~pl.col("forward_timeout")))
             .sort(["n_touches_at", "forward_bars", "event_time"], descending=[True, False, True]))
        out["false_break"][s] = [Example("false_break", s, r["obj_type"], r["obj_id"], r["event_time"], r["bar_idx"],
                                         r["bar_idx"] + H, r["n_touches_at"], "false_break",
                                         f"{ROLE_HU[r['side']]} {'szint' if r['obj_type'] == 'level' else 'trendvonal'} "
                                         f"átütve, {r['forward_bars']} bar múlva vissza", [r["bar_idx"]])
                                 for r in d.head(10).iter_rows(named=True)]
    return out


def select_examples(cands: dict[str, dict[str, list[Example]]], per_category: int = EXAMPLES_PER_CATEGORY,
                    min_gap: timedelta = EXAMPLE_MIN_GAP) -> list[Example]:
    """Round-robin over the scales (l, m, s) per category.

    An object is used at most once, and key events closer than ``min_gap`` to an already picked
    example are skipped (one market episode is shown once).
    """
    used: set[str] = set()
    picked: list[Example] = []

    def clash(ex: Example) -> bool:
        return ex.obj_id in used or any(abs(ex.key_time - p.key_time) < min_gap for p in picked)

    for cat in CATEGORIES:
        pools = {s: list(cands[cat].get(s, [])) for s in ("l", "m", "s")}
        chosen = 0
        while chosen < per_category and any(pools.values()):
            for s in ("l", "m", "s"):
                pool = pools[s]
                while pool and clash(pool[0]):
                    pool.pop(0)
                if pool and chosen < per_category:
                    ex = pool.pop(0)
                    used.add(ex.obj_id)
                    picked.append(ex)
                    chosen += 1
    return picked


def _example_fig(ex: Example, res: det.ScaleResult, fwd: pl.DataFrame, params: dict) -> go.Figure:
    H = int(params["forward_bars"])
    bars_all = res.bars
    n = bars_all.height
    if ex.obj_type == "level":
        row = res.levels.filter(pl.col("level_id") == ex.obj_id).row(0, named=True)
        first = row["first_touch"]
    else:
        row = res.trendlines.filter(pl.col("line_id") == ex.obj_id).row(0, named=True)
        first = row["anchor1_time"]
    first_bar = int(np.searchsorted(bars_all["open_time"].to_numpy(), np.datetime64(first, "us")))
    start = max(0, min(first_bar - 8, ex.key_bar - 30), ex.key_bar - EXAMPLE_PRE_BARS)
    stop = min(n, ex.end_bar + 6)
    bars = bars_all.slice(start, stop - start)
    lo, hi = bars["open_time"][0], bars["close_time"][-1]
    bar_len = bars["close_time"][0] - bars["open_time"][0]
    grid = bars["open_time"].to_list()

    fig = go.Figure([_candles(bars, f"SOL {params['scales'][ex.scale]['bar']}")])
    objs = (_level_objs(res, params, lo, hi) if ex.obj_type == "level" else _line_objs(res, params, lo, hi))
    focal = [o for o in objs if o.obj_id == ex.obj_id]
    o = focal[0]
    # formation (first touch -> confirmed_at): thin dotted, not yet known to the detector
    f0 = max(first, lo)
    if f0 < o.start:
        fig.add_trace(go.Scatter(x=[f0, o.start], y=[o.price_fn(f0), o.price_fn(o.start)], mode="lines",
                                 name="kialakulás (még nem ismert)", hoverinfo="skip",
                                 line={"color": theme.NEUTRAL, "width": 1.2, "dash": "dot"}, legendrank=50))
    label = f"{ex.scale} {'szint' if ex.obj_type == 'level' else 'trendvonal'}"
    _add_objects(fig, focal, grid, lo, hi, dash="solid", label=label, legend_rank=100)
    # broken line: dotted continuation through the retest window
    if ex.obj_type == "line" and o.info.get("broken_at") is not None:
        t1 = min(o.info["broken_at"] + bar_len * int(params["retest_bars"]), hi)
        t0 = o.info["broken_at"]
        if t1 > t0:
            fig.add_trace(go.Scatter(x=[t0, t1], y=[o.price_fn(t0), o.price_fn(t1)], mode="lines",
                                     name="törés után (retest ablak)", hoverinfo="skip", legendrank=60,
                                     line={"color": ROLE_COLOR[o.role_at(t0)], "width": 1.4, "dash": "dot"}))
    # pivots: all of this scale in the window (small) + the object's touch pivots (large ring)
    piv = res.pivots.filter((pl.col("pivot_time") >= lo) & (pl.col("pivot_time") <= hi))
    if piv.height:
        fig.add_trace(_pivot_trace(piv, "pivotok", size=8))
    tch_all = (res.level_touches if ex.obj_type == "level" else res.line_touches).filter(pl.col("obj_id") == ex.obj_id)
    tch = tch_all.filter((pl.col("pivot_time") >= lo) & (pl.col("pivot_time") <= hi))
    if tch.height:
        fig.add_trace(go.Scatter(
            x=tch["pivot_time"].to_list(), y=tch["price"].to_list(), mode="markers", name="az objektum érintés-pivotjai",
            marker={"symbol": "circle-open", "size": 15, "color": PIVOT_COLOR, "line": {"width": 2}},
            hovertext=[f"érintés-pivot {r['pivot_id']}<br>ár {r['price']:.3f}<br>megerősítve {_ts(r['confirmed_at'])}"
                       for r in tch.iter_rows(named=True)], hoverinfo="text", legendrank=800))
    # confirmed_at marker (hover = object parameters); outside the window -> note at the left edge
    if o.start >= lo:
        fig.add_trace(go.Scatter(
            x=[o.start], y=[o.price_fn(o.start)], mode="markers", name="confirmed_at",
            marker={"symbol": "star", "size": 16, "color": "#ffffff", "line": {"color": theme.ACCENT, "width": 1}},
            hovertext=[_obj_hover(o, o.start)], hoverinfo="text", legendrank=700))
        fig.add_vline(x=o.start, line={"color": theme.NEUTRAL, "width": 1, "dash": "dot"})
        fig.add_annotation(x=o.start, y=1.0, yref="paper", text="megerősítve", showarrow=False, yanchor="bottom",
                           font={"size": 11, "color": theme.NEUTRAL})
    else:
        before = int((tch_all["pivot_time"] < lo).sum())
        fig.add_trace(go.Scatter(
            x=[lo], y=[o.price_fn(lo)], mode="markers", name="confirmed_at (az ablak előtt)",
            marker={"symbol": "star-open", "size": 14, "color": "#ffffff"},
            hovertext=[_obj_hover(o, lo)], hoverinfo="text", legendrank=700))
        fig.add_annotation(x=lo, y=1.0, yref="paper", xanchor="left", yanchor="bottom", showarrow=False,
                           text=f"megerősítve {_ts(o.start)} (az ablak előtt) · {before} érintés-pivot az ablak előtt",
                           font={"size": 11, "color": theme.NEUTRAL})
    # events of the object inside the window
    oev = fwd.filter((pl.col("obj_id") == ex.obj_id) & (pl.col("bar_idx") >= start) & (pl.col("bar_idx") < stop))
    otimes = bars_all["open_time"]
    for et in ("touch", "break", "retest"):
        g = oev.filter(pl.col("event_type") == et)
        if g.height == 0:
            continue
        fig.add_trace(go.Scatter(
            x=[otimes[int(b)] for b in g["bar_idx"]], y=g["price"].to_list(), mode="markers",
            name=f"esemény: {EVENT_HU[et]}", legendrank=600,
            marker={"symbol": EVENT_SYMBOL[et], "size": 12, "color": "#ffffff" if et != "break" else theme.ORANGE,
                    "line": {"width": 2, "color": theme.ORANGE if et != "touch" else "#ffffff"}},
            hovertext=[f"<b>{EVENT_HU[et]}</b> ({r['side']}) · bar zár {_ts(r['event_time'])}<br>objektum-ár {r['price']:.3f}"
                       f" · záró {r['bar_close']:.3f} · ATR {r['atr']:.3f}<br>nyers forward: {r['forward_label']}"
                       f" ({r['forward_bars']} bar{', timeout' if r['forward_timeout'] else ''}) · MFE {fmt(r['forward_mfe_atr'], 2)} / "
                       f"MAE {fmt(r['forward_mae_atr'], 2)} ATR" for r in g.iter_rows(named=True)],
            hoverinfo="text"))
    # key event annotation + forward horizon shading
    for k, b in enumerate(ex.events):
        r = oev.filter(pl.col("bar_idx") == b)
        if r.height == 0:
            continue
        r0 = r.row(0, named=True)
        txt = f"{EVENT_HU[r0['event_type']]} → {r0['forward_label']}"
        fig.add_annotation(x=otimes[b], y=r0["price"], text=txt, showarrow=True, arrowhead=2, arrowcolor=theme.ORANGE,
                           ax=-40 if k == 0 else 40, ay=-42 if r0["side"] == "support" else 42,
                           font={"size": 12, "color": theme.ORANGE}, bgcolor="rgba(0,0,0,0.35)")
    kb = ex.events[-1]
    x0 = otimes[kb]
    x1 = otimes[min(kb + H, n - 1)]
    fig.add_vrect(x0=x0, x1=x1, fillcolor=theme.BLUE, opacity=0.07, line_width=0,
                  annotation_text=f"forward {H} bar", annotation_position="top left",
                  annotation_font={"size": 10, "color": theme.NEUTRAL})
    _price_layout(fig, bars, height=500)
    fig.update_xaxes(range=[lo, hi])
    return fig


# --------------------------------------------------------------------------- active levels table


def _active_now(results: dict[str, det.ScaleResult], params: dict) -> pd.DataFrame:
    frames = []
    for res in results.values():
        end = _data_end(res)
        lv = det.levels_as_of(res, end, active_only=True, params=params)
        if lv.height:
            frames.append(lv.select("scale", "level_id", "price", "role", "n_touches", "strength", "confirmed_at",
                                    "last_touch", "n_role_flips", "is_round"))
    if not frames:
        return pd.DataFrame({"scale": []})
    return pl.concat(frames).sort(["scale", "price"], descending=[False, True]).to_pandas()


# --------------------------------------------------------------------------- report


def _param_table(params: dict) -> pd.DataFrame:
    keys = ["atr_window", "pivot_n", "pivot_min_prominence_atr", "level_tolerance_atr", "level_min_touches",
            "strength_tau_frac", "round_step", "round_step_secondary", "line_tolerance_atr", "line_min_touches",
            "line_dedup_slope_atr", "break_atr", "retest_bars", "forward_bars", "forward_move_atr"]
    rows = [{"param": k, "value": str(params.get(k))} for k in keys if k in params]
    rows += [{"param": f"scales.{s}", "value": f"{v['bar']} bar, lookback {v['lookback_days']} nap"}
             for s, v in params["scales"].items()]
    return pd.DataFrame(rows)


def build(root: Path | None = None, *, df_1m: pl.DataFrame | None = None, params: dict | None = None,
          asset_id: str | None = None) -> Path | None:
    """Build and write the report; ``None`` when no OHLCV data is available.

    Args:
        root     : Report root override (tests); default ``_doc_/reports``.
        df_1m    : 1m OHLCV (tests / reuse); default: the live ``ohlcv`` table, read-only.
        params   : Detector params; default ``config/features.json`` ``levels_detector``.
        asset_id : Asset key for the live DB (default asset when None).
    """
    p = det.load_params(params)
    if df_1m is None:
        df_1m = load_ohlcv_1m(asset_id)
    if df_1m is None or df_1m.height == 0:
        logger.warning("%s skipped: no OHLCV data", SLUG)
        return None
    scales = [s for s in SCALES if s in p["scales"]]
    results = det.detect_all_scales(df_1m, scales=scales, params=p)
    fwd = {s: det.forward_label_events(r.bars, r.events, p) for s, r in results.items()}
    first, last = df_1m["open_time"].min(), df_1m["open_time"].max()

    summary = _scale_summary(results, p)
    rb = ReportBuilder(
        scope="general", slug=SLUG, order=20, root=root,
        title="S/R szintek és trendvonalak — detektálás vizuális ellenőrzése",
        summary="A t910 detektor kimenete SOLUSDT-n (s/m/l lépték): darabszámok, eloszlások, nyers kimenet-arányok, "
                "áttekintő gyertyás képek és determinisztikusan választott kinagyított példák. Ellenőrzőpont a feature-építés előtt.",
        tags=["sr", "trendline", "levels", "epic_071", "detector"], source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél\n\n"
        "**Ellenőrzőpont (epic_071 / t911):** mielőtt a detektált alakzatok feature-ként a modellbe kerülnek (t912), "
        "vizuálisan és számokkal ellenőrizzük, hogy a detektor *jó alakzatokat* talál-e: értelmes vízszintes "
        "támasz/ellenállás szinteket és trendvonalakat, ésszerű darabszámmal és időben (look-ahead nélkül) megerősítve.\n\n"
        "**Kapcsolódó dokumentáció:** módszertan `_doc_/methodology_doc/1700_features_sr_trendline.md` (definíciók, "
        "paraméter-indoklás, placebo és go/no-go), kód `src/data_handling/sync_tables/_levels_polars.py` "
        "(t910, sémák a docstringekben).\n\n"
        f"**Adat:** SOLUSDT `ohlcv` 1m, {_ts(first)} → {_ts(last)} UTC ({df_1m.height:,} sor), read-only. "
        "Generálás: `uv run python -m reports.builders.sr_detection`."
    )
    rb.add_markdown(
        "## Módszer röviden\n\n"
        "- **Léptékek:** az 1m adatból epoch-igazított scale-bar (`s` = 1h / 7 nap, `m` = 4h / 30 nap, `l` = 1d / 180 nap), ATR14 a scale-baron.\n"
        "- **Pivot:** fraktál, n = 3 bar mindkét oldalon, prominencia ≥ 1·ATR; `confirmed_at` = az i+n. bar *záróideje*.\n"
        "- **Szint:** megerősített pivotok online klaszterezése 0.25·ATR tűréssel, ≥ 2 érintéstől szint; az ár az első két "
        "érintés átlaga, utána fagyasztva. Elévül az utolsó érintés megerősítése + lookback után. A törést túléli (szerepcsere).\n"
        "- **Trendvonal:** emelkedő mélypontok (támasz) / csökkenő csúcsok (ellenállás), ≥ 3 érintés ε = 0.25·ATR-en belül, "
        "a horgonyok között nincs ε-nál nagyobb vonalon túli záróár; duplikátumszűrés. A törés után a 12 bar retest-ablak végén meghal.\n"
        "- **Események:** `touch` (bar-szélsőérték ε-on belül a megközelítés oldaláról), `break` (záróár ≥ 0.5·ATR-rel túl), "
        "`retest` (break után 12 baron belül vissza ε-on belülre).\n"
        "- **Erő:** `strength = Σ exp(−kor/τ)`, τ = lookback/3 — kb. a „friss érintések száma”."
    )
    rb.add_table("params", _param_table(p), title="Detektor paraméterek (config/features.json → levels_detector)",
                 columns={"param": "Paraméter", "value": "Érték"})

    # ---- 1. summary
    rb.add_markdown("## 1. Összefoglaló léptékenként\n\nTeljes SOL 1m történet. A szerep (támasz/ellenállás) a szint "
                    "megerősítésekor érvényes oldal (záróár ≥ szint → támasz). Élettartam = `confirmed_at` → elévülés; a "
                    "még élő szinteknél az adat végéig (cenzorált). Kerek szám: 0.25·ATR-en belül 10 (ill. csak 5) többszöröséhez.")
    rb.add_table("scale_summary", summary, title="Pivotok és szintek léptékenként", columns={
        "scale": "Lépték", "bar": "Bar", "lookback_days": "Lookback (nap)", "bars": "Barok", "first_bar": "Első bar",
        "pivots": "Pivot", "pivots_high": "Pivot csúcs", "pivots_low": "Pivot mélypont", "levels": "Szint",
        "levels_support": "Támasz (megerősítéskor)", "levels_resistance": "Ellenállás (megerősítéskor)",
        "touches_median": {"label": "Érintés medián", "digits": 1}, "touches_p90": {"label": "Érintés p90", "digits": 1},
        "touches_max": "Érintés max", "life_days_median": {"label": "Élettartam medián (nap)", "digits": 1},
        "life_days_p90": {"label": "Élettartam p90 (nap)", "digits": 1}, "round10_pct": {"label": "Kerek 10", "type": "pct", "digits": 1},
        "round5_pct": {"label": "Kerek 5 (csak)", "type": "pct", "digits": 1},
        "role_flip_pct": {"label": "Szerepcserés szint", "type": "pct", "digits": 1},
        "levels_per_month": {"label": "Új szint / hó", "digits": 1},
    })
    rb.add_table("level_touch_dist", _touch_distribution(results, "level"), title="Szintek érintésszám-eloszlása",
                 columns={"n_touches": "Érintés", **{f"{s}_n": f"{s} db" for s in results},
                          **{f"{s}_pct": {"label": f"{s} %", "type": "pct", "digits": 1} for s in results}})
    rb.add_markdown("### Trendvonalak\n\nHossz = első horgony → utolsó érintés scale-barban; meredekség ATR/bar a "
                    "megerősítéskori ATR-rel normálva; „törve ért véget” = a vonal élete kitöréssel zárult (nem elévüléssel).")
    lines_df = _line_summary(results, p)
    rb.add_table("line_summary", lines_df, title="Trendvonalak léptékenként és típusonként", columns={
        "scale": "Lépték", "kind": "Típus", "lines": "Vonal", "touches_median": {"label": "Érintés medián", "digits": 1},
        "touches_max": "Érintés max", "span_bars_median": {"label": "Hossz medián (bar)", "digits": 0},
        "span_bars_p90": {"label": "Hossz p90 (bar)", "digits": 0}, "life_days_median": {"label": "Élet medián (nap)", "digits": 1},
        "slope_atr_p10": {"label": "Meredekség p10", "digits": 3}, "slope_atr_median": {"label": "Meredekség medián", "digits": 3},
        "slope_atr_p90": {"label": "Meredekség p90", "digits": 3}, "broken_pct": {"label": "Törve ért véget", "type": "pct", "digits": 1},
    })
    rb.add_table("line_touch_dist", _touch_distribution(results, "line"), title="Trendvonalak érintésszám-eloszlása",
                 columns={"n_touches": "Érintés", **{f"{s}_n": f"{s} db" for s in results},
                          **{f"{s}_pct": {"label": f"{s} %", "type": "pct", "digits": 1} for s in results}})
    rb.add_markdown("### Események")
    rb.add_table("event_counts", _event_counts(results), title="Események típusonként", columns={
        "scale": "Lépték", "obj_type": "Objektum", "touch": "touch", "break": "break", "retest": "retest", "total": "Összesen"})
    yearly = _event_yearly(results)
    rb.add_table("event_yearly", yearly, title="Események évenként (lépték_típus)",
                 columns={"year": {"label": "Év", "type": "str"}})
    rb.add_chart("event_monthly", themed(_monthly_events_fig(results), height=340), title="Események havonta léptékenként (log skála)", height=340)
    fs = _forward_summary(fwd)
    rb.add_markdown(
        "### Nyers kimenet-arányok — ⚠️ NYERS, placebo nélkül, NEM bizonyíték\n\n"
        "Az alábbi arányok a `forward_label_events` címkéi (24 scale-bar horizont, 1·ATR küszöb) **kontroll nélkül**. "
        "Nem mondanak semmit arról, hogy a szintek *többet* érnek-e véletlen áraknál: ugyanilyen arány véletlen "
        "szinteken is kijöhet (pl. a bounce 1·ATR-es küszöbe egy volatilis piacon gyakran teljesül magától). "
        "A szignifikancia kérdése a t913 placebo event study feladata; itt csak a címkék eloszlásának józanságát nézzük.\n\n"
        "Pozitív címke: touch → `bounce`, break → `follow_through`, retest → `hold`. Timeout = a horizonton belül nem "
        "dőlt el (negatív címkét kap). A retest `hold` szigorú: bármely 24 baron belüli záróár a régi oldalon már `fail`."
    )
    rb.add_table("forward_raw", fs, title="NYERS forward kimenet-arányok (placebo nélkül)", columns={
        "scale": "Lépték", "obj_type": "Objektum", "event_type": "Esemény", "positive_label": "Pozitív címke",
        "events": "Esemény db", "labelled": "Címkézett", "raw_positive_rate": {"label": "NYERS pozitív arány", "type": "pct", "digits": 1},
        "raw_positive_support_rate": {"label": "… támasz oldal", "type": "pct", "digits": 1},
        "raw_positive_resistance_rate": {"label": "… ellenállás oldal", "type": "pct", "digits": 1},
        "timeout_rate": {"label": "Timeout", "type": "pct", "digits": 1}, "mfe_atr_median": {"label": "MFE medián (ATR)", "digits": 2},
    })

    # ---- 2. overview charts
    fig_ml, cnt_ml = _overview(results, p, base="m", days=OVERVIEW_M_L_DAYS,
                               layers=[("l", "level", "solid"), ("m", "level", "dash")]) if {"m", "l"} <= results.keys() else (None, {})
    fig_s, cnt_s = _overview(results, p, base="s", days=OVERVIEW_S_DAYS,
                             layers=[("s", "level", "solid"), ("s", "line", "dash")]) if "s" in results else (None, {})
    rb.add_markdown(
        "## 2. Áttekintő képek\n\n"
        "**Jelölés:** zöld = támasz, piros = ellenállás (a *pillanatnyi* szerep: egy szint színe a kitörésnél vált). "
        "A vonalvastagság és átlátszóság az erő: szintnél az adott pillanatban ismert `strength` "
        f"(< {LEVEL_TIER_EDGES[0]:g} gyenge, < {LEVEL_TIER_EDGES[1]:g} közepes, különben erős), trendvonalnál az addig "
        "ismert érintésszám (3 / 4 / ≥ 5). Minden objektum **csak a `confirmed_at`-tól** az elévülésig (vonalnál a törésig) "
        "látszik — amit a detektor abban a pillanatban tudott. Sárga háromszög = pivot (▼ csúcs, ▲ mélypont). "
        "A jelmagyarázatra kattintva egy réteg ki-/bekapcsolható; hover a szakaszok végein és közepén adja a paramétereket. "
        "Az y-tengely a gyertyákhoz igazodik, a képen kívül eső szintek nem látszanak."
    )
    if fig_ml is not None:
        rb.add_markdown(
            f"### Utolsó {OVERVIEW_M_L_DAYS} nap, 4h gyertya — `l` (folytonos) és `m` (szaggatott) szintek\n\n"
            f"Látható objektumok: {cnt_ml.get('l_level', 0)} `l` és {cnt_ml.get('m_level', 0)} `m` szint "
            f"(képen kívül: {cnt_ml.get('l_level_offscreen', 0)} / {cnt_ml.get('m_level_offscreen', 0)}).")
        rb.add_chart("overview_4h_m_l", fig_ml, title=f"SOL 4h, utolsó {OVERVIEW_M_L_DAYS} nap — m/l szintek", height=640)
    if fig_s is not None:
        rb.add_markdown(
            f"### Utolsó {OVERVIEW_S_DAYS} nap, 1h gyertya — `s` szintek (folytonos) és trendvonalak (szaggatott)\n\n"
            f"Látható objektumok: {cnt_s.get('s_level', 0)} szint, {cnt_s.get('s_line', 0)} trendvonal "
            f"(képen kívül: {cnt_s.get('s_level_offscreen', 0)} / {cnt_s.get('s_line_offscreen', 0)}).")
        rb.add_chart("overview_1h_s", fig_s, title=f"SOL 1h, utolsó {OVERVIEW_S_DAYS} nap — s szintek + trendvonalak", height=640)
    rb.add_markdown("### Jelenleg aktív szintek (adat vége)\n\nA `levels_as_of(adat vége)` pillanatkép — összevethető a saját chartoddal.")
    rb.add_table("active_levels_now", _active_now(results, p), title="Aktív szintek az adat végén", columns={
        "scale": "Lépték", "level_id": "Szint", "price": {"label": "Ár", "digits": 3}, "role": "Szerep",
        "n_touches": "Érintés", "strength": {"label": "Erő", "digits": 2}, "confirmed_at": "Megerősítve",
        "last_touch": "Utolsó érintés", "n_role_flips": "Szerepcsere", "is_round": "Kerek 10"})

    # ---- 3. examples
    examples = select_examples(_candidates(fwd, results, p))
    rb.add_markdown(
        "## 3. Kinagyított példák\n\n"
        f"Az utolsó {EXAMPLE_LOOKBACK_DAYS} nap eseményeiből, **determinisztikus** kiválasztással (nem kézi): "
        "kategóriánként léptékenként rangsor — az esemény pillanatában már ismert érintésszám csökkenő, utána a "
        "kimenet nagysága (MFE ATR-ben; hamis kitörésnél a visszatérés gyorsasága), végül a frissebb esemény; a léptékek "
        f"körforgásban (`l` → `m` → `s`), kategóriánként {EXAMPLES_PER_CATEGORY} példa, egy objektum legfeljebb egyszer, "
        f"és két példa kulcseseménye legalább {EXAMPLE_MIN_GAP.days} nap távolságra (egy piaci epizód egyszer szerepel).\n\n"
        "**Jelölés a példákon:** a gyertyák a példa saját léptékének barjai. Pontozott szürke = a kialakulás szakasza "
        "(első érintés → `confirmed_at`, a detektor ekkor még *nem* ismeri); fehér csillag + függőleges pontozott vonal = "
        "`confirmed_at` (hover: az objektum minden paramétere); nagy sárga gyűrű = az objektum érintés-pivotjai, kis "
        "háromszög = a lépték többi pivotja; ○ = touch, ✕ = break, ◆ = retest esemény (hover: nyers forward címke, "
        "MFE/MAE); kék sáv = a kulcsesemény 24 bar-os forward horizontja; nyíl = kulcsesemény a nyers címkéjével."
    )
    ex_rows = []
    for k, ex in enumerate(examples, start=1):
        if k == 1 or examples[k - 2].category != ex.category:
            rb.add_markdown(f"### {CATEGORIES[ex.category]}")
        res = results[ex.scale]
        fig = _example_fig(ex, res, fwd[ex.scale], p)
        cid = f"ex{k:02d}_{ex.category}_{ex.scale}"
        bar = p["scales"][ex.scale]["bar"]
        rb.add_markdown(f"**Példa {k}** — `{ex.obj_id}` ({ex.scale}, {bar}), kulcsesemény {_ts(ex.key_time)} UTC; "
                        f"érintés az eseménykor: {ex.n_touches_at}; {ex.note}.")
        rb.add_chart(cid, fig, title=f"Példa {k}: {CATEGORIES[ex.category]} — {ex.obj_id} @ {_ts(ex.key_time)}", height=500)
        ex_rows.append({"example": k, "category": CATEGORIES[ex.category], "scale": ex.scale, "obj_id": ex.obj_id,
                        "key_event_time": ex.key_time, "n_touches_at": ex.n_touches_at, "label": ex.label, "note": ex.note})
    if not examples:
        rb.add_markdown("_Nincs a kritériumoknak megfelelő példa az időszakban._")
    else:
        missing = [CATEGORIES[c] for c in CATEGORIES if not any(e.category == c for e in examples)]
        if missing:
            rb.add_markdown("_Nem talált példát: " + "; ".join(missing) + "._")
        rb.add_table("examples", pd.DataFrame(ex_rows), title="A kiválasztott példák", columns={
            "example": "#", "category": "Kategória", "scale": "Lépték", "obj_id": "Objektum",
            "key_event_time": "Kulcsesemény (bar zárás)", "n_touches_at": "Érintés eddig", "label": "Nyers címke", "note": "Megjegyzés"})

    # ---- 4. checklist / open questions
    rb.add_markdown(
        "## 4. Mire figyelj az ellenőrzésnél\n\n"
        "1. **Túl sok vonal?** Az áttekintő képeken egyszerre hány objektum él, és hány közülük „semmi” (egyetlen gyenge, "
        "szinte érintetlen szakasz)? Ha zavaró a sűrűség, a min. érintésszám vagy a prominencia emelése a fő eszköz.\n"
        "2. **Hiányzik nyilvánvaló szint?** Van-e szemmel jól látható, többször tesztelt ár, amit a detektor nem jelöl? "
        "(Gyakori ok: a pivot prominenciája < 1·ATR, vagy az érintések 0.25·ATR-nél jobban szórnak.)\n"
        "3. **Késői megerősítés?** A csillag (`confirmed_at`) és az első érintés közötti távolság mutatja, mennyit kell "
        "várni egy szintre (min. a 2. pivot + 3 bar). Ez a look-ahead-mentesség ára — elfogadható-e?\n"
        "4. **Szint ára:** az első két érintés átlaga fagyasztva — illeszkedik-e a későbbi érintésekhez, vagy elcsúszik?\n"
        "5. **Trendvonalak:** a 3+ érintéses vonalak „rajzolná-e így egy trader”? Nem túl laposak/meredekek? A dedup "
        "nem hagy-e szinte párhuzamos duplikátumokat?\n"
        "6. **Események:** a touch / break / retest jelölések ott vannak-e, ahol a képen is érintést/kitörést látsz? "
        "A hamis kitörések tényleg visszatérnek-e? Oldalazásban egy sokszor érintett szint többször is oda-vissza "
        "„törik” (szerepcsere-sorozat), mert a 0.5·ATR-es záróár-küszöb a kis léptéken könnyen teljesül — zaj-e ez?\n\n"
        "## Nyitott kérdések a paraméterhangoláshoz (user döntés)\n\n"
        "- **Szelekciós torzítás:** a módszertan (1700) szerint a paraméterek a t911 előtt fagyasztva vannak; ha e "
        "riport alapján változtatunk, az új verzió, és a t913 placebo-tesztet a módosított paraméterekkel kell futtatni.\n"
        "- **OOS-szennyeződés:** a riport (és a példák) a 2025–2026-os időszakot is mutatják, így a t913 OOS része "
        "ezzel „látottá” vált — kontaminált jelölés vagy train-re korlátozott riport (1700: nyitott döntés).\n"
        "- `level_min_touches` = 2 elég-e, vagy a 3 érintéses szint a tisztább? (a szintek érintés-eloszlása fent)\n"
        "- `pivot_min_prominence_atr` = 1.0 és `level_tolerance_atr` = 0.25: sok-e a közeli, párhuzamos szint?\n"
        "- A retest `hold` definíciója nagyon szigorú (bármely régi oldali záróár = fail) — ezért ritka a `hold`.\n"
        "- A kerek szám flag (10 / 5 USDT) SOL árszintjén mennyire informatív? (aránya fent)\n"
        "- A 2 érintéses trendvonal-jelöltek nincsenek a kimenetben (t910 döntés) — kell-e flaggel?"
    )
    s_row = summary.set_index("scale")
    parts = [f"`{s}`: {int(s_row.loc[s, 'levels'])} szint, medián élettartam {fmt(s_row.loc[s, 'life_days_median'], 1)} nap"
             for s in results]
    flips = ", ".join(f"`{s}` {fmt(100 * s_row.loc[s, 'role_flip_pct'], 0)}%" for s in results)
    rounds = ", ".join(f"`{s}` {fmt(100 * s_row.loc[s, 'round10_pct'], 0)}%" for s in results)
    lg = lines_df[lines_df["lines"] > 0].groupby("scale", sort=False).agg(
        broken=("broken_pct", "mean"), life=("life_days_median", "mean"))
    line_txt = ", ".join(f"`{s}` {fmt(100 * r.broken, 0)}% / {fmt(r.life, 1)} nap" for s, r in lg.iterrows())
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"A darabszámok léptékenként arányosak ({'; '.join(parts)}), a szintek többsége 2–3 érintéses. A szint "
        "élettartama konstrukció szerint ≥ lookback (elévülés = utolsó érintés + lookback), ezért a medián a lookback körül van.\n\n"
        "**Feltűnő mintázatok a képek mellé:**\n\n"
        f"- A szintek nagy része legalább egyszer szerepet cserél ({flips}): a szint a törést túléli, és oldalazásban "
        "többször oda-vissza „törik”. Ez a szerepcsere-feature-öket zajossá teheti.\n"
        f"- A trendvonalak szinte mindig kitöréssel érnek véget, rövid élettel (törve arány / medián élet: {line_txt}): "
        "a 3. érintés megerősítése (pivot + 3 bar) gyakran már a vonal végéhez közel jön — lásd a vonal-példákat.\n"
        f"- A 10-es kerek szám flag aránya ({rounds}) a nagy léptéken a 0.25·ATR tűrés miatt közel triviális — "
        "1d ATR mellett a ±tűrés a 10 USDT-s rács jelentős részét lefedi.\n\n"
        "A számok és a képek alapján a detektor *strukturálisan* működik (élettartam-korlátos, időben megerősített "
        "objektumok, értelmes eseményszámok); a **döntés a vizuális ellenőrzésé**: ha a képek a fenti 1–6. szempont szerint rendben vannak, a "
        "paraméterek fagyasztva maradnak és jöhet a t912/t913. A nyers kimenet-arányok nem használhatók a hasznosság "
        "megítélésére — erre csak a placebo-kontrollált t913 alkalmas."
    )
    return rb.write()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(build())
