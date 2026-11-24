# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false
"""General report ``scout_events``: Impulse Scout multi-asset event dataset + order-flow features (epic_073 / t930).

Source: live DBs ``ohlcv`` (SOLUSDT, BTCUSDT, ETHUSDT 1m, read-only) -> :mod:`reports.builders.scout_events`
(5200 v2 per asset, 5300 additions). Writes the tables to the SOL LAB DB (unless ``store=False``) and the report to
``_doc_/reports/scout_events/``.

Hygiene (5300 holdout-2 rule): every example, distribution and label rate is TRAIN-only. OOS and holdout-2 appear as
COUNTS only (no class, no label); holdout-2 label columns are NULL in the table. The holdout-2 feasibility estimate uses
the train impulse rate and event counts that need no label.

Regenerate::

    uv run --group research python -m reports.builders.scout_events_report     (cwd: repo root; --no-store: report only)
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots
from scipy.stats import rankdata

from reports import ReportBuilder, theme
from reports.builders import scout_events as se
from reports.builders import wave_events as we
from reports.builders._common import fmt, source_info, themed

logger = logging.getLogger(__name__)

SLUG = "scout_events"
GENERATOR = "reports.builders.scout_events_report"
CLASSES = we.CLASS_ORDER_V2
CLASS_HU = we.CLASS_HU_V2
CLASS_COLOR = {"impulse": theme.LONG, "false_start": theme.SHORT, "sideways": theme.AMBER}
ASSET_COLOR = {"solusdt": theme.BLUE, "btcusdt": theme.ORANGE, "ethusdt": theme.NEUTRAL}
ASSET_HU = {"solusdt": "SOL", "btcusdt": "BTC", "ethusdt": "ETH"}
SIM_HOURS = 4.0
#: 5300 minimum counts (train, W4h_H8h) and holdout-2 minimums
MIN_POOLED = (1500, 300)
MIN_ASSET = (300, 50)
MIN_BLOCK = 100
HO_MIN = {"events": 250, "impulses": 40, "days": 60, "per_asset": 50}
A_BINS = (1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 99.0)
KEY_FLOW = ("tb_ratio_diff", "tb_ratio_rebound", "trades_rel_prefix", "vol_rebound_over_decline", "qv_logz_prefix",
            "vol_peak_rel")
EXAMPLE_ASSET_ORDER = ("solusdt", "btcusdt", "ethusdt")


def _a(asset: str) -> str:
    return ASSET_HU.get(asset, asset)


def _ts(value: Any) -> str:
    return "-" if value is None else pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")


def primary(ev: pl.DataFrame) -> pl.DataFrame:
    return ev.filter(pl.col("cell_id") == se.PRIMARY_CELL)


def train(ev: pl.DataFrame) -> pl.DataFrame:
    return ev.filter(pl.col("split") == "train")


# --------------------------------------------------------------------------- tables (pure functions, tested)


def split_counts(ev: pl.DataFrame) -> pd.DataFrame:
    """Asset x cell x split counts (OOS / holdout-2 are counts only, never classes)."""
    g = (ev.group_by("asset_id", "cell_id", "split").len().pivot(on="split", index=["asset_id", "cell_id"], values="len")
         .fill_null(0))
    for s in ("train", "oos", "holdout2", "purged"):
        if s not in g.columns:
            g = g.with_columns(pl.lit(0).alias(s))
    order = {a: k for k, a in enumerate(se.ASSETS)}
    g = g.with_columns(pl.col("asset_id").replace_strict(order, default=99).alias("_o")).sort("cell_id", "_o")
    return g.select("asset_id", "cell_id", "train", "oos", "holdout2", "purged").to_pandas()


def class_block_table(ev: pl.DataFrame, bounds: list[datetime]) -> pd.DataFrame:
    """Train x primary cell: asset x CV block x class counts (``n_test`` = usable, non-straddling events)."""
    tr = train(primary(ev))
    rows = []
    for asset in (*se.ASSETS, "pooled"):
        a = tr if asset == "pooled" else tr.filter(pl.col("asset_id") == asset)
        for k in range(len(bounds) - 1):
            b = a.filter(pl.col("cv_block") == k)
            u = b.filter(~pl.col("cv_straddle"))
            rows.append({
                "asset": asset, "block": k + 1, "start": bounds[k].date().isoformat(),
                "end": (bounds[k + 1] - timedelta(days=1)).date().isoformat(),
                "n_events": b.height, "n_straddle": b.height - u.height, "n_test": u.height,
                **{f"n_{c}": int((u["label_class"] == c).sum()) for c in CLASSES},
            })
    return pd.DataFrame(rows)


def min_checks(ev: pl.DataFrame, blocks: pd.DataFrame) -> pd.DataFrame:
    """5300 go/no-go step 1: pooled >= 1500 events / >= 300 impulses, per asset >= 300 / >= 50, per block >= 100 pooled."""
    tr = train(primary(ev))
    rows = []

    def add(scope: str, n: int, n_imp: int, need_n: int, need_imp: int | None) -> None:
        ok = n >= need_n and (need_imp is None or n_imp >= need_imp)
        rows.append({"scope": scope, "events": n, "impulses": n_imp if need_imp is not None else None,
                     "min_events": need_n, "min_impulses": need_imp, "result": "PASS" if ok else "FAIL"})

    add("pooled (train)", tr.height, int((tr["label_class"] == "impulse").sum()), *MIN_POOLED)
    for a in se.ASSETS:
        t = tr.filter(pl.col("asset_id") == a)
        add(f"{_a(a)} (train)", t.height, int((t["label_class"] == "impulse").sum()), *MIN_ASSET)
    for _, r in blocks[blocks["asset"] == "pooled"].iterrows():
        add(f"blokk {int(r['block'])} pooled teszt-esemény ({r['start']} → {r['end']})", int(r["n_test"]), 0, MIN_BLOCK, None)
    return pd.DataFrame(rows)


def a_sensitivity(variants: dict[str, pl.DataFrame]) -> pd.DataFrame:
    """Train, primary cell: events / impulse rate / events per day per asset and A threshold variant."""
    days = (pd.Timestamp(we.WaveParams().oos_start) - pd.Timestamp(we.WaveParams().train_start)).days
    rows = []
    for var, ev in variants.items():
        tr = train(primary(ev))
        for asset in (*se.ASSETS, "pooled"):
            t = tr if asset == "pooled" else tr.filter(pl.col("asset_id") == asset)
            n_imp = int((t["label_class"] == "impulse").sum())
            rows.append({
                "variant": f"A ≥ {se.VARIANTS[var]:g}·σ_W", "asset": asset, "events": t.height, "impulses": n_imp,
                "impulse_rate": n_imp / t.height if t.height else None,
                "false_start_rate": float((t["label_class"] == "false_start").mean()) if t.height else None,
                "sideways_rate": float((t["label_class"] == "sideways").mean()) if t.height else None,
                "per_day": t.height / days / (1 if asset != "pooled" else 1),
            })
    return pd.DataFrame(rows)


def rate_by_a_sigma(ev_low: pl.DataFrame) -> pd.DataFrame:
    """Impulse rate by A/sigma_W bin per asset (train, primary cell) from the most permissive variant (``a10``)."""
    tr = train(primary(ev_low))
    edges = np.array(A_BINS)
    rows = []
    for asset in (*se.ASSETS, "pooled"):
        t = tr if asset == "pooled" else tr.filter(pl.col("asset_id") == asset)
        x = t["a_sigma"].to_numpy()
        imp = (t["label_class"] == "impulse").to_numpy()
        idx = np.searchsorted(edges, x, side="right") - 1
        for k in range(len(edges) - 1):
            msk = idx == k
            n = int(msk.sum())
            lab = f"{edges[k]:g}–{edges[k + 1]:g}" if edges[k + 1] < 90 else f"≥ {edges[k]:g}"
            rows.append({"asset": asset, "bin": lab, "lo": float(edges[k]), "events": n,
                         "impulses": int(imp[msk].sum()), "impulse_rate": float(imp[msk].mean()) if n else None})
    return pd.DataFrame(rows)


def simultaneity(ev: pl.DataFrame, hours: float = SIM_HOURS) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Share of events (by ``t_c``) with another asset's event within +-``hours``; pairwise matrix; day clustering.

    Train, primary cell. Also the independence benchmark ``1 - exp(-rate_other * 2 * hours)`` and, for matched pairs,
    the impulse rate given the nearest other-asset event is / is not an impulse.
    """
    tr = train(primary(ev)).sort("t_c")
    tc = {a: tr.filter(pl.col("asset_id") == a)["t_c"].to_numpy().astype("datetime64[m]").astype("int64")
          for a in se.ASSETS}
    imp = {a: (tr.filter(pl.col("asset_id") == a)["label_class"] == "impulse").to_numpy() for a in se.ASSETS}
    span_days = (pd.Timestamp(we.WaveParams().oos_start) - pd.Timestamp(we.WaveParams().train_start)).days
    win = int(hours * 60)
    pair_rows = []
    near: dict[str, np.ndarray] = {}
    for a in se.ASSETS:
        any_other = np.zeros(tc[a].size, dtype=bool)
        for b in se.ASSETS:
            if a == b:
                continue
            d = np.abs(tc[a][:, None] - tc[b][None, :]).min(axis=1)
            hit = d <= win
            any_other |= hit
            rate_b = tc[b].size / span_days
            exp = 1 - np.exp(-rate_b * 2 * hours / 24)
            # label agreement for matched pairs (nearest event of b)
            j = np.abs(tc[a][:, None] - tc[b][None, :]).argmin(axis=1)
            both = hit & imp[b][j]
            neither = hit & ~imp[b][j]
            pair_rows.append({
                "asset": a, "other": b, "share": float(hit.mean()), "expected_indep": float(exp),
                "lift": float(hit.mean() / exp), "matched": int(hit.sum()),
                "p_imp_if_other_imp": float(imp[a][both].mean()) if both.sum() else None,
                "n_other_imp": int(both.sum()),
                "p_imp_if_other_not": float(imp[a][neither].mean()) if neither.sum() else None,
                "n_other_not": int(neither.sum()),
            })
        near[a] = any_other
    asset_rows = [{"asset": a, "events": int(tc[a].size), "share_any_other": float(near[a].mean()),
                   "n_matched": int(near[a].sum())} for a in se.ASSETS]
    allv = np.concatenate([near[a] for a in se.ASSETS])
    asset_rows.append({"asset": "pooled", "events": int(allv.size), "share_any_other": float(allv.mean()),
                       "n_matched": int(allv.sum())})
    days = tr.select(pl.col("t_c").dt.date().alias("d")).unique().height
    n_all = tr.height
    # clusters: connected components of events chained with gaps <= hours (any asset)
    t_all = np.sort(np.concatenate(list(tc.values())))
    n_cl = int(1 + (np.diff(t_all) > win).sum()) if t_all.size else 0
    info = {"events": n_all, "event_days": days, "clusters": n_cl, "events_per_cluster": n_all / max(n_cl, 1),
            "events_per_day": n_all / max(days, 1)}
    return pd.DataFrame(asset_rows), pd.DataFrame(pair_rows), info


def class_flow_table(fl_tr: pl.DataFrame) -> pd.DataFrame:
    """Train flow feature medians (and IQR) by class, pooled primary cell."""
    rows = []
    for f in se.FLOW_COLS:
        row: dict[str, Any] = {"feature": f}
        for c in CLASSES:
            x = fl_tr.filter(pl.col("label_class") == c)[f].drop_nulls().drop_nans()
            row[f"{c}_med"] = float(x.median()) if x.len() else None
            row[f"{c}_iqr"] = float(x.quantile(0.75) - x.quantile(0.25)) if x.len() else None
        rows.append(row)
    return pd.DataFrame(rows)


def auc(score: np.ndarray, y: np.ndarray) -> float | None:
    """Mann-Whitney AUC of ``score`` for ``y`` (ties averaged); ``None`` when a class is empty."""
    ok = np.isfinite(score)
    s, yy = score[ok], y[ok].astype(bool)
    n1, n0 = int(yy.sum()), int((~yy).sum())
    if n1 == 0 or n0 == 0:
        return None
    r = rankdata(s)
    return float((r[yy].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def flow_auc_table(fl_tr: pl.DataFrame) -> pd.DataFrame:
    """Descriptive univariate AUC (impulse vs rest) of each flow feature, pooled and per asset (train; no selection)."""
    rows = []
    y_all = (fl_tr["label_class"] == "impulse").to_numpy()
    for f in se.FLOW_COLS:
        row: dict[str, Any] = {"feature": f, "pooled": auc(fl_tr[f].to_numpy(), y_all)}
        for a in se.ASSETS:
            t = fl_tr.filter(pl.col("asset_id") == a)
            row[a] = auc(t[f].to_numpy(), (t["label_class"] == "impulse").to_numpy())
        rows.append(row)
    return pd.DataFrame(rows)


def holdout_feasibility(ev_all: pl.DataFrame, data_last: dict[str, datetime], train_impulse_rate: float,
                        today: datetime | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """When does holdout-2 reach the 5300 minimum (>= 250 events, >= 40 impulses, >= 60 distinct UTC days,
    >= 50 per asset)? Uses COUNTS only (events with a fully run-out label window = ``split == holdout2``) and the
    TRAIN impulse rate; rate sources: observed in holdout-2, trailing 365 days before holdout-2 (all splits, t0 counts),
    and the long-run train rate. The estimate uses the pessimistic (min) rate per asset."""
    p = we.WaveParams()
    ho0 = datetime.fromisoformat(p.holdout2_start)
    cell = primary(ev_all)
    h_min = int(cell["h_min"][0])
    end = min(data_last.values())
    obs_days = max((end - timedelta(minutes=h_min) - ho0).total_seconds() / 86400, 1e-9)
    ho = cell.filter(pl.col("split") == "holdout2")
    t365 = cell.filter((pl.col("t0") >= ho0 - timedelta(days=365)) & (pl.col("t0") < ho0))
    trn = train(cell)
    tdays = (pd.Timestamp(p.oos_start) - pd.Timestamp(p.train_start)).days
    rows = []
    for a in se.ASSETS:
        n_ho = ho.filter(pl.col("asset_id") == a).height
        r_obs = n_ho / obs_days
        r_365 = t365.filter(pl.col("asset_id") == a).height / 365
        r_run = trn.filter(pl.col("asset_id") == a).height / tdays
        rows.append({"asset": a, "holdout2_events": n_ho, "min_per_asset": HO_MIN["per_asset"],
                     "rate_observed": r_obs, "rate_trailing365": r_365, "rate_train": r_run,
                     "rate_used_pess": min(r_obs, r_365, r_run), "rate_used_opt": max(r_obs, r_365, r_run),
                     "days_for_per_asset_pess": max(HO_MIN["per_asset"] - n_ho, 0) / max(min(r_obs, r_365, r_run), 1e-9),
                     "days_for_per_asset_opt": max(HO_MIN["per_asset"] - n_ho, 0) / max(max(r_obs, r_365, r_run), 1e-9)})
    per = pd.DataFrame(rows)
    # distinct UTC days with >= 1 event (pooled): observed fraction and trailing-365 fraction
    day_ho = ho.select(pl.col("t_c").dt.date().alias("d")).unique().height
    f_obs = day_ho / obs_days
    f_365 = t365.select(pl.col("t_c").dt.date().alias("d")).unique().height / 365
    n_pool = int(ho.height)
    out_rows = []

    def crit(name: str, now: float, need: float, rate_pess: float, rate_opt: float) -> dict[str, Any]:
        return {"criterion": name, "now": now, "need": need, "days_pess": max(need - now, 0) / max(rate_pess, 1e-9),
                "days_opt": max(need - now, 0) / max(rate_opt, 1e-9)}

    rp, ro = float(per["rate_used_pess"].sum()), float(per["rate_used_opt"].sum())
    out_rows.append(crit("pooled események", n_pool, HO_MIN["events"], rp, ro))
    out_rows.append(crit(f"impulzusok (train arány {100 * train_impulse_rate:.1f}%)", n_pool * train_impulse_rate,
                         HO_MIN["impulses"], rp * train_impulse_rate, ro * train_impulse_rate))
    out_rows.append(crit("különböző UTC napok eseménnyel", day_ho, HO_MIN["days"], min(f_obs, f_365), max(f_obs, f_365)))
    out_rows.append({"criterion": "assetenként ≥ 50 (leglassabb asset)", "now": float(per["holdout2_events"].min()),
                     "need": HO_MIN["per_asset"], "days_pess": float(per["days_for_per_asset_pess"].max()),
                     "days_opt": float(per["days_for_per_asset_opt"].max())})
    cr = pd.DataFrame(out_rows)
    # events counted now have a run-out label window up to ``end``; new events arrive after ``end - H`` and count once their
    # own window ran out, so the date is ``end`` + the remaining days
    cr["date_pess"] = [(end + timedelta(days=float(d))).strftime("%Y-%m-%d") for d in cr["days_pess"]]
    cr["date_opt"] = [(end + timedelta(days=float(d))).strftime("%Y-%m-%d") for d in cr["days_opt"]]
    binding_p, binding_o = cr["days_pess"].idxmax(), cr["days_opt"].idxmax()
    info = {
        "obs_days": obs_days, "n_pool": n_pool, "day_ho": day_ho, "f_obs": f_obs, "f_365": f_365,
        "date_pess": cr.loc[binding_p, "date_pess"], "date_opt": cr.loc[binding_o, "date_opt"],
        "binding_pess": cr.loc[binding_p, "criterion"], "binding_opt": cr.loc[binding_o, "criterion"],
        "data_end": end.strftime("%Y-%m-%d %H:%M"),
        "met_now": bool(n_pool >= HO_MIN["events"] and n_pool * train_impulse_rate >= HO_MIN["impulses"]
                        and day_ho >= HO_MIN["days"] and per["holdout2_events"].min() >= HO_MIN["per_asset"]),
    }
    return per, cr, info


# --------------------------------------------------------------------------- figures


def _block_fig(blocks: pd.DataFrame) -> go.Figure:
    fig = make_subplots(rows=1, cols=4, shared_yaxes=True, subplot_titles=[_a(a) for a in se.ASSETS] + ["pooled"],
                        horizontal_spacing=0.03)
    for c, asset in enumerate((*se.ASSETS, "pooled"), start=1):
        b = blocks[blocks["asset"] == asset]
        for cls in CLASSES:
            fig.add_trace(go.Bar(x=[f"B{k}" for k in b["block"]], y=b[f"n_{cls}"].tolist(), name=CLASS_HU[cls],
                                 marker={"color": CLASS_COLOR[cls]}, legendgroup=cls, showlegend=c == 1,
                                 hovertemplate="%{x}: %{y}<extra>" + CLASS_HU[cls] + "</extra>"), row=1, col=c)
    fig.update_layout(barmode="stack")
    themed(fig, height=360, hovermode="closest", showlegend=True, margin={"l": 56, "r": 16, "t": 40, "b": 48})
    fig.update_yaxes(title="train események (használható)", row=1, col=1)
    return fig


def _rate_fig(r: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for asset in (*se.ASSETS, "pooled"):
        d = r[r["asset"] == asset]
        fig.add_trace(go.Scatter(
            x=d["bin"].tolist(), y=d["impulse_rate"].tolist(), mode="lines+markers", name=_a(asset) if asset != "pooled" else "pooled",
            line={"color": ASSET_COLOR.get(asset, theme.ACCENT), "width": 3 if asset == "pooled" else 1.8,
                  "dash": "solid" if asset == "pooled" else "dot"},
            customdata=d["events"].tolist(), hovertemplate="%{x}: %{y:.1%} (n=%{customdata})<extra>" + asset + "</extra>"))
    themed(fig, height=360, hovermode="x unified", x_title="A / σ_W (A ≥ 1,0·σ_W változat)")
    fig.update_yaxes(title="impulzus-arány (train)", tickformat=".0%")
    return fig


def _flow_box_fig(fl_tr: pl.DataFrame) -> go.Figure:
    fig = make_subplots(rows=2, cols=3, subplot_titles=list(KEY_FLOW), vertical_spacing=0.14, horizontal_spacing=0.07)
    for k, f in enumerate(KEY_FLOW):
        r, c = divmod(k, 3)
        allv = fl_tr[f].drop_nulls().drop_nans().to_numpy()
        lo, hi = (float(np.quantile(allv, 0.01)), float(np.quantile(allv, 0.99))) if allv.size else (0, 1)
        for cls in CLASSES:
            x = fl_tr.filter(pl.col("label_class") == cls)[f].drop_nulls().drop_nans().to_numpy()
            fig.add_trace(go.Box(y=x.tolist(), name=CLASS_HU[cls], marker={"color": CLASS_COLOR[cls]}, boxpoints=False,
                                 showlegend=k == 0, legendgroup=cls), row=r + 1, col=c + 1)
        fig.update_yaxes(range=[lo, hi], row=r + 1, col=c + 1)
    themed(fig, height=560, hovermode="closest", showlegend=True, margin={"l": 56, "r": 16, "t": 40, "b": 40})
    return fig


def _ratio15(df_1m: pl.DataFrame, lo: datetime, hi: datetime) -> pl.DataFrame:
    return (df_1m.filter((pl.col("open_time") >= lo) & (pl.col("open_time") < hi)).sort("open_time")
            .group_by_dynamic("open_time", every="15m", closed="left", label="left")
            .agg(pl.col("open").first(), pl.col("high").max(), pl.col("low").min(), pl.col("close").last(),
                 (pl.col("taker_buy_base").sum() / pl.col("volume").sum()).alias("tb")))


def _example_fig(df_1m: pl.DataFrame, rows: list[dict[str, Any]], asset: str) -> go.Figure:
    """One column per class: 15m candles of the prefix (+ faded label window) and the 15m taker-buy ratio of the prefix."""
    n = len(rows)
    titles = [f"{CLASS_HU[r['label_class']]} · t_c {_ts(r['t_c'])}" for r in rows]
    fig = make_subplots(rows=2, cols=n, shared_xaxes="columns", row_heights=[0.7, 0.3], vertical_spacing=0.06,
                        horizontal_spacing=0.05, subplot_titles=titles)
    for c, r in enumerate(rows, start=1):
        lo, hi = r["event_start"], r["label_end"]
        bars = _ratio15(df_1m, lo, hi).with_columns((pl.col("open_time") + timedelta(minutes=15)).alias("ct"))
        pref = bars.filter(pl.col("ct") <= r["t_c"])
        fut = bars.filter(pl.col("ct") > r["t_c"])
        for d, col, fill, op in ((pref, theme.BLUE, "rgba(13,110,253,0.12)", 1.0), (fut, "#8b949e", "rgba(139,148,158,0.15)", 0.45)):
            if d.height:
                fig.add_trace(go.Candlestick(
                    x=d["open_time"].to_list(), open=d["open"].to_list(), high=d["high"].to_list(), low=d["low"].to_list(),
                    close=d["close"].to_list(), showlegend=False, opacity=op, name="15m",
                    increasing={"line": {"color": col, "width": 1}, "fillcolor": fill},
                    decreasing={"line": {"color": col, "width": 1}, "fillcolor": col}), row=1, col=c)
        fig.add_trace(go.Scatter(x=[r["t_c"], r["label_end"]], y=[r["p_star"]] * 2, mode="lines", showlegend=False,
                                 line={"color": theme.LONG, "dash": "dash", "width": 1.4}, hoverinfo="skip"), row=1, col=c)
        fig.add_trace(go.Scatter(x=[r["t0"], r["label_end"]], y=[r["low_t0"]] * 2, mode="lines", showlegend=False,
                                 line={"color": theme.SHORT, "dash": "dot", "width": 1.2}, hoverinfo="skip"), row=1, col=c)
        fig.add_vline(x=r["t0"], line={"color": theme.NEUTRAL, "width": 1, "dash": "dot"}, row=1, col=c)
        fig.add_vline(x=r["t_c"], line={"color": theme.ACCENT, "width": 1.4, "dash": "dash"}, row=1, col=c)
        fig.add_trace(go.Scatter(x=pref["open_time"].to_list(), y=pref["tb"].to_list(), mode="lines", showlegend=False,
                                 line={"color": theme.BLUE, "width": 1.5}, name="taker-buy hányad"), row=2, col=c)
        fig.add_hline(y=0.5, line={"color": theme.NEUTRAL, "width": 1, "dash": "dot"}, row=2, col=c)
        fig.add_vline(x=r["t0"], line={"color": theme.NEUTRAL, "width": 1, "dash": "dot"}, row=2, col=c)
        fig.update_xaxes(rangeslider={"visible": False}, type="date", row=1, col=c)
    fig.update_yaxes(title=f"{_a(asset)}USDT", row=1, col=1)
    fig.update_yaxes(title="taker-buy hányad (15m)", row=2, col=1)
    themed(fig, height=520, hovermode="closest", showlegend=False, margin={"l": 56, "r": 16, "t": 48, "b": 40})
    return fig


def _pick_examples(ev: pl.DataFrame, asset: str) -> list[dict[str, Any]]:
    """Deterministic train example per class (median-time event of the class), primary cell."""
    tr = train(primary(ev)).filter(pl.col("asset_id") == asset).sort("t0")
    out = []
    for cls in CLASSES:
        g = tr.filter(pl.col("label_class") == cls)
        if g.height:
            out.append(g.row(g.height // 2, named=True))
    return out


# --------------------------------------------------------------------------- report


def _params_table(p: we.WaveParams, bounds: list[datetime], spans: dict[str, tuple[datetime, datetime, int]]) -> pd.DataFrame:
    rows = [
        ("Esemény, címke", "5200 v2 per asset: first-touch, same-bar = hamis indulás, W = B = 4h, H ∈ {4h, 8h}; elsődleges W4h_H8h"),
        ("A-küszöb (elsődleges)", f"A ≥ {p.min_a_sigma:g}·σ_W; változatok: " + ", ".join(f"{x:g}" for x in se.VARIANTS.values())),
        ("σ, ATR, nem-átfedő szűrő", "assetenként külön; σ: a t0-ig zárt 7 napos 1m log-hozam szórás; teljes-intervallumú nem-átfedés"),
        ("Split", f"train {p.train_start} → {p.oos_start} · OOS {p.oos_start} → {p.holdout2_start} · holdout-2 {p.holdout2_start} →"),
        ("CV-blokkok", f"{p.cv_blocks} közös naptári blokk: " + ", ".join(b.date().isoformat() for b in bounds)),
        ("τ", "tau_min = az első érintés perce t_c után (oldalazás: H, cenzorált); first_touch_min = NULL oldalazásnál"),
        ("Holdout-2 címkék", "label_end > holdout2_start → minden címke-függő oszlop NULL (label_masked); a P* megmarad (t_c-ig ismert)"),
    ]
    for a, (f, last, n) in spans.items():
        rows.append((f"1m adat {_a(a)}", f"{_ts(f)} → {_ts(last)} ({n:,} sor)".replace(",", " ")))
    return pd.DataFrame(rows, columns=["param", "value"])


def build(root: Path | None = None, *, dfs: dict[str, pl.DataFrame] | None = None, store: bool = True,
          lab_path: str | None = None) -> Path:
    dfs = dfs or {a: se.load_ohlcv_flow(a) for a in se.ASSETS}
    ds = se.build_dataset(dfs)
    meta = se.persist(ds, lab_path=lab_path) if store else None
    ev = ds.events
    p = ds.params[se.PRIMARY_VARIANT]
    bounds = we.cv_block_bounds(p)
    prim = primary(ev)
    tr = train(prim)
    flow = ds.flow

    blocks = class_block_table(ev, bounds)
    checks = min_checks(ev, blocks)
    sens = a_sensitivity(ds.variants)
    rate_a = rate_by_a_sigma(ds.variants["a10"])
    sim_asset, sim_pair, sim_info = simultaneity(ev)
    fl_tr = tr.join(flow.drop("asset_id", "t0", "t_c"), on="flow_key", how="left")
    cls_flow = class_flow_table(fl_tr)
    auc_tbl = flow_auc_table(fl_tr)
    imp_rate = float((tr["label_class"] == "impulse").mean())
    data_last = {a: we._dt(we._epoch_min(s[1])) + timedelta(minutes=1) for a, s in ds.data_span.items()}
    ho_per, ho_crit, ho_info = holdout_feasibility(ev, data_last, imp_rate)
    spl = split_counts(ev)

    pooled_n, pooled_imp = tr.height, int((tr["label_class"] == "impulse").sum())
    asset_stats = {a: (tr.filter(pl.col("asset_id") == a).height,
                       int((tr.filter(pl.col("asset_id") == a)["label_class"] == "impulse").sum())) for a in se.ASSETS}
    all_pass = bool((checks["result"] == "PASS").all())
    rb = ReportBuilder(
        scope="general", slug=SLUG, order=31,
        title="Impulse Scout eseményadatkészlet — SOL + BTC + ETH, order flow",
        summary="5300 (t930): megerősített lokális mélypontok SOL/BTC/ETH 1m-en, 5200 v2 first-touch címke + első érintés ideje, "
                "közös CV-blokkok, A-küszöb érzékenység, order-flow prefix-jellemzők, minimum-ellenőrzés, asset-közi egyidejűség, "
                "holdout-2 megvalósíthatóság. Csak train; OOS / holdout-2 csak darabszám.",
        tags=["scout", "epic_073", "events", "dataset", "order_flow", "multi_asset"], root=root,
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    tbl_txt = (f"`{', '.join(meta['tables'])}` + `scout_flow_v1` a SOL lab DB-ben" if meta else "nem íródott a lab DB-be (`--no-store`)")
    rb.add_markdown(
        "## Cél\n\n"
        "Az `5300_impulse_scout.md` v1 szerinti, több-asset eseményadatkészlet: ugyanaz az esemény és címke, mint az 5200 v2-ben "
        "(`wave_events` v2), **assetenként külön** alkalmazva (saját σ, ATR, nem-átfedő szűrő), kiegészítve az első érintés "
        "idejével (`τ`), a közös CV-blokkokkal, az A-küszöb érzékenységi változataival és a prefixből (`[t0 − B, t_c]`) számolt "
        "order-flow jellemzőkkel. A riport a t931 benchmark és minden későbbi Scout-modell bemenete; a `methodology_doc` "
        "indoklása az 5300, a kód-referencia a `8330_reports.md`.\n\n"
        f"**Táblák:** {tbl_txt}. A riport **csak train** adatot mutat: az OOS és a holdout-2 események csak darabszámként, "
        "osztály / címke nélkül szerepelnek (5300 holdout-2 szabály); a holdout-2 sorok címke-oszlopai a táblában NULL.")
    rb.add_table("params", _params_table(p, bounds, ds.data_span), title="Definíciók és adat", columns={"param": "Paraméter", "value": "Érték"})

    rb.add_markdown(
        "## 1. Darabszámok asset × split\n\n"
        "Az OOS és a holdout-2 oszlop **csak darabszám**. A `purged` a split-határt átlógó, hiányos címke-ablakú, hiányos adatú, "
        "vagy a train előtti (SOL: 2020-09 → 2021-01) események. A BTC / ETH 1m adat 2021-01-01-től indul, így az első "
        "7 nap σ nélkül nem ad eseményt (SOL: adat 2020-09-14-től, ezért 2021-01-01-től teljes).")
    rb.add_table("splits", spl, title="Események asset × cella × split (OOS / holdout-2: darabszám)", columns={
        "asset_id": "Asset", "cell_id": "Cella", "train": "train", "oos": "OOS", "holdout2": "holdout-2", "purged": "purged"})
    rb.add_table("funnel", ds.funnel.filter(pl.col("a_variant") == se.PRIMARY_VARIANT).drop("a_variant").to_pandas(),
                 title="Detektálási tölcsér (A ≥ 1,5·σ_W)", columns={
                     "asset_id": "Asset", "cell_id": "Cella", "candidates": "Jelölt", "passing_a": "A-szűrő", "accepted": "Elfogadott"})

    rb.add_markdown(
        "## 2. Train: asset × osztály × CV-blokk (W4h_H8h)\n\n"
        "A blokkhatárok a három assetre **közösek** (naptári, egész napos). A `használható` esemény nem lóg át a blokkhatáron "
        "(az átlógók egyik foldba sem kerülnek). Az osztályok a train-eseményekre vonatkoznak.")
    rb.add_chart("block_classes", _block_fig(blocks), title="Train események osztályonként CV-blokkonként és assetenként", height=360)
    rb.add_table("blocks", blocks, title="asset × CV-blokk × osztály (train, W4h_H8h)", columns={
        "asset": "Asset", "block": "Blokk", "start": "Kezdet", "end": "Vég", "n_events": "Esemény", "n_straddle": "Átlógó",
        "n_test": "Használható", "n_impulse": "Impulzus", "n_false_start": "Hamis indulás", "n_sideways": "Oldalazás"})
    h4 = (train(ev.filter(pl.col("cell_id") == "W4h_H4h")).group_by("asset_id", "label_class").len()
          .pivot(on="label_class", index="asset_id", values="len").fill_null(0).to_pandas())
    rb.add_markdown("A leíró W4h_H4h cella (csak összehasonlításra; nem elsődleges) train osztály-darabszámai:")
    rb.add_table("h4_classes", h4, title="W4h_H4h train osztály-darabszám asset szerint", columns={"asset_id": "Asset"})

    rb.add_markdown(
        "## 3. Eseménygyakoriság és az impulzus-arány az A / σ_W függvényében\n\n"
        "Események naponta (train, W4h_H8h): " + "; ".join(
            f"{_a(a)} {fmt(asset_stats[a][0] / ((pd.Timestamp(p.oos_start) - pd.Timestamp(p.train_start)).days), 3)}" for a in se.ASSETS)
        + ". Az **A-küszöb érzékenység**: a lazább küszöb több eseményt ad, de a nem-átfedő szűrő miatt a halmazok nem egymás "
        "részhalmazai (külön táblák: `scout_events_v1`, `_a125`, `_a10`).")
    rb.add_table("a_variants", sens, title="A-küszöb változatok: darabszám és osztályarány (train, W4h_H8h)", columns={
        "variant": "Változat", "asset": "Asset", "events": "Esemény", "impulses": "Impulzus",
        "impulse_rate": {"label": "Impulzus-arány", "type": "pct"}, "false_start_rate": {"label": "Hamis indulás", "type": "pct"},
        "sideways_rate": {"label": "Oldalazás", "type": "pct"}, "per_day": {"label": "Esemény / nap", "digits": 3}})
    rb.add_chart("rate_a_sigma", _rate_fig(rate_a), title="Impulzus-arány A/σ_W szerint assetenként (train)", height=360)
    rb.add_table("rate_a_table", rate_a.drop(columns="lo"), title="Impulzus-arány A/σ_W sávonként (a10 változat, train)", columns={
        "asset": "Asset", "bin": "A/σ_W", "events": "Esemény", "impulses": "Impulzus", "impulse_rate": {"label": "Impulzus-arány", "type": "pct"}})

    rb.add_markdown(
        "## 4. Minimum-ellenőrzés (5300 go/no-go 1. lépés)\n\n"
        "Pooled train ≥ 1500 esemény és ≥ 300 impulzus; assetenként ≥ 300 és ≥ 50; CV-blokkonként ≥ 100 pooled használható "
        "teszt-esemény. Nem teljesülés: NO-GO (nem küszöb-csökkentés).")
    rb.add_table("min_checks", checks, title="5300 minimum-ellenőrzés (train, W4h_H8h)", columns={
        "scope": "Hatókör", "events": "Esemény", "impulses": "Impulzus", "min_events": "Min. esemény",
        "min_impulses": "Min. impulzus", "result": "Eredmény"})

    rb.add_markdown(
        f"## 5. Asset-közi egyidejűség (±{SIM_HOURS:g}h, `t_c` szerint, train)\n\n"
        "Az események aránya, amelynél egy **másik** asset eseménye is van ±4h-n belül. A „várt” oszlop a függetlenségi "
        "viszonyítás (Poisson, `1 − exp(−r·8h)`); a lift = megfigyelt / várt. A címke-egyezés oszlopok: a párok esetén az "
        "impulzus-arány aszerint, hogy a másik asset legközelebbi eseménye impulzus-e.")
    rb.add_table("sim_asset", sim_asset, title="Egyidejű események aránya (bármely másik asset ±4h)", columns={
        "asset": "Asset", "events": "Esemény", "share_any_other": {"label": "Arány", "type": "pct"}, "n_matched": "Párosított"})
    rb.add_table("sim_pair", sim_pair, title="Páronként: arány, függetlenségi várt, lift, címke-egyezés", columns={
        "asset": "Asset", "other": "Másik", "share": {"label": "Arány", "type": "pct"},
        "expected_indep": {"label": "Várt (független)", "type": "pct"}, "lift": {"label": "Lift", "digits": 2},
        "matched": "Pár", "p_imp_if_other_imp": {"label": "P(imp | másik imp)", "type": "pct"}, "n_other_imp": "n",
        "p_imp_if_other_not": {"label": "P(imp | másik nem-imp)", "type": "pct"}, "n_other_not": "n "})
    rb.add_markdown(
        f"**Hatékony mintaméret.** A pooled train {sim_info['events']} esemény {sim_info['event_days']} különböző naptári napra "
        f"esik ({fmt(sim_info['events_per_day'], 2)} esemény / eseménynap), és {sim_info['clusters']} idő-klaszterbe rendezhető "
        f"(≤ 4h-n belüli láncolás; {fmt(sim_info['events_per_cluster'], 2)} esemény / klaszter). A nap-klaszter újramintavétel "
        "(5300) ennek megfelelően a napot tekinti egységnek.")

    rb.add_markdown(
        "## 6. Order-flow / mikrostruktúra jellemzők (prefix `[t0 − B, t_c]`)\n\n"
        "A jellemzők csak a `t_c`-ig zárt 1 perces bar-okból épülnek; a baseline (volumen, kötésszám, quote-volume) a múltbeli, "
        "a `t0`-t megelőző 7 napból. **Szakaszok:** *decline* = `[t0 − B, t0]`, *rebound* = `(t0, t_c]`. "
        "`tb_ratio` = taker-buy / összvolumen (0,5 = kiegyensúlyozott); `tb_ratio_diff` = rebound − decline; `tb_ratio_slope_1h` az "
        "órás hányad OLS-meredeksége a prefixen; `delta_*_norm` = (taker-buy − taker-sell) / (7 napos átlagvolumen × percek); "
        "`trades_rel_*` = kötés/perc / 7 napos átlag; `trade_size_rel` = volumen/kötés a 7 napos arányhoz; `vol_*_rel` = "
        "átlagvolumen / 7 napos átlag; `vol_peak_rel`, `vol_peak_pos` = a legnagyobb 15m volumen relatív nagysága és helye a "
        "prefixen (0 = eleje, 1 = vége); `qv_logz_prefix` = a prefix log quote-volume összegének z-score-ja a megelőző 7 nap azonos "
        "hosszúságú ablakaihoz képest.")
    rb.add_chart("flow_boxes", _flow_box_fig(fl_tr), title="Fő order-flow jellemzők eloszlása osztályonként (train, pooled)", height=560)
    rb.add_table("flow_by_class", cls_flow, title="Order-flow jellemzők mediánja és IQR-je osztályonként (train, pooled)",
                 columns={"feature": "Jellemző", **{f"{c}_med": {"label": f"{CLASS_HU[c]} medián", "digits": 4} for c in CLASSES},
                          **{f"{c}_iqr": {"label": f"{CLASS_HU[c]} IQR", "digits": 4} for c in CLASSES}})
    rb.add_markdown(
        "**Leíró egyváltozós AUC** (impulzus vs a többi; 0,5 = nincs elválasztás; train, nincs szelekció, nem modell). "
        "Ez nem jellemző-kiválasztás: a modell-értékelés a t931 benchmark dolga, a skalár alapon felül.")
    rb.add_table("flow_auc", auc_tbl, title="Egyváltozós AUC jellemzőnként (impulzus vs többi, train)", columns={
        "feature": "Jellemző", "pooled": {"label": "pooled", "digits": 3}, "solusdt": {"label": "SOL", "digits": 3},
        "btcusdt": {"label": "BTC", "digits": 3}, "ethusdt": {"label": "ETH", "digits": 3}})

    rb.add_markdown("## 7. Train példák assetenként és osztályonként\n\n"
                    "Osztályonként az idő szerinti középső train esemény (nincs válogatás). Kék: prefix (modell-bemenet, 15m, zár ≤ t_c); "
                    "halvány: a címke-ablak (csak szemléltetés); alul a 15m taker-buy hányad a prefixen (szaggatott: t0).")
    ex_rows = []
    for asset in EXAMPLE_ASSET_ORDER:
        rows = _pick_examples(ev, asset)
        rb.add_chart(f"ex_{_a(asset).lower()}", _example_fig(dfs[asset], rows, asset),
                     title=f"{_a(asset)}: train példák (impulzus / hamis indulás / oldalazás)", height=520)
        ex_rows += [{"asset": _a(asset), "class": CLASS_HU[r["label_class"]], "t0": _ts(r["t0"]), "t_c": _ts(r["t_c"]),
                     "a_sigma": r["a_sigma"], "tau_min": r["tau_min"]} for r in rows]
    rb.add_table("examples", pd.DataFrame(ex_rows), title="A kiválasztott train példák", columns={
        "asset": "Asset", "class": "Osztály", "t0": "t0", "t_c": "t_c", "a_sigma": {"label": "A/σ_W", "digits": 2},
        "tau_min": {"label": "τ (perc)", "digits": 0}})

    rb.add_markdown(
        "## 8. Holdout-2 megvalósíthatóság (csak darabszám)\n\n"
        f"Holdout-2 (2026-08-01 →): az eseményszám **címke nélkül** számolható. A jelenlegi, teljesen kifutott címke-ablakú "
        f"események (adat vége: {ho_info['data_end']}): **{ho_info['n_pool']}** pooled esemény, **{ho_info['day_ho']}** különböző napon. "
        f"A 5300 minimuma: ≥ {HO_MIN['events']} esemény, ≥ {HO_MIN['impulses']} impulzus (a **train** impulzus-aránnyal, "
        f"{100 * imp_rate:.1f}%), ≥ {HO_MIN['days']} különböző UTC nap, assetenként ≥ {HO_MIN['per_asset']}. "
        "Ráta-források: megfigyelt a holdout-2-ben, az utolsó 365 nap a holdout-2 előtt, és a train hosszú távú átlaga; a "
        "pesszimista becslés a legkisebbet, az optimista a legnagyobbat használja.")
    rb.add_table("ho_assets", ho_per, title="Holdout-2 esemény / nap asset szerint", columns={
        "asset": "Asset", "holdout2_events": "Holdout-2 esemény", "min_per_asset": "Min.", "rate_observed": {"label": "ráta megfigyelt", "digits": 4},
        "rate_trailing365": {"label": "ráta 365 nap", "digits": 4}, "rate_train": {"label": "ráta train", "digits": 4},
        "rate_used_pess": {"label": "ráta pessz.", "digits": 4}, "rate_used_opt": {"label": "ráta opt.", "digits": 4},
        "days_for_per_asset_pess": {"label": "hátralévő nap a 50-hez (pessz.)", "digits": 0},
        "days_for_per_asset_opt": {"label": "hátralévő nap a 50-hez (opt.)", "digits": 0}})
    rb.add_table("ho_criteria", ho_crit, title="Holdout-2 minimum-feltételek: jelenlegi állás és becsült dátum", columns={
        "criterion": "Feltétel", "now": {"label": "Most", "digits": 1}, "need": "Kell", "days_pess": {"label": "hátralévő nap (pessz.)", "digits": 0},
        "days_opt": {"label": "hátralévő nap (opt.)", "digits": 0}, "date_pess": "Dátum (pessz.)", "date_opt": "Dátum (opt.)"})

    # ---- interpretation
    def rng(a: str) -> str:
        n, i = asset_stats[a]
        return f"{_a(a)} {n} esemény / {i} impulzus"

    bad = checks[checks["result"] == "FAIL"]
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"**Minta (train, W4h_H8h):** pooled {pooled_n} esemény, {pooled_imp} impulzus ({100 * imp_rate:.1f}%); " +
        "; ".join(rng(a) for a in se.ASSETS) + ". A 5300 minimum-ellenőrzés " +
        ("**mind PASS**" if all_pass else f"**{len(bad)} FAIL: " + ", ".join(bad["scope"].tolist()) + "**") + ".\n\n"
        f"**Függőség:** az események {fmt(100 * float(sim_asset.iloc[-1]['share_any_other']), 1)}%-ának van másik assetnél eseménye "
        f"±4h-n belül (a függetlenségi várt érték ennél jóval kisebb, lásd a lift oszlopot): a hatékony mintaméret az eseményszámnál "
        f"kisebb ({sim_info['clusters']} idő-klaszter {sim_info['events']} eseményre), ezért a nap-klaszter újramintavétel kötelező.\n\n"
        f"**Holdout-2:** jelenleg {ho_info['n_pool']} esemény; a minimum becsült elérése {ho_info['date_pess']} (pesszimista, kötő: "
        f"{ho_info['binding_pess']}) és {ho_info['date_opt']} (optimista, kötő: {ho_info['binding_opt']}). "
        + ("A minimum **már teljesül**." if ho_info["met_now"] else "A minimum még **nem** teljesül: várakozás, nem küszöb-csökkentés.") +
        "\n\n**Order flow:** az osztályonkénti különbségek a fenti mediánokban és az egyváltozós AUC-ban láthatók; ezek leíró, "
        "train-only mutatók, a skalár alapon felüli többletet csak a t931 benchmark (azonos foldok, permutációs null) dönti el.")
    return rb.write()


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or SLUG).split("\n")[0])
    ap.add_argument("--no-store", action="store_true", help="do not write the scout tables into the lab DB")
    ap.add_argument("--root", default=None, help="report root override (e.g. scratchpad when the report folder is locked)")
    args = ap.parse_args()
    print(build(Path(args.root) if args.root else None, store=not args.no_store))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
