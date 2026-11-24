# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false, reportGeneralTypeIssues=false, reportReturnType=false
"""General report ``wave_image_clusters``: wave-start prefix images, AE embedding, clusters (epic_072 / t919).

Cell ``W4h_H8h`` only (user decision 2026-11-20). Source: ``research."wave_events_v2"`` (TRAIN rows only are read) +
live ``ohlcv`` 1m (read-only, cut at the train end: no OOS / holdout-2 bar is ever loaded). Library:
:mod:`reports.builders.wave_image_model`; methodology: ``_doc_/methodology_doc/5200_wave_start_image_model.md`` (v2).

Everything (embedding, clusters, k-selection, baselines, descriptive class shares / cones) uses train events only.
The k choice follows 5200: lowest out-of-fold log-loss of the cluster -> impulse-rate predictor over the shared 6-block
purged CV (``cv_fold``; purge = full interval, embargo = H). The train class shares per cluster are DESCRIPTIVE
(in-sample), not evidence: the evidence is t920's CV delta AUC.

Regenerate::

    uv run --group research python -m reports.builders.wave_image_clusters     (cwd: repo root)
"""

from __future__ import annotations

import argparse
import logging
import time
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
from reports.builders import wave_image_model as wm
from reports.builders._common import source_info, themed
from reports.builders.sr_detection import load_ohlcv_1m

logger = logging.getLogger(__name__)

SLUG = "wave_image_clusters"
GENERATOR = "reports.builders.wave_image_clusters"
CLASSES = we.CLASS_ORDER_V2
HU = we.CLASS_HU_V2
CLASS_COLOR = {"impulse": theme.LONG, "false_start": theme.SHORT, "sideways": theme.AMBER}
METHOD_COLOR = {"ae_kmeans": theme.BLUE, "kshape": theme.ORANGE, "dtw_kmeans": theme.NEUTRAL}
CLUSTER_PALETTE = ["#0d6efd", "#fd7e14", "#198754", "#dc3545", "#6f42c1", "#20c997", "#d63384", "#6c757d", "#b08900", "#0dcaf0"]
N_RECON = 6


# --------------------------------------------------------------------------- forward cone


def forward_paths(m: we.MinuteData, ev: pl.DataFrame, h_min: int, step: int = 15) -> np.ndarray:
    """``(n, h/step + 1)`` close path over ``[t_c, t_c + H]`` in units of ``A`` (``log(close / close[t_c]) / A``).

    Column 0 is 0 by construction; +1 = the impulse level ``P*``, -1 = the false-start level ``low[t0]`` (both are
    symmetric in log terms because ``P* = close[t_c] * exp(A)`` and ``A = log(close[t_c] / low[t0])``).
    """
    out = np.zeros((ev.height, h_min // step + 1))
    for i, r in enumerate(ev.iter_rows(named=True)):
        ti = int(pd.Timestamp(r["t_c"]).value // 60_000_000_000) - m.m0 - 1      # bar whose close is t_c
        idx = ti + step * np.arange(out.shape[1])
        if idx[-1] >= m.n or not np.isclose(m.close[ti], r["close_tc"]):
            raise ValueError("forward path outside the loaded data")
        out[i] = np.log(m.close[idx] / r["close_tc"]) / r["a_log"]
    return out


def _cone(paths: np.ndarray) -> np.ndarray:
    return np.quantile(paths, [0.1, 0.5, 0.9], axis=0)


# --------------------------------------------------------------------------- tables


def _shares(lab: np.ndarray, cls: np.ndarray, k: int) -> pd.DataFrame:
    rows = []
    for c in range(k):
        sel = lab == c
        n = int(sel.sum())
        row: dict[str, Any] = {"cluster": c, "n": n, "share": n / lab.size}
        for name in CLASSES:
            row[f"n_{name}"] = int((cls[sel] == name).sum())
            row[f"{name}_rate"] = float((cls[sel] == name).mean()) if n else float("nan")
        rows.append(row)
    return pd.DataFrame(rows)


def _cluster_summary(lab: np.ndarray, ev: pl.DataFrame, paths: np.ndarray, k: int) -> pd.DataFrame:
    cls = ev["label_class"].to_numpy()
    df = _shares(lab, cls, k)
    a_sig = ev["a_sigma"].to_numpy()
    mfe = (ev["fwd_mfe_log"] / ev["a_log"]).to_numpy()
    mae = (ev["fwd_mae_log"] / ev["a_log"]).to_numpy()
    df["a_sigma_med"] = [float(np.median(a_sig[lab == c])) for c in range(k)]
    df["end_p10"] = [float(np.quantile(paths[lab == c, -1], 0.1)) for c in range(k)]
    df["end_med"] = [float(np.median(paths[lab == c, -1])) for c in range(k)]
    df["end_p90"] = [float(np.quantile(paths[lab == c, -1], 0.9)) for c in range(k)]
    df["mfe_med"] = [float(np.median(mfe[lab == c])) for c in range(k)]
    df["mae_med"] = [float(np.median(mae[lab == c])) for c in range(k)]
    df["cluster"] = [f"C{c}" for c in range(k)]
    return df


# --------------------------------------------------------------------------- figures


def _heat(z: np.ndarray, *, showscale: bool = False, zmax: float | None = None) -> go.Heatmap:
    return go.Heatmap(z=np.round(z, 3), colorscale="Greys", zmin=0, zmax=zmax or 1, showscale=showscale, hoverinfo="skip")


def _clean_axes(fig: go.Figure) -> None:
    fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False)
    fig.update_yaxes(showticklabels=False, showgrid=False, zeroline=False, autorange="reversed")


def _image_grid(imgs: list[np.ndarray], titles: list[str], ncols: int, height: int, zmax: float | None = None) -> go.Figure:
    nrows = -(-len(imgs) // ncols)
    fig = make_subplots(rows=nrows, cols=ncols, subplot_titles=titles, horizontal_spacing=0.02, vertical_spacing=0.09)
    for i, im in enumerate(imgs):
        fig.add_trace(_heat(im, zmax=zmax), row=i // ncols + 1, col=i % ncols + 1)
    _clean_axes(fig)
    fig.update_annotations(font_size=11)
    return themed(fig, height=height, hovermode="closest", margin={"l": 8, "r": 8, "t": 36, "b": 8})


def _cone_fig(paths: np.ndarray, lab: np.ndarray, k: int, h_min: int) -> go.Figure:
    x = np.arange(paths.shape[1]) * 15 / 60
    ncols = min(5, k)
    nrows = -(-k // ncols)
    uncond = _cone(paths)
    fig = make_subplots(rows=nrows, cols=ncols, subplot_titles=[f"C{c} (n={int((lab == c).sum())})" for c in range(k)],
                        shared_yaxes=True, horizontal_spacing=0.02, vertical_spacing=0.12)
    for c in range(k):
        r, col = c // ncols + 1, c % ncols + 1
        cc = _cone(paths[lab == c])
        color = CLUSTER_PALETTE[c % len(CLUSTER_PALETTE)]
        first = c == 0
        fig.add_trace(go.Scatter(x=x, y=uncond[2], mode="lines", line={"width": 0}, showlegend=False, hoverinfo="skip"), row=r, col=col)
        fig.add_trace(go.Scatter(x=x, y=uncond[0], mode="lines", line={"width": 0}, fill="tonexty", fillcolor="rgba(108,117,125,0.22)",
                                 name="feltétel nélküli p10–p90", showlegend=first, hoverinfo="skip"), row=r, col=col)
        fig.add_trace(go.Scatter(x=x, y=cc[2], mode="lines", line={"width": 0}, showlegend=False, hoverinfo="skip"), row=r, col=col)
        fig.add_trace(go.Scatter(x=x, y=cc[0], mode="lines", line={"width": 0}, fill="tonexty", fillcolor=_rgba(color, 0.30),
                                 name="klaszter p10–p90", showlegend=first, hoverinfo="skip"), row=r, col=col)
        fig.add_trace(go.Scatter(x=x, y=cc[1], mode="lines", line={"color": color, "width": 2}, name="klaszter medián",
                                 showlegend=first, hovertemplate="%{x:.2f} h: %{y:.2f} A<extra>medián</extra>"), row=r, col=col)
        fig.add_trace(go.Scatter(x=x, y=uncond[1], mode="lines", line={"color": theme.NEUTRAL, "width": 1, "dash": "dot"},
                                 name="feltétel nélküli medián", showlegend=first, hoverinfo="skip"), row=r, col=col)
        fig.add_hline(y=1, line={"color": theme.LONG, "width": 1, "dash": "dash"}, row=r, col=col)
        fig.add_hline(y=-1, line={"color": theme.SHORT, "width": 1, "dash": "dash"}, row=r, col=col)
    fig.update_xaxes(title_text="idő t_c után (óra)", title_font_size=10, tickfont_size=9)
    fig.update_yaxes(tickfont_size=9)
    fig.update_yaxes(title_text="log(close/close[t_c]) / A", title_font_size=10, col=1)
    fig.update_annotations(font_size=11)
    return themed(fig, height=260 * nrows + 80, hovermode="closest", margin={"l": 56, "r": 8, "t": 36, "b": 48})


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    return f"rgba({int(h[0:2], 16)},{int(h[2:4], 16)},{int(h[4:6], 16)},{alpha})"


def _kselect_fig(summ: pd.DataFrame, methods: list[str], col: str, ytitle: str, hline: float | None) -> go.Figure:
    fig = go.Figure()
    for m in methods:
        s = summ[summ["method"] == m].sort_values("k")
        fig.add_trace(go.Scatter(x=s["k"], y=s[col], mode="lines+markers", name=wm.METHOD_HU[m], line={"color": METHOD_COLOR[m]},
                                 hovertemplate="k=%{x}: %{y:.4f}<extra>" + wm.METHOD_HU[m] + "</extra>"))
    if hline is not None:
        fig.add_hline(y=hline, line={"color": theme.ACCENT, "width": 1, "dash": "dash"})
    fig.update_xaxes(title_text="k", dtick=1)
    fig.update_yaxes(title_text=ytitle)
    return themed(fig, height=340)


def _share_fig(df: pd.DataFrame, prior: dict[str, float]) -> go.Figure:
    fig = go.Figure()
    xs = [f"{c}<br>n={n}" for c, n in zip(df["cluster"], df["n"], strict=True)]
    for name in CLASSES:
        fig.add_trace(go.Bar(x=xs, y=df[f"{name}_rate"], name=HU[name], marker={"color": CLASS_COLOR[name]},
                             hovertemplate="%{y:.1%}<extra>" + HU[name] + "</extra>"))
    fig.add_hline(y=prior["impulse"], line={"color": theme.ACCENT, "width": 1, "dash": "dash"},
                  annotation_text=f"train impulzus-arány {prior['impulse']:.1%}", annotation_position="top right")
    fig.update_layout(barmode="stack")
    fig.update_yaxes(title_text="osztályarány (train, leíró)", tickformat=".0%")
    return themed(fig, height=380)


def _size_fig(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure(go.Bar(x=df["cluster"], y=df["n"], marker={"color": [CLUSTER_PALETTE[i % 10] for i in range(len(df))]},
                           text=[f"{s:.1%}" for s in df["share"]], textposition="outside",
                           hovertemplate="%{x}: %{y} esemény<extra></extra>"))
    fig.update_yaxes(title_text="train esemény")
    return themed(fig, height=300)


def _method_rates_fig(per_method: dict[str, pd.DataFrame], prior: float) -> go.Figure:
    ms = list(per_method)
    fig = make_subplots(rows=1, cols=len(ms), subplot_titles=[wm.METHOD_HU[m] for m in ms], shared_yaxes=True,
                        horizontal_spacing=0.03)
    for i, m in enumerate(ms, start=1):
        d = per_method[m]
        fig.add_trace(go.Bar(x=d["cluster"], y=d["impulse_rate"], marker={"color": METHOD_COLOR[m]}, showlegend=False,
                             text=[f"n={n}" for n in d["n"]], textposition="outside",
                             hovertemplate="%{x}: %{y:.1%}<extra></extra>"), row=1, col=i)
        fig.add_hline(y=prior, line={"color": theme.ACCENT, "width": 1, "dash": "dash"}, row=1, col=i)
    fig.update_yaxes(title_text="impulzus-arány (train, leíró)", tickformat=".0%", col=1)
    fig.update_annotations(font_size=11)
    return themed(fig, height=340, hovermode="closest")


def _recon_examples(emb: wm.EmbeddingModel, pre: wm.Prefixes, ev: pl.DataFrame) -> go.Figure:
    pos = np.unique(np.floor((np.arange(N_RECON) + 0.5) / N_RECON * pre.n).astype(int))
    sub = pre.take(pos)
    orig = wm.render_images(sub, emb.render)[:, 0]
    rec = wm.reconstruct(emb, sub)
    imgs: list[np.ndarray] = []
    titles: list[str] = []
    for j, p in enumerate(pos):
        t0 = pd.Timestamp(ev["t0"][int(p)]).strftime("%Y-%m-%d %H:%M")
        imgs += [orig[j], rec[j]]
        titles += [f"{t0} eredeti", "rekonstrukció (valószínűség)"]
    return _image_grid(imgs, titles, 4, 230 * (-(-len(imgs) // 4)) + 40)


# --------------------------------------------------------------------------- build


def build(root: Path | None = None, *, df_1m: pl.DataFrame | None = None, events: pl.DataFrame | None = None,
          params: we.WaveParams | None = None, cell: str = wm.CELL, ks: tuple[int, ...] = wm.KS,
          methods: tuple[str, ...] = wm.METHODS, cfg: wm.AEConfig | None = None, seed: int = wm.SEED,
          asset_id: str | None = None) -> Path | None:
    """Fit on train only, select k by the 5200 OOF log-loss rule and write the ``wave_image_clusters`` report."""
    t_start = time.perf_counter()
    p = params or we.params_v2()
    oos0 = datetime.fromisoformat(p.oos_start)
    cfg = cfg or wm.AEConfig()
    _, w_min, h_min = next(c for c in p.cells if c[0] == cell)
    if events is None:
        events = we.load_events(asset_id, split="train", version="v2")       # SQL-filtered: no OOS / holdout-2 row is loaded
    ev = events.filter((pl.col("cell_id") == cell) & (pl.col("split") == "train")).sort("t0")
    if df_1m is None:
        df_1m = load_ohlcv_1m(asset_id)
        if df_1m is None:
            logger.error("live db unavailable")
            return None
    df_1m = df_1m.filter(pl.col("open_time") < oos0)                          # nothing from the OOS period enters memory
    m = we.to_minute_grid(df_1m)
    b15 = we.to_bars15(m, p)
    pre = wm.build_prefixes(b15, ev)
    bounds = we.cv_block_bounds(p)
    ok_ev = ev.filter(~pl.col("cv_straddle"))
    n_tr, n_ok = ev.height, ok_ev.height
    prior_rate = {c: float((ev["label_class"] == c).mean()) for c in CLASSES}

    # ---- k-selection (5200: OOF log-loss, 6-block purged CV) + baselines
    t = time.perf_counter()
    summ, folds, extra = wm.select_k(ev, pre, bounds, h_min, ks=ks, methods=methods, cfg=cfg, seed=seed)
    t_cv = time.perf_counter() - t
    k_ae = wm.best_k(summ, "ae_kmeans")
    best = {mm: wm.best_k(summ, mm) for mm in methods}

    # ---- final (descriptive) fit on all train events
    t = time.perf_counter()
    emb = wm.fit_embedding(ev, pre, cfg=cfg, seed=seed)
    z = wm.embed(emb, pre)
    model, lab = wm.fit_clusterer(emb, z, k_ae, seed=seed)
    t_fit = time.perf_counter() - t
    cls = ev["label_class"].to_numpy()
    paths = forward_paths(m, ev, h_min)
    csum = _cluster_summary(lab, ev, paths, k_ae)
    per_method: dict[str, pd.DataFrame] = {"ae_kmeans": csum}
    base_rows = []
    for mm in methods:
        if mm == "ae_kmeans":
            continue
        _, lb = wm.fit_raw_clusterer(ev, pre, mm, k_ae, seed=seed)
        per_method[mm] = _shares(lb, cls, k_ae).assign(cluster=lambda d: [f"C{c}" for c in d["cluster"]])
    for mm in methods:
        d = per_method[mm]
        s = summ[(summ["method"] == mm) & (summ["k"] == k_ae)].iloc[0]
        base_rows.append({
            "method": wm.METHOD_HU[mm], "k": k_ae, "delta_ll": s["delta_ll"], "blocks_better": int(s["blocks_better"]),
            "ari": s["ari"], "jaccard": s["jaccard"], "min_share": s["min_share"],
            "rate_min": float(d["impulse_rate"].min()), "rate_max": float(d["impulse_rate"].max()),
            "best_k": best[mm], "best_delta_ll": float(summ[(summ["method"] == mm) & (summ["k"] == best[mm])]["delta_ll"].iloc[0]),
        })
    base_tbl = pd.DataFrame(base_rows)
    cl_rows = []
    for mm in methods:
        for _, r in per_method[mm].iterrows():
            cl_rows.append({"method": wm.METHOD_HU[mm], "cluster": r["cluster"], "n": int(r["n"]), "share": r["share"],
                            **{f"{c}_rate": r[f"{c}_rate"] for c in CLASSES}})
    runtime = time.perf_counter() - t_start

    # ---- report
    s_ae = summ[summ["method"] == "ae_kmeans"].set_index("k")
    row = s_ae.loc[k_ae]
    rb = ReportBuilder(
        scope="general", slug=SLUG, order=31, root=root,
        title=f"Hullámkezdet kép-klaszterek — AE embedding, k-választás, tölcsérek ({cell}, train)",
        summary=f"5200 v2 / t919: a {cell} cella prefix-képei (15m OHLC + volumen), konvolúciós autoencoder embedding, k-means "
                f"(k = {k_ae}, a 6 blokkos purged CV OOF log-loss szerint), k-Shape / DTW baseline, klaszter-centroid képek és "
                "kimenet-tölcsérek. Csak train; a klaszterenkénti osztályarányok leíróak, nem bizonyítékok.",
        tags=["wave", "epic_072", "clustering", "embedding", "v2"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél\n\n"
        "**t919 (epic_072):** a hullámkezdet-események prefix-képeiből (`[t0 − B, t_c]`, `_doc_/methodology_doc/5200_wave_start_image_model.md`, v2) "
        "konvolúciós autoencoder-embeddinget tanulunk, majd kis `k`-jú klaszterezést (k ∈ 4..10); baseline a nyers, z-normalizált "
        "záróár-útvonal k-Shape / DTW k-means klaszterezése. Megnézzük, mely alakok különülnek el, hogyan néz ki a klaszterenkénti "
        "kimenet-tölcsér (p10–p90 kúp a `(t_c, t_c + H]` ablakban) a feltétel nélküli kúphoz képest, és mennyire stabil a klaszterezés.\n\n"
        f"**Adat:** `research.\"wave_events_v2\"`, cella `{cell}` (W = {w_min // 60}h, H = {h_min // 60}h), **{n_tr} train esemény** "
        f"(ebből {n_ok} nem lóg át CV-blokkhatáron), SOLUSDT 15m prefixek. Újragenerálás: "
        "`uv run --group research python -m reports.builders.wave_image_clusters`.\n\n"
        "> ⚠️ **OOS-tisztaság:** az embedding, a klaszterezés, a k-választás, a baseline és minden itt látható szám **kizárólag a train** "
        f"({p.train_start} → {(oos0 - timedelta(days=1)).date().isoformat()}) eseményein fut; az OOS és a holdout-2 sorokat az adatbetöltés sem olvassa be, az 1m árfolyam "
        "a train végén le van vágva. OOS-hozzárendelés nincs (t920 dönt, hogy kell-e).\n\n"
        "> ⚠️ **A klaszterenkénti osztályarányok leíró (in-sample) számok, nem bizonyíték.** A klaszter a train eseményeken lett "
        "illesztve, az arányokat ugyanazokon mérjük; az egyetlen bizonyíték a t920 purged CV ΔAUC-ja a skalár alap (S0 + C) fölött. "
        "Itt az out-of-fold log-loss csak a `k` kiválasztásához (5200) és a klaszterek használhatóságának előzetes jelzéséhez szerepel.")
    prm = pd.DataFrame([
        ("Kép", f"{wm.PX_PER_BAR} px / 15m bar (nyitás-pötty bal, high–low vonal középen, zárás-pötty jobb), {wm.IMG_H} × {wm.PX_PER_BAR * pre.n_bars} px, "
                f"{pre.n_bars} bar (jobbra igazítva); ár-panel {wm.PRICE_ROWS} sor, volumen-panel {wm.VOL_ROWS} sor; fekete-fehér"),
        ("Árskála", f"(ár − low[t0]) / ATR14(15m), FIX tengely 0 … {emb.render.p_hi:.2f} ATR (a train 99,5. percentilise) — a rebound mérete megmarad"),
        ("Volumen-skála", f"15m volumen / 7 napos átlag, tengely 0 … {emb.render.v_hi:.2f} (a train 99. percentilise)"),
        ("Autoencoder", f"3 konvolúciós réteg (stride 2, {cfg.width}/{2 * cfg.width}/{4 * cfg.width} csatorna) → {cfg.latent} dim latens → tükör dekóder; "
                        f"BCE (pozitív pixel-súly), Adam lr {cfg.lr}, {cfg.epochs} epoch, batch {cfg.batch}; torch CPU, 1 szál, seed {seed}"),
        ("Klaszterezés", f"k-means (n_init = 10) a standardizált latensen, k ∈ {ks[0]}..{ks[-1]}; a 0. klaszter a legnagyobb"),
        ("Baseline", "tslearn k-Shape (SBD) / DTW k-means a z-normalizált 15m záróár-útvonalon (≤ 32 pont)"),
        ("k-választás", f"5200: a 6 blokkos purged CV (`cv_fold`, purge = teljes intervallum, embargo = H = {h_min // 60}h) out-of-fold log-loss-a; "
                       f"prediktor: klaszter-impulzusarány a fold-train prior felé zsugorítva (erősség {wm.SMOOTH_A:.0f}); legkisebb OOF log-loss nyer"),
        ("Fold-illesztés", "minden foldban az AE-t és a klasztert csak a fold tanító eseményein illesztjük; a teszt-fold csak transzformálódik / hozzárendelődik"),
        ("Stabilitás", "a foldonkénti illesztések partíciójának páronkénti egyezése a közös (nem átlógó) train eseményeken: ARI és párosított Jaccard"),
    ], columns=["param", "value"])
    rb.add_table("params", prm, title="Definíciók és beállítások", columns={"param": "Paraméter", "value": "Érték"})

    # 1. k-selection
    rb.add_markdown(
        "## 1. k-választás: out-of-fold log-loss (5200) és stabilitás\n\n"
        f"A 6 blokkos purged CV-ben (összesen {extra['n_oof']} out-of-fold teszt-esemény) mindegyik `k`-ra és módszerre a klaszter → impulzus-arány "
        f"prediktor log-loss-át mérjük, és a fold-train priorhoz (konstans prediktor, log-loss **{extra['prior_ll']:.4f}**) hasonlítjuk. "
        "**Negatív ΔLL = jobb a priornál.** A kiválasztott `k` a legkisebb OOF log-loss (5200 szabály; döntetlen esetén a kisebb `k`).")
    rb.add_chart("kselect_ll", _kselect_fig(summ, list(methods), "delta_ll", "OOF log-loss − prior log-loss (kisebb = jobb)", 0.0),
                 title="k-választás: OOF ΔLL a konstans prediktorhoz képest", height=340)
    rb.add_chart("kselect_stability", _kselect_fig(summ, list(methods), "ari", "fold-partíciók páronkénti ARI-ja", None),
                 title="Klaszter-stabilitás foldok között (ARI)", height=340)
    cols = {"method": "Módszer", "k": "k", "oof_ll": {"label": "OOF log-loss", "digits": 4}, "prior_ll": {"label": "Prior log-loss", "digits": 4},
            "delta_ll": {"label": "ΔLL", "digits": 4}, "blocks_better": "Blokk (6-ból) < prior",
            "ari": {"label": "ARI", "digits": 3}, "jaccard": {"label": "Jaccard", "digits": 3},
            "min_share": {"label": "Legkisebb klaszter (átl. arány)", "type": "pct", "digits": 1}}
    kt = summ.copy()
    kt["method"] = kt["method"].map(wm.METHOD_HU)
    rb.add_table("kselect", kt, title="k-választás cellánként és módszerenként (OOF, 6 blokk)", columns=cols)
    pf = folds[folds["k"] == k_ae].copy()
    pf["method"] = pf["method"].map(wm.METHOD_HU)
    rb.add_table("kselect_blocks", pf[["method", "block", "n_train", "n_test", "ll", "ll_prior", "delta_ll"]],
                 title=f"Blokkonkénti OOF log-loss a kiválasztott k = {k_ae} mellett", columns={
                     "method": "Módszer", "block": "Teszt-blokk", "n_train": "Fold-train", "n_test": "Teszt-esemény",
                     "ll": {"label": "Log-loss", "digits": 4}, "ll_prior": {"label": "Prior", "digits": 4}, "delta_ll": {"label": "ΔLL", "digits": 4}})
    rb.add_markdown(
        f"**Választott k = {k_ae}** (AE + k-means; OOF log-loss {row['oof_ll']:.4f}, ΔLL = {row['delta_ll']:+.4f}, "
        f"{int(row['blocks_better'])}/6 blokkban jobb a priornál, ARI {row['ari']:.2f}, Jaccard {row['jaccard']:.2f}). "
        "A baseline-ok saját legjobb `k`-ja: " + "; ".join(f"{wm.METHOD_HU[mm]} k = {best[mm]}" for mm in methods) + ".")

    # 2. centroid images
    sizes = [f"C{c} (n={int((lab == c).sum())})" for c in range(k_ae)]
    imgs = wm.render_images(pre, emb.render)[:, 0]
    cent_imgs = [imgs[lab == c].mean(axis=0) for c in range(k_ae)]
    d2 = ((z[:, None, :] - model.centroids[None, :, :]) ** 2).sum(axis=2)
    medoid = [int(np.flatnonzero(lab == c)[np.argmin(d2[lab == c, c])]) for c in range(k_ae)]
    ncols = min(5, k_ae)
    gh = 250 * (-(-k_ae // ncols)) + 40
    rb.add_markdown(
        f"## 2. Klaszter-centroid képek (k = {k_ae})\n\n"
        "A centroid-kép a klaszter tagjainak **átlag-képe** (a pixel sötétsége = a pixel gyakorisága a klaszterben). Fent az ár-panel "
        "(ATR-egységben, a `low[t0]` az alsó él), alatta a relatív volumen; az idő balról jobbra, a jobb szél a `t_c`. "
        "A medoid-kép a klaszter latens-centroidjához legközelebbi valódi esemény bináris képe.")
    rb.add_chart("centroid_images", _image_grid(cent_imgs, sizes, ncols, gh), title=f"Klaszter-centroid (átlag) képek, k = {k_ae}", height=gh)
    rb.add_chart("medoid_images", _image_grid([imgs[i] for i in medoid], [f"C{c} medoid: {pd.Timestamp(ev['t0'][i]).strftime('%Y-%m-%d')}"
                                                                        for c, i in enumerate(medoid)], ncols, gh),
                 title="Klaszter-medoid (a centroidhoz legközelebbi) események képei", height=gh)

    # 3. sizes + class shares
    rb.add_markdown(
        "## 3. Klaszter-méret és osztályarány (train, leíró)\n\n"
        "> **Leíró, in-sample:** a klaszterek a train eseményeken illeszkednek, az arányok ugyanezeken számolódnak. Nem bizonyíték — "
        "a bizonyíték a t920 CV ΔAUC. Kis klaszternél az arány zajos.")
    rb.add_chart("cluster_sizes", _size_fig(csum), title="Klaszter-méretek (train esemény)", height=300)
    rb.add_chart("cluster_shares", _share_fig(csum, prior_rate), title="Osztályarányok klaszterenként (impulzus / hamis indulás / oldalazás, train, leíró)", height=380)
    rb.add_table("cluster_summary", csum, title="Klaszterek összegzése (train, leíró)", columns={
        "cluster": "Klaszter", "n": "Esemény", "share": {"label": "Arány", "type": "pct", "digits": 1},
        "n_impulse": "Impulzus (db)", "n_false_start": "Hamis indulás (db)", "n_sideways": "Oldalazás (db)",
        "impulse_rate": {"label": "Impulzus", "type": "pct", "digits": 1}, "false_start_rate": {"label": "Hamis indulás", "type": "pct", "digits": 1},
        "sideways_rate": {"label": "Oldalazás", "type": "pct", "digits": 1}, "a_sigma_med": {"label": "A/σ_W medián", "digits": 2},
        "end_p10": {"label": "H végén p10 (A)", "digits": 2}, "end_med": {"label": "H végén medián (A)", "digits": 2},
        "end_p90": {"label": "H végén p90 (A)", "digits": 2}, "mfe_med": {"label": "MFE medián (A)", "digits": 2},
        "mae_med": {"label": "MAE medián (A)", "digits": 2}})

    # 4. cones
    uncond = _cone(paths)
    rb.add_markdown(
        "## 4. Kimenet-tölcsér: klaszter vs feltétel nélküli kúp\n\n"
        "Az útvonal a `t_c` utáni `H` ablakban `log(close / close[t_c]) / A` egységben (15 perces mintavétel); **+1 = az impulzus-szint `P*`**, "
        "**−1 = a hamis indulás szintje `low[t0]`** (szimmetrikus, mert `P* = close[t_c]·exp(A)`). Szürke sáv = az összes train esemény p10–p90 "
        f"kúpja (a `H` végén medián {uncond[1, -1]:+.2f}, p10 {uncond[0, -1]:+.2f}, p90 {uncond[2, -1]:+.2f}); színes sáv = a klaszter p10–p90, vonal = medián. "
        "A tölcsér a train eseményekre vonatkozik (leíró).")
    rb.add_chart("cones", _cone_fig(paths, lab, k_ae, h_min), title=f"Klaszterenkénti p10–p90 kimenet-tölcsér a feltétel nélkülihez képest (k = {k_ae})",
                 height=260 * (-(-k_ae // min(5, k_ae))) + 80)

    # 5. baseline
    rb.add_markdown(
        "## 5. Baseline-összehasonlítás: k-Shape és DTW k-means a nyers úton\n\n"
        f"Mindhárom módszer ugyanazokkal a foldokkal és szabályokkal fut (az AE-féle `k = {k_ae}` mellett összevetve). "
        "Az utolsó két oszlop az in-sample klaszterenkénti impulzus-arány min / max értéke (leíró).")
    rb.add_table("baseline", base_tbl, title="AE embedding vs nyers útvonal baseline-ok", columns={
        "method": "Módszer", "k": "k", "delta_ll": {"label": "OOF ΔLL", "digits": 4}, "blocks_better": "Blokk (6-ból) < prior",
        "ari": {"label": "ARI", "digits": 3}, "jaccard": {"label": "Jaccard", "digits": 3},
        "min_share": {"label": "Legkisebb klaszter", "type": "pct", "digits": 1},
        "rate_min": {"label": "Impulzus-arány min", "type": "pct", "digits": 1}, "rate_max": {"label": "Impulzus-arány max", "type": "pct", "digits": 1},
        "best_k": "Saját legjobb k", "best_delta_ll": {"label": "ΔLL a saját legjobb k-n", "digits": 4}})
    rb.add_chart("method_rates", _method_rates_fig(per_method, prior_rate["impulse"]),
                 title="Klaszterenkénti impulzus-arány módszerenként (train, leíró)", height=340)
    cl = pd.DataFrame(cl_rows)
    rb.add_table("baseline_clusters", cl, title="Klaszter-méretek és osztályarányok módszerenként (train, leíró)", columns={
        "method": "Módszer", "cluster": "Klaszter", "n": "Esemény", "share": {"label": "Arány", "type": "pct", "digits": 1},
        "impulse_rate": {"label": "Impulzus", "type": "pct", "digits": 1}, "false_start_rate": {"label": "Hamis indulás", "type": "pct", "digits": 1},
        "sideways_rate": {"label": "Oldalazás", "type": "pct", "digits": 1}})

    # 6. AE
    rb.add_markdown(
        "## 6. Autoencoder: rekonstrukció-példák\n\n"
        f"Hat, időrendben egyenletesen kiválasztott train esemény eredeti képe és az AE rekonstrukciója (a tanítás végi BCE {emb.losses[-1]:.4f}, "
        f"az első epochban {emb.losses[0]:.4f}). A rekonstrukció valószínűség-kép; a lényeg, hogy a nagy alakzatok (esés, rebound, volumen-tüske) "
        "megmaradnak a 16 dimenziós latensben.")
    rb.add_chart("recon", _recon_examples(emb, pre, ev), title="AE rekonstrukció-példák (train)", height=230 * 3 + 40)
    rb.add_table("runtime", pd.DataFrame([
        ("Teljes futás", runtime), ("k-választás (6 fold × AE + 7 k + baseline-ok)", t_cv), ("Végső illesztés (AE + k-means)", t_fit)],
        columns=["step", "seconds"]), title="Futási idő (CPU, 1 szál)", columns={"step": "Lépés", "seconds": {"label": "mp", "digits": 1}})

    # 7. interpretation
    base = {mm: summ[(summ["method"] == mm) & (summ["k"] == k_ae)].iloc[0] for mm in methods}
    ae_row = base["ae_kmeans"]
    gain = float(ae_row["delta_ll"])
    rates = csum["impulse_rate"].to_numpy()
    sm = csum.loc[csum["n"] >= 30]
    if gain >= 0:
        ll_txt = (f"Az AE-klaszterek OOF log-loss-a **nem jobb** a konstans prediktornál (ΔLL = {gain:+.4f}): out-of-fold a klaszter-tagság "
                  "nem ad impulzus-információt a prior fölött.")
    elif gain > -0.003 or int(ae_row["blocks_better"]) < 4:
        ll_txt = (f"Az AE-klaszterek OOF log-loss-a csak elhanyagolhatóan jobb a priornál (ΔLL = {gain:+.4f}, {int(ae_row['blocks_better'])}/6 "
                  "blokkban jobb): ez zajszintű, nem hordoz kimutatható impulzus-információt.")
    else:
        ll_txt = (f"Az AE-klaszterek OOF log-loss-a jobb a priornál (ΔLL = {gain:+.4f}, {int(ae_row['blocks_better'])}/6 blokkban), "
                  "de ez még nem a skalár alap fölötti többlet: az `A/σ_W` önmagában is erős (lásd a t925 riportot).")
    near = [int(k) for k in s_ae.index if float(s_ae.loc[k, "delta_ll"]) - gain <= 0.001]
    edge_txt = ""
    if len(near) > 1 or k_ae in (ks[0], ks[-1]):
        edge_txt = (" **Figyelem a k-választásra:** a legkisebb OOF log-loss"
                    + (" a jelölt-tartomány szélére esik, és" if k_ae in (ks[0], ks[-1]) else "")
                    + f" a következő k-értékek ΔLL-je legfeljebb 0,001-gyel tér el ({', '.join(map(str, near))}) — ez zajszint a ~{extra['n_oof']} OOF eseményen; "
                    f"a legstabilabb AE-partíció k = {int(s_ae['ari'].idxmax())} (ARI {float(s_ae['ari'].max()):.2f}), a választott k = {k_ae} ARI-ja {float(ae_row['ari']):.2f}. "
                    "A 5200 szabály (argmin) szerint maradt a választás; a t920-nak érdemes több k-n (pl. a legstabilabb k-n) érzékenységi futást adnia.")
    cmp_txt = "; ".join(f"{wm.METHOD_HU[mm]}: ΔLL {base[mm]['delta_ll']:+.4f}, ARI {base[mm]['ari']:.2f}" for mm in methods)
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"**Klaszterezés:** k = {k_ae} (5200: legkisebb OOF log-loss a {len(ks)} jelölt között); klaszter-méretek: "
        + ", ".join(f"C{c} {int(n)}" for c, n in enumerate(csum["n"])) + f". Stabilitás foldok között: ARI {ae_row['ari']:.2f}, párosított Jaccard {ae_row['jaccard']:.2f} "
        "(1 = azonos partíció).\n\n"
        f"**Prediktív jel (előzetes, nem bizonyíték):** {ll_txt} Baseline-összevetés ugyanazon `k`-n: {cmp_txt}.{edge_txt}\n\n"
        f"**Leíró osztályarány (in-sample):** a klaszterenkénti impulzus-arány {100 * float(rates.min()):.1f}% … {100 * float(rates.max()):.1f}% "
        f"(train átlag {100 * prior_rate['impulse']:.1f}%); legalább 30 eseményes klaszterekben {100 * float(sm['impulse_rate'].min()):.1f}% … "
        f"{100 * float(sm['impulse_rate'].max()):.1f}%. A klaszter-mediánok `A/σ_W` értéke {float(csum['a_sigma_med'].min()):.2f} … "
        f"{float(csum['a_sigma_med'].max()):.2f}: ha a klaszterek főleg a rebound méretét különítik el, a kép-modell ugyanazt az információt "
        "ismétli, amit az S0 alap már tartalmaz — ezt csak a t920 skalár-alap elleni ΔAUC-ja dönti el.\n\n"
        "**Állásfoglalás:** a klaszterezés ezen a cellán technikailag működik (determinisztikus, per-fold illeszthető), de a bizonyíték a t920-é; "
        "ez a riport egyik eredményét sem szabad go/no-go érvként használni. A `wave_image_model.fit_fold` a t920 számára kész (fold-train-only AE + klaszter, "
        "a teszt-fold csak transzformálódik).")
    return rb.write()


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or SLUG).split("\n")[0])
    ap.add_argument("--asset-id", default=None)
    ap.add_argument("--epochs", type=int, default=wm.AEConfig().epochs)
    ap.add_argument("--root", default=None, help="report root (default: _doc_/reports)")
    args = ap.parse_args()
    out = build(Path(args.root) if args.root else None, asset_id=args.asset_id, cfg=wm.AEConfig(epochs=args.epochs))
    print(out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
