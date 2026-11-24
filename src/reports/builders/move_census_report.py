"""Report builder of the large-move census (epic_077 / t950): ``_doc_/reports/move_census/``.

``uv run python -m reports.builders.move_census_report --asset-id btcusdt [--no-store]``
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

import numpy as np
import plotly.graph_objects as go
import polars as pl

from reports import theme
from reports.builder import ReportBuilder
from reports.builders import move_census as mc
from reports.builders._common import themed

logger = logging.getLogger(__name__)

SLUG = "move_census"
K_COLORS = {3.0: theme.BLUE, 4.0: theme.ACCENT, 6.0: theme.SHORT}


# --------------------------------------------------------------------------------------------------------------
# analytics (pure; reused by t951 / t952)
# --------------------------------------------------------------------------------------------------------------

def duration_quantiles(moves: pl.DataFrame) -> pl.DataFrame:
    """Duration / net-size / efficiency quantiles per (scale, k)."""
    return (moves.group_by(["scale_min", "k"]).agg(
        pl.len().alias("n"),
        pl.col("dur_min").min().alias("dur_min_"), pl.col("dur_min").quantile(0.10).alias("dur_p10"),
        pl.col("dur_min").median().alias("dur_med"), pl.col("dur_min").quantile(0.90).alias("dur_p90"),
        pl.col("dur_min").max().alias("dur_max"),
        pl.col("er").median().alias("er_med"), pl.col("er").quantile(0.10).alias("er_p10"),
        pl.col("er").quantile(0.90).alias("er_p90"),
        (pl.col("net_log").median() * 100).alias("net_med_pct"),
        pl.col("mdd_ratio").median().alias("mdd_ratio_med"),
    ).sort(["k", "scale_min"]))


def per_year_rates(moves: pl.DataFrame, p: mc.CensusParams) -> pl.DataFrame:
    """Big moves per year (rate = count / fraction-of-year covered by the window), by (scale, k, year of start)."""
    w0, w1 = np.datetime64(p.window_start, "m"), np.datetime64(p.window_end, "m")
    years = list(range(int(str(w0)[:4]), int(str(w1)[:4]) + 1))
    exposure = {}
    for y in years:
        a = max(w0, np.datetime64(f"{y}-01-01", "m"))
        b = min(w1 + 1, np.datetime64(f"{y + 1}-01-01", "m"))
        exposure[y] = float((b - a).astype("int64")) / (365.25 * 1440)
    df = moves.with_columns(pl.col("start_time").dt.year().alias("year"))
    g = df.group_by(["scale_min", "k", "year"]).agg(pl.len().alias("n")).sort(["scale_min", "k", "year"])
    return g.with_columns(
        pl.col("year").replace_strict(exposure, return_dtype=pl.Float64).alias("exposure_yr"),
    ).with_columns((pl.col("n") / pl.col("exposure_yr")).alias("rate_per_yr"))


def _merge(iv: np.ndarray) -> np.ndarray:
    """Merge touching/overlapping intervals (rows ``[start, end]`` sorted by start)."""
    if iv.size == 0:
        return iv.reshape(0, 2)
    iv = iv[np.argsort(iv[:, 0])]
    out = [iv[0].copy()]
    for a, b in iv[1:]:
        if a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append(np.array([a, b]))
    return np.array(out)


def _ts_min(col: pl.Series) -> np.ndarray:
    return col.dt.epoch("ms").to_numpy() // 60_000


def coverage_and_nesting(moves: pl.DataFrame, p: mc.CensusParams, i_window_min: int) -> pl.DataFrame:
    """Per k: share of window time inside any big move (all scales), number of merged clusters, and per scale the
    share of moves lying fully inside the union of moves of strictly larger scales (nesting)."""
    rows = []
    for k in p.ks:
        mk = moves.filter(pl.col("k") == k)
        s, e = _ts_min(mk["start_time"]), _ts_min(mk["end_time"])
        merged = _merge(np.column_stack([s, e]).astype(np.int64))
        covered = float((merged[:, 1] - merged[:, 0]).sum())
        rows.append({"k": k, "scale_min": 0, "n_moves": mk.height, "n_clusters": int(merged.shape[0]),
                     "time_covered_pct": 100 * covered / i_window_min, "inside_larger_pct": float("nan")})
        for sc in p.scales:
            cur = mk.filter(pl.col("scale_min") == sc)
            larger = mk.filter(pl.col("scale_min") > sc)
            if cur.height == 0:
                continue
            if larger.height == 0:
                rows.append({"k": k, "scale_min": sc, "n_moves": cur.height, "n_clusters": cur.height,
                             "time_covered_pct": float("nan"), "inside_larger_pct": float("nan")})  # largest scale: n/a
                continue
            lm = _merge(np.column_stack([_ts_min(larger["start_time"]), _ts_min(larger["end_time"])]).astype(np.int64))
            cs, ce = _ts_min(cur["start_time"]), _ts_min(cur["end_time"])
            j = np.searchsorted(lm[:, 0], cs, side="right") - 1
            ok = (j >= 0) & (ce <= lm[np.clip(j, 0, None), 1])
            rows.append({"k": k, "scale_min": sc, "n_moves": cur.height, "n_clusters": cur.height,
                         "time_covered_pct": float("nan"), "inside_larger_pct": 100 * float(ok.mean())})
    return pl.DataFrame(rows)


# --------------------------------------------------------------------------------------------------------------
# figures
# --------------------------------------------------------------------------------------------------------------

def _scale_label(m: int) -> str:
    if m < 60:
        return f"{m} perc"
    if m < 1440:
        return f"{m // 60} óra"
    return f"{m / 1440:g} nap"


def fig_count_heatmap(cells: pl.DataFrame, p: mc.CensusParams) -> go.Figure:
    z = [[int(cells.filter((pl.col("scale_min") == sc) & (pl.col("k") == k))["n_big"][0]) for sc in p.scales] for k in p.ks]
    fig = go.Figure(go.Heatmap(z=np.log10(np.maximum(z, 1)), x=[_scale_label(s) for s in p.scales],
                               y=[f"k = {k:g}" for k in p.ks], text=z, texttemplate="%{text}", colorscale="Blues",
                               showscale=False, hoverinfo="skip"))
    fig.update_xaxes(title="detektor-skála T (σ-egység időhossza)")
    return themed(fig, height=300, hovermode="closest")


def fig_dur_over_scale(q: pl.DataFrame, p: mc.CensusParams) -> go.Figure:
    fig = go.Figure()
    for k in p.ks:
        d = q.filter(pl.col("k") == k).sort("scale_min")
        fig.add_trace(go.Scatter(x=d["scale_min"], y=d["dur_med"] / d["scale_min"], mode="lines+markers", name=f"k = {k:g}",
                                 line={"color": K_COLORS[k]}))
    fig.update_xaxes(type="log", title="detektor-skála T (perc)")
    fig.update_yaxes(title="medián hossz / T", rangemode="tozero")
    return themed(fig, height=380)


def fig_dur_box(moves: pl.DataFrame, k: float, p: mc.CensusParams) -> go.Figure:
    fig = go.Figure()
    for sc in p.scales:
        d = moves.filter((pl.col("k") == k) & (pl.col("scale_min") == sc))["dur_min"].to_numpy()
        if d.size == 0:
            continue
        q = np.percentile(d, [10, 25, 50, 75, 90])
        fig.add_trace(go.Box(x=[_scale_label(sc)], lowerfence=[q[0]], q1=[q[1]], median=[q[2]], q3=[q[3]], upperfence=[q[4]],
                             name=_scale_label(sc), marker_color=theme.BLUE, showlegend=False))
    fig.update_yaxes(type="log", title="a nagy mozgás hossza (perc, 1m-ben)")
    fig.update_xaxes(title="detektor-skála T")
    return themed(fig, height=400, hovermode="closest")


def fig_er_box(moves: pl.DataFrame, k: float, p: mc.CensusParams) -> go.Figure:
    fig = go.Figure()
    for sc in p.scales:
        d = moves.filter((pl.col("k") == k) & (pl.col("scale_min") == sc))["er"].to_numpy()
        if d.size == 0:
            continue
        q = np.percentile(d, [10, 25, 50, 75, 90])
        fig.add_trace(go.Box(x=[_scale_label(sc)], lowerfence=[q[0]], q1=[q[1]], median=[q[2]], q3=[q[3]], upperfence=[q[4]],
                             name=_scale_label(sc), marker_color=theme.LONG, showlegend=False))
    fig.update_yaxes(title="hatékonysági arány (net / út)", range=[0, 1])
    fig.update_xaxes(title="detektor-skála T")
    return themed(fig, height=360, hovermode="closest")


def fig_year_rates(yr: pl.DataFrame, k: float, scales: tuple[int, ...]) -> go.Figure:
    fig = go.Figure()
    for i, sc in enumerate(scales):
        d = yr.filter((pl.col("k") == k) & (pl.col("scale_min") == sc)).sort("year")
        fig.add_trace(go.Bar(x=d["year"].cast(pl.Utf8), y=d["rate_per_yr"], name=_scale_label(sc),
                             marker_color=theme.SEQUENCE[i % len(theme.SEQUENCE)]))
    fig.update_layout(barmode="group")
    fig.update_yaxes(type="log", title="nagy mozgás / év (log)")
    return themed(fig, height=360)


# --------------------------------------------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------------------------------------------

def build_report(moves: pl.DataFrame, cells: pl.DataFrame, meta: dict[str, Any], p: mc.CensusParams, asset_id: str,
                 *, write: bool = True) -> tuple[ReportBuilder, dict[str, Any]]:
    q = duration_quantiles(moves)
    yr = per_year_rates(moves, p)
    n_min = int((np.datetime64(p.window_end, "m") - np.datetime64(p.window_start, "m")).astype("int64")) + 1
    cov = coverage_and_nesting(moves, p, n_min)
    n_yr = n_min / (365.25 * 1440)

    rb = ReportBuilder("general", SLUG, f"Nagy-mozgás katalógus — {asset_id.upper()} long (epic_077 / t950)", order=75,
                       summary=f"{asset_id.upper()} 1m, train-ablak: skálafüggetlen zigzag-katalógus a nagy emelkedésekről "
                               f"({moves.height} mozgás 27 cellában; leíró, hindsight).",
                       tags=["research", "scout2", "move_census"],
                       source={"kind": "analysis", "generator": mc.GENERATOR})
    rb.add_markdown(f"""
## Cél

Hány **nagy emelkedés** (long) van {asset_id.upper()} 1 perces adatán, és milyen hosszú szokott lenni? A definíció skálafüggetlen:
egy σ-normált zigzag felfelé tartó lába, ahol a „nagy" = nettó emelkedés ≥ k × σ_T (σ_T: az adott skálán, T-perces hozamokon mért,
30 napos trailing szórás — empirikus, nem √T-skálázás). A zigzag akkor fordul, ha a ár a csúcsról ρ·k·σ_T-t visszaesik (ρ = 0,5), így
egy nagy mozgáson belül a visszaesés mindig kisebb a minimális méret felénél. Rács: T ∈ 15 perc … 3 nap (9 skála, duplázó lépés),
k ∈ {{3, 4, 6}} (előre rögzítve). Ablak: **{meta['window_start'][:10]} → {meta['window_end'][:10]}** ({n_yr:.2f} év), OOS és holdout-2 nem töltődött be.

**Fontos:** a katalógus utólagos (hindsight) — egy lábat csak a vége után ismerünk fel. Leíró keret, nem előrejelzés. A `T` az
„egység" skálája (mennyi σ-t mérünk), a mozgás tényleges hossza (percekben) ebből **adódik**, nem rögzített.
""")
    rb.add_markdown("## Darabszámok skála × k\n\nA cellák egymástól függetlenek, ugyanaz a mozgás több cellában is szerepelhet (alább: beágyazottság).")
    rb.add_chart("count_heatmap", fig_count_heatmap(cells, p), title="Nagy emelkedések száma (train-ablak)", height=300)
    rb.add_table("cells", cells.to_pandas().assign(big_per_year=lambda d: d.big_per_year.round(1)), title="Cella-összesítő")
    rb.add_markdown("## Mennyi ideig tart?\n\nA medián hossz / T arány k-nként nagyjából állandó a skálák között (az arányok skálafüggetlensége látszik); k-val "
                    "négyzetesen nő, ami a **véletlen bolyongás** várható értéke is (mérete kσ_T-hez ~k²·T idő kell). Ezért kell a placebo (t951).")
    rb.add_chart("dur_over_scale", fig_dur_over_scale(q, p), title="Medián hossz / T a skála függvényében", height=380)
    rb.add_chart("dur_box_k4", fig_dur_box(moves, 4.0, p), title="Hossz-eloszlás skálánként (k = 4; whisker p10–p90)", height=400)
    rb.add_table("dur_quantiles", q.to_pandas().round(3), title="Hossz-, méret-, trendesség-kvantilisek")
    rb.add_markdown("## Mennyire trendes?\n\nA hatékonysági arány (net / út, T/8 mintavétellel) mutatja, mennyire egyenes a mozgás; 1 = tökéletesen egyenes.")
    rb.add_chart("er_box_k4", fig_er_box(moves, 4.0, p), title="Hatékonysági arány skálánként (k = 4)", height=360)
    rb.add_markdown("## Időbeli stabilitás\n\nA nagy mozgások éves rátája (db / év, az ablakban lévő részév arányában).")
    rb.add_chart("year_rates_k4", fig_year_rates(yr, 4.0, (60, 240, 960, 4320)), title="Éves ráta (k = 4)", height=360)
    rb.add_table("year_rates", yr.to_pandas().round(2), title="Éves ráta minden cellára")
    rb.add_markdown("## Átfedés és beágyazottság\n\nEgy cellán belül a mozgások nem fedik egymást (a zigzag láb-váltakozó). Cellák között igen: ugyanaz a nagy "
                    "emelkedés kisebb skálán több kisebb mozgásra is eshet. A `scale_min = 0` sor az összes skála uniójáról szól.")
    rb.add_table("coverage", cov.to_pandas().round(2), title="Lefedettség és beágyazottság")
    rb.add_markdown("## Értelmezés\n\nLásd az epic zárójelentésében; ez a riport a t950 leíró képe. A következő lépés a placebo (t951).")
    if write:
        rb.write()
    return rb, {"q": q, "yr": yr, "cov": cov}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--no-store", action="store_true", help="skip the lab DB write")
    ap.add_argument("--no-report", action="store_true")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    p = mc.CensusParams()
    m = mc.load_minutes(a.asset_id)
    moves, cells, meta = mc.build_census(m, p)
    if not a.no_store:
        meta["lab_path"] = mc.persist(moves, cells, meta, a.asset_id)
    if not a.no_report:
        rb, _ = build_report(moves, cells, meta, p, a.asset_id)
        logger.info("report written: %s", rb.output_dir)
    logger.info("moves=%d", moves.height)


if __name__ == "__main__":
    main()
