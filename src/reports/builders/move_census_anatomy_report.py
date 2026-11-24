"""Report builder of the move anatomy (epic_077 / t952): ``_doc_/reports/move_census_anatomy/``.

``uv run python -m reports.builders.move_census_anatomy_report --asset-id btcusdt``
"""

from __future__ import annotations

import argparse
import logging

import numpy as np
import plotly.graph_objects as go
import polars as pl

from reports import theme
from reports.builder import ReportBuilder
from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an
from reports.builders._common import themed
from reports.builders.move_census_report import _scale_label

logger = logging.getLogger(__name__)
SLUG = "move_census_anatomy"
CYC = ("cyc0", "cyc1", "cyc2", "cyc3p")
CYC_HU = {"cyc0": "0 ciklus (tiszta lökés)", "cyc1": "1 ciklus", "cyc2": "2 ciklus", "cyc3p": "3+ ciklus"}


def fig_compare(cmp: pl.DataFrame, stats: tuple[str, ...], labels: dict[str, str], c: float, k: float = 4.0, scale: int = 0,
                *, ytitle: str) -> go.Figure:
    """Real value vs the placebo band per statistic (grouped markers)."""
    d = cmp.filter((pl.col("c_frac") == c) & (pl.col("k") == k) & (pl.col("scale_min") == scale))
    fig = go.Figure()
    for kind, color, off in (("sign", theme.ORANGE, -0.12), ("block", theme.BLUE, 0.12)):
        g = d.filter(pl.col("kind") == kind)
        xs, mu, lo, hi = [], [], [], []
        for i, st in enumerate(stats):
            r = g.filter(pl.col("stat") == st)
            if r.height == 0:
                continue
            xs.append(i + off)
            mu.append(r["plc_mean"][0])
            lo.append(r["plc_mean"][0] - r["plc_p025"][0])
            hi.append(r["plc_p975"][0] - r["plc_mean"][0])
        fig.add_trace(go.Scatter(x=xs, y=mu, mode="markers", name=f"placebo: {kind}", marker={"color": color, "size": 9},
                                 error_y={"type": "data", "symmetric": False, "array": hi, "arrayminus": lo, "color": color}))
    g = d.filter(pl.col("kind") == "sign")
    xs = [i for i, st in enumerate(stats) if g.filter(pl.col("stat") == st).height]
    ys = [g.filter(pl.col("stat") == st)["real"][0] for st in stats if g.filter(pl.col("stat") == st).height]
    fig.add_trace(go.Scatter(x=xs, y=ys, mode="markers", name="valós BTC", marker={"color": theme.ACCENT, "size": 13, "symbol": "diamond"}))
    fig.update_xaxes(tickmode="array", tickvals=list(range(len(stats))), ticktext=[labels[s] for s in stats])
    fig.update_yaxes(title=ytitle)
    return themed(fig, height=380, hovermode="closest")


def first_leg_frame(moves: pl.DataFrame, logp: np.ndarray, vol: np.ndarray, m0: int, c: float, k: float = 4.0) -> pl.DataFrame:
    """Duration / size of the first impulse per scale (1m minutes), for the moves of one k."""
    mk = moves.filter(pl.col("k") == k)
    legs = an.leg_features(an.decompose(mk, logp, vol, m0, c))
    f = legs.filter(pl.col("j") == 1).join(
        mk.with_row_index("move_id").select("move_id", "sigma"), on="move_id").with_columns((pl.col("net_j") / pl.col("sigma")).alias("net1_z"))
    return f.group_by("scale_min").agg(
        pl.len().alias("n"),
        pl.col("dur_j").min().alias("dur_min"), pl.col("dur_j").quantile(0.10).alias("dur_p10"),
        pl.col("dur_j").quantile(0.25).alias("dur_p25"), pl.col("dur_j").median().alias("dur_med"),
        pl.col("dur_j").quantile(0.75).alias("dur_p75"), pl.col("dur_j").quantile(0.90).alias("dur_p90"),
        pl.col("dur_j").max().alias("dur_max"), pl.col("net1_z").median().alias("net_z_med"),
        pl.col("share_dur").median().alias("share_of_move_dur_med"),
    ).sort("scale_min")


def fig_first_leg(frame: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    for r in frame.iter_rows(named=True):
        fig.add_trace(go.Box(x=[_scale_label(r["scale_min"])], lowerfence=[r["dur_p10"]], q1=[r["dur_p25"]], median=[r["dur_med"]],
                             q3=[r["dur_p75"]], upperfence=[r["dur_p90"]], marker_color=theme.LONG, showlegend=False))
    fig.update_yaxes(type="log", title="az első lökés hossza (perc, 1m-ben)")
    fig.update_xaxes(title="detektor-skála T")
    return themed(fig, height=380, hovermode="closest")


def fig_invariance(ms: pl.DataFrame, col: str, title: str, k: float = 4.0) -> go.Figure:
    d = ms.filter(pl.col("k") == k).group_by("scale_min").agg(
        pl.col(col).median().alias("med"), pl.col(col).quantile(0.25).alias("q1"), pl.col(col).quantile(0.75).alias("q3"),
        pl.len().alias("n")).sort("scale_min")
    xs = [_scale_label(s) for s in d["scale_min"].to_list()]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=xs + xs[::-1], y=d["q3"].to_list() + d["q1"].to_list()[::-1], fill="toself",
                             fillcolor="rgba(13,110,253,0.15)", line={"width": 0}, name="IQR", hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=xs, y=d["med"], mode="lines+markers", name="medián", line={"color": theme.BLUE}))
    fig.update_yaxes(title=title, rangemode="tozero")
    fig.update_xaxes(title="detektor-skála T")
    return themed(fig, height=340)


def slope_table(cells: pl.DataFrame, plc: pl.DataFrame, p: mc.CensusParams) -> pl.DataFrame:
    """Log-log slope of the count vs T per k: real vs the placebo reps (cells with >= 10 moves only)."""
    rows = []
    for k in p.ks:
        real = an.loglog_slope(cells, k)
        for kind in plc["kind"].unique().sort():
            sl = np.array([an.loglog_slope(g, k) for _, g in plc.filter(pl.col("kind") == kind).group_by("rep")])
            sl = sl[np.isfinite(sl)]
            rows.append({"k": k, "kind": kind, "real_slope": real, "plc_mean": float(sl.mean()), "plc_p025": float(np.percentile(sl, 2.5)),
                         "plc_p975": float(np.percentile(sl, 97.5)), "n_rep": int(sl.size)})
    return pl.DataFrame(rows)


def build_report(asset_id: str, *, write: bool = True) -> ReportBuilder:
    from reports.builders import move_census_placebo as pc

    p = mc.CensusParams()
    moves = mc.load_table(asset_id, mc.TABLE_MOVES)
    cells = mc.load_table(asset_id, mc.TABLE_CELLS)
    plc_census = mc.load_table(asset_id, pc.TABLE_PLACEBO)
    real = mc.load_table(asset_id, an.TABLE_STATS)
    plc = mc.load_table(asset_id, an.TABLE_PLC)
    cmp = an.compare(real, plc)
    m = mc.load_minutes(asset_id)
    logp = np.log(m.close)
    frames = {c: first_leg_frame(moves, logp, m.volume, m.m0, c) for c in (0.20, 0.33)}
    legs_primary = mc.load_table(asset_id, an.TABLE_LEGS)
    ms_primary = an.move_summary(legs_primary)
    slopes = slope_table(cells, plc_census, p)
    n_rep = int(plc.filter(pl.col("kind") == "sign")["rep"].n_unique())

    rb = ReportBuilder("general", SLUG, f"A nagy mozgások anatómiája — {asset_id.upper()} long (epic_077 / t952)", order=77,
                       summary="Leg-felbontás, ciklusszám, első lökés időkerete és skála-invariancia, placebóval összevetve.",
                       tags=["research", "scout2", "move_census", "anatomy"], source={"kind": "analysis", "generator": an.__name__})
    rb.add_markdown(f"""
## Cél

Egy nagy emelkedésen (t950) belül mi a szerkezet? Hány impulzus–korrekció ciklusból áll, mekkora és milyen hosszú az **első lökés**
(a „keret"), mennyire hasonlít egymásra a különböző skálákon, és mennyi ebből a véletlen bolyongás sajátja? Minden nagy mozgást a saját méretéhez
viszonyított zigzaggal bontottunk legekre: egy leg akkor fordul, ha a ár a mozgás nettójának `c` törtrészét visszaadja.
Rögzített szintek: **c = 0,10 (elsődleges), 0,05 és 0,20 (érzékenység)**. A **0,33-as szint utólag került be** (a 0,10-es bontásban a medián mozgás ~4 impulzusra esett
szét, ami nagyrészt finom szerkezet); ez áll legközelebb az „1-2-1-2" képhez. A fogalmak: *ciklusszám* = a legbelső korrekciók száma, nincs Elliott-számozás.
A placebo: {n_rep}-{n_rep} útvonal (véletlen előjel, 7 napos blokk-bootstrap), k = 4 mozgásokon.
""")
    rb.add_markdown("## Hány ciklusból áll egy nagy mozgás?")
    for c in (0.20, 0.33):
        rb.add_chart(f"cycles_c{int(round(c * 100))}", fig_compare(cmp, CYC, CYC_HU, c, ytitle="a mozgások aránya"),
                     title=f"Ciklusszám-eloszlás, valós vs placebo (c = {c:g}, k = 4, minden skála)", height=380)
    rb.add_markdown("## Az első lökés időkerete (1m-ben)\n\nA `dur_*` oszlopok az első felfelé szakasz hosszát adják percben, skálánként (k = 4).")
    for c in (0.20, 0.33):
        rb.add_chart(f"first_leg_c{int(round(c * 100))}", fig_first_leg(frames[c]), title=f"Első lökés hossza skálánként (c = {c:g}; whisker p10–p90)", height=380)
        rb.add_table(f"first_leg_table_c{int(round(c * 100))}", frames[c].to_pandas().round(3), title=f"Első lökés: hossz- és méretkvantilisek (c = {c:g})")
    rb.add_markdown("## Mit hoz a korrekció utáni lökés? (elsődleges bontás, c = 0,10)")
    rb.add_chart("order_stats", fig_compare(cmp, ("speed2_med", "speed3p_med", "netvs2_med", "netvs3p_med"),
                                            {"speed2_med": "2. lökés sebessége / 1.", "speed3p_med": "3.+ lökés sebessége / 1.",
                                             "netvs2_med": "2. lökés mérete / 1.", "netvs3p_med": "3.+ lökés mérete / 1."},
                                            0.10, ytitle="medián arány az 1. lökéshez"), title="A későbbi lökések az elsőhöz képest (valós vs placebo)", height=380)
    rb.add_chart("retr_stats", fig_compare(cmp, ("retr_before2_med", "retr_before3p_med", "jmax1", "jmax2", "jmax3p"),
                                           {"retr_before2_med": "korrekció mélysége a 2. előtt", "retr_before3p_med": "korrekció mélysége a 3.+ előtt",
                                            "jmax1": "a legnagyobb lökés az 1.", "jmax2": "a legnagyobb a 2.", "jmax3p": "a legnagyobb a 3.+"},
                                           0.10, ytitle="érték"), title="Korrekció-mélység és a legnagyobb lökés helye (valós vs placebo)", height=380)
    vol = real.filter((pl.col("c_frac") == 0.10) & (pl.col("k") == 4.0) & (pl.col("scale_min") == 0) & pl.col("stat").str.starts_with("volcorr"))
    rb.add_markdown("A volumen-arány (korrekció / előtte lévő lökés átlagos volumene, medián) csak a valós adatra értelmezhető (a placebo útvonalon a volumen a valós idősor marad, "
                    "így a hozam–volumen kapcsolat megszakad):")
    rb.add_table("volume_ratio", vol.to_pandas().round(4), title="Volumen-arány valós adaton (c = 0,10, k = 4)")
    rb.add_markdown("## Skála-invariancia\n\nA fraktál-feltevés: az arányok skálánként hasonlók. Elsődleges bontás (c = 0,10) mozgás-szintű arányai skálánként:")
    for col, title, slug in (("first_share_net", "az első lökés a mozgás nettójának hányada", "inv_net"),
                             ("first_share_dur", "az első lökés a mozgás idejének hányada", "inv_dur"), ("retr1", "az első korrekció mélysége / első lökés", "inv_retr")):
        rb.add_chart(slug, fig_invariance(ms_primary, col, title), title=title, height=340)
    ks = [an.ks_to_pooled(ms_primary.filter(pl.col("k") == 4.0), col).with_columns(pl.lit(col).alias("mutató")) for col in
          ("first_share_net", "first_share_dur", "retr1")]
    rb.add_table("ks_to_pooled", pl.concat(ks).to_pandas().round(4), title="KS-távolság a pooled eloszlástól (távolság, nem p-érték: a beágyazott mozgások nem függetlenek)")
    rb.add_markdown("## Darabszám-skálázás\n\nA nagy mozgások száma a skálával hatványfüggvény szerint csökken; a log-log meredekség (N ~ T^b) valós és placebo útvonalon:")
    rb.add_table("slopes", slopes.to_pandas().round(3), title="Darabszám log-log meredekség (cellák ≥ 10 mozgással)")
    rb.add_markdown("## Értelmezés\n\nLásd az epic zárójelentését. Ez a riport a t952 leíró mérése.")
    if write:
        rb.write()
    return rb


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    rb = build_report(a.asset_id)
    logger.info("report: %s", rb.output_dir)


if __name__ == "__main__":
    main()
