"""Report builder of the candidate denominator (epic_077 / t953): ``_doc_/reports/move_census_candidates/``.

``uv run python -m reports.builders.move_census_candidates_report --asset-id btcusdt``
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
from reports.builders import move_census_candidates as cd
from reports.builders._common import themed
from reports.builders.move_census_report import _scale_label

logger = logging.getLogger(__name__)
SLUG = "move_census_candidates"
BUCKET_HU = {0: "c = 0 (friss mélypont)", 1: "c = 1 (1 ciklus után)", 2: "c ≥ 2", -1: "összes jelölt"}
BUCKET_COLOR = {0: theme.BLUE, 1: theme.ORANGE, 2: theme.LONG, -1: theme.ACCENT}
MARTINGALE = {1: 0.5, 2: 1 / 3, 3: 0.25}


def _band(fig: go.Figure, d: pl.DataFrame, xs: list[str], color: str, name: str) -> None:
    fig.add_trace(go.Scatter(x=xs + xs[::-1], y=d["plc_p975"].to_list() + d["plc_p025"].to_list()[::-1], fill="toself",
                             fillcolor="rgba(253,126,20,0.18)", line={"width": 0}, name=name, hoverinfo="skip"))


def fig_p_big(summ: pl.DataFrame, cmp: pl.DataFrame, k: float, p: mc.CensusParams) -> go.Figure:
    fig = go.Figure()
    xs = [_scale_label(s) for s in p.scales]
    pl_sign = cmp.filter((pl.col("kind") == "sign") & (pl.col("k") == k) & (pl.col("cbucket") == -1) & (pl.col("stat") == "p_big")).sort("scale_min")
    if pl_sign.height:
        _band(fig, pl_sign, [_scale_label(s) for s in pl_sign["scale_min"].to_list()], theme.ORANGE, "placebo (véletlen előjel, összes jelölt, 95%)")
    for b in (0, 1, 2, -1):
        d = summ.filter((pl.col("variant") == "real") & (pl.col("k") == k) & (pl.col("cbucket") == b)).sort("scale_min")
        fig.add_trace(go.Scatter(x=[_scale_label(s) for s in d["scale_min"].to_list()], y=d["p_big"], mode="lines+markers", name=BUCKET_HU[b],
                                 line={"color": BUCKET_COLOR[b], "width": 3 if b == -1 else 1.5}))
    fig.add_hline(y=float(np.exp(-1)), line={"color": theme.NEUTRAL, "dash": "dash", "width": 1}, annotation_text="e⁻¹ = 0,368 (zárt alak, folytonos bolyongás)")
    fig.update_xaxes(categoryorder="array", categoryarray=xs, title="detektor-skála T")
    fig.update_yaxes(title="P(nagy mozgás lesz | jelölt)", range=[0.25, 0.6])
    return themed(fig, height=400)


def fig_hits(summ: pl.DataFrame, cmp: pl.DataFrame, k: float, scales_min: int = 60) -> go.Figure:
    """+1R / +2R / +3R hit rate (pooled over scales >= ``scales_min``) vs the martingale value."""
    real = summ.filter((pl.col("variant") == "real") & (pl.col("k") == k) & (pl.col("cbucket") == -1) & (pl.col("scale_min") >= scales_min))
    w = real["n"].to_numpy().astype(float)
    fig = go.Figure()
    xs = ["+1R", "+2R", "+3R"]
    vals = [float(np.average(real[f"p_hit{n}"].to_numpy(), weights=w)) for n in (1, 2, 3)]
    fig.add_trace(go.Bar(x=xs, y=vals, name="valós BTC", marker_color=theme.ACCENT))
    fig.add_trace(go.Scatter(x=xs, y=[MARTINGALE[n] for n in (1, 2, 3)], mode="markers", name="martingál (1 / (n+1))",
                             marker={"symbol": "line-ew", "size": 36, "line": {"width": 3, "color": theme.SHORT}}))
    fig.update_yaxes(title="találati arány a stop előtt", range=[0, 0.6])
    return themed(fig, height=340, hovermode="closest")


def fig_edge(cand: pl.DataFrame, summ: pl.DataFrame, cmp: pl.DataFrame, k: float, n_target: int, p: mc.CensusParams) -> go.Figure:
    """Gross and net E[R] per scale with a day-cluster bootstrap CI; placebo band for the gross value."""
    fig = go.Figure()
    cc = cd.with_returns(cand.filter(pl.col("k") == k))
    day = cc["conf_time"].dt.truncate("1d").dt.epoch("s").to_numpy() // 86400
    xs, gm, glo, ghi, nm, nlo, nhi = [], [], [], [], [], [], []
    for sc in p.scales:
        msk = (cc["scale_min"] == sc).to_numpy()
        if msk.sum() < 30:
            continue
        g = cd.cluster_ci(day[msk], cc[f"R{n_target}"].to_numpy()[msk].astype(float))
        n = cd.cluster_ci(day[msk], cc[f"Rnet{n_target}"].to_numpy()[msk].astype(float))
        xs.append(_scale_label(sc))
        gm.append(g[0])
        glo.append(g[0] - g[1])
        ghi.append(g[2] - g[0])
        nm.append(n[0])
        nlo.append(n[0] - n[1])
        nhi.append(n[2] - n[0])
    pb = cmp.filter((pl.col("kind") == "sign") & (pl.col("k") == k) & (pl.col("cbucket") == -1) & (pl.col("stat") == f"ER{n_target}")).sort("scale_min")
    if pb.height:
        _band(fig, pb, [_scale_label(s) for s in pb["scale_min"].to_list()], theme.ORANGE, "placebo, bruttó (véletlen előjel, 95%)")
    fig.add_trace(go.Scatter(x=xs, y=gm, mode="markers+lines", name="bruttó E[R]", line={"color": theme.ACCENT},
                             error_y={"type": "data", "symmetric": False, "array": ghi, "arrayminus": glo}))
    fig.add_trace(go.Scatter(x=xs, y=nm, mode="markers+lines", name="E[R] költségek után (13 bps)", line={"color": theme.SHORT},
                             error_y={"type": "data", "symmetric": False, "array": nhi, "arrayminus": nlo}))
    fig.add_hline(y=0, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    fig.update_xaxes(categoryorder="array", categoryarray=[_scale_label(s) for s in p.scales], title="detektor-skála T")
    fig.update_yaxes(title=f"átlagos R a +{n_target}R céllal, a stop = a mélypont")
    return themed(fig, height=400)


def strata(cand: pl.DataFrame, k: float, scales_min: int = 60) -> pl.DataFrame:
    """P(big) and gross E[R2] by retracement depth and by volume-ratio bin (candidates with a previous impulse only)."""
    c = cd.with_returns(cand.filter((pl.col("k") == k) & (pl.col("scale_min") >= scales_min) & pl.col("retr").is_not_null()))
    day = c["conf_time"].dt.truncate("1d").dt.epoch("s").to_numpy() // 86400
    out = []
    specs = (("korrekció mélysége", "retr", [0.0, 0.25, 0.5, 0.75, 1.0, 9.0], ["<0,25", "0,25–0,5", "0,5–0,75", "0,75–1,0", ">1,0"]),
             ("korrekció-volumen / lökés-volumen", "vol_ratio", [0.0, 0.5, 0.8, 1.2, 99.0], ["<0,5", "0,5–0,8", "0,8–1,2", ">1,2"]))
    for name, col, edges, labels in specs:
        v = c[col].to_numpy().astype(float)
        for i, lab in enumerate(labels):
            msk = (v >= edges[i]) & (v < edges[i + 1])
            if msk.sum() < 30:
                continue
            pb = cd.cluster_ci(day[msk], c["big"].to_numpy()[msk].astype(float))
            r2 = cd.cluster_ci(day[msk], c["R2"].to_numpy()[msk].astype(float))
            out.append({"szempont": name, "sáv": lab, "n": int(msk.sum()), "napok": int(np.unique(day[msk]).size), "p_big": pb[0],
                        "p_big_lo": pb[1], "p_big_hi": pb[2], "E_R2": r2[0], "E_R2_lo": r2[1], "E_R2_hi": r2[2]})
    return pl.DataFrame(out)


def fig_strata(s: pl.DataFrame, which: str, col: str, base: float, title: str) -> go.Figure:
    d = s.filter(pl.col("szempont") == which)
    fig = go.Figure(go.Scatter(x=d["sáv"], y=d[col], mode="markers", marker={"size": 11, "color": theme.ACCENT},
                               error_y={"type": "data", "symmetric": False, "array": (d[f"{col}_hi"] - d[col]).to_list(),
                                        "arrayminus": (d[col] - d[f"{col}_lo"]).to_list()}))
    fig.add_hline(y=base, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    fig.update_yaxes(title=title)
    return themed(fig, height=320, hovermode="closest")


def build_report(asset_id: str, *, write: bool = True) -> ReportBuilder:
    p = mc.CensusParams()
    cand = mc.load_table(asset_id, cd.TABLE_CAND)
    summ = mc.load_table(asset_id, cd.TABLE_CAND_SUM)
    plc = mc.load_table(asset_id, cd.TABLE_CAND_PLC)
    cmp = cd.compare_cands(summ.filter(pl.col("variant") == "real"), plc)
    n_rep = int(plc.filter(pl.col("kind") == "sign")["rep"].n_unique())
    eff = (cand.with_columns(pl.col("conf_time").dt.truncate("1d").alias("day")).group_by(["scale_min", "k"])
           .agg(pl.len().alias("jelöltek"), pl.col("day").n_unique().alias("különböző napok")).sort(["k", "scale_min"]))
    s4 = strata(cand, 4.0)
    rb = ReportBuilder("general", SLUG, f"Korai-impulzus jelöltek: a nevező — {asset_id.upper()} (epic_077 / t953)", order=78,
                       summary="Kauzális jelöltek (megerősített mélypont), a nagy mozgássá válás bázisrátája és az R-szorzós élük, placebóval.",
                       tags=["research", "scout2", "move_census", "candidates"], source={"kind": "analysis", "generator": cd.__name__})
    rb.add_markdown(f"""
## Cél

A t950–t952 katalógus utólagos: csak a már befejezett nagy mozgásokat látja. Itt a **nevezőt** építjük: azokat a helyzeteket, amelyek **valós időben** felismerhetők
(egy megerősített mélypont, ami után a ár a skálájához tartozó σ-egység felét visszapattanta), és megnézzük, hány lett belőlük nagy mozgás, és mennyit hoz az R-szorzós belépés.

* **Jelölt:** a (T, k) zigzag megerősített mélypontja; a megerősítés pillanatáig ismert adat az egyetlen bemenet.
* **Ciklusszám (c):** egymást követő *emelkedő mélypontok* száma (0 = friss mélypont, a Scout-féle „1-2" indulás; 1 = egy ciklus után; ≥ 2 = később).
* **Címke 1:** a jelöltből indul-e nagy mozgás (a t950 definícióval).
* **Címke 2 (R-szorzó):** belépés a megerősítő bar zárásán, stop = a mélypont (strukturális), cél = +1R / +2R / +3R, ahol R = belépő − mélypont. Zárás alapú, nincs időlimit. Martingálnál `P(+nR) = 1/(n+1)`, `E[R] = 0` költség előtt.
* **Költség:** 13 bps (taker be + ki: 2 × 5; csúszás be + ki: 2 × 1,5), R-egységben `13 bps / R`.
* **Placebo:** {n_rep}-{n_rep} véletlen-előjeles és blokk-bootstrap útvonal, ugyanazon a kódon.
* **CI:** nap-klaszteres bootstrap (egy nap jelöltjei együtt mintavételezve).
""")
    rb.add_markdown("## Mennyi a bázisráta?")
    for k in (3.0, 4.0):
        rb.add_chart(f"p_big_k{int(k)}", fig_p_big(summ, cmp, k, p), title=f"P(nagy mozgás | jelölt) skálánként és ciklusszámonként (k = {k:g})", height=400)
    rb.add_markdown("## R-szorzós találati arányok és él")
    rb.add_chart("hits_k4", fig_hits(summ, cmp, 4.0), title="Találati arány a stop előtt (k = 4, T ≥ 1 óra)", height=340)
    for n in (1, 2, 3):
        rb.add_chart(f"edge_r{n}", fig_edge(cand, summ, cmp, 4.0, n, p), title=f"Átlagos R a +{n}R céllal (k = 4)", height=400)
    rb.add_markdown("## Van-e mintázat a jelölt jellemzőiben? (leíró, k = 4, T ≥ 1 óra)\n\nA sávok és szempontok száma miatt ez feltáró, nem megerősítő elemzés; a CI-k nap-klaszteresek.")
    rb.add_chart("strata_retr", fig_strata(s4, "korrekció mélysége", "p_big", float(np.exp(-1)), "P(nagy mozgás)"), title="Nagy mozgássá válás a korrekció mélysége szerint", height=320)
    rb.add_chart("strata_vol", fig_strata(s4, "korrekció-volumen / lökés-volumen", "p_big", float(np.exp(-1)), "P(nagy mozgás)"), title="Nagy mozgássá válás a volumen-arány szerint", height=320)
    rb.add_table("strata", s4.to_pandas().round(4), title="Sávok: P(nagy) és E[R2] nap-klaszteres CI-vel")
    rb.add_markdown("## Effektív mintaméret")
    rb.add_table("effective_n", eff.to_pandas(), title="Jelöltek és különböző napok skálánként")
    rb.add_table("summary", summ.filter(pl.col("variant") == "real").to_pandas().round(4), title="Összesítő minden cellára (valós)")
    rb.add_table("summary_demeaned", summ.filter(pl.col("variant") == "real_demeaned").to_pandas().round(4), title="Ugyanez a drift-mentesített valós útvonalon")
    rb.add_table("placebo_compare", cmp.to_pandas().round(4), title="Valós vs placebo")
    rb.add_markdown("## Értelmezés\n\nLásd az epic zárójelentését. Ez a riport a t953 mérése.")
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
