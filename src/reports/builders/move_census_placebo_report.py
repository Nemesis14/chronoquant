"""Report builder of the placebo comparison (epic_077 / t951): ``_doc_/reports/move_census_placebo/``.

``uv run python -m reports.builders.move_census_placebo_report --asset-id btcusdt``
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
from reports.builders import move_census_placebo as pc
from reports.builders._common import themed
from reports.builders.move_census_report import _scale_label

logger = logging.getLogger(__name__)
SLUG = "move_census_placebo"
KIND_HU = {"sign": "véletlen előjel (vol-burkoló megtartva)", "block": "7 napos blokk-bootstrap"}
KIND_COLOR = {"sign": theme.ORANGE, "block": theme.BLUE}
STAT_HU = {"n_big": "darabszám", "share_big": "nagy-arány (nagy / összes felfelé szakasz)", "median_dur_min": "medián hossz (perc)",
           "er_med": "medián hatékonysági arány", "mdd_ratio_med": "medián belső visszaesés / net", "vr": "variancia-arány"}


def fig_band(summ: pl.DataFrame, stat: str, k: float, p: mc.CensusParams, *, ratio: bool = False, ytitle: str,
             log_y: bool = False) -> go.Figure:
    """Real value vs the placebo band (2.5-97.5 %) per scale, one band per placebo kind."""
    fig = go.Figure()
    xs = [_scale_label(s) for s in p.scales]
    d0 = summ.filter((pl.col("stat") == stat) & (pl.col("k") == k))
    for kind in ("sign", "block"):
        d = d0.filter(pl.col("kind") == kind).sort("scale_min")
        if d.height == 0:
            continue
        den = d["plc_mean"].to_numpy() if ratio else np.ones(d.height)
        x = [_scale_label(s) for s in d["scale_min"].to_list()]
        fig.add_trace(go.Scatter(x=x + x[::-1], y=list(d["plc_p975"].to_numpy() / den) + list(d["plc_p025"].to_numpy() / den)[::-1],
                                 fill="toself", fillcolor=_alpha(KIND_COLOR[kind], 0.18), line={"width": 0},
                                 name=f"placebo: {KIND_HU[kind]} (95%)", hoverinfo="skip"))
        fig.add_trace(go.Scatter(x=x, y=d["plc_mean"] / den, mode="lines", line={"color": KIND_COLOR[kind], "dash": "dot"},
                                 name=f"placebo átlag: {kind}", showlegend=False))
    d = d0.filter(pl.col("kind") == "sign").sort("scale_min")
    den = d["plc_mean"].to_numpy() if ratio else np.ones(d.height)
    fig.add_trace(go.Scatter(x=[_scale_label(s) for s in d["scale_min"].to_list()], y=d["real"] / den, mode="lines+markers",
                             name="valós BTC", line={"color": theme.ACCENT, "width": 3}))
    fig.update_xaxes(categoryorder="array", categoryarray=xs, title="detektor-skála T")
    fig.update_yaxes(title=ytitle, type="log" if log_y else "linear")
    if ratio:
        fig.add_hline(y=1.0, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    return themed(fig, height=380)


def _alpha(hex_color: str, a: float) -> str:
    h = hex_color.lstrip("#")
    return f"rgba({int(h[0:2], 16)},{int(h[2:4], 16)},{int(h[4:6], 16)},{a})"


def fig_vr(summ: pl.DataFrame, p: mc.CensusParams) -> go.Figure:
    return fig_band(summ, "vr", 4.0, p, ytitle="σ_T / (σ_1m·√T)")


def build_report(real_cells: pl.DataFrame, plc: pl.DataFrame, p: mc.CensusParams, asset_id: str, *, write: bool = True
                 ) -> tuple[ReportBuilder, pl.DataFrame]:
    summ = pc.summarize(real_cells, plc)
    n_rep = int(plc.filter(pl.col("kind") == "sign")["rep"].n_unique())
    rb = ReportBuilder("general", SLUG, f"Placebo-null a nagy-mozgás katalógushoz — {asset_id.upper()} (epic_077 / t951)", order=76,
                       summary=f"Ugyanaz a detektor és rács {n_rep}+{n_rep} placebo-útvonalon: mennyi a katalógusból véletlenül is kijön.",
                       tags=["research", "scout2", "move_census", "placebo"], source={"kind": "analysis", "generator": pc.__name__})
    rb.add_markdown(f"""
## Cél

A t950 katalógus (27 cella, ~40 ezer nagy emelkedés) önmagában nem mond semmit, amíg nem tudjuk, **mennyi jön ki ugyanebből egy véletlen
bolyongásból, amelynek a volatilitása ugyanolyan**. Ugyanazt a detektort és rácsot lefuttattuk {n_rep}-{n_rep} placebo-útvonalon:

* **véletlen előjel:** a valós 1m hozamok abszolút értéke marad a helyén (a vol-klaszterezés, a napszaki mintázat és a tüskék megmaradnak),
  az előjel véletlen. Ez minden irány-függést (trend, visszafordulás) elvesz.
* **7 napos blokk-bootstrap:** a valós hozamok 7 napos, nap-igazított blokkjai visszatevéssel; a napon és héten belüli szerkezet (irány is)
  megmarad, ami hosszabb, az összekeveredik.

Minden realizáción ugyanaz az ablak ({mc.WINDOW_START[:10]} → {mc.WINDOW_END[:10]}) és ugyanaz a σ-normálás. A p-érték kétoldali, empirikus:
`(1 + #{{|placebo − átlag| ≥ |valós − átlag|}}) / (N + 1)`. A valós és a placebo azonos mintájú, ezért a kis p-ket nem szabad egyenként
értelmezni: 27 cella × 6 mutató = több száz teszt fut egyszerre.
""")
    rb.add_markdown("## Darabszám: valós / placebo\n\nAz 1,0 körüli érték azt jelenti, hogy a valós adatban pont annyi nagy emelkedés van, mint egy ugyanilyen volatilitású véletlen bolyongásban.")
    for k in (3.0, 4.0, 6.0):
        rb.add_chart(f"count_ratio_k{int(k)}", fig_band(summ, "n_big", k, p, ratio=True, ytitle="valós / placebo-átlag"),
                     title=f"Nagy emelkedések száma, valós / placebo-átlag (k = {k:g})", height=380)
    rb.add_markdown("## Mennyire trendes?\n\nA hatékonysági arány (net / út) és a hossz azt mutatja, mennyire egyenes egy nagy mozgás a véletlenhez képest.")
    rb.add_chart("er_k4", fig_band(summ, "er_med", 4.0, p, ytitle="medián hatékonysági arány"), title="Medián hatékonysági arány (k = 4)", height=380)
    rb.add_chart("dur_k4", fig_band(summ, "median_dur_min", 4.0, p, ratio=True, ytitle="valós / placebo-átlag"),
                 title="Medián hossz, valós / placebo-átlag (k = 4)", height=380)
    rb.add_chart("share_big_k4", fig_band(summ, "share_big", 4.0, p, ytitle="nagy / összes felfelé szakasz"),
                 title="A felfelé szakaszok mekkora része nagy (k = 4)", height=380)
    rb.add_markdown("## Skálázás: variancia-arány\n\nA σ_T / (σ_1m·√T) arány 1 a véletlen bolyongásnál; fölötte a mozgás trendesebb (momentum), alatta visszaforduló.")
    rb.add_chart("vr", fig_vr(summ, p), title="Variancia-arány a skála függvényében", height=380)
    rb.add_markdown("## Összesítő tábla\n\nMinden cella, mutató és placebo-fajta egy sor; `ratio` = valós / placebo-átlag.")
    rb.add_table("summary", summ.to_pandas().round(4), title="Valós vs placebo (minden cella és mutató)")
    rb.add_markdown("## Értelmezés\n\nLásd a zárójelentést (epic_077). Ez a riport a t951 placebo-mérése.")
    if write:
        rb.write()
    return rb, summ


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    cells = mc.load_table(a.asset_id, mc.TABLE_CELLS)
    plc = mc.load_table(a.asset_id, pc.TABLE_PLACEBO)
    rb, _ = build_report(cells, plc, mc.CensusParams(), a.asset_id)
    logger.info("report: %s", rb.output_dir)


if __name__ == "__main__":
    main()
