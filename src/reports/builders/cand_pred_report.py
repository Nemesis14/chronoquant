"""Report builder of epic_078 (t964): ``_doc_/reports/cand_pred/``.

``uv run python -m reports.builders.cand_pred_report --asset-id btcusdt``
"""

from __future__ import annotations

import argparse
import logging

import plotly.graph_objects as go
import polars as pl

from reports import theme
from reports.builder import ReportBuilder
from reports.builders import cand_pred_model as cm
from reports.builders import cand_pred_placebo as cpl
from reports.builders import cand_pred_timescale as cts
from reports.builders import move_census as mc
from reports.builders._common import themed

logger = logging.getLogger(__name__)
SLUG = "cand_pred"
GROUP_HU = {"z": "zigzag-geometria", "v": "volumen", "w": "VWAP", "f": "kereskedési adat", "q": "quant / klasszikus",
            "s": "alakok (Elliott-szerű)"}
GROUP_EX = {
    "z": "visszapattanás mérete a megerősítésig (σ-ban), korrekció mélysége, ciklusszám, időarányok (T egységben), sebességarány",
    "v": "a visszapattanás / korrekció / lökés átlagos volumene a 7 napos átlaghoz képest, volumen a mélypontnál",
    "w": "távolság a gördülő (1 nap, 7 nap) és a lehorgonyzott (előző csúcstól, mélypontól, előző mélypontól) VWAP-tól, σ-ban",
    "f": "taker-vétel arány (3 ablakon), átlagos ügyletméret, ügyletszám, funding (utolsó és 3 átlag)",
    "q": "1T/3T/10T/30T hozam σ-ban, helyzet a 10T/30T sávban, RSI_T, volatilitás-állapot, 1m RSI14 / Bollinger-pozíció / abszolút hozam",
    "s": "fib-távolságok (0,382 / 0,5 / 0,618), a visszapattanás és az esés aránya, az utolsó lökés a megelőzőhöz képest",
}
MODEL_HU = {"base": "alap (csak skála)", "only_z": "csak zigzag", "only_v": "csak volumen", "only_w": "csak VWAP",
            "only_f": "csak kereskedési adat", "only_q": "csak quant", "only_s": "csak alakok", "cum_z": "+ zigzag",
            "cum_v": "+ volumen", "cum_w": "+ VWAP", "cum_f": "+ kereskedési adat", "cum_q": "+ quant", "cum_s": "+ alakok (= mind)",
            "all": "mind (logisztikus)", "gbm_all": "mind (GBM)", "greedy": "forward-kiválasztás"}
MODEL_ORDER = ["base", "only_z", "only_v", "only_w", "only_f", "only_q", "only_s", "cum_z", "cum_v", "cum_w", "cum_f", "cum_q", "all",
               "gbm_all", "greedy"]
TARGET_HU = {"big": "nagy mozgás lesz (≥ 4σ_T)", "hit1": "+1R a stop előtt"}


def fig_timescale(ts: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    ys = [f"T = {int(s)} perc" for s in ts["scale_min"]]
    for name, lo, med, hi, color, dy in (
            ("mélypont → megerősítés (ekkor ismerjük a jelöltet)", "conf_after_low_min_p25", "conf_after_low_min_med", "conf_after_low_min_p75", theme.BLUE, 0.17),
            ("a teljes nagy mozgás hossza", "move_dur_min_p25", "move_dur_min_med", "move_dur_min_p75", theme.ORANGE, -0.17)):
        pos = [i + dy for i in range(ts.height)]
        fig.add_trace(go.Bar(y=pos, x=(ts[hi] - ts[lo]).to_list(), base=ts[lo].to_list(), orientation="h", width=0.3, marker_color=color,
                             opacity=0.45, name=name + " (p25–p75)", hoverinfo="skip"))
        fig.add_trace(go.Scatter(y=pos, x=ts[med].to_list(), mode="markers", marker={"symbol": "diamond", "size": 11, "color": color},
                                 name=name + " (medián)", showlegend=False,
                                 hovertemplate="medián %{x:.0f} perc<extra></extra>"))
    fig.update_xaxes(type="log", title="idő a mélyponttól (perc, log-skála)", tickvals=[15, 60, 240, 1440, 4320, 10080],
                     ticktext=["15 p", "1 óra", "4 óra", "1 nap", "3 nap", "7 nap"])
    fig.update_yaxes(tickvals=list(range(ts.height)), ticktext=ys, autorange="reversed")
    return themed(fig, height=360, barmode="overlay", hovermode="closest", legend={"orientation": "h", "y": -0.28})


def fig_auc(cmp: pl.DataFrame, met: pl.DataFrame, target: str) -> go.Figure:
    """Real AUC (95% day-cluster CI) vs the placebo mean and its 95% band, per model."""
    models = [m for m in MODEL_ORDER if m in set(met["model"].to_list())]
    r = met.filter(pl.col("target") == target)
    c = cmp.filter((pl.col("target") == target) & (pl.col("stat") == "auc"))
    ys = list(range(len(models)))
    real = [r.filter(pl.col("model") == m).row(0, named=True) for m in models]
    plc = [c.filter(pl.col("model") == m).row(0, named=True) for m in models]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[p["plc_mean"] for p in plc], y=[y + 0.17 for y in ys], mode="markers", name="placebo (véletlen előjel): átlag és 95%",
                             marker={"color": theme.ORANGE, "size": 8},
                             error_x={"type": "data", "symmetric": False, "array": [p["plc_p975"] - p["plc_mean"] for p in plc],
                                      "arrayminus": [p["plc_mean"] - p["plc_p025"] for p in plc], "color": theme.ORANGE, "thickness": 2}))
    fig.add_trace(go.Scatter(x=[q["auc"] for q in real], y=[y - 0.17 for y in ys], mode="markers", name="valós BTC: AUC és 95% CI",
                             marker={"color": theme.BLUE, "size": 9},
                             error_x={"type": "data", "symmetric": False, "array": [q["auc_hi"] - q["auc"] for q in real],
                                      "arrayminus": [q["auc"] - q["auc_lo"] for q in real], "color": theme.BLUE, "thickness": 2}))
    fig.add_vline(x=0.5, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    fig.update_yaxes(tickvals=ys, ticktext=[MODEL_HU[m] for m in models], autorange="reversed")
    fig.update_xaxes(title="AUC (0,5 = véletlen rangsor)", range=[0.44, 0.58])
    return themed(fig, height=520, hovermode="closest", legend={"orientation": "h", "y": -0.16})


def fig_univar(cu: pl.DataFrame, target: str, n: int = 14) -> go.Figure:
    d = cu.filter(pl.col("target") == target).with_columns((pl.col("real_auc") - 0.5).abs().alias("dev")).sort("dev", descending=True).head(n)
    names = d["feature"].to_list()
    ys = list(range(len(names)))
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d["plc_mean"].to_list(), y=[y + 0.17 for y in ys], mode="markers", name="placebo: átlag és 95%",
                             marker={"color": theme.ORANGE, "size": 8},
                             error_x={"type": "data", "symmetric": False, "array": (d["plc_p975"] - d["plc_mean"]).to_list(),
                                      "arrayminus": (d["plc_mean"] - d["plc_p025"]).to_list(), "color": theme.ORANGE, "thickness": 2}))
    fig.add_trace(go.Scatter(x=d["real_auc"].to_list(), y=[y - 0.17 for y in ys], mode="markers", name="valós BTC",
                             marker={"color": theme.BLUE, "size": 9}, text=[f"p = {p:.3f}" for p in d["p_two"]],
                             hovertemplate="%{x:.3f} (%{text})<extra></extra>"))
    fig.add_vline(x=0.5, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    fig.update_yaxes(tickvals=ys, ticktext=names, autorange="reversed")
    fig.update_xaxes(title="egyváltozós AUC (a nyers érték mint pontszám)", range=[0.44, 0.58])
    return themed(fig, height=480, hovermode="closest", legend={"orientation": "h", "y": -0.16})


def groups_table(fcols: dict[str, list[str]]) -> pl.DataFrame:
    return pl.DataFrame([{"csoport": f"{g} — {GROUP_HU[g]}", "változók száma": len(fcols[g]), "példák": GROUP_EX[g]} for g in cm.ORDER])


def build_report(asset_id: str, *, write: bool = True) -> ReportBuilder:
    ts = mc.load_table(asset_id, cts.TABLE)
    met = mc.load_table(asset_id, cm.TABLE_METRICS)
    cmp = mc.load_table(asset_id, cpl.TABLE_COMPARE)
    cu = mc.load_table(asset_id, cpl.TABLE_COMPARE_UNI)
    sel = mc.load_table(asset_id, cm.TABLE_SELECTED)
    feats = mc.load_table(asset_id, "cand_pred_v1_features")
    fcols = cm.feature_columns(feats)
    n_rep = int(cmp["n_rep"].to_numpy().max())
    rb = ReportBuilder("general", SLUG, f"Előrejelezhető-e a nagy lökés az első szakaszból? — {asset_id.upper()} (epic_078)", order=79,
                       summary="Hat változócsoport forward, idő szerinti próbája a valós időben felismert jelölteken, placebóval.",
                       tags=["research", "scout2", "move_census", "predictability"], source={"kind": "analysis", "generator": cm.__name__})
    rb.add_markdown(f"""
## Cél

Az epic_077 szerint az ár geometriája önmagában nem ad a véletlennél jobb folytatás-becslést. Itt **magyarázó változókat** adunk a valós időben felismerhető
jelöltekhez (megerősített mélypont, k = 4, T = 60 / 240 / 960 perc), és megmérjük, hogy egy csoport **idő szerinti** (out-of-time) előrejelzése jobb-e a véletlennél.

* **Célok:** (1) a jelöltből nagy mozgás lesz (≥ 4σ_T), (2) +1R a strukturális stop (a mélypont) előtt.
* **Mérce:** AUC és lift a legjobb tizedben; a véletlen: az alap (csak skála-dummy) és a véletlen előjelű placebo (**{n_rep}** realizáció, a teljes lánccal újrafuttatva).
* **Validáció:** expanding-window éves fold-ok, teszt 2023 / 2024 / 2025; a tanítás csak azokat a jelölteket látja, amelyek címkéje a tesztév előtt ismert (purge). Az OOS (2026-02-től) érintetlen.
* **Modell:** L2 logisztikus regresszió (C = 0,1, fix), winsorizált + standardizált változók; a GBM csak összehasonlítás. A „forward-kiválasztás" belső idő-CV-n választ csoportot, a külső tesztévet nem látja.
""")
    rb.add_markdown("## Mekkora időtávon élnek a modellek?\n\nA jelöltet a mélypont után kb. egy skálányi idővel ismerjük meg (a megerősítés pillanata), a teljes nagy mozgás kb. 5 skálányi.")
    rb.add_chart("timescale", fig_timescale(ts), title="Mélypont → megerősítés és a teljes nagy mozgás hossza (k = 4)", height=360)
    show = ts.select(["scale_min", "sigma_T_pct", "range_T_pct_mean", "n_candidates", "p_big", "conf_after_low_min_med", "move_dur_min_med",
                      "move_dur_min_p25", "move_dur_min_p75", "move_net_pct_med", "first_dur_min_med", "first_net_pct_med"]).rename({
        "scale_min": "T (perc)", "sigma_T_pct": "σ_T (%)", "range_T_pct_mean": "átl. T-perces range (%)", "n_candidates": "jelölt",
        "p_big": "P(nagy)", "conf_after_low_min_med": "megerősítés (perc, medián)", "move_dur_min_med": "nagy mozgás hossza (perc, medián)",
        "move_dur_min_p25": "p25", "move_dur_min_p75": "p75", "move_net_pct_med": "nagy mozgás nettó (%, medián)",
        "first_dur_min_med": "első lökés (c=0,33, perc)", "first_net_pct_med": "első lökés (%, medián)"})
    rb.add_table("timescale_table", show.to_pandas().round(3), title="Időtáv skálánként (k = 4)")
    rb.add_markdown("## Mit próbáltunk?\n\nHat csoport, összesen " + str(sum(len(v) for v in fcols.values())) + " változó, mind a megerősítő bar zárásáig ismert adatból (look-ahead teszt: a levágott adaton azonos).")
    rb.add_table("groups", groups_table(fcols).to_pandas(), title="Változócsoportok")
    for t in cm.TARGETS:
        rb.add_markdown(f"## Eredmény — cél: {TARGET_HU[t]}\n\nPont = valós AUC (95% nap-klaszteres CI), narancs = a placebo átlaga és 95%-os sávja.")
        rb.add_chart(f"auc_{t}", fig_auc(cmp, met, t), title=f"AUC modellenként — {TARGET_HU[t]}", height=520)
    rb.add_markdown("## Melyik változó?\n\nEgyváltozós AUC a 2023–25-ös tesztidőszakon (a nyers változó mint pontszám), a placebo-sávval; a 14 legnagyobb eltérés.")
    for t in cm.TARGETS:
        rb.add_chart(f"univar_{t}", fig_univar(cu, t), title=f"Egyváltozós AUC — {TARGET_HU[t]}", height=480)
    rb.add_table("metrics", met.to_pandas().round(4), title="Valós metrikák (AUC, lift, log-loss skill, CI-k)")
    rb.add_table("compare", cmp.to_pandas().round(4), title="Valós vs placebo (AUC, ΔAUC, lift)")
    rb.add_table("compare_univar", cu.to_pandas().round(4), title="Egyváltozós AUC: valós vs placebo")
    rb.add_table("selected", sel.to_pandas(), title="A forward-kiválasztás által választott csoportok évenként")
    rb.add_markdown("""
## Értelmezés

* A valós modellek AUC-ja a „nagy mozgás" célnál ~0,52, de a placebón (ahol nincs irány-információ) ugyanennyi: a zigzag-definíció mechanikus hatása
  (minél nagyobb a megerősítésig megtett visszapattanás, annál közelebb a 4σ cél). A forward-kiválasztás a placebón is szinte mindig a zigzag-csoportot választja.
* A +1R célnál minden modell a placebo sávjában van (≈ 0,50).
* Egyetlen enyhén pozitív jel: a VWAP-csoport (a mélyponttól / csúcstól lehorgonyzott VWAP-távolság), p ≈ 0,03–0,05, de 16 modell és 48 változó mellett ez a véletlen szintje, és a +1R célon nem jön elő.
* A nem-kimerítő keret miatt a negatív eredmény csak erre a változókészletre, BTC-re, long irányra és 1 perces információra érvényes.
""")
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
