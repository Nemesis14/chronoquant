# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Leaderboard report of the Impulse Scout benchmark (``_doc_/reports/scout_leaderboard/``, report format).

``python -m reports.builders.scout_bench.report`` runs the registered models on the available event frame (SOL
``wave_events_v2`` adapter until t930's ``scout_events_v1`` exists), persists ``research."scout_leaderboard_v1"`` and writes the
report. t932-t936 register their models in :data:`scout_bench.REGISTRY` and call :func:`build`.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from reports import ReportBuilder, theme
from reports.builders import scout_bench as sb
from reports.builders import wave_events as we
from reports.builders._common import fmt, source_info, themed
from reports.builders.scout_bench.runner import BenchResult

logger = logging.getLogger(__name__)

SLUG = "scout_leaderboard"
GENERATOR = "reports.builders.scout_bench.report"
T920_AUC_A = 0.6647            # epic_072 t920: SOL W4h_H8h, model A (S0 + C, C = 0.1), pooled OOF AUC
REGRESSION_TOL = 5e-4


def regression_check(res: BenchResult) -> dict[str, Any]:
    """Baseline reproduction against the t920 SOL number (only meaningful for a SOL-only frame)."""
    row = res.leaderboard[res.leaderboard["is_baseline"]].iloc[0]
    single_sol = res.run["assets"] == "solusdt"
    return {"auc": float(row["auc"]), "reference": T920_AUC_A, "applies": single_sol,
            "ok": bool(single_sol and abs(float(row["auc"]) - T920_AUC_A) <= REGRESSION_TOL)}


def _reliability_fig(res: BenchResult, name: str) -> go.Figure:
    tab = res.tables[f"calibration:{name}"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[0, float(max(tab["mean_pred"].max(), tab["obs_rate"].max())) * 1.1], y=[0, float(max(tab["mean_pred"].max(), tab["obs_rate"].max())) * 1.1],
                             mode="lines", line={"color": theme.NEUTRAL, "dash": "dash"}, name="ideális"))
    fig.add_trace(go.Scatter(x=tab["mean_pred"], y=tab["obs_rate"], mode="lines+markers", marker={"color": theme.ACCENT},
                             name=name, hovertemplate="becsült %{x:.3f} → megfigyelt %{y:.3f}<extra></extra>"))
    fig.update_xaxes(title_text="átlagos becsült P(impulzus)")
    fig.update_yaxes(title_text="megfigyelt impulzus-arány")
    return themed(fig, height=340)


def _block_fig(res: BenchResult, name: str) -> go.Figure:
    t = res.tables[f"blocks:{name}"]
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Blokkonkénti AUC", "Blokkonkénti ΔAUC a baseline-hoz"), horizontal_spacing=0.1)
    x = [f"B{b}" for b in t["block"]]
    fig.add_trace(go.Bar(x=x, y=t["auc_model"], name=name, marker={"color": theme.ORANGE}), row=1, col=1)
    fig.add_trace(go.Bar(x=x, y=t["auc_base"], name="baseline", marker={"color": theme.BLUE}), row=1, col=1)
    fig.add_hline(y=0.5, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1}, row=1, col=1)
    fig.add_trace(go.Bar(x=x, y=t["delta_auc"], marker={"color": [theme.LONG if v > 0 else theme.SHORT for v in t["delta_auc"]]},
                         showlegend=False), row=1, col=2)
    fig.update_yaxes(range=[0.3, 0.9], col=1)
    fig.update_layout(barmode="group")
    fig.update_annotations(font_size=11)
    return themed(fig, height=340)


def _null_fig(res: BenchResult) -> go.Figure | None:
    null = res.perm_null.get("_auc_base")
    if null is None or not null.size:
        return None
    obs = float(res.leaderboard[res.leaderboard["is_baseline"]]["auc"].iloc[0])
    fig = go.Figure(go.Histogram(x=null, nbinsx=50, marker={"color": theme.NEUTRAL}, name="permutációs null"))
    fig.add_vline(x=obs, line={"color": theme.ACCENT, "width": 2}, annotation_text=f"megfigyelt {obs:.4f}",
                  annotation_position="top right")
    fig.update_xaxes(title_text="baseline összevont OOF AUC a nap-csomagosan permutált címkéken")
    fig.update_yaxes(title_text="permutációk száma")
    return themed(fig, height=320, showlegend=False)


def _lb_table(lb: pd.DataFrame) -> pd.DataFrame:
    cols = ["model", "family", "hypothesis", "round", "auc", "delta_auc", "ci_lo", "ci_hi", "logloss3", "delta_logloss3",
            "prec_top20", "lift_top20", "prec_top10", "lift_top10", "blocks_prec_better", "prec_top20_trthr", "n_top20_trthr",
            "prec_top10_trthr", "n_top10_trthr", "blocks_pos", "dauc_wo_best_block_mean", "cal_slope",
            "perm_p", "holm_p", "spa_p", "go_status", "go_failed"]
    return lb[[c for c in cols if c in lb.columns]].assign(hypothesis=lambda d: d["hypothesis"].replace("", "kísérleti"))


LB_COLUMNS: dict[str, Any] = {
    "model": "Modell", "family": "Család", "hypothesis": "Státusz", "round": "Forduló",
    "auc": {"label": "AUC", "digits": 4}, "delta_auc": {"label": "ΔAUC", "digits": 4},
    "ci_lo": {"label": "CI alsó", "digits": 4}, "ci_hi": {"label": "CI felső", "digits": 4},
    "logloss3": {"label": "3-oszt. LL", "digits": 4}, "delta_logloss3": {"label": "ΔLL", "digits": 4},
    "prec_top20": {"label": "Prec. felső 20% (fold-rang)", "digits": 3}, "lift_top20": {"label": "Lift 20%", "digits": 2},
    "prec_top10": {"label": "Prec. felső 10% (fold-rang)", "digits": 3}, "lift_top10": {"label": "Lift 10%", "digits": 2},
    "blocks_prec_better": "Top20 jobb blokk",
    "prec_top20_trthr": {"label": "Prec. 20% (train-küszöb)", "digits": 3}, "n_top20_trthr": "n 20% (train-küszöb)",
    "prec_top10_trthr": {"label": "Prec. 10% (train-küszöb)", "digits": 3}, "n_top10_trthr": "n 10% (train-küszöb)",
    "blocks_pos": "ΔAUC>0 blokk", "dauc_wo_best_block_mean": {"label": "ΔAUC legjobb blokk nélkül", "digits": 4},
    "cal_slope": {"label": "Kal. meredekség", "digits": 2}, "perm_p": {"label": "perm. p", "digits": 4},
    "holm_p": {"label": "Holm p", "digits": 4}, "spa_p": {"label": "SPA p", "digits": 4},
    "go_status": "Go / no-go", "go_failed": "Bukott feltételek",
}


def _trade_table(res: BenchResult) -> pd.DataFrame:
    rows = []
    for _, r in res.leaderboard.iterrows():
        for q, label in (("20", "felső 20%"), ("10", "felső 10%"), ("20t", "felső 20% (train-küszöb)"),
                         ("10t", "felső 10% (train-küszöb)"), ("all", "minden esemény")):
            if f"trade{q}_n" not in r:
                continue
            rows.append({"model": r["model"], "signal": label, "n": r[f"trade{q}_n"], "hit": r[f"trade{q}_hit_rate"],
                         "stop": r[f"trade{q}_stop_rate"], "timeout": r[f"trade{q}_timeout_rate"],
                         "be_hit": r[f"trade{q}_break_even_hit"], "net_bps": r[f"trade{q}_mean_net_bps"],
                         "ci_lo": r[f"trade{q}_ci_lo_bps"], "ci_hi": r[f"trade{q}_ci_hi_bps"],
                         "net_sigma": r[f"trade{q}_mean_net_sigma"], "pf": r[f"trade{q}_profit_factor"],
                         "dd": r[f"trade{q}_max_dd_bps"]})
    return pd.DataFrame(rows)


def build(res: BenchResult, root: Path | None = None, *, history: pd.DataFrame | None = None, costs_note: str = "") -> Path:
    """Write the ``scout_leaderboard`` report of one benchmark run."""
    lb = res.leaderboard
    run = res.run
    base = lb[lb["is_baseline"]].iloc[0]
    name = str(base["model"])
    reg = regression_check(res)
    fr = res.frame
    n, n_pos = fr.height, int(run["n_impulse"])
    n_assets = len(run["assets"].split(","))
    rb = ReportBuilder(
        scope="general", slug=SLUG, order=33, root=root,
        title=f"Impulse Scout leaderboard — közös benchmark keret ({run['cell_id']}, train, purged CV)",
        summary=f"5300 / epic_073 t931: {len(lb)} modell a közös 6 blokkos purged CV-n ({n} esemény, {run['assets']}); baseline S0 + C "
                f"AUC {base['auc']:.4f}; permutáció, SPA, triple-barrier szimuláció a konfigurált költségekkel.",
        tags=["scout", "epic_073", "leaderboard", "benchmark"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél\n\n"
        "**t931 (epic_073):** az Impulse Scout modellcsalád közös, újrahasznosítható benchmark kerete a "
        "`_doc_/methodology_doc/5300_impulse_scout.md` szerint (kód: `src/reports/builders/scout_bench/`, referencia: "
        "`_doc_/database_and_code_doc/8330_reports.md`). Egyetlen protokoll minden modellre: azonos foldok, azonos metrikák, azonos "
        "inferencia (nap-klaszter bootstrap, nap-csomagos permutáció, SPA), azonos kereskedési szimuláció. A modellek (t932-t935) "
        "ugyanebbe a táblába regisztrálnak; a leaderboard **minden kipróbált változatot** tartalmaz iteráció-sorszámmal.\n\n"
        f"Ez a futás (`{run['run_id']}`): **{n} CV-ben részt vevő train esemény**, ebből **{n_pos} impulzus** "
        f"({100 * n_pos / n:.1f}%), {n_assets} asset ({run['assets']}); a t930 multi-asset tábla (`research.\"scout_events_v1\"`) még "
        "nem létezik, a keretet a SOL `research.\"wave_events_v2\"` W4h_H8h cellán, adapteren keresztül fejlesztettük (a t930 tábla "
        "ugyanazzal az oszlop-szerződéssel beilleszthető).\n\n"
        "> **Holdout-védelem:** csak a train split vesz részt (`guard_no_holdout`), az OOS és a holdout-2 sorok nem kerültek a "
        "memóriába; az 1m árak a train végén levágottak; a szimuláció a CV-eseményeken fut.")
    rb.add_markdown(
        "## Protokoll\n\n"
        f"- **Foldok:** {len(res.folds)} közös naptári blokk (`cv_block_bounds`, egész napos határok), embargó **H = {res.folds.embargo_min // 60} h**, "
        "teljes `[t0 − B, t_c + H]` purge; azonos foldok minden modellnek, fold-hash: "
        f"`{run['fold_hash']}`, adat-hash: `{run['data_hash']}`.\n"
        "- **Baseline:** skalár logit S0 + C (a_sigma, velocity_1h, pretrend_3d, log_sigma_1m, drop_depth, volume_ratio), "
        "fold-train szerint standardizálva, C = 0,1, belső hangolás nélkül (t920 Modell A). Háromosztályos valószínűség: P(impulzus) + a "
        "fold-train P(hamis indulás | nem impulzus) állandó aránya, így a bináris score érintetlen.\n"
        "- **Felső-q% (elsődleges nézet, epic döntés 2026-11-21):** a teszt-foldon belüli rangsor, a fold eseményeinek felső "
        "20% / 10%-a (címke nélkül, foldonként azonos jelzés-arány), a foldokon összevonva; a go/no-go és a szimuláció ezt "
        "használja. **Másodlagos nézet:** a fold-train score 80. / 90. percentilise mint küszöb (`_trthr` oszlopok, `n_top`-pal).\n"
        f"- **Bootstrap:** nap-klaszter (UTC nap, minden asset együtt), N = {run['n_boot']}; **permutáció:** nap-csomagos, N = {run['n_perm']}, "
        f"teljes refit; **SPA:** Hansen, stacionárius bootstrap (várható blokk {run['spa_mean_block']:g} nap), {run['spa_boot']} újramintavétel, "
        "a teljes modell-univerzumra.\n"
        "- **Szimuláció:** hosszú belépő a `t_c` utáni első 1m bar nyitóján (taker + csúszás), cél `P*` (maker), stop `low[t0]` (taker + csúszás), "
        "időkorlát `t_c + H` (taker + csúszás), same-bar = stop. Költségek: `config/strategies.json` `costs` "
        f"(`{run['costs_json']}`). Finanszírozási díj nincs modellezve. {costs_note}")

    # 1. leaderboard
    rb.add_markdown("## 1. Leaderboard\n\n"
                    "A ΔAUC és CI a baseline-hoz viszonyított, összevont OOF mintán (nap-klaszter bootstrap). A `perm. p` a baseline soránál a "
                    "baseline **AUC-jának** nap-csomagos permutációs p-értéke (a jel valódisága), az elsődleges modelleknél a ΔAUC-é "
                    "(Holm a H1-H3 családon). A `SPA p` a teljes modell-univerzum legjobb tagjára vonatkozik.")
    rb.add_table("leaderboard", _lb_table(lb), title="Leaderboard (a legutóbbi futás)", columns=LB_COLUMNS)
    nul = _null_fig(res)
    p_base = float(base["perm_p"])
    if nul is not None:
        rb.add_markdown(f"A baseline AUC-ja **{base['auc']:.4f}**, a nap-csomagos permutációs null mellett **p = {p_base:.4f}** "
                        f"(N = {run['n_perm']}, a legkisebb elérhető érték {1 / (run['n_perm'] + 1):.4f}).")
        rb.add_chart("null_baseline", nul, title="Baseline AUC a nap-csomagosan permutált címkéken", height=320)

    # 2. regression
    rb.add_markdown(
        "## 2. Baseline-reprodukció (regressziós ellenőrzés)\n\n"
        + (f"A harness skalár logitja a t920 Modell A számát reprodukálja: összevont OOF AUC **{reg['auc']:.4f}** "
           f"(t920 referencia {reg['reference']:.4f}, tűrés ±{REGRESSION_TOL:g}) → **{'rendben' if reg['ok'] else 'ELTÉRÉS'}**."
           if reg["applies"] else "A t920 referencia (SOL) csak egy-asset SOL kerethez értelmezhető; ez a futás többasszetes."))
    # 3. baseline details
    rb.add_markdown(
        "## 3. Blokk-, asset- és kalibráció-bontás (baseline)\n\n"
        f"Blokkonként {int(res.tables[f'blocks:{name}']['n'].min())}-{int(res.tables[f'blocks:{name}']['n'].max())} teszt-esemény; a ΔAUC "
        "a baseline-nál definíció szerint 0 (a bontás a modellek közös szerkezete: a 4 / 6 blokk-előjel és a legjobb-blokk-nélküli "
        "feltétel ezekből számolódik).")
    rb.add_chart("blocks", _block_fig(res, name), title="Blokkonkénti AUC (baseline)", height=340)
    rb.add_table("blocks_table", res.tables[f"blocks:{name}"], title="Blokk-táblázat (baseline)",
                 columns={"block": "Blokk", "n": "Esemény", "n_impulse": "Impulzus", "auc_model": {"label": "AUC", "digits": 4},
                          "auc_base": {"label": "AUC baseline", "digits": 4}, "delta_auc": {"label": "ΔAUC", "digits": 4}})
    rb.add_table("assets_table", res.tables[f"assets:{name}"], title="Asset-bontás (baseline)",
                 columns={"asset_id": "Asset", "n": "Esemény", "n_impulse": "Impulzus", "auc_model": {"label": "AUC", "digits": 4},
                          "auc_base": {"label": "AUC baseline", "digits": 4}, "delta_auc": {"label": "ΔAUC", "digits": 4}})
    cal_slope = float(base["cal_slope"])
    rb.add_markdown(f"Kalibráció: meredekség **{cal_slope:.2f}**, tengelymetszet {float(base['cal_intercept']):.2f}, Brier {float(base['brier']):.4f} "
                    f"(a 0,7-1,3 sávhoz képest {'sávon belül' if 0.7 <= cal_slope <= 1.3 else 'sávon kívül'}).")
    rb.add_chart("reliability", _reliability_fig(res, name), title="Megbízhatósági görbe (baseline, 5 kvantilis-sáv)", height=340)
    rb.add_table("calibration_asset", res.tables[f"calibration_asset:{name}"], title="Kalibráció assetenként",
                 columns={"asset_id": "Asset", "n": "Esemény", "slope": {"label": "Meredekség", "digits": 2},
                          "intercept": {"label": "Tengelymetszet", "digits": 2}, "mean_pred": {"label": "Átl. becslés", "digits": 3},
                          "obs_rate": {"label": "Megfigyelt", "digits": 3}})

    # 4. trades
    tt = _trade_table(res)
    rb.add_markdown(
        "## 4. Triple-barrier szimuláció (díjak után)\n\n"
        "A jelzések a modell score-ján a teszt-foldon belüli rangsor szerint (elsődleges); a `train-küszöb` sorok a másodlagos "
        "nézet; a `minden esemény` sor a szelekció nélküli referencia. "
        "A CI nap-klaszter bootstrap (bázispont / esemény). A break-even hit-arány az átlagos nyerő (cél) és vesztő (stop, időkorlát) "
        "nettó hozamból következik.")
    rb.add_table("trades", tt, title="Szimuláció: jelzés-szám, hit / stop / időkorlát arány, nettó hozam",
                 columns={"model": "Modell", "signal": "Jelzés", "n": "Esemény", "hit": {"label": "Cél", "digits": 3},
                          "stop": {"label": "Stop", "digits": 3}, "timeout": {"label": "Időkorlát", "digits": 3},
                          "be_hit": {"label": "Break-even hit", "digits": 3}, "net_bps": {"label": "Nettó bp / esemény", "digits": 1},
                          "ci_lo": {"label": "CI alsó", "digits": 1}, "ci_hi": {"label": "CI felső", "digits": 1},
                          "net_sigma": {"label": "Nettó σ_W", "digits": 3}, "pf": {"label": "Profit faktor", "digits": 2},
                          "dd": {"label": "Max DD bp", "digits": 0}})
    if history is not None and len(history):
        history = history.astype(object).where(history.notna(), None)
        rb.add_table("runs", history, title="Futások a lab DB `research.\"scout_leaderboard_v1__runs\"` táblájában")

    # interpretation
    b20 = tt[(tt["model"] == name) & (tt["signal"] == "felső 20%")]
    b10 = tt[(tt["model"] == name) & (tt["signal"] == "felső 10%")]
    ball = tt[(tt["model"] == name) & (tt["signal"] == "minden esemény")]

    def cell(df: pd.DataFrame) -> str:
        if df.empty:
            return "nincs szimuláció"
        r = df.iloc[0]
        return (f"{r['net_bps']:+.1f} bp / esemény, CI [{r['ci_lo']:+.1f}; {r['ci_hi']:+.1f}], hit {100 * r['hit']:.1f}% "
                f"(break-even {100 * r['be_hit']:.1f}%), n = {int(r['n'])}")
    sel_note = ""
    if not b20.empty and not ball.empty:
        d20 = float(b20.iloc[0]["net_bps"]) - float(ball.iloc[0]["net_bps"])
        sel_note = (f"A felső 20% szelekció a szelekció nélküli referenciához képest {d20:+.1f} bp / esemény különbséget ad: "
                    + ("a rangsoroló erő (AUC) ebben a szimulációban nem jelenik meg többlet nettó hozamként." if d20 <= 0
                       else "a rangsoroló erő többlet nettó hozamként is látszik (a CI-k átfedése miatt bizonytalanul)."))
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"- **Keret:** a baseline a t920 számát reprodukálja ({'igen' if reg['ok'] else 'nem'}), a foldok minden modellnek azonosak "
        f"(hash `{run['fold_hash']}`), a metrikák, a permutáció és az SPA gépe tesztelt; a modellek (t932-t935) a `REGISTRY`-be "
        "regisztrálva ugyanezt a protokollt kapják.\n"
        f"- **Baseline jel:** AUC {base['auc']:.4f}, permutációs p = {fmt(base['perm_p'])}; felső 20% precizitás {fmt(base['prec_top20'], 3)} "
        f"(lift {fmt(base['lift_top20'], 2)}), felső 10% {fmt(base['prec_top10'], 3)} (lift {fmt(base['lift_top10'], 2)}).\n"
        f"- **Gazdasági előszűrő (baseline):** felső 20%: {cell(b20)}; felső 10%: {cell(b10)}; szelekció nélkül: {cell(ball)}. "
        "A CI alsó határa a nullát tartalmazza, ha negatív: ekkor a CV-szimuláció nem igazol díjak utáni élt (és az előszűrő a "
        "kiválasztási torzítás miatt amúgy is csak előszűrő; a döntő a holdout-2). " + sel_note + "\n"
        "- **Korlátok:** a szimuláció nem modellez finanszírozási díjat; egy asseten a nap-klaszter a sima nap-bootstrap; a "
        "BTC / ETH és az order flow t930 / t937 után kerül be; egy modell csak ebben a keretben, regisztrálva hasonlítható.")
    return rb.write()


def build_safe(res: BenchResult, root: Path | None = None, **kw: Any) -> Path:
    """Build the report; on a Windows folder-replace error (``WinError 5``) build in a temp dir and copy over."""
    try:
        return build(res, root, **kw)
    except PermissionError:
        tmp = Path(tempfile.mkdtemp(prefix="scout_report_"))
        out = build(res, tmp, **kw)
        target = (root or _default_root()) / SLUG
        target.mkdir(parents=True, exist_ok=True)
        shutil.copytree(out, target, dirs_exist_ok=True)
        return target


def _default_root() -> Path:
    return Path(__file__).resolve().parents[4] / "_doc_" / "reports"


def load_runs(asset_id: str | None = None) -> pd.DataFrame | None:
    """Run metadata rows from the lab DB (read-only), newest first; ``None`` when the table does not exist."""
    import duckdb

    import utils

    try:
        con = duckdb.connect(utils.lab_db_path(asset_id), read_only=True)
    except duckdb.Error:
        return None
    try:
        return con.execute(f'SELECT run_id, created_at, n_events, n_impulse, assets, n_perm, spa_p, fold_hash, note FROM '
                           f'research."{sb.runner.RUNS_TABLE}" ORDER BY created_at DESC').df()
    except duckdb.Error:
        return None
    finally:
        con.close()


def run_default(*, n_perm: int = 1000, n_boot: int = 2000, spa_boot: int = 5000, persist_lab: bool = True,
                root: Path | None = None, asset_id: str | None = None, note: str = "") -> tuple[BenchResult, Path]:
    """Load the SOL adapter frame + 1m bars (train period only), run the REGISTRY, persist and write the report."""
    from reports.builders.sr_detection import load_ohlcv_1m

    p = we.params_v2()
    frame = sb.load_wave_events_v2(asset_id)
    df = load_ohlcv_1m(asset_id)
    if df is None:
        raise RuntimeError("live db unavailable")
    df = df.filter(pl.col("open_time") < datetime.fromisoformat(p.oos_start))     # nothing from OOS / holdout-2 enters memory
    from reports.builders.wave_oos_eval import assert_no_holdout

    assert_no_holdout(df, sb.frame.HOLDOUT2_START)
    bars = {frame["asset_id"][0]: we.to_minute_grid(df)}
    res = sb.run_benchmark(frame, sb.REGISTRY, bars, n_perm=n_perm, n_boot=n_boot, spa_boot=spa_boot, note=note)
    hist = None
    if persist_lab:
        meta = sb.persist(res, asset_id)
        logger.info("persisted %s", meta)
        hist = load_runs(asset_id)
    return res, build_safe(res, root, history=hist)


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or SLUG).split("\n")[0])
    ap.add_argument("--asset-id", default=None)
    ap.add_argument("--n-perm", type=int, default=1000)
    ap.add_argument("--no-persist", action="store_true")
    ap.add_argument("--root", default=None, help="report root (default: _doc_/reports)")
    ap.add_argument("--note", default="")
    a = ap.parse_args()
    _, out = run_default(n_perm=a.n_perm, persist_lab=not a.no_persist, root=Path(a.root) if a.root else None,
                         asset_id=a.asset_id, note=a.note)
    print(out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
