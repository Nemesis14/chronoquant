# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Report of Impulse Scout model 3 (epic_073 / t934): discrete-time competing-risks hazard (``_doc_/reports/scout_hazard/``).

``python -m reports.builders.scout_hazard_report [--n-perm 1000]`` loads the t930 train events (+ flow subset) and the
train-period 1m bars, runs the shared ``scout_bench`` protocol on baseline + ``scout_hazard_r1`` (H3), persists ONLY the
hazard row (``scout_models_hazard.persist_model_rows``) and writes the report: leaderboard comparison, cumulative incidence
curves by predicted-score quintile (OOF, observed vs predicted = time-to-hit calibration), hazard shape, coefficients,
block / asset breakdown and the trade simulation.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from reports import ReportBuilder, theme
from reports.builders import scout_bench as sb
from reports.builders import scout_models_hazard as hz
from reports.builders._common import fmt, source_info, themed

logger = logging.getLogger(__name__)

SLUG = "scout_hazard"
GENERATOR = "reports.builders.scout_hazard_report"
QUINTILE_COLORS = [theme.NEUTRAL, theme.BLUE, theme.AMBER, theme.ORANGE, theme.SHORT]


# --------------------------------------------------------------------------- OOF curves


def oof_curves(frame: pl.DataFrame, folds: sb.FoldSet, *, seed: int = 20260101) -> dict[str, Any]:
    """OOF cumulative incidence curves ``(n, n_bins, 2)`` and hazards ``(n, n_bins, 3)`` on the shared folds (same fits
    as the harness: deterministic model, same fold training sets) + a descriptive full-train fit (coefficients)."""
    n = frame.height
    cif = np.full((n, hz.N_BINS, 2), np.nan)
    haz = np.full((n, hz.N_BINS, 3), np.nan)
    n_pp = []
    for f in folds:
        m = hz.HazardModel()
        m.fit(frame[f.train_idx], None, seed=seed)
        h = m.hazards(frame[f.test_idx])
        haz[f.test_idx] = h
        cif[f.test_idx] = hz.cif_from_hazards(h)
        n_pp.append({"block": f.block + 1, "train_events": int(f.train_idx.size), "person_period_rows": m.n_pp_rows,
                     "test_events": int(f.test_idx.size)})
    full = hz.HazardModel()
    full.fit(frame, None, seed=seed)
    return {"cif": cif, "haz": haz, "pp": pd.DataFrame(n_pp), "full": full}


def observed_cif(label_class: np.ndarray, tau_min: np.ndarray, n_bins: int = hz.N_BINS) -> np.ndarray:
    """Empirical CIF at bin ends ``(n_bins, 2)``. Censoring happens only at ``H``, so the share of events with the cause
    and ``tau`` in ``(0, t_j]`` is exactly the Aalen-Johansen estimate."""
    j = hz.time_bin(tau_min)
    out = np.zeros((n_bins, 2))
    for k, c in enumerate(("impulse", "false_start")):
        hit = label_class == c
        for b in range(n_bins):
            out[b, k] = float((hit & (j <= b)).mean()) if label_class.size else float("nan")
    return out


def quintile_curves(frame: pl.DataFrame, score: np.ndarray, cif: np.ndarray) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(long curve table, summary table) of observed vs mean predicted CIF per pooled OOF score quintile."""
    cls = frame["label_class"].to_numpy()
    tau = frame["tau_min"].to_numpy()
    q = pd.qcut(pd.Series(score).rank(method="first"), 5, labels=False).to_numpy() + 1
    rows, summ = [], []
    t_end = (np.arange(hz.N_BINS) + 1) * hz.BIN_MIN
    for k in range(1, 6):
        sel = q == k
        obs = observed_cif(cls[sel], tau[sel])
        pred = cif[sel].mean(axis=0)
        for b in range(hz.N_BINS):
            rows.append({"quintile": k, "t_min": int(t_end[b]), "obs_imp": obs[b, 0], "pred_imp": pred[b, 0],
                         "obs_fs": obs[b, 1], "pred_fs": pred[b, 1]})
        imp_tau = tau[sel & (cls == "impulse")]
        summ.append({"quintile": k, "n": int(sel.sum()), "score_lo": float(score[sel].min()), "score_hi": float(score[sel].max()),
                     "obs_imp": obs[-1, 0], "pred_imp": pred[-1, 0], "obs_fs": obs[-1, 1], "pred_fs": pred[-1, 1],
                     "obs_side": 1 - obs[-1].sum(), "pred_side": 1 - pred[-1].sum(),
                     "median_tau_imp": float(np.median(imp_tau)) if imp_tau.size else float("nan")})
    return pd.DataFrame(rows), pd.DataFrame(summ)


def time_calibration(frame: pl.DataFrame, cif: np.ndarray) -> pd.DataFrame:
    """Pooled observed vs mean predicted CIF at each bin end (the time-to-hit calibration)."""
    obs = observed_cif(frame["label_class"].to_numpy(), frame["tau_min"].to_numpy())
    pred = cif.mean(axis=0)
    return pd.DataFrame({"t_min": (np.arange(hz.N_BINS) + 1) * hz.BIN_MIN, "obs_imp": obs[:, 0], "pred_imp": pred[:, 0],
                         "obs_fs": obs[:, 1], "pred_fs": pred[:, 1]})


def coef_table(model: hz.HazardModel) -> pd.DataFrame:
    """Covariate coefficients (standardised scale) of the full-train fit; multinomial logit vs the ``none`` outcome."""
    coef = model.clf.coef_                      # (3, p): none / impulse / false
    k = len(model.cols)
    rows = []
    for i, c in enumerate(model.cols):
        rows.append({"covariate": c + (" (log)" if c in hz.LOG_COLS else ""),
                     "impulse": float(coef[1, i] - coef[0, i]), "false_start": float(coef[2, i] - coef[0, i])})
    rows.append({"covariate": f"(idő-dummy-k: {coef.shape[1] - k} db)", "impulse": float("nan"), "false_start": float("nan")})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- figures


def _quintile_fig(curves: pd.DataFrame) -> go.Figure:
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Impulzus: kumulált incidencia", "Hamis indulás: kumulált incidencia"),
                        horizontal_spacing=0.1)
    for k in range(1, 6):
        d = curves[curves["quintile"] == k]
        col = QUINTILE_COLORS[k - 1]
        for c, (o, p) in enumerate((("obs_imp", "pred_imp"), ("obs_fs", "pred_fs")), start=1):
            fig.add_trace(go.Scatter(x=d["t_min"], y=d[o], mode="lines+markers", line={"color": col}, marker={"size": 4},
                                     name=f"Q{k} megfigyelt", legendgroup=f"q{k}", showlegend=c == 1,
                                     hovertemplate=f"Q{k} megfigyelt, %{{x}} perc: %{{y:.3f}}<extra></extra>"), row=1, col=c)
            fig.add_trace(go.Scatter(x=d["t_min"], y=d[p], mode="lines", line={"color": col, "dash": "dash"},
                                     name=f"Q{k} becsült", legendgroup=f"q{k}", showlegend=c == 1,
                                     hovertemplate=f"Q{k} becsült, %{{x}} perc: %{{y:.3f}}<extra></extra>"), row=1, col=c)
    fig.update_xaxes(title_text="perc a t_c után", dtick=60)
    fig.update_yaxes(title_text="CIF", row=1, col=1)
    return themed(fig, height=420)


def _time_cal_fig(tc: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for o, p, col, lab in (("obs_imp", "pred_imp", theme.LONG, "impulzus"), ("obs_fs", "pred_fs", theme.SHORT, "hamis indulás")):
        fig.add_trace(go.Scatter(x=tc["t_min"], y=tc[o], mode="lines+markers", line={"color": col}, name=f"{lab} megfigyelt"))
        fig.add_trace(go.Scatter(x=tc["t_min"], y=tc[p], mode="lines", line={"color": col, "dash": "dash"}, name=f"{lab} becsült"))
    fig.update_xaxes(title_text="perc a t_c után", dtick=60)
    fig.update_yaxes(title_text="kumulált incidencia (pooled OOF)")
    return themed(fig, height=340)


def _hazard_fig(haz: np.ndarray) -> go.Figure:
    t_mid = (np.arange(hz.N_BINS) + 0.5) * hz.BIN_MIN
    fig = go.Figure()
    fig.add_trace(go.Bar(x=t_mid, y=haz[:, :, 1].mean(axis=0), name="impulzus", marker={"color": theme.LONG}))
    fig.add_trace(go.Bar(x=t_mid, y=haz[:, :, 2].mean(axis=0), name="hamis indulás", marker={"color": theme.SHORT}))
    fig.update_layout(barmode="group")
    fig.update_xaxes(title_text="idősáv közepe (perc a t_c után)", dtick=60)
    fig.update_yaxes(title_text="átlagos diszkrét hazard / 30 perc")
    return themed(fig, height=320)


def _block_fig(blk: pd.DataFrame) -> go.Figure:
    fig = go.Figure(go.Bar(x=[f"B{int(b)}" for b in blk["block"]], y=blk["delta_auc"],
                           marker={"color": [theme.LONG if v > 0 else theme.SHORT for v in blk["delta_auc"]]},
                           hovertemplate="%{x}: ΔAUC %{y:.4f}<extra></extra>"))
    fig.update_yaxes(title_text="ΔAUC (hazard − baseline)")
    return themed(fig, height=300)


def _null_fig(null: np.ndarray, obs: float) -> go.Figure:
    fig = go.Figure(go.Histogram(x=null, nbinsx=40, marker={"color": theme.NEUTRAL}, name="null ΔAUC"))
    fig.add_vline(x=obs, line={"color": theme.SHORT, "width": 2})
    fig.update_xaxes(title_text="ΔAUC a nap-csomagosan permutált címkéken (teljes refit)")
    fig.update_yaxes(title_text="permutációk")
    return themed(fig, height=300)


# --------------------------------------------------------------------------- report


CMP_COLS: dict[str, Any] = {
    "model": "Modell", "auc": {"label": "AUC", "digits": 4}, "delta_auc": {"label": "ΔAUC", "digits": 4},
    "ci_lo": {"label": "CI alsó", "digits": 4}, "ci_hi": {"label": "CI felső", "digits": 4},
    "logloss3": {"label": "3-oszt. LL", "digits": 4}, "delta_logloss3": {"label": "ΔLL", "digits": 4},
    "prec_top20": {"label": "Prec. felső 20%", "digits": 3}, "lift_top20": {"label": "Lift 20%", "digits": 2},
    "prec_top10": {"label": "Prec. felső 10%", "digits": 3},
    "prec_top20_trthr": {"label": "Prec. 20% (train-küszöb)", "digits": 3}, "n_top20_trthr": "n 20% (train-küszöb)",
    "blocks_pos": "ΔAUC>0 blokk",
    "blocks_prec_better": "Top20 jobb blokk", "dauc_wo_best_block_mean": {"label": "ΔAUC legjobb blokk nélkül", "digits": 4},
    "assets_nonneg": "ΔAUC≥0 asset", "cal_slope": {"label": "Kal. meredekség", "digits": 2},
    "perm_p": {"label": "perm. p", "digits": 4}, "holm_p": {"label": "Holm p", "digits": 4}, "spa_p": {"label": "SPA p", "digits": 4},
    "trade20_mean_net_bps": {"label": "Nettó bp (felső 20%)", "digits": 1},
    "trade20_ci_lo_bps": {"label": "CI alsó bp", "digits": 1}, "trade20_ci_hi_bps": {"label": "CI felső bp", "digits": 1},
    "go_status": "Go / no-go", "go_failed": "Bukott feltételek",
}


def _f(row: pd.Series, k: str) -> float:
    v = row.get(k, float("nan"))
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("nan")


def build(res: sb.BenchResult, curves: dict[str, Any], root: Path | None = None, *, persisted: dict[str, Any] | None = None) -> Path:
    lb = res.leaderboard
    run = res.run
    fr = res.frame
    base = lb[lb["is_baseline"]].iloc[0]
    row = lb[lb["model"] == hz.MODEL_NAME].iloc[0]
    out = res.outputs[hz.MODEL_NAME]
    n, n_pos = fr.height, int(run["n_impulse"])
    cif = curves["cif"]
    # consistency: the report's own OOF fits must equal the harness ones (deterministic model, same folds)
    max_diff = float(np.abs(cif[:, -1, 0] - out.proba[:, 0]).max())
    qc, qs = quintile_curves(fr, out.score, cif)
    tc = time_calibration(fr, cif)

    rb = ReportBuilder(
        scope="general", slug=SLUG, order=36, root=root,
        title="Impulse Scout — Modell 3: diszkrét idejű versengő kockázatok (H3, 1. kör)",
        summary=f"epic_073 t934: hazard modell (impulzus vs hamis indulás, oldalazás cenzorált H-nál), {n} train esemény; AUC "
                f"{row['auc']:.4f} vs baseline {base['auc']:.4f} (ΔAUC {row['delta_auc']:+.4f}), perm. p {fmt(row.get('perm_p'))}.",
        tags=["scout", "epic_073", "hazard", "competing_risks", "benchmark"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél\n\n"
        "**t934 (epic_073), H3:** a 5300 P3 családja — a megerősített mélypont (`t_c`) utáni két, egymást kizáró esemény "
        "(**impulzus**: high > P*, **hamis indulás**: low < low[t0]) **versenye az idő függvényében**, az oldalazás mint `H = 480` "
        "percnél cenzorált kimenet. A modell a végpont-címke mellett az első érintés idejét (`τ`) is felhasználja; a P(impulzus) a "
        "kumulált incidencia `H`-nál. Az összehasonlítás a közös benchmark keretben (`scout_bench`, t931/t932) fut, a skalár S0 + C "
        "baseline ellen, ugyanazokon a foldokon.\n\n"
        f"Futás `{run['run_id']}`: **{n} CV-esemény**, ebből **{n_pos} impulzus** ({100 * n_pos / n:.1f}%), assetek: {run['assets']}, "
        f"cella {run['cell_id']}. Fold-hash `{run['fold_hash']}`, adat-hash `{run['data_hash']}`.\n\n"
        "> **Holdout-védelem:** csak a train split (`sb.load_scout_events`), a flow-jellemzők csak a train események `flow_key`-ére "
        "olvasva, az 1m árak SQL-szűrővel `open_time < 2026-02-01`; holdout-2 sor (≥ 2026-08-01) nem került memóriába.")
    rb.add_markdown(
        "## Modell (előre rögzített 1. köri konfiguráció)\n\n"
        f"- **Idő-diszkretizálás:** `(0, {hz.H_MIN}]` perc, **{hz.N_BINS} × {hz.BIN_MIN} perc** sáv; a `j` sáv `(30j, 30(j+1)]`.\n"
        "- **Személy-periódus kiterjesztés:** impulzus / hamis indulás a `j` sávban → `0..j` sorok, a `j`-ben az esemény, előtte "
        "`nincs`; oldalazás → mind a 16 sor `nincs` (cenzorált `H`-nál). A kiterjesztés a fold tanító eseményein belül, a `fit`-ben "
        "készül, így egy esemény sorai sosem lépnek át foldot (a purge eseményekre hat).\n"
        f"- **Tanuló:** multinomiális logit (`nincs / impulzus / hamis indulás`), L2, C = {hz.C_REG}; idő-tagok: sáv-dummy-k "
        "(arányos kovariáns-hatások).\n"
        f"- **Kovariánsok (`t_c`-ig ismert):** S0 + C ({', '.join(sb.S0C_COLS)}) + 4 order-flow ({', '.join(hz.FLOW_SET)}; "
        f"log: {', '.join(hz.LOG_COLS)}); fold-train medián-imputálás, standardizálás, ±{hz.CLIP_Z:g} vágás.\n"
        "- **Háromosztályos valószínűség:** `CIF_imp(H)`, `CIF_fs(H)`, oldalazás = `S(H) = 1 − összeg` (a konstrukció miatt ≤ 1). "
        "Bináris score = `CIF_imp(H)`.\n"
        f"- **Ellenőrzés:** a riport saját OOF illesztése és a harness OOF valószínűsége közti max. eltérés {max_diff:.2e}.")
    rb.add_table("person_period", curves["pp"], title="Személy-periódus sorok foldonként",
                 columns={"block": "Teszt-blokk", "train_events": "Tanító esemény", "person_period_rows": "Személy-periódus sor",
                          "test_events": "Teszt esemény"})

    # 1. leaderboard comparison
    cmp = lb[lb["model"].isin([base["model"], hz.MODEL_NAME])]
    cmp = cmp[[c for c in CMP_COLS if c in cmp.columns]]
    rb.add_markdown(
        "## 1. Eredmény a baseline-hoz képest\n\n"
        "ΔAUC és CI: összevont OOF, nap-klaszter bootstrap. A felső-q% elsődleges nézete a **teszt-foldon belüli rangsor** "
        "(epic döntés, t932 harness). A `perm. p` a hazardnál a ΔAUC nap-csomagos permutációs p-értéke (N = "
        f"{run['n_perm']}, teljes refit mindkét modellen); ebben a futásban a Holm-család csak a H3-at tartalmazza, a H1–H3 "
        "együttes Holm-korrekciója a t936 leaderboardon történik. Az SPA itt egyetlen modellre fut (a modell-univerzum a t936-ban).")
    rb.add_table("comparison", cmp, title="Hazard vs baseline (összevont OOF)", columns={k: v for k, v in CMP_COLS.items() if k in cmp.columns})
    null = res.perm_null.get(hz.MODEL_NAME)
    if null is not None and len(null):
        rb.add_chart("null_dauc", _null_fig(null, float(row["delta_auc"])), title="ΔAUC permutációs null-eloszlása (piros: megfigyelt)", height=300)

    # 2. CIF by quintile
    rb.add_markdown(
        "## 2. Kumulált incidencia a becsült score kvintilisei szerint\n\n"
        "A pooled OOF `CIF_imp(H)` score szerint 5 egyenlő csoport; folytonos vonal: megfigyelt CIF (a cenzorálás csak `H`-nál "
        "történik, így a kumulált arány pontosan az Aalen–Johansen becslés), szaggatott: a modell átlagos becsült görbéje. A két "
        "görbe illeszkedése az **időbeli (time-to-hit) kalibráció**.")
    rb.add_chart("cif_quintiles", _quintile_fig(qc), title="CIF kvintilisenként: impulzus és hamis indulás (megfigyelt vs becsült)", height=420)
    rb.add_table("quintiles", qs, title="Kvintilisek: CIF(H) megfigyelt vs becsült, impulzus medián τ",
                 columns={"quintile": "Kvintilis", "n": "Esemény", "score_lo": {"label": "Score min", "digits": 3},
                          "score_hi": {"label": "Score max", "digits": 3}, "obs_imp": {"label": "Impulzus megf.", "digits": 3},
                          "pred_imp": {"label": "Impulzus becs.", "digits": 3}, "obs_fs": {"label": "Hamis megf.", "digits": 3},
                          "pred_fs": {"label": "Hamis becs.", "digits": 3}, "obs_side": {"label": "Oldalazás megf.", "digits": 3},
                          "pred_side": {"label": "Oldalazás becs.", "digits": 3}, "median_tau_imp": {"label": "Impulzus medián τ (perc)", "digits": 0}})
    rb.add_chart("time_calibration", _time_cal_fig(tc), title="Időbeli kalibráció: pooled CIF megfigyelt vs becsült", height=340)
    rb.add_table("time_calibration_table", tc, title="Időbeli kalibráció sávonként (pooled OOF)",
                 columns={"t_min": "Perc", "obs_imp": {"label": "Impulzus megf.", "digits": 4}, "pred_imp": {"label": "Impulzus becs.", "digits": 4},
                          "obs_fs": {"label": "Hamis megf.", "digits": 4}, "pred_fs": {"label": "Hamis becs.", "digits": 4}})

    # 3. hazard shape and coefficients
    rb.add_markdown("## 3. Hazard-alak és együtthatók\n\n"
                    "Átlagos OOF diszkrét hazard sávonként (az események kockázatban lévő halmazától függetlenül, minden eseményre "
                    "kiértékelve), és a teljes train-en illesztett modell standardizált együtthatói (log-odds a `nincs` kimenethez képest; "
                    "leíró, nem döntési alap).")
    rb.add_chart("hazard_shape", _hazard_fig(curves["haz"]), title="Átlagos diszkrét hazard 30 perces sávonként", height=320)
    rb.add_table("coefficients", coef_table(curves["full"]), title="Együtthatók (standardizált skála, teljes train illesztés)",
                 columns={"covariate": "Kovariáns", "impulse": {"label": "Impulzus", "digits": 3},
                          "false_start": {"label": "Hamis indulás", "digits": 3}})

    # 4. breakdowns
    blk = res.tables[f"blocks:{hz.MODEL_NAME}"]
    rb.add_markdown("## 4. Blokk-, asset- és kalibráció-bontás")
    rb.add_chart("blocks", _block_fig(blk), title="Blokkonkénti ΔAUC (hazard − baseline)", height=300)
    auc_cols = {"n": "Esemény", "n_impulse": "Impulzus", "auc_model": {"label": "AUC hazard", "digits": 4},
                "auc_base": {"label": "AUC baseline", "digits": 4}, "delta_auc": {"label": "ΔAUC", "digits": 4}}
    rb.add_table("blocks_table", blk, title="Blokk-táblázat", columns={"block": "Blokk", **auc_cols})
    rb.add_table("assets_table", res.tables[f"assets:{hz.MODEL_NAME}"], title="Asset-bontás", columns={"asset_id": "Asset", **auc_cols})
    rb.add_table("calibration_asset", res.tables[f"calibration_asset:{hz.MODEL_NAME}"], title="Kalibráció (P(impulzus)) assetenként",
                 columns={"asset_id": "Asset", "n": "Esemény", "slope": {"label": "Meredekség", "digits": 2},
                          "intercept": {"label": "Tengelymetszet", "digits": 2}, "mean_pred": {"label": "Átl. becslés", "digits": 3},
                          "obs_rate": {"label": "Megfigyelt", "digits": 3}})

    # 5. trades
    trows = []
    for _, r in cmp.iterrows():
        full = lb[lb["model"] == r["model"]].iloc[0]
        for q, label in (("20", "felső 20%"), ("10", "felső 10%"), ("20t", "felső 20% (train-küszöb)"),
                         ("10t", "felső 10% (train-küszöb)"), ("all", "minden esemény")):
            if f"trade{q}_n" not in full:
                continue
            trows.append({"model": r["model"], "signal": label, "n": full[f"trade{q}_n"], "hit": full[f"trade{q}_hit_rate"],
                          "be_hit": full[f"trade{q}_break_even_hit"], "net_bps": full[f"trade{q}_mean_net_bps"],
                          "ci_lo": full[f"trade{q}_ci_lo_bps"], "ci_hi": full[f"trade{q}_ci_hi_bps"],
                          "pf": full[f"trade{q}_profit_factor"]})
    tt = pd.DataFrame(trows)
    if len(tt):
        rb.add_markdown("## 5. Triple-barrier szimuláció (díjak után)\n\n"
                        "Jelzés a teszt-foldon belüli rangsor szerint; költségek a `config/strategies.json`-ból "
                        f"(`{run['costs_json']}`); CI nap-klaszter bootstrap.")
        rb.add_table("trades", tt, title="Szimuláció: hazard vs baseline",
                     columns={"model": "Modell", "signal": "Jelzés", "n": "Esemény", "hit": {"label": "Cél (hit)", "digits": 3},
                              "be_hit": {"label": "Break-even hit", "digits": 3}, "net_bps": {"label": "Nettó bp / esemény", "digits": 1},
                              "ci_lo": {"label": "CI alsó", "digits": 1}, "ci_hi": {"label": "CI felső", "digits": 1},
                              "pf": {"label": "Profit faktor", "digits": 2}})

    # interpretation
    q5, q1 = qs.iloc[-1], qs.iloc[0]
    gap_imp = float(np.abs(tc["obs_imp"] - tc["pred_imp"]).max())
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"- **Rangsorolás:** AUC **{row['auc']:.4f}** vs baseline {base['auc']:.4f}, ΔAUC **{row['delta_auc']:+.4f}** "
        f"(CI [{_f(row, 'ci_lo'):+.4f}; {_f(row, 'ci_hi'):+.4f}]), perm. p **{fmt(row.get('perm_p'))}**; ΔAUC > 0 a blokkok "
        f"{int(row['blocks_pos'])}/{int(row['n_blocks'])}-ában, legjobb blokk nélkül átlag {_f(row, 'dauc_wo_best_block_mean'):+.4f}.\n"
        f"- **3 osztály:** log-loss {row['logloss3']:.4f} vs {base['logloss3']:.4f} (ΔLL {row['delta_logloss3']:+.4f}); a hazard a "
        "hamis indulás / oldalazás szétválasztását is a kovariánsokból becsüli (a baseline ezt konstans aránnyal osztja).\n"
        f"- **Szelekció:** felső 20% precizitás {_f(row, 'prec_top20'):.3f} vs {_f(base, 'prec_top20'):.3f} "
        f"(lift {_f(row, 'lift_top20'):.2f}); a legfelső kvintilisben a megfigyelt CIF_imp(H) {q5['obs_imp']:.3f} (becsült "
        f"{q5['pred_imp']:.3f}), a legalsóban {q1['obs_imp']:.3f}.\n"
        f"- **Időbeli kalibráció:** a pooled megfigyelt és becsült impulzus-CIF legnagyobb eltérése {gap_imp:.4f}; az impulzusok "
        f"medián első érintése a felső kvintilisben {fmt(q5['median_tau_imp'], 0)} perc.\n"
        f"- **Go / no-go (5300, harness):** {row.get('go_status', 'n/a')}"
        + (f" — bukott feltételek: {row.get('go_failed')}" if row.get("go_failed") else "") + ". "
        "A H3 végleges Holm-korrigált értékelése a t936-ban (H1–H3 együtt) történik.\n"
        "- **Korlátok:** az 1. kör rögzített, hangolatlan konfiguráció; a flow-részhalmazt a t930 leíró (train) táblája alapján "
        "választottuk (enyhe előzetes adat-ismeret); finanszírozási díj nincs; a holdout-2 érintetlen."
        + (f"\n\nLab DB: csak a `{hz.MODEL_NAME}` sor + a futás sora íródott (`{persisted['run_id']}`)." if persisted else ""))
    return rb.write()


def build_safe(res: sb.BenchResult, curves: dict[str, Any], root: Path | None = None, **kw: Any) -> Path:
    """Build; on a Windows folder-replace error build in a temp dir and copy over."""
    try:
        return build(res, curves, root, **kw)
    except PermissionError:
        tmp = Path(tempfile.mkdtemp(prefix="scout_hazard_"))
        out = build(res, curves, tmp, **kw)
        from reports import store

        target = store.report_dir(store.GENERAL_OWNER, SLUG, root)
        target.mkdir(parents=True, exist_ok=True)
        shutil.copytree(out, target, dirs_exist_ok=True)
        return target


def run(*, n_perm: int = 1000, n_boot: int = 2000, persist_lab: bool = True, root: Path | None = None,
        note: str = "t934 scout_hazard_r1") -> tuple[sb.BenchResult, Path]:
    frame = hz.load_frame()
    bars = hz.load_bars(sorted(frame["asset_id"].unique().to_list()))
    res = sb.run_benchmark(frame, hz.registry(), bars, n_perm=n_perm, n_boot=n_boot, note=note)
    curves = oof_curves(res.frame, res.folds)
    meta = hz.persist_model_rows(res) if persist_lab else None
    if meta:
        logger.info("persisted %s", meta)
    return res, build_safe(res, curves, root, persisted=meta)


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or SLUG).split("\n")[0])
    ap.add_argument("--n-perm", type=int, default=1000)
    ap.add_argument("--n-boot", type=int, default=2000)
    ap.add_argument("--no-persist", action="store_true")
    ap.add_argument("--root", default=None, help="report root (default: _doc_/reports)")
    a = ap.parse_args()
    res, out = run(n_perm=a.n_perm, n_boot=a.n_boot, persist_lab=not a.no_persist, root=Path(a.root) if a.root else None)
    cols = [c for c in ("model", "auc", "delta_auc", "ci_lo", "ci_hi", "logloss3", "prec_top20", "blocks_pos", "perm_p", "holm_p",
                        "spa_p", "trade20_mean_net_bps", "go_status") if c in res.leaderboard.columns]
    print(res.leaderboard[cols].to_string())
    print(out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
