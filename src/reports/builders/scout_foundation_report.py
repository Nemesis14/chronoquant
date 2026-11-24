# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Report of Impulse Scout model 4 (epic_073 / t935, X2, exploratory): Chronos-Bolt embedding + linear head
(``_doc_/reports/scout_foundation/``).

``python -m reports.builders.scout_foundation_report [--n-perm 200]`` (needs ``uv run --group research``):

1. loads the t930 train events and the train-period 1m bars (SQL-filtered, cached as parquet in ``data/scout_foundation``),
   builds the 1m prefix (t933 ``build_prefix``, step 1) and the frozen Chronos-Bolt embedding (cached ``.npz``);
2. runs the shared ``scout_bench`` protocol on baseline + ``scout_chronos_r1`` + ``scout_chronos_s0c_r1``;
3. the exploratory day-bundle permutation null (head refit only, the embedding is label free; default N = 200) patches
   the two rows' ``perm_p``;
4. persists ONLY the two X2 rows (``scout_models_hazard.persist_model_rows``: delete + insert by ``(run_id, model)``)
   and writes the report. ``--rebuild`` re-renders the report from the pickled result (no recompute).
"""

from __future__ import annotations

import argparse
import logging
import pickle
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl

from reports import ReportBuilder, theme
from reports.builders import scout_bench as sb
from reports.builders import scout_models_foundation as fm
from reports.builders._common import source_info, themed
from reports.builders.scout_bench.report import (
    LB_COLUMNS,
    _block_fig,
    _lb_table,
    _reliability_fig,
    _trade_table,
)
from reports.builders.scout_bench.runner import BenchResult

logger = logging.getLogger(__name__)

SLUG = "scout_foundation"
GENERATOR = "reports.builders.scout_foundation_report"
SEED = 20260101
RESULT_FILE = "bench_foundation.pkl"


# --------------------------------------------------------------------------- descriptive diagnostics


def univariate_auc(frame: pl.DataFrame, fe: fm.FoundationEmbedding, n_pc: int = 10) -> pd.DataFrame:
    """Descriptive (full train, in-sample) impulse-vs-rest AUC of the loc / scale scalars and of the first PCs, with the
    largest |corr| to an S0 + C scalar (how much of the embedding signal is already in the baseline)."""
    y = sb.frame.y_binary(frame)
    rows_id = frame["row_id"].to_numpy()
    basis = fm.fit_pca(fe.emb[rows_id], max(n_pc, 1))
    pcs = fm.pca_transform(basis, fe.emb[rows_id])
    s0c = frame.select([pl.col(c).cast(pl.Float64) for c in sb.S0C_COLS]).to_numpy()
    out = []
    cols = [(n, fe.stats[rows_id, k]) for k, n in enumerate(fe.stat_names)]
    cols += [(f"PC{k + 1} ({100 * basis.explained[k]:.1f}%)", pcs[:, k]) for k in range(n_pc)]
    for name, v in cols:
        a = sb.auc(y, v)
        cors = [abs(float(np.corrcoef(v, s0c[:, j])[0, 1])) for j in range(s0c.shape[1])]
        j = int(np.nanargmax(cors))
        out.append({"feature": name, "auc": a, "auc_sym": max(a, 1 - a), "max_corr": cors[j], "s0c": sb.S0C_COLS[j]})
    return pd.DataFrame(out)


def explained_curve(frame: pl.DataFrame, fe: fm.FoundationEmbedding, k: int = 128) -> pd.DataFrame:
    basis = fm.fit_pca(fe.emb[frame["row_id"].to_numpy()], k)
    return pd.DataFrame({"k": np.arange(1, basis.explained.size + 1), "cum": np.cumsum(basis.explained)})


# --------------------------------------------------------------------------- figures


def _explained_fig(ev: pd.DataFrame) -> go.Figure:
    fig = go.Figure(go.Scatter(x=ev["k"], y=ev["cum"], mode="lines", line={"color": theme.BLUE, "width": 2},
                               hovertemplate="%{x} komponens: %{y:.3f}<extra></extra>"))
    fig.add_vline(x=fm.N_PCA, line={"color": theme.ACCENT, "dash": "dash"}, annotation_text=f"fej: {fm.N_PCA} komponens")
    fig.update_xaxes(title_text="PCA komponensek száma (1024 dim, standardizált)")
    fig.update_yaxes(title_text="kumulált magyarázott variancia", range=[0, 1])
    return themed(fig, height=300, showlegend=False)


def _null_fig(res: BenchResult) -> go.Figure | None:
    fig = go.Figure()
    colors = {fm.EMB_NAME: theme.BLUE, fm.EMB_S0C_NAME: theme.AMBER}
    any_null = False
    for m in fm.MODEL_NAMES:
        null = res.perm_null.get(m)
        if null is None or not np.size(null):
            continue
        any_null = True
        obs = float(res.leaderboard.loc[res.leaderboard["model"] == m, "delta_auc"].iloc[0])
        fig.add_trace(go.Histogram(x=null, nbinsx=40, name=f"{m} null", marker={"color": colors[m]}, opacity=0.6))
        fig.add_vline(x=obs, line={"color": colors[m], "width": 2})
    if not any_null:
        return None
    fig.update_layout(barmode="overlay")
    fig.update_xaxes(title_text="ΔAUC (modell − baseline) nap-csomagosan permutált címkéken (fej-refit); függőleges: megfigyelt")
    fig.update_yaxes(title_text="permutációk")
    return themed(fig, height=320)


def _f(v: Any, d: int = 4) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return str(v)
    return "n/a" if np.isnan(x) else f"{x:.{d}f}"


def _signed(v: Any, d: int = 4) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return str(v)
    return "n/a" if np.isnan(x) else f"{x:+.{d}f}"


# --------------------------------------------------------------------------- report


def build(res: BenchResult, fe: fm.FoundationEmbedding, root: Path | None = None, *, extra: dict[str, Any] | None = None
          ) -> Path:
    extra = extra or {}
    lb = res.leaderboard
    run = res.run
    fr = res.frame
    rows = {r["model"]: r for _, r in lb.iterrows()}
    base, emb, s0c = rows[sb.BASELINE_NAME], rows[fm.EMB_NAME], rows[fm.EMB_S0C_NAME]
    n, n_pos = fr.height, int(run["n_impulse"])
    n_perm = int(np.size(res.perm_null.get(fm.EMB_NAME, [])))
    meta = fe.meta
    rb = ReportBuilder(
        scope="general", slug=SLUG, order=37, root=root,
        title="Impulse Scout — Modell 4: Chronos-Bolt embedding + lineáris fej (X2, kísérleti)",
        summary=(f"epic_073 t935: frozen Chronos-Bolt-small embedding; ΔAUC csak embedding {_signed(emb['delta_auc'])}, "
                 f"embedding + S0C {_signed(s0c['delta_auc'])} (baseline AUC {_f(base['auc'])}); perm. p "
                 f"{_f(emb['perm_p'], 3)} / {_f(s0c['perm_p'], 3)} (N = {n_perm}, fej-refit); {n} train esemény."),
        tags=["scout", "epic_073", "chronos", "foundation_model", "embedding", "leaderboard"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél\n\n"
        "**t935 (epic_073), X2 — kísérleti, hipotézis nélkül:** hordoz-e egy előtanított idősor-alapmodell (foundation model) "
        "**befagyasztott** reprezentációja a `[t0 − B, t_c]` prefixről olyan információt, amely a skalár S0 + C baseline-on túl "
        "segít az impulzus előrejelzésében. A 5300 szerint a pénzügyi, alacsony jel/zaj arányú adaton az ilyen embeddingek "
        "eredménye irodalmilag gyenge / vegyes, ezért a modell a Holm-családon kívül, kísérleti sorként fut (SPA-belépő).\n\n"
        f"Futás `{run['run_id']}`: **{n} train esemény**, ebből **{n_pos} impulzus** ({100 * n_pos / n:.1f}%), assetek: "
        f"{run['assets']}, cella {run['cell_id']}; közös 6 blokkos purged CV (fold-hash `{run['fold_hash']}`).\n\n"
        "> **Holdout-védelem:** csak a train split (`sb.load_scout_events`); az 1m bar-ok SQL-szűrővel `open_time < 2026-02-01`; "
        "a prefix kizárólag `t_c`-ig záruló bar-okból épül; holdout-2 sor (≥ 2026-08-01) nem került memóriába. Az embedding "
        "címke nélkül, befagyasztott modellel készül → fold-szivárgás nincs.")
    rb.add_markdown(
        "## Modell\n\n"
        "**Alapmodell-választás.** A `chronos-forecasting` csomag (Chronos-Bolt, Chronos-2) CPU-n gyors és pip-ből "
        "telepíthető; a **Kronos** (K-line alapmodell) nyílt checkpointjai (HF `NeoQuasar/Kronos-*`) csak a GitHub-repó "
        "kódjával futnak (nincs pip-csomag), ezért az 1. körből kimaradt. Választás: **Chronos-Bolt-small** (T5 encoder–decoder, "
        "d_model 512, 6 encoder-réteg, 16-os patch-ek, `[REG]` token).\n\n"
        f"- **Checkpoint:** `{fm.FM_ID}` @ `{fm.FM_REVISION[:12]}` ({fm.FM_PACKAGE}; letöltés ~191 MB, `model.safetensors`), "
        "float32, CPU, eval mód.\n"
        f"- **Prefix:** a `[t0 − B, t_c − 1m]` nyitású **1m** bar-ok (L = {fm.PREFIX_LEN}, mind `t_c`-ig zárul; a t933 "
        "`build_prefix` lépésköz 1-gyel), két csatorna: `close_sigma` = `log(close / low[t0]) / σ_W` és `log_vol_rel` = "
        "`log1p(volume / vol7d_mean_1m)`.\n"
        "- **Embedding:** a Chronos-Bolt egyváltozós → csatornánként külön; a 30 patch-token encoder-állapotának **átlaga** "
        "(a `[REG]` nélkül) → 2 × 512 = 1024 dim. A Chronos-Bolt a bemenetet **példányonként normalizálja** (átlag / szórás), ami "
        "a 5300 szerint lényeges amplitúdót eldobná, ezért a normalizálás statisztikái (`loc`, `log scale`) csatornánként "
        "explicit skalárként megmaradnak (4 db).\n"
        f"- **Fej (rögzített, hangolatlan):** fold-train standardizálás → fold-train **PCA {fm.N_PCA}** komponens (címkefüggetlen, "
        f"foldonként cache-elve) + 4 loc/scale skalár (a `+S0C` változatban + 6 S0 + C skalár) → fold-train standardizálás, ±"
        f"{fm.CLIP_Z:g} vágás → **multinomiális L2 logit** (lbfgs, C = {fm.C_HEAD}) → 3 osztály valószínűsége, score = "
        "P(impulzus). Determinisztikus.\n"
        f"- **Futásidő:** modell-betöltés {_f(meta.get('seconds_model_load'), 0)} s, embedding {_f(meta.get('seconds_embed'), 0)} s "
        f"({meta.get('n')} esemény × 2 csatorna), egyszer, cache-elve (`data/scout_foundation/emb_{meta.get('key')}.npz`, "
        "gitignored).\n\n"
        f"Konfiguráció-hash: `{emb['config_hash']}` (embedding), `{s0c['config_hash']}` (embedding + S0C).")
    ev = explained_curve(fr, fe)
    rb.add_chart("pca_explained", _explained_fig(ev), title="Az embedding PCA-spektruma (teljes train, leíró)", height=300)
    ua = univariate_auc(fr, fe)
    rb.add_markdown(
        "## Az embedding leíró vizsgálata (teljes train, in-sample)\n\n"
        "Egyváltozós AUC (impulzus vs többi) a loc/scale skalárokra és az első PCA-komponensekre, valamint a legerősebb "
        "korreláció egy S0 + C skalárral — mennyi a jelből, ami már a baseline-ban is benne van. Leíró, nem döntési alap.")
    rb.add_table("univariate", ua, title="Egyváltozós AUC és átfedés az S0 + C skalárokkal",
                 columns={"feature": "Jellemző", "auc": {"label": "AUC", "digits": 3},
                          "auc_sym": {"label": "max(AUC, 1−AUC)", "digits": 3},
                          "max_corr": {"label": "max |corr| S0C", "digits": 2}, "s0c": "Legközelebbi S0C"})

    rb.add_markdown(
        "## 1. Leaderboard (ez a futás)\n\n"
        "ΔAUC és CI: összevont OOF, nap-klaszter bootstrap. A felső-q% elsődleges nézete a teszt-foldon belüli rangsor. "
        f"A `perm. p` a ΔAUC nap-csomagos permutációs p-értéke **N = {n_perm}** permutációval (kísérleti modell → csökkentett N, "
        f"minimális elérhető p = {1 / (n_perm + 1):.4f}); a permutációban az embedding (és a fold-PCA) címkefüggetlen, így "
        "azonos, csak a fej tanul újra — ez a 5300 szerinti teljes refit. Holm kísérleti modellre nem vonatkozik; az SPA itt a "
        "két X2 sorra fut (a teljes univerzum a t936-ban). A baseline sort ide nem perzisztáltuk (a t931/t932 adja).")
    rb.add_table("leaderboard", _lb_table(lb), title="Leaderboard: baseline + Chronos embedding (+ S0C)", columns=LB_COLUMNS)
    fig = _null_fig(res)
    if fig is not None:
        rb.add_chart("null_dauc", fig, title=f"ΔAUC permutációs null-eloszlás (N = {n_perm}, fej-refit)", height=320)

    rb.add_markdown("## 2. Blokk- és asset-bontás")
    for m, lab in ((fm.EMB_NAME, "csak embedding"), (fm.EMB_S0C_NAME, "embedding + S0C")):
        rb.add_chart(f"blocks_{m}", _block_fig(res, m), title=f"Blokkonkénti AUC és ΔAUC — {lab}", height=340)
    asset_cols = {"asset_id": "Asset", "n": "Esemény", "n_impulse": "Impulzus", "auc_model": {"label": "AUC modell", "digits": 4},
                  "auc_base": {"label": "AUC baseline", "digits": 4}, "delta_auc": {"label": "ΔAUC", "digits": 4}}
    assets = pd.concat([res.tables[f"assets:{m}"].assign(model=m) for m in fm.MODEL_NAMES])
    rb.add_table("assets", assets, title="Asset-bontás", columns={"model": "Modell", **asset_cols})

    rb.add_markdown("## 3. Kalibráció")
    for m, lab in ((fm.EMB_NAME, "csak embedding"), (fm.EMB_S0C_NAME, "embedding + S0C")):
        rb.add_chart(f"reliability_{m}", _reliability_fig(res, m), title=f"Megbízhatósági görbe — {lab}", height=320)

    rb.add_markdown(
        "## 4. Kereskedési szimuláció (triple barrier, díjak után)\n\n"
        f"Jelzés a teszt-foldon belüli rangsor szerint; költségek a `config/strategies.json`-ból (`{run['costs_json']}`); "
        "CI nap-klaszter bootstrap. A CV-szimuláció csak előszűrő (5300 B).")
    rb.add_table("trades", _trade_table(res), title="Szimuláció: jelzés-szám, hit / stop / időkorlát, nettó hozam (bp)")

    rt = [{"tétel": "Chronos-Bolt modell-betöltés", "másodperc": meta.get("seconds_model_load")},
          {"tétel": f"embedding ({meta.get('n')} esemény × 2 csatorna, CPU)", "másodperc": meta.get("seconds_embed")}]
    rt += [{"tétel": f"CV (6 fold) — {m}", "másodperc": float(rows[m]["fit_seconds"])} for m in rows]
    rt.append({"tétel": "benchmark (CV, bootstrap, SPA, szimuláció)", "másodperc": float(run["seconds"])})
    if extra.get("perm_seconds") is not None:
        rt.append({"tétel": f"permutációs null (N = {n_perm}, baseline + 2 fej refit)", "másodperc": extra["perm_seconds"]})
    rb.add_markdown("## 5. Futásidő")
    rb.add_table("runtime", pd.DataFrame(rt), title="Futásidő (CPU, párhuzamos agent-futások mellett)",
                 columns={"tétel": "Tétel", "másodperc": {"label": "Másodperc", "digits": 1}})

    best_stat = ua.iloc[ua["auc_sym"].astype(float).idxmax()]
    lines = [
        "## Értelmezés\n",
        f"- **Csak embedding:** AUC **{_f(emb['auc'])}** vs baseline {_f(base['auc'])}, ΔAUC **{_signed(emb['delta_auc'])}** "
        f"(CI [{_signed(emb['ci_lo'])}; {_signed(emb['ci_hi'])}]), perm. p {_f(emb['perm_p'], 3)}; 3-oszt. log-loss "
        f"{_f(emb['logloss3'])} (Δ {_signed(emb['delta_logloss3'])}); felső 20% precizitás {_f(emb['prec_top20'], 3)} vs "
        f"{_f(base['prec_top20'], 3)}. A befagyasztott reprezentáció önmagában **nem éri el** a hat kézi skalárt.",
        f"- **Embedding + S0C:** AUC **{_f(s0c['auc'])}**, ΔAUC **{_signed(s0c['delta_auc'])}** (CI [{_signed(s0c['ci_lo'])}; "
        f"{_signed(s0c['ci_hi'])}]), perm. p {_f(s0c['perm_p'], 3)}; ΔAUC > 0 blokk {int(s0c['blocks_pos'])}/{int(s0c['n_blocks'])}; "
        f"felső 20% precizitás {_f(s0c['prec_top20'], 3)} (lift {_f(s0c['lift_top20'], 2)}). Az embedding a baseline-hoz "
        "adva sem ad zajtól megkülönböztethető többletet.",
        f"- **Leíró:** a legerősebb egyedi embedding-jellemző `{best_stat['feature']}` (max(AUC, 1−AUC) "
        f"{_f(best_stat['auc_sym'], 3)}, |corr| a `{best_stat['s0c']}`-vel {_f(best_stat['max_corr'], 2)}): ami jel van, az "
        "jórészt az amplitúdó-skalárokban és a baseline-nal átfedve jelenik meg.",
        f"- **Gazdasági előszűrő:** nettó bp / esemény (felső 20%) {_f(emb.get('trade20_mean_net_bps'), 1)} (embedding), "
        f"{_f(s0c.get('trade20_mean_net_bps'), 1)} (+ S0C), baseline {_f(base.get('trade20_mean_net_bps'), 1)}.",
        f"- **Go / no-go (harness):** embedding {emb['go_status']}" + (f" (bukott: {emb['go_failed']})" if str(emb["go_failed"] or "") else "")
        + f"; embedding + S0C {s0c['go_status']}" + (f" (bukott: {s0c['go_failed']})" if str(s0c["go_failed"] or "") else "") + ".",
        *([("- **SPA-figyelmeztetés (harness-hiba gyanú):** az SPA p = "
            f"{_f(res.spa['p_consistent'], 4)}, de egyik X2 sor sem jobb a baseline-nál bináris log-loss-ban (napi átlagos "
            f"különbség {', '.join(f'{v:+.4f}' for v in np.atleast_1d(res.spa['dbar']))}, t-stat = 0). A `stats.spa_test` "
            "`p = mean(tb > tstat)` szigorú egyenlőtlensége t-stat = 0 mellett hamisan kicsi p-t ad (helyesen `>=` → p ≈ 1). "
            "A `gate_spa` = igaz ezért nem érdemi; a döntést (NO-GO) nem befolyásolja. Javítás a t932/t936 hatáskör.")]
          if res.spa is not None and float(res.spa.get("t_stat", 1.0)) == 0.0 else []),
        "- **Korlátok:** egy alapmodell (Chronos-Bolt-small) és egy rögzített fej (PCA 64, C = 0,1), hangolás nélkül; a "
        "Chronos-Bolt előrejelzésre (nem osztályozásra) tanult, a mean-pool az alak finom részleteit elmoshatja; a Kronos és "
        "a Chronos-2 (többváltozós) kimaradt. A permutáció csökkentett N-nel futott (kísérleti). Holdout-2 érintetlen.",
    ]
    if extra.get("persisted"):
        lines.append(f"\nLab DB: csak a `{fm.EMB_NAME}` és `{fm.EMB_S0C_NAME}` sor + a futás sora íródott (`{run['run_id']}`).")
    rb.add_markdown("\n".join(lines))
    return rb.write()


def build_safe(res: BenchResult, fe: fm.FoundationEmbedding, root: Path | None = None, **kw: Any) -> Path:
    """Build; on a Windows folder-replace error build in a temp dir and copy over."""
    try:
        return build(res, fe, root, **kw)
    except PermissionError:
        tmp = Path(tempfile.mkdtemp(prefix="scout_foundation_"))
        out = build(res, fe, tmp, **kw)
        from reports import store

        target = store.report_dir(store.GENERAL_OWNER, SLUG, root)
        target.mkdir(parents=True, exist_ok=True)
        shutil.copytree(out, target, dirs_exist_ok=True)
        return target


# --------------------------------------------------------------------------- run


def load_inputs(cache: Path = fm.CACHE_DIR) -> tuple[pl.DataFrame, dict[str, Any], fm.FoundationEmbedding]:
    """Prepared train frame, train-period 1m bars (trade simulation) and the (cached) embedding."""
    from reports.builders import scout_models_rocket as rk

    fr = sb.prepare_frame(sb.load_scout_events())
    bars, tbb = rk.load_bars(sorted(fr["asset_id"].unique().to_list()), cache_dir=cache)
    pre = rk.build_prefix(fr, bars, tbb, step=fm.PREFIX_STEP)
    return fr, bars, fm.load_or_embed(fr, pre.x, cache_dir=cache)


def run(*, n_perm: int = 200, n_boot: int = 2000, spa_boot: int = 5000, persist_lab: bool = True, root: Path | None = None,
        cache: Path = fm.CACHE_DIR) -> tuple[BenchResult, Path]:
    from reports.builders import scout_models_hazard as hz

    cache.mkdir(parents=True, exist_ok=True)
    t = time.perf_counter()
    fr, bars, fe = load_inputs(cache)
    t_load = time.perf_counter() - t
    res = sb.run_benchmark(fr, fm.registry(), bars, prefix_data=fe, prepared=True, seed=SEED, n_perm=0, n_boot=n_boot,
                           spa_boot=spa_boot, note="t935: Chronos-Bolt embedding X2 (exploratory, own head-refit perm null)")
    res.run["seconds_load"] = t_load
    perm_seconds = None
    if n_perm > 0:
        t = time.perf_counter()
        null = fm.permutation_null(res.frame, res.folds, fe, n_perm=n_perm, seed=SEED)
        perm_seconds = time.perf_counter() - t
        y = sb.frame.y_binary(res.frame)
        a_base = sb.auc(y, res.outputs[sb.BASELINE_NAME].score)
        lbd = res.leaderboard
        lbd.loc[lbd["model"] == sb.BASELINE_NAME, "perm_p"] = sb.perm_p(a_base, null["_auc_base"])
        for m in fm.MODEL_NAMES:
            res.perm_null[m] = null[m]
            lbd.loc[lbd["model"] == m, "perm_p"] = sb.perm_p(sb.auc(y, res.outputs[m].score) - a_base, null[m])
        res.perm_null["_auc_base"] = null["_auc_base"]
        res.run["n_perm"] = n_perm
        res.run["note"] = f"{res.run['note']}; X2 perm_p: N={n_perm} head refit (exploratory, reduced)"
    extra: dict[str, Any] = {"perm_seconds": perm_seconds, "persisted": None}
    if persist_lab:
        extra["persisted"] = [hz.persist_model_rows(res, m, tries=60, delay=5.0) for m in fm.MODEL_NAMES]
        logger.info("persisted %s", extra["persisted"])
    with open(cache / RESULT_FILE, "wb") as fh:
        pickle.dump({"res": res, "fe": fe, "extra": extra}, fh)
    return res, build_safe(res, fe, root, extra=extra)


def rebuild(root: Path | None = None, cache: Path = fm.CACHE_DIR) -> Path:
    with open(cache / RESULT_FILE, "rb") as fh:
        d = pickle.load(fh)
    return build_safe(d["res"], d["fe"], root, extra=d["extra"])


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or SLUG).split("\n")[0])
    ap.add_argument("--n-perm", type=int, default=200)
    ap.add_argument("--n-boot", type=int, default=2000)
    ap.add_argument("--no-persist", action="store_true")
    ap.add_argument("--rebuild", action="store_true", help="re-render the report from the pickled result")
    ap.add_argument("--cache-dir", default=str(fm.CACHE_DIR))
    ap.add_argument("--root", default=None, help="report root (default: _doc_/reports)")
    a = ap.parse_args()
    root = Path(a.root) if a.root else None
    if a.rebuild:
        print(rebuild(root, Path(a.cache_dir)))
        return
    res, out = run(n_perm=a.n_perm, n_boot=a.n_boot, persist_lab=not a.no_persist, root=root, cache=Path(a.cache_dir))
    cols = [c for c in ("model", "auc", "delta_auc", "ci_lo", "ci_hi", "logloss3", "delta_logloss3", "prec_top20", "lift_top20",
                        "blocks_pos", "perm_p", "spa_p", "trade20_mean_net_bps", "trade20_ci_lo_bps", "trade20_ci_hi_bps",
                        "go_status", "go_failed", "fit_seconds") if c in res.leaderboard.columns]
    print(res.leaderboard[cols].to_string())
    print(out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
