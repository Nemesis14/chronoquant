# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""t933 run + report: MiniRocket (P2 / H2) and InceptionTime (X1) on the Impulse Scout benchmark
(``_doc_/reports/scout_rocket/``, report format).

Stages (``python -m reports.builders.scout_rocket_report --stage all``; intermediate results in ``--cache-dir``):

1. ``bench``  : ``sb.run_benchmark`` with baseline + both models, ``n_perm`` day-bundle permutations (full refit) for
   the primary H2 (and the baseline AUC); the BenchResult is pickled.
2. ``perm``   : the exploratory InceptionTime's own permutation null (``--n-perm-x1``, default 100, parallel workers;
   identical permutation ``i`` as in stage 1).
3. ``final``  : patch the X1 ``perm_p``, persist ONLY the t933 rows (``sb.persist(res, models=...)``) and
   write the report.
"""

from __future__ import annotations

import argparse
import logging
import pickle
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from reports import ReportBuilder, theme
from reports.builders import scout_bench as sb
from reports.builders import scout_models_rocket as rk
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

SLUG = "scout_rocket"
GENERATOR = "reports.builders.scout_rocket_report"
SEED = 20260101


# --------------------------------------------------------------------------- stages


def load_inputs(cache: Path | None = None) -> tuple[Any, dict[str, Any], rk.RocketPrefix]:
    fr = sb.prepare_frame(sb.load_scout_events())
    bars, tbb = rk.load_bars(sorted(fr["asset_id"].unique().to_list()), cache_dir=cache)
    return fr, bars, rk.build_prefix(fr, bars, tbb)


def stage_bench(cache: Path, *, n_perm: int, n_boot: int = 2000, spa_boot: int = 5000) -> BenchResult:
    t = time.perf_counter()
    fr, bars, pre = load_inputs(cache)
    t_load = time.perf_counter() - t
    res = sb.run_benchmark(fr, rk.registry(), bars, prefix_data=pre, prepared=True, seed=SEED, n_perm=n_perm,
                           n_boot=n_boot, spa_boot=spa_boot,
                           note="t933: MiniRocket H2 (n_perm full refit) + InceptionTime X1 (exploratory, own perm null)")
    res.run["seconds_load"] = t_load
    np.save(cache / "prefix.npy", pre.x)
    with open(cache / "bench.pkl", "wb") as fh:
        pickle.dump(res, fh)
    return res


def stage_perm(cache: Path, *, n_perm_x1: int, workers: int) -> np.ndarray:
    with open(cache / "bench.pkl", "rb") as fh:
        res: BenchResult = pickle.load(fh)
    pre = rk.RocketPrefix(x=np.load(cache / "prefix.npy"))      # stage-1 prefix: the live DB is not reopened
    if pre.x.shape[0] != res.frame.height:
        raise ValueError("prefix / frame mismatch")
    t = time.perf_counter()
    null = rk.permutation_null_parallel(res.frame, res.folds, sb.baseline_spec(), rk.inception_spec(), pre,
                                        n_perm=n_perm_x1, seed=SEED, workers=workers)
    np.save(cache / "inception_null.npy", null)
    (cache / "inception_null_seconds.txt").write_text(f"{time.perf_counter() - t:.1f}")
    return null


def stage_final(cache: Path, *, persist: bool = True, root: Path | None = None, prefix_x: np.ndarray | None = None) -> Path:
    with open(cache / "bench.pkl", "rb") as fh:
        res: BenchResult = pickle.load(fh)
    null_path = cache / "inception_null.npy"
    perm_seconds = None
    if null_path.exists():
        null = np.load(null_path)
        y = sb.frame.y_binary(res.frame)
        obs = sb.auc(y, res.outputs[rk.INCEPTION_NAME].score) - sb.auc(y, res.outputs[sb.BASELINE_NAME].score)
        res.perm_null[rk.INCEPTION_NAME] = null
        res.leaderboard.loc[res.leaderboard["model"] == rk.INCEPTION_NAME, "perm_p"] = sb.perm_p(obs, null)
        res.run["note"] = f"{res.run['note']}; X1 perm_p: N={null.size} (exploratory, reduced)"
        sec = cache / "inception_null_seconds.txt"
        perm_seconds = float(sec.read_text()) if sec.exists() else None
    else:
        res.run["note"] = f"{res.run['note']}; X1 perm_p: not run (stopped by user, CPU)"
    refresh_spa(res)
    if persist:
        logger.info("persisted %s", sb.persist(res, models=(rk.ROCKET_NAME, rk.INCEPTION_NAME), tries=40, delay=5.0))
    return build(res, root, perm_seconds_x1=perm_seconds, prefix_x=prefix_x)


def refresh_spa(res: BenchResult) -> None:
    """Recompute the SPA (and the go / no-go gates that use it) with the CURRENT ``scout_bench.stats`` on the stored
    OOF scores, exactly as ``run_benchmark`` does (same model order, seed, bootstrap settings): a harness fix of the SPA
    (t932 validator, ``>=`` in the p-value) then reaches this run without re-running the CV / permutations."""
    from reports.builders.scout_bench import metrics as mt
    from reports.builders.scout_bench import runner as rn
    from reports.builders.scout_bench import stats as st

    lb, run = res.leaderboard, res.run
    others = [m for m in lb["model"] if m != sb.BASELINE_NAME]
    y = sb.frame.y_binary(res.frame)
    clusters = st.day_clusters(res.frame["t0"].to_numpy())
    base = mt.logloss_binary_each(y, res.outputs[sb.BASELINE_NAME].score)
    d = np.column_stack([base - mt.logloss_binary_each(y, res.outputs[m].score) for m in others])
    spa = st.spa_test(st.day_sums(d, clusters.day_of, clusters.n_days), n_boot=int(run["spa_boot"]),
                      mean_block=float(run["spa_mean_block"]), seed=int(run["seed"]))
    spa["best_name"] = others[spa["best"]]
    res.spa = spa
    run["spa_p"], run["spa_best"] = spa["p_consistent"], spa["best_name"]
    ok = rn.min_events_ok(res.frame)
    for i in lb.index[lb["model"] != sb.BASELINE_NAME]:
        lb.loc[i, "spa_p"] = spa["p_consistent"]
        lb.loc[i, "spa_best"] = lb.loc[i, "model"] == spa["best_name"]
        for k, v in rn.go_no_go(lb.loc[i].to_dict(), min_events=ok).items():
            lb.loc[i, k] = v


# --------------------------------------------------------------------------- report


def _null_fig(res: BenchResult, name: str) -> go.Figure | None:
    null = res.perm_null.get(name)
    if null is None or not np.size(null):
        return None
    obs = float(res.leaderboard.loc[res.leaderboard["model"] == name, "delta_auc"].iloc[0])
    fig = go.Figure(go.Histogram(x=null, nbinsx=40, marker={"color": theme.NEUTRAL}, name="permutációs null"))
    fig.add_vline(x=obs, line={"color": theme.ACCENT, "width": 2}, annotation_text=f"megfigyelt ΔAUC {obs:+.4f}",
                  annotation_position="top right")
    fig.update_xaxes(title_text="ΔAUC (modell − baseline) a nap-csomagosan permutált címkéken, teljes refit")
    fig.update_yaxes(title_text="permutációk száma")
    return themed(fig, height=320, showlegend=False)


def _prefix_fig(res: BenchResult, pre_x: np.ndarray, channel: int) -> go.Figure:
    lab = res.frame["label_class"].to_numpy()
    step = rk.PREFIX_STEP
    t = (np.arange(pre_x.shape[2]) + 1) * step / 60 - 4.0              # hours relative to t0 (bar close times)
    fig = go.Figure()
    colors = {"impulse": theme.LONG, "false_start": theme.SHORT, "sideways": theme.NEUTRAL}
    for c in sb.CLASSES:
        sel = lab == c
        fig.add_trace(go.Scatter(x=t, y=pre_x[sel, channel, :].mean(axis=0), mode="lines", name=f"{c} (n={int(sel.sum())})",
                                 line={"color": colors[c], "width": 2}))
    fig.add_vline(x=0, line={"color": theme.NEUTRAL, "dash": "dash", "width": 1})
    fig.update_xaxes(title_text="óra a t0-hoz képest (a prefix vége = t_c = +4 h)")
    fig.update_yaxes(title_text=rk.CHANNELS[channel])
    return themed(fig, height=320)


def _runtime_table(res: BenchResult, perm_seconds_x1: float | None) -> pd.DataFrame:
    lb = res.leaderboard.set_index("model")
    rows = [{"tétel": f"CV (6 fold) — {m}", "másodperc": float(lb.loc[m, "fit_seconds"])} for m in lb.index]
    rows.append({"tétel": f"teljes benchmark futás (CV + {res.run['n_perm']} permutáció baseline + H2, bootstrap, SPA, szimuláció)",
                 "másodperc": float(res.run["seconds"])})
    if perm_seconds_x1 is not None:
        n = int(np.size(res.perm_null.get(rk.INCEPTION_NAME, [])))
        rows.append({"tétel": f"InceptionTime permutációs null (N = {n}, párhuzamos)", "másodperc": perm_seconds_x1})
    return pd.DataFrame(rows)


def _f(v: Any, d: int = 4) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return str(v)
    return "n/a" if np.isnan(x) else f"{x:.{d}f}"


def build(res: BenchResult, root: Path | None = None, *, perm_seconds_x1: float | None = None,
          prefix_x: np.ndarray | None = None) -> Path:
    lb = res.leaderboard
    run = res.run
    fr = res.frame
    rows = {r["model"]: r for _, r in lb.iterrows()}
    base, rkt, inc = rows[sb.BASELINE_NAME], rows[rk.ROCKET_NAME], rows[rk.INCEPTION_NAME]
    n, n_pos = fr.height, int(run["n_impulse"])
    n_x1 = int(np.size(res.perm_null.get(rk.INCEPTION_NAME, [])))
    rb = ReportBuilder(
        scope="general", slug=SLUG, order=35, root=root,
        title="Impulse Scout — Modell 2: MiniRocket (H2) + InceptionTime a nyers prefixen",
        summary=(f"epic_073 t933: MiniRocket ΔAUC {_f(rkt['delta_auc'])} [{_f(rkt['ci_lo'])}; {_f(rkt['ci_hi'])}], perm. p "
                 f"{_f(rkt['perm_p'])} (N={run['n_perm']}); InceptionTime ΔAUC {_f(inc['delta_auc'])}; baseline AUC "
                 f"{_f(base['auc'])}; {n} train esemény (SOL+BTC+ETH)."),
        tags=["scout", "epic_073", "minirocket", "inceptiontime", "leaderboard"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél\n\n"
        "**t933 (epic_073):** az Impulse Scout második modellcsaládja a `_doc_/methodology_doc/5300_impulse_scout.md` szerint: "
        "**P2 MiniRocket** (elsődleges, előre regisztrált **H2: P2 > baseline**, 1. kör, rögzített konfiguráció) és **X1 "
        "InceptionTime** (kísérleti, hipotézis nélkül), mindkettő a **nyers, normalizált prefixen**. A kérdés: a kézi skalár "
        "jellemzők (S0 + C) mellett hordoz-e a sorozat alakja többletet az impulzus előrejelzésében.\n\n"
        f"Futás `{run['run_id']}`: **{n} train esemény**, ebből **{n_pos} impulzus** ({100 * n_pos / n:.1f}%), assetek: "
        f"{run['assets']}; közös 6 blokkos purged CV (fold-hash `{run['fold_hash']}`), embargó {res.folds.embargo_min // 60} h. "
        "A számok a t931 benchmark kereten (`scout_bench`, a t932 fold-rang top-q nézetével) születtek; a leaderboard "
        "elsődleges top-q nézete a **teszt-foldon belüli rangsor**.\n\n"
        "> **Holdout-védelem:** csak a train split; az 1m bar-ok SQL-szűrővel `open_time < 2026-02-01` (OOS és holdout-2 nem "
        "került memóriába); a prefix kizárólag a `t_c` előtt záruló bar-okból épül (teszttel ellenőrizve).")
    rb.add_markdown(
        "## Bemenet: a nyers prefix\n\n"
        f"A `[t0 − B, t_c − 1m]` nyitású 1m bar-ok (B = W = 4 h, 480 bar, mind `t_c`-ig zárul) **{rk.PREFIX_STEP} perces** "
        f"bar-okra összevonva (L = {480 // rk.PREFIX_STEP}), három csatorna:\n\n"
        "| csatorna | definíció | megjegyzés |\n|---|---|---|\n"
        "| `close_sigma` | `log(close / low[t0]) / σ_W` | az amplitúdó megmarad (5300: a jel a visszapattanás relatív mérete) |\n"
        "| `log_vol_rel` | `log1p(volume / (5 · vol7d_mean_1m))` | az asset saját, `t0`-ig ismert 7 napos átlagához |\n"
        "| `taker_imb` | `taker_buy_base / volume − 0,5` | order flow a bar-okból (üres bar: 0) |\n\n"
        "Asset-azonosító nincs (5300). Az osztályonkénti átlagos prefix leíró jellegű (train címkék):")
    if prefix_x is not None:
        rb.add_chart("prefix_close", _prefix_fig(res, prefix_x, 0), title="Átlagos close_sigma prefix osztályonként", height=320)
        rb.add_chart("prefix_imb", _prefix_fig(res, prefix_x, 2), title="Átlagos taker_imb prefix osztályonként", height=320)
    rb.add_markdown(
        "## Modellek (rögzített konfiguráció)\n\n"
        "**P2 MiniRocket (H2):** a Dempster–Schmidt–Webb (2021) MiniRocket saját numpy-implementációja (nincs sktime / aeon "
        "függőség): 84 rögzített, 9 hosszú {−1, 2} súlyú mag × a sorozathosszból adódó dilatációk, többváltozós "
        "csatorna-kombinációk, a bias-kvantilisek a fold-train példákból (seed), PPV jellemzők (~10 000). A transzformáció "
        "**címkefüggetlen**, foldonként egyszer számolódik és cache-elődik: a permutációban a jellemzők azonosak, a címkefüggő "
        "fej újratanul (a 5300 szerinti teljes refit). Fej: ridge regresszió az egy-forró osztályokra (zárt alak a fold-train "
        "Gram-mátrix sajátfelbontásával), `alpha` pontos leave-one-out hibával egy rögzített rácson (10⁰…10⁷, 15 pont, mint a "
        "`RidgeClassifierCV`), majd multinomiális logisztikus kalibrátor (C = 1) a LOO ridge-kimeneteken → 3 osztály "
        "valószínűsége.\n\n"
        "**X1 InceptionTime (kísérleti):** kis változat (3 Inception-modul, 8 szűrő, magméretek 15 / 7 / 3, bottleneck 8, "
        "reziduális a 3. modul után, GAP + lineáris fej), Adam lr 1e-3, weight decay 1e-4, batch 128, max 25 epoch, "
        "**early stopping (türelem 4) a fold-train utolsó 20%-án (időrend)** — a teszt-fold soha. Egy háló (nincs ensemble), "
        "1 szál, determinisztikus. A méretet a CPU-futásidő határozta meg (párhuzamos agent-futások mellett), nem eredmény.\n\n"
        f"Konfiguráció-hash: MiniRocket `{rkt['config_hash']}`, InceptionTime `{inc['config_hash']}`.")
    rb.add_markdown("## 1. Leaderboard (ez a futás)\n\n"
                    "A baseline sor referencia (a közös tábla baseline-sorát a t931/t932 futás adja; ide csak a t933 sorai "
                    "kerültek perzisztálásra).")
    rb.add_table("leaderboard", _lb_table(lb), title="Leaderboard: baseline + MiniRocket + InceptionTime", columns=LB_COLUMNS)

    p_txt = (f"**perm. p = {_f(rkt['perm_p'])}** (N = {run['n_perm']}, nap-csomagos, teljes refit; Holm ebben a futásban "
             f"egyetlen elsődlegesre = {_f(rkt['holm_p'])}, a H1–H3 közös Holm-ja a t936-ban)")
    rb.add_markdown(
        "## 2. H2: MiniRocket vs baseline\n\n"
        f"Összevont OOF AUC **{_f(rkt['auc'])}** a baseline {_f(base['auc'])} mellett: ΔAUC **{_f(rkt['delta_auc'], 4)}**, "
        f"nap-klaszter bootstrap 95% CI [{_f(rkt['ci_lo'])}; {_f(rkt['ci_hi'])}]; {p_txt}. "
        f"3-osztályos log-loss {_f(rkt['logloss3'])} (Δ {_f(rkt['delta_logloss3'])}); felső 20% (fold-rang) precizitás "
        f"{_f(rkt['prec_top20'], 3)} vs baseline {_f(rkt['base_prec_top20'], 3)} (lift {_f(rkt['lift_top20'], 2)}); "
        f"ΔAUC > 0 blokkok: {int(rkt['blocks_pos'])}/{int(rkt['n_blocks'])}. Go / no-go: **{rkt['go_status']}**"
        + (f" (bukott: {rkt['go_failed']})." if str(rkt["go_failed"] or "") else "."))
    fig = _null_fig(res, rk.ROCKET_NAME)
    if fig is not None:
        rb.add_chart("null_rocket", fig, title="MiniRocket ΔAUC permutációs null (N = 1000, teljes refit)", height=320)
    rb.add_chart("blocks_rocket", _block_fig(res, rk.ROCKET_NAME), title="Blokkonkénti AUC és ΔAUC — MiniRocket", height=340)
    rb.add_table("assets_rocket", res.tables[f"assets:{rk.ROCKET_NAME}"], title="Asset-bontás — MiniRocket")

    rb.add_markdown(
        "## 3. X1: InceptionTime (kísérleti)\n\n"
        f"Összevont OOF AUC **{_f(inc['auc'])}**, ΔAUC **{_f(inc['delta_auc'])}** [{_f(inc['ci_lo'])}; {_f(inc['ci_hi'])}], "
        f"3-oszt. log-loss {_f(inc['logloss3'])} (Δ {_f(inc['delta_logloss3'])}); felső 20% precizitás "
        f"{_f(inc['prec_top20'], 3)} (lift {_f(inc['lift_top20'], 2)}); ΔAUC > 0 blokkok {int(inc['blocks_pos'])}/"
        f"{int(inc['n_blocks'])}; perm. p **{_f(inc['perm_p'])}** "
        + (f"(**csökkentett N = {n_x1}**, a minimális elérhető p = {1 / (n_x1 + 1):.4f}; kísérleti modellre Holm nem "
           "vonatkozik). " if n_x1 else "(**nem futott**: a csökkentett, N = 100-as X1 permutációt a user CPU-terhelés miatt leállította; a t936 szükség esetén újrafuttathatja — a perm-függő érték hiányzik, nem teljesült). ")
        + f"Go / no-go: **{inc['go_status']}**" + (f" (bukott: {inc['go_failed']})." if str(inc["go_failed"] or "") else "."))
    fig = _null_fig(res, rk.INCEPTION_NAME)
    if fig is not None:
        rb.add_chart("null_inception", fig, title=f"InceptionTime ΔAUC permutációs null (N = {n_x1})", height=320)
    rb.add_chart("blocks_inception", _block_fig(res, rk.INCEPTION_NAME), title="Blokkonkénti AUC és ΔAUC — InceptionTime",
                 height=340)
    rb.add_table("assets_inception", res.tables[f"assets:{rk.INCEPTION_NAME}"], title="Asset-bontás — InceptionTime")

    rb.add_markdown("## 4. Kalibráció")
    rb.add_chart("reliability_rocket", _reliability_fig(res, rk.ROCKET_NAME), title="Megbízhatósági görbe — MiniRocket", height=320)
    rb.add_chart("reliability_inception", _reliability_fig(res, rk.INCEPTION_NAME), title="Megbízhatósági görbe — InceptionTime",
                 height=320)
    cal = pd.concat([res.tables[f"calibration_asset:{m}"].assign(model=m) for m in (rk.ROCKET_NAME, rk.INCEPTION_NAME)])
    rb.add_table("calibration_asset", cal, title="Kalibráció assetenként")

    rb.add_markdown(
        "## 5. Kereskedési szimuláció (triple barrier, `config/strategies.json` költségek)\n\n"
        "Elsődleges jelzés-halmaz: felső 20% / 10% a teszt-foldon belüli rangsor szerint; a train-küszöbös nézet másodlagos. "
        "A CV-szimuláció csak előszűrő (5300 B).")
    rb.add_table("trades", _trade_table(res), title="Szimuláció: jelzés-szám, hit / stop / időkorlát, nettó hozam (bp)")

    rb.add_markdown("## 6. Futásidő")
    rb.add_table("runtime", _runtime_table(res, perm_seconds_x1), title="Futásidő (4 magos CPU, párhuzamos agent-futások mellett)")

    rb.add_markdown(_conclusion(rkt, inc, run, n_x1))
    return rb.write()


def _conclusion(rkt: Any, inc: Any, run: dict[str, Any], n_x1: int) -> str:
    sig = float(rkt["holm_p"]) < 0.01 if not np.isnan(float(rkt["holm_p"])) else False
    lines = ["## Értelmezés\n"]
    if sig and rkt["go_status"] != "NO-GO":
        lines.append(f"- **H2 elsődleges teszt: szignifikáns** (perm. p {_f(rkt['perm_p'])}), és a többi go-feltétel is teljesül.")
    elif sig:
        lines.append(f"- **H2:** a permutációs p {_f(rkt['perm_p'])} a küszöb alatt, de a go-feltételek nem teljesülnek "
                     f"({rkt['go_failed']}) → NO-GO.")
    else:
        lines.append(f"- **H2 nem igazolt:** a MiniRocket ΔAUC {_f(rkt['delta_auc'])} (CI [{_f(rkt['ci_lo'])}; "
                     f"{_f(rkt['ci_hi'])}]), perm. p {_f(rkt['perm_p'])} ≥ 0,01 → a nyers prefix alakja a skalár alapon túl "
                     "nem ad zajtól megkülönböztethető többletet; státusz **NO-GO** (a baseline marad).")
    if float(rkt["delta_logloss3"]) < 0:
        lines.append(f"- A MiniRocket 3-osztályos log-lossza jobb a baseline-nál (Δ {_f(rkt['delta_logloss3'])}), miközben a "
                     "bináris rangsorolás (AUC, felső 20%) nem javul: a többlet a hamis indulás / oldalazás szétválasztásából "
                     "és a kalibrációból jön, nem az impulzus-rangsorból, ami a H2 döntési metrikája.")
    lines.append(f"- Assetenként a ΔAUC előjele nem egységes (nem negatív: {int(rkt['assets_nonneg'])}/{int(rkt['n_assets'])}, "
                 f"legrosszabb {_f(rkt['asset_min_dauc'])}); blokkonként {int(rkt['blocks_pos'])}/{int(rkt['n_blocks'])} pozitív.")
    lines.append(f"- **InceptionTime (X1):** ΔAUC {_f(inc['delta_auc'])}, perm. p {_f(inc['perm_p']) if n_x1 else 'nem futott (user leállította, CPU)'}; "
                 f"kísérleti modell, státusz {inc['go_status']}.")
    lines.append("- A H1–H3 közös Holm-korrekcióját, a teljes modell-univerzum SPA-ját és a 2. kört a **t936** végzi; ez a "
                 "riport az 1. kör rögzített konfigurációinak eredménye, iteráció nélkül.")
    lines.append("- Holdout-2 érintetlen; az OOS sem került felhasználásra.")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or SLUG).split("\n")[0])
    ap.add_argument("--stage", choices=["bench", "perm", "final", "all"], default="all")
    ap.add_argument("--cache-dir", required=True)
    ap.add_argument("--n-perm", type=int, default=1000)
    ap.add_argument("--n-perm-x1", type=int, default=100)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--no-persist", action="store_true")
    ap.add_argument("--root", default=None)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cache = Path(a.cache_dir)
    cache.mkdir(parents=True, exist_ok=True)
    if a.stage in ("bench", "all"):
        stage_bench(cache, n_perm=a.n_perm)
    if a.stage in ("perm", "all"):
        stage_perm(cache, n_perm_x1=a.n_perm_x1, workers=a.workers)
    if a.stage in ("final", "all"):
        print(stage_final(cache, persist=not a.no_persist, root=Path(a.root) if a.root else None,
                          prefix_x=np.load(cache / "prefix.npy")))


if __name__ == "__main__":
    main()
