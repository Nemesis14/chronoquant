# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false, reportOptionalSubscript=false, reportGeneralTypeIssues=false, reportReturnType=false
"""Impulse Scout leaderboard closing (epic_073 / t936): round-1 universe, round-2 trading rules, final report.

``uv run --group research python -m reports.builders.scout_leaderboard --stage all`` (stages: ``oof`` / ``eval`` /
``report`` / ``refresh``):

1. ``oof`` — recomputes the pooled OOF predictions of every round-1 model (baseline, LightGBM H1, MiniRocket H2, hazard
   H3, InceptionTime X1, Chronos X2 and X2 + S0C) on the shared folds (all deterministic), checks them against the
   stored leaderboard AUC, caches them (``data/scout_leaderboard/oof_round1.parquet``) and stores them in the lab DB
   (``research."scout_oof_v1"``). Inputs: t930 train events, train-period 1m bars (parquet cache of t935), t935
   embedding cache; nothing from OOS / holdout-2 is loaded.
2. ``eval`` — replays the stored OOF through ``scout_bench.run_benchmark`` (one run, the whole universe): universe SPA
   (binary log-loss vs baseline), joint Holm over H1-H3 (the stored N = 1000 permutation p-values), gates with
   ``gate_spa`` only for the selected best model and gate B on top 20% + 10%; round 2: the pre-registered trading
   rules (t936 ticket, :data:`ROUND2_RULES`) on the baseline score, strategy SPA over round-1 + round-2 (benchmark: no
   trade, and the baseline default rule). Persists the round-1 rows' new SPA / Holm / gates
   (``research."scout_leaderboard_v1"``) and the strategy universe (``research."scout_universe_v1"``).
3. ``report`` — ``_doc_/reports/scout_leaderboard/``; ``refresh`` — patches the SPA / Holm / gate values of the round-1
   reports (scout_lgbm, scout_rocket, scout_hazard, scout_foundation).
"""

from __future__ import annotations

import argparse
import json
import logging
import pickle
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl

from reports import ReportBuilder, theme
from reports.builders import scout_bench as sb
from reports.builders._common import source_info, themed
from reports.builders.scout_bench import metrics as mt
from reports.builders.scout_bench import rules as rl
from reports.builders.scout_bench import stats as st
from reports.builders.scout_bench import universe as uv
from reports.builders.scout_bench.runner import BenchResult

logger = logging.getLogger(__name__)

SLUG = "scout_leaderboard"
GENERATOR = "reports.builders.scout_leaderboard"
SEED = 20260101
REPO = Path(__file__).resolve().parents[3]
CACHE_DIR = REPO / "data" / "scout_leaderboard"
OOF_FILE = "oof_round1.parquet"
EVAL_FILE = "eval.pkl"
OOF_TABLE = "scout_oof_v1"
UNIVERSE_TABLE = "scout_universe_v1"
ROUND1_MODELS = ("S0C_logit", "scout_lgbm_r1", "scout_minirocket_r1", "scout_hazard_r1", "scout_inception_r1",
                 "scout_chronos_r1", "scout_chronos_s0c_r1")
#: round-1 report folders refreshed by ``--stage refresh`` (slug -> models shown there)
ROUND1_REPORTS = {"scout_lgbm": ("scout_lgbm_r1",), "scout_rocket": ("scout_minirocket_r1", "scout_inception_r1"),
                  "scout_hazard": ("scout_hazard_r1",), "scout_foundation": ("scout_chronos_r1", "scout_chronos_s0c_r1")}

# --------------------------------------------------------------------------- round 2 (pre-registered, t936 ticket)

#: the 12 pre-registered round-2 variants (t936 ticket "2. kör — ELŐZETES REGISZTRÁCIÓ", fixed before any run):
#: name -> (signal, rule); signal: (top-q fraction, a_sigma upper bound or None)
ROUND2_RULES: dict[str, tuple[tuple[float, float | None], rl.TradeRule]] = {
    "R2_01_sym1s": ((0.20, None), rl.TradeRule("R2_01_sym1s", tp=("sigma", 1.0), sl=("sigma", 1.0),
                                               description="TP +1 σ_W / SL −1 σ_W / 8 h")),
    "R2_02_tp2s_sl1s": ((0.20, None), rl.TradeRule("R2_02_tp2s_sl1s", tp=("sigma", 2.0), sl=("sigma", 1.0),
                                                   description="TP +2 σ_W / SL −1 σ_W / 8 h")),
    "R2_03_tp3s_sl15s": ((0.20, None), rl.TradeRule("R2_03_tp3s_sl15s", tp=("sigma", 3.0), sl=("sigma", 1.5),
                                                    description="TP +3 σ_W / SL −1,5 σ_W / 8 h")),
    "R2_04_atr2_1": ((0.20, None), rl.TradeRule("R2_04_atr2_1", tp=("atr", 2.0), sl=("atr", 1.0),
                                                description="TP +2 ATR / SL −1 ATR / 8 h")),
    "R2_05_atr4_2": ((0.20, None), rl.TradeRule("R2_05_atr4_2", tp=("atr", 4.0), sl=("atr", 2.0),
                                                description="TP +4 ATR / SL −2 ATR / 8 h")),
    "R2_06_partial_be": ((0.20, None), rl.TradeRule("R2_06_partial_be", partial_frac=0.5, tp1=("half", 0.0),
                                                    be_after_tp1=True,
                                                    description="50% a félúton (A/2), majd break-even stop; P* / low[t0] / 8 h")),
    "R2_07_be1s": ((0.20, None), rl.TradeRule("R2_07_be1s", be_trigger_sigma=1.0,
                                              description="P* / low[t0]; +1 σ_W után break-even stop / 8 h")),
    "R2_08_time4h": ((0.20, None), rl.TradeRule("R2_08_time4h", time_min=240, description="P* / low[t0] / 4 h")),
    "R2_09_time2h": ((0.20, None), rl.TradeRule("R2_09_time2h", time_min=120, description="P* / low[t0] / 2 h")),
    "R2_10_top20_alo": ((0.20, 2.5), rl.TradeRule("R2_10_top20_alo", description="alap szabály, felső 20% és a_sigma < 2,5")),
    "R2_11_top30": ((0.30, None), rl.TradeRule("R2_11_top30", description="alap szabály, felső 30%")),
    "R2_12_top10_tp2s_sl1s": ((0.10, None), rl.TradeRule("R2_12_top10_tp2s_sl1s", tp=("sigma", 2.0), sl=("sigma", 1.0),
                                                         description="TP +2 σ_W / SL −1 σ_W / 8 h, felső 10%")),
}
REF_STRATEGY = "S0C_logit|default|top20"


# --------------------------------------------------------------------------- inputs + OOF (stage 1)


def load_inputs() -> tuple[pl.DataFrame, dict[str, Any], dict[str, np.ndarray]]:
    """Prepared t930 train frame + train-period 1m bars / taker-buy grids (t935 parquet cache; SQL-filtered on a miss)."""
    from reports.builders import scout_models_foundation as fm
    from reports.builders import scout_models_rocket as rk

    fr = sb.prepare_frame(sb.load_scout_events())
    sb.guard_no_holdout(fr)
    bars, tbb = rk.load_bars(sorted(fr["asset_id"].unique().to_list()), cache_dir=fm.CACHE_DIR)
    return fr, bars, tbb


def _same_events(a: pl.DataFrame, b: pl.DataFrame, what: str) -> None:
    if a.height != b.height or not (a["asset_id"] == b["asset_id"]).all() or not (a["t0"] == b["t0"]).all():
        raise RuntimeError(f"{what}: event frame differs from the benchmark frame")


def compute_oof(fr: pl.DataFrame, bars: dict[str, Any], tbb: dict[str, np.ndarray], models: tuple[str, ...] = ROUND1_MODELS
                ) -> dict[str, sb.runner.ModelOutput]:
    """OOF predictions of the round-1 models on the shared folds (same seed as their runs; all deterministic)."""
    from reports.builders import scout_models_foundation as fm
    from reports.builders import scout_models_hazard as hz
    from reports.builders import scout_models_lgbm as lg
    from reports.builders import scout_models_rocket as rk

    folds = sb.make_folds(fr)
    out: dict[str, sb.runner.ModelOutput] = {}

    def cv(spec: sb.ModelSpec, frame: pl.DataFrame, prefix: Any = None) -> None:
        if spec.name not in models:
            return
        _same_events(frame, fr, spec.name)
        if sb.make_folds(frame).hash != folds.hash:
            raise RuntimeError(f"{spec.name}: fold hash differs")
        t = time.perf_counter()
        out[spec.name] = sb.cross_val(spec, frame, folds, prefix, seed=SEED, thresholds=False)
        logger.info("OOF %s: %.0f s", spec.name, time.perf_counter() - t)

    cv(sb.baseline_spec(), fr)
    if "scout_hazard_r1" in models:
        cv(hz.spec(), sb.prepare_frame(hz.load_frame()))
    if "scout_lgbm_r1" in models:
        cv(lg.SPEC, lg.build_features(fr, bars, lg.load_flow_train()))
    if {"scout_minirocket_r1", "scout_inception_r1"} & set(models):
        pre = rk.build_prefix(fr, bars, tbb)
        cv(rk.rocket_spec(), fr, pre)
        cv(rk.inception_spec(), fr, pre)
    if {"scout_chronos_r1", "scout_chronos_s0c_r1"} & set(models):
        pre1 = rk.build_prefix(fr, bars, tbb, step=fm.PREFIX_STEP)
        fe = fm.load_or_embed(fr, pre1.x, cache_dir=fm.CACHE_DIR)
        cv(fm.emb_spec(), fr, fe)
        cv(fm.emb_s0c_spec(), fr, fe)
    return out


def oof_frame(fr: pl.DataFrame, outs: Mapping[str, sb.runner.ModelOutput], fold_hash: str) -> pl.DataFrame:
    parts = []
    for name, o in outs.items():
        parts.append(pl.DataFrame({
            "model": name, "row_id": fr["row_id"], "asset_id": fr["asset_id"], "t0": fr["t0"], "cv_block": fr["cv_block"],
            "score": o.score, "p_impulse": o.proba[:, 0], "p_false_start": o.proba[:, 1], "p_sideways": o.proba[:, 2],
        }))
    return pl.concat(parts).with_columns(pl.lit(fold_hash).alias("fold_hash"))


def stored_leaderboard(asset_id: str | None = None) -> pd.DataFrame:
    """Round-1 rows of ``research."scout_leaderboard_v1"`` (read-only)."""
    import utils

    con = _connect(utils.lab_db_path(asset_id), read_only=True)
    try:
        return con.execute(f'SELECT * FROM {sb.runner.SCHEMA}."{sb.runner.LEADERBOARD_TABLE}"').df()
    finally:
        con.close()


def _connect(path: str, *, read_only: bool, tries: int = 120, delay: float = 5.0) -> Any:
    import duckdb

    last: Exception | None = None
    for attempt in range(tries):
        try:
            return duckdb.connect(path, read_only=read_only)
        except duckdb.Error as exc:                 # another process holds the lab DB: wait, never kill it
            last = exc
            logger.warning("lab db locked (attempt %d/%d): %s", attempt + 1, tries, exc)
            time.sleep(delay)
    raise RuntimeError(f"lab db unavailable: {last}")


def check_oof(fr: pl.DataFrame, outs: Mapping[str, sb.runner.ModelOutput], stored: pd.DataFrame) -> pd.DataFrame:
    """Recomputed vs stored pooled OOF AUC (the stored pooled baseline AUC comes from a model row's ``base_auc``)."""
    y = sb.frame.y_binary(fr)
    st_rows = stored[stored["model"] != sb.BASELINE_NAME].set_index("model")
    rows = []
    for name, o in outs.items():
        a = mt.auc(y, o.score)
        ref = float(st_rows["base_auc"].iloc[0]) if name == sb.BASELINE_NAME else (
            float(st_rows.loc[name, "auc"]) if name in st_rows.index else float("nan"))
        rows.append({"model": name, "auc_recomputed": a, "auc_stored": ref, "abs_diff": abs(a - ref)})
    return pd.DataFrame(rows)


def stage_oof(cache: Path = CACHE_DIR, *, persist_lab: bool = True) -> pl.DataFrame:
    cache.mkdir(parents=True, exist_ok=True)
    fr, bars, tbb = load_inputs()
    folds = sb.make_folds(fr)
    outs = compute_oof(fr, bars, tbb)
    chk = check_oof(fr, outs, stored_leaderboard())
    logger.info("OOF check:\n%s", chk.to_string())
    bad = chk[chk["abs_diff"] > 1e-4]
    if len(bad):
        raise RuntimeError(f"recomputed OOF differs from the stored leaderboard:\n{bad}")
    oof = oof_frame(fr, outs, folds.hash)
    oof.write_parquet(cache / OOF_FILE)
    chk.to_csv(cache / "oof_check.csv", index=False)
    if persist_lab:
        write_table(oof.to_pandas(), OOF_TABLE, replace=True)
    return oof


# --------------------------------------------------------------------------- stored-OOF replay


class StoredOOF(sb.ScoutModel):
    """Replays stored OOF probabilities (indexed by ``row_id``); ``fit`` is a no-op. Only for test-fold predictions:
    the train-fold (in-sample) scores of the secondary threshold view are NOT reproduced."""

    def __init__(self, proba: np.ndarray) -> None:
        self.proba = proba

    def fit(self, train: pl.DataFrame, prefix_data: Any, *, seed: int) -> None:
        return None

    def predict_proba(self, df: pl.DataFrame, prefix_data: Any) -> np.ndarray:
        return self.proba[df["row_id"].to_numpy()]


def original_specs() -> dict[str, sb.ModelSpec]:
    from reports.builders import scout_models_foundation as fm
    from reports.builders import scout_models_hazard as hz
    from reports.builders import scout_models_lgbm as lg
    from reports.builders import scout_models_rocket as rk

    specs = [lg.SPEC, rk.rocket_spec(), hz.spec(), rk.inception_spec(), fm.emb_spec(), fm.emb_s0c_spec()]
    return {s.name: s for s in specs}


def replay_registry(oof: pl.DataFrame, n: int) -> sb.ModelRegistry:
    """Baseline (real, refit: exact incl. its train-threshold view) + every stored round-1 model as a replay spec with
    the ORIGINAL name / family / hypothesis / config (same ``config_hash``)."""
    reg = sb.new_registry()
    for name, spec in original_specs().items():
        sub = oof.filter(pl.col("model") == name).sort("row_id")
        if sub.height != n:
            raise RuntimeError(f"stored OOF of {name} has {sub.height} rows, frame {n}")
        proba = sub.select("p_impulse", "p_false_start", "p_sideways").to_numpy()
        reg.register(sb.ModelSpec(name, (lambda p=proba: StoredOOF(p)), family=spec.family, hypothesis=spec.hypothesis,
                                  round=spec.round, config=spec.config, description=spec.description))
    return reg


# --------------------------------------------------------------------------- evaluation (stage 2)


@dataclass
class EvalResult:
    res: BenchResult
    leaderboard: pd.DataFrame                 # round-1 universe rows (pooled, universe SPA / joint Holm / gates)
    holm: dict[str, float]
    perm_source: dict[str, Any]
    strategies: pd.DataFrame                  # round-1 (14) + round-2 (12) strategy universe
    spa0: dict[str, Any]
    spa_ref: dict[str, Any]
    selected: str
    curves: dict[str, pd.DataFrame] = field(default_factory=dict)
    blocks: pd.DataFrame | None = None
    assets: pd.DataFrame | None = None
    oof_check: pd.DataFrame | None = None
    created: str = ""


def _signal(score: np.ndarray, blocks: np.ndarray, frac: float, a_sigma: np.ndarray | None = None,
            a_max: float | None = None) -> np.ndarray:
    sel = mt.fold_rank_select(score, blocks, frac)
    if a_max is not None and a_sigma is not None:
        sel &= a_sigma < a_max
    return sel


def _strategy_row(name: str, rnd: int, model: str, rule: str, signal: str, trades: pd.DataFrame, fr: pl.DataFrame,
                  *, n_boot: int) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    t0 = fr["t0"].to_numpy()
    blocks, assets = fr["cv_block"].to_numpy(), fr["asset_id"].to_numpy()
    rid = trades["row_id"].to_numpy(dtype=np.int64)
    s = sb.summarise(trades, t0[rid], n_boot=n_boot, seed=SEED)
    tb = pd.DataFrame({"block": blocks[rid] + 1, "asset_id": assets[rid], "net_bps": trades["net_bps"].to_numpy()})
    by_b = tb.groupby("block")["net_bps"].agg(["mean", "size"]).reindex(range(1, 7))
    by_a = tb.groupby("asset_id")["net_bps"].agg(["mean", "size"])
    row: dict[str, Any] = {"strategy": name, "round": rnd, "model": model, "rule": rule, "signal": signal, **s,
                           "blocks_pos": int((by_b["mean"] > 0).sum()), "n_blocks": int(by_b["mean"].notna().sum()),
                           "assets_pos": int((by_a["mean"] > 0).sum()), "n_assets": int(len(by_a))}
    for b, r in by_b.iterrows():
        row[f"b{b}_net_bps"] = float(r["mean"]) if pd.notna(r["mean"]) else float("nan")
    for a, r in by_a.iterrows():
        row[f"{a}_net_bps"] = float(r["mean"])
    order = np.argsort(fr["t_c"].to_numpy()[rid], kind="stable")
    curve = pd.DataFrame({"t_c": fr["t_c"].to_numpy()[rid][order], "cum_net_bps": np.cumsum(trades["net_bps"].to_numpy()[order])})
    return row, by_b.reset_index().assign(strategy=name), curve


def _econ_gates(row: Mapping[str, Any], *, selected: bool, spa_p: float) -> dict[str, Any]:
    be = row.get("break_even_hit")
    g = {"g_net": bool(row["mean_net_bps"] > 0 and row["ci_lo_bps"] > 0),
         "g_hit": bool(row["hit_rate"] > be) if be is not None and np.isfinite(be) else None,
         "g_blocks": bool(row["blocks_pos"] >= 4), "g_assets": bool(row["assets_pos"] >= 2),
         "g_spa": bool(spa_p < 0.05) if selected else None}
    ok = all(v is True for k, v in g.items() if k not in ("g_hit",)) and g["g_hit"] is not False
    g["econ_status"] = "gazdasági jelölt" if ok else "NO-GO"
    g["econ_failed"] = ",".join(k[2:] for k, v in g.items() if k.startswith("g_") and v is False)
    if g["g_spa"] is None:
        g["econ_failed"] = ",".join(x for x in (g["econ_failed"], "not_selected") if x)
    return g


def evaluate(fr: pl.DataFrame, bars: dict[str, Any], oof: pl.DataFrame, stored: pd.DataFrame, *, n_boot: int = 2000,
             spa_boot: int = 5000) -> EvalResult:
    """Round-1 universe (replayed stored OOF) + round-2 rules + strategy SPA."""
    costs = sb.load_costs()
    res = sb.run_benchmark(fr, replay_registry(oof, fr.height), bars, prepared=True, seed=SEED, n_perm=0, n_boot=n_boot,
                           spa_boot=spa_boot, costs=costs, note="t936: round-1 universe (stored OOF replay) + universe SPA")
    lb = res.leaderboard.copy()
    st_idx = stored.drop_duplicates("model", keep="last").set_index("model")
    # the replayed rows' secondary (train-threshold) view is not reproducible from OOF -> stored values
    sec_cols = [c for c in lb.columns if c.endswith("_trthr") or c.startswith(("trade20t_", "trade10t_"))
                or c == "blocks_prec_better_trthr"]
    perm_src: dict[str, Any] = {}
    for i in lb.index:
        name = lb.at[i, "model"]
        if name == sb.BASELINE_NAME:
            continue
        for c in sec_cols:
            if c in st_idx.columns and name in st_idx.index:
                lb.at[i, c] = st_idx.at[name, c]
        p = st_idx.at[name, "perm_p"] if name in st_idx.index else float("nan")
        lb.at[i, "perm_p"] = p
        perm_src[name] = {"perm_p": None if pd.isna(p) else float(p),
                          "run_id": st_idx.at[name, "run_id"] if name in st_idx.index else None}
    primaries = {lb.at[i, "model"]: lb.at[i, "perm_p"] for i in lb.index if bool(lb.at[i, "primary"])}
    holm = uv.joint_holm(primaries)
    lb = uv.apply_universe(lb, spa_p=float(res.spa["p_consistent"]), spa_best=str(res.spa["best_name"]), holm_p=holm,
                           min_events=sb.runner.min_events_ok(fr))

    # ---- strategies: round 1 (every model x top 20 / 10, default rule) + round 2 (pre-registered rules)
    blocks = fr["cv_block"].to_numpy()
    a_sigma = fr["a_sigma"].to_numpy()
    n = fr.height
    rows, curves, blk_rows, pnl = [], {}, [], {}
    for name, o in res.outputs.items():
        for q, frac in (("top20", 0.20), ("top10", 0.10)):
            sel = _signal(o.score, blocks, frac)
            sub = res.trades[res.trades["row_id"].isin(np.flatnonzero(sel))]
            key = f"{name}|default|{q}"
            r, b, c = _strategy_row(key, 1, name, "default", q, sub, fr, n_boot=n_boot)
            rows.append(r)
            blk_rows.append(b)
            curves[key] = c
            pnl[key] = uv.event_pnl(n, sub)
    base_score = res.outputs[sb.BASELINE_NAME].score
    rule_desc = {}
    for name, ((frac, a_max), rule) in ROUND2_RULES.items():
        sel = _signal(base_score, blocks, frac, a_sigma, a_max)
        sub = rl.simulate_rule(fr, bars, rule, costs, rows=np.flatnonzero(sel))
        sig = f"top{int(round(frac * 100))}" + (f"&a_sigma<{a_max:g}" if a_max is not None else "")
        r, b, c = _strategy_row(name, 2, sb.BASELINE_NAME, name, sig, sub, fr, n_boot=n_boot)
        r["tp_any_rate"] = float(sub["tp_any"].mean()) if len(sub) else float("nan")
        rows.append(r)
        blk_rows.append(b)
        curves[name] = c
        pnl[name] = uv.event_pnl(n, sub)
        rule_desc[name] = rule.description
    strat = pd.DataFrame(rows)
    strat["description"] = strat["strategy"].map(rule_desc).fillna("alap triple barrier (P* / low[t0] / 8 h)")
    t0 = fr["t0"].to_numpy()
    spa0 = uv.pnl_spa(pnl, t0, n_boot=spa_boot, seed=SEED)
    spa_ref = uv.pnl_spa({k: v for k, v in pnl.items() if k != REF_STRATEGY}, t0, benchmark=pnl[REF_STRATEGY],
                         n_boot=spa_boot, seed=SEED)
    r2 = list(ROUND2_RULES)
    selected = uv.studentized_best(pnl, t0, r2, n_boot=spa_boot, seed=SEED)
    # per-strategy studentized daily mean (vs 0) for the table
    cl = st.day_clusters(t0)
    daily = st.day_sums(np.column_stack([pnl[k] for k in strat["strategy"]]), cl.day_of, cl.n_days)
    strat["daily_mean_bps"] = daily.mean(axis=0)
    strat["selected"] = strat["strategy"] == selected
    strat["spa_p_vs0"] = float(spa0["p_consistent"])
    strat["spa_p_vsref"] = float(spa_ref["p_consistent"])
    gates = [_econ_gates(r, selected=bool(r["selected"]), spa_p=float(spa0["p_consistent"])) for _, r in strat.iterrows()]
    strat = pd.concat([strat, pd.DataFrame(gates)], axis=1)
    return EvalResult(res, lb, holm, perm_src, strat, spa0, spa_ref, selected, curves,
                      pd.concat(blk_rows, ignore_index=True), None,
                      created=datetime.now(UTC).replace(microsecond=0).isoformat())


# --------------------------------------------------------------------------- persistence


def write_table(df: pd.DataFrame, table: str, *, replace: bool, asset_id: str | None = None) -> None:
    """Replace (or append to) a lab DB research table in one transaction (retries while the DB is locked)."""
    import utils

    con = _connect(utils.lab_db_path(asset_id), read_only=False)
    try:
        arrow = pl.from_pandas(df).to_arrow()  # noqa: F841
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {sb.runner.SCHEMA}")
        con.execute("BEGIN TRANSACTION")
        if replace:
            con.execute(f'CREATE OR REPLACE TABLE {sb.runner.SCHEMA}."{table}" AS SELECT * FROM arrow')
        else:
            con.execute(f'INSERT INTO {sb.runner.SCHEMA}."{table}" BY NAME SELECT * FROM arrow')
        con.execute("COMMIT")
    finally:
        con.close()


UPDATE_COLS = ("spa_p", "spa_best", "holm_p", "gate_min_events", "gate_dauc", "gate_holm", "gate_logloss", "gate_top20",
               "gate_stability", "gate_spa", "gate_econ", "go_status", "go_failed")


def update_round1_rows(lb: pd.DataFrame, *, asset_id: str | None = None) -> int:
    """UPDATE the round-1 model rows of ``research."scout_leaderboard_v1"`` with the universe SPA / Holm / gates
    (metrics untouched; the t931 SOL baseline row stays as it is)."""
    import utils

    rows = lb[~lb["is_baseline"].astype(bool)]
    con = _connect(utils.lab_db_path(asset_id), read_only=False)
    n = 0
    try:
        con.execute("BEGIN TRANSACTION")
        tab = f'{sb.runner.SCHEMA}."{sb.runner.LEADERBOARD_TABLE}"'
        have = {r[0]: r[1] for r in con.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
            [sb.runner.SCHEMA, sb.runner.LEADERBOARD_TABLE]).fetchall()}
        for c in UPDATE_COLS:
            if c not in have:
                typ = "DOUBLE" if c.endswith("_p") else "BOOLEAN" if c.startswith(("gate_", "spa_best")) else "VARCHAR"
                con.execute(f'ALTER TABLE {tab} ADD COLUMN "{c}" {typ}')
        for _, r in rows.iterrows():
            vals = []
            for c in UPDATE_COLS:
                v = r.get(c)
                vals.append(None if v is None or (isinstance(v, float) and np.isnan(v)) else
                            bool(v) if isinstance(v, (bool, np.bool_)) else v)
            sets = ", ".join(f'"{c}" = ?' for c in UPDATE_COLS)
            con.execute(f"UPDATE {tab} SET {sets} WHERE model = ?", [*vals, r["model"]])
            n += 1
        con.execute("COMMIT")
    finally:
        con.close()
    return n


def persist_eval(ev: EvalResult) -> dict[str, Any]:
    n = update_round1_rows(ev.leaderboard)
    s = ev.strategies.copy()
    s.insert(0, "created_at", ev.created)
    s.insert(0, "run_id", ev.res.run["run_id"])
    s["spa_best_vs0"] = s["strategy"] == ev.spa0["best_name"]
    s["fold_hash"] = ev.res.run["fold_hash"]
    s["costs_json"] = ev.res.run["costs_json"]
    write_table(s.astype({c: float for c in s.columns if s[c].dtype == object and c.endswith("_bps")}), UNIVERSE_TABLE,
                replace=True)
    return {"round1_rows_updated": n, "universe_rows": len(s)}


def stage_eval(cache: Path = CACHE_DIR, *, persist_lab: bool = True, n_boot: int = 2000, spa_boot: int = 5000) -> EvalResult:
    fr, bars, _ = load_inputs()
    oof = pl.read_parquet(cache / OOF_FILE)
    if oof["fold_hash"][0] != sb.make_folds(fr).hash:
        raise RuntimeError("stored OOF fold hash differs from the current frame")
    stored = stored_leaderboard()
    ev = evaluate(fr, bars, oof, stored, n_boot=n_boot, spa_boot=spa_boot)
    chk = cache / "oof_check.csv"
    ev.oof_check = pd.read_csv(chk) if chk.exists() else None
    if persist_lab:
        logger.info("persisted %s", persist_eval(ev))
    with open(cache / EVAL_FILE, "wb") as fh:
        pickle.dump(ev, fh)
    return ev


def load_eval(cache: Path = CACHE_DIR) -> EvalResult:
    with open(cache / EVAL_FILE, "rb") as fh:
        return pickle.load(fh)


# --------------------------------------------------------------------------- report (stage 3)


def _f(v: Any, d: int = 4) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "n/a"
    return "n/a" if not np.isfinite(x) else f"{x:.{d}f}"


def _s(v: Any, d: int = 1) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "n/a"
    return "n/a" if not np.isfinite(x) else f"{x:+.{d}f}"


LB_COLS: dict[str, Any] = {
    "model": "Modell", "family": "Család", "hypothesis": "Státusz", "auc": {"label": "AUC", "digits": 4},
    "delta_auc": {"label": "ΔAUC", "digits": 4}, "ci_lo": {"label": "CI alsó", "digits": 4},
    "ci_hi": {"label": "CI felső", "digits": 4}, "delta_logloss3": {"label": "ΔLL (3 oszt.)", "digits": 4},
    "prec_top20": {"label": "Prec. 20%", "digits": 3}, "prec_top10": {"label": "Prec. 10%", "digits": 3},
    "blocks_pos": "ΔAUC>0 blokk", "assets_nonneg": "ΔAUC≥0 asset", "cal_slope": {"label": "Kal. mered.", "digits": 2},
    "perm_p": {"label": "perm. p", "digits": 4}, "holm_p": {"label": "Holm p (H1–H3)", "digits": 4},
    "dbar": {"label": "SPA napi Δ log-loss", "digits": 4}, "spa_p": {"label": "SPA p (univerzum)", "digits": 4},
    "spa_best": "SPA-legjobb", "trade20_mean_net_bps": {"label": "Nettó bp 20%", "digits": 1},
    "trade10_mean_net_bps": {"label": "Nettó bp 10%", "digits": 1}, "go_status": "Go / no-go",
    "go_failed": "Bukott feltételek",
}

GATE_NAMES = {"gate_min_events": "A1 eseményszám", "gate_dauc": "A2 ΔAUC ≥ 0,02 és CI > 0", "gate_holm": "A3 Holm p < 0,01",
              "gate_logloss": "A4 3-oszt. log-loss", "gate_top20": "A5 top 20%", "gate_stability": "A6 stabilitás",
              "gate_spa": "A7 SPA (csak a legjobb)", "gate_econ": "B gazdasági (20% és 10%)"}


def _lb_view(ev: EvalResult) -> pd.DataFrame:
    lb = ev.leaderboard.copy()
    lb["hypothesis"] = lb["hypothesis"].replace("", "kísérleti")
    lb.loc[lb["is_baseline"].astype(bool), "hypothesis"] = "baseline"
    dbar = ev.res.spa.get("dbar_by_model") or dict(zip([m for m in lb["model"] if m != sb.BASELINE_NAME],
                                                       map(float, ev.res.spa["dbar"]), strict=True))
    lb["dbar"] = lb["model"].map(dbar)
    return lb[[c for c in LB_COLS if c in lb.columns]]


def _gate_view(ev: EvalResult) -> pd.DataFrame:
    rows = []
    for _, r in ev.leaderboard.iterrows():
        if bool(r["is_baseline"]):
            continue
        d = {"model": r["model"]}
        for k, lab in GATE_NAMES.items():
            v = r.get(k)
            d[lab] = "n/a" if v is None or (isinstance(v, float) and np.isnan(v)) else ("igen" if bool(v) else "nem")
        d["Go / no-go"] = r["go_status"]
        rows.append(d)
    return pd.DataFrame(rows)


def _dauc_fig(ev: EvalResult) -> go.Figure:
    lb = ev.leaderboard[~ev.leaderboard["is_baseline"].astype(bool)].sort_values("delta_auc")
    fig = go.Figure(go.Scatter(
        x=lb["delta_auc"], y=lb["model"], mode="markers", marker={"color": theme.ACCENT, "size": 10},
        error_x={"type": "data", "symmetric": False, "array": lb["ci_hi"] - lb["delta_auc"],
                 "arrayminus": lb["delta_auc"] - lb["ci_lo"], "color": theme.NEUTRAL},
        hovertemplate="%{y}: ΔAUC %{x:.4f}<extra></extra>"))
    fig.add_vline(x=0, line={"color": theme.NEUTRAL, "dash": "dash"})
    fig.add_vline(x=0.02, line={"color": theme.LONG, "dash": "dot"}, annotation_text="go-küszöb 0,02")
    fig.update_xaxes(title_text="ΔAUC a baseline-hoz (95% nap-klaszter CI)")
    return themed(fig, height=360, showlegend=False)


def _equity_fig(ev: EvalResult, keys: list[str], title_ref: str = REF_STRATEGY) -> go.Figure:
    fig = go.Figure()
    palette = [theme.ACCENT, theme.BLUE, theme.ORANGE, theme.LONG, theme.SHORT, theme.AMBER, theme.NEUTRAL]
    for i, k in enumerate(keys):
        c = ev.curves[k]
        ref = k == title_ref
        fig.add_trace(go.Scatter(x=c["t_c"], y=c["cum_net_bps"], mode="lines", name=k.replace("S0C_logit|", "S0C "),
                                 line={"width": 3 if ref or k == ev.selected else 1.4,
                                       "color": theme.NEUTRAL if ref else palette[i % len(palette)],
                                       "dash": "dash" if ref else "solid"}))
    fig.add_hline(y=0, line={"color": theme.NEUTRAL, "width": 1})
    fig.update_yaxes(title_text="kumulált nettó bp (1 egység / jelzés)")
    return themed(fig, height=460)


def _blocks_fig(ev: EvalResult, keys: list[str]) -> go.Figure:
    b = ev.blocks
    fig = go.Figure()
    for k in keys:
        s = b[b["strategy"] == k]
        fig.add_trace(go.Bar(x=[f"B{x}" for x in s["block"]], y=s["mean"], name=k.replace("S0C_logit|", "S0C ")))
    fig.add_hline(y=0, line={"color": theme.NEUTRAL, "width": 1})
    fig.update_yaxes(title_text="átlagos nettó bp / jelzés")
    fig.update_layout(barmode="group")
    return themed(fig, height=400)


STRAT_COLS: dict[str, Any] = {
    "strategy": "Stratégia", "round": "Kör", "signal": "Jelzés", "description": "Szabály", "n": "Jelzés-szám",
    "hit_rate": {"label": "Cél", "digits": 3}, "stop_rate": {"label": "Stop", "digits": 3},
    "timeout_rate": {"label": "Időkorlát", "digits": 3}, "break_even_hit": {"label": "Break-even hit", "digits": 3},
    "mean_net_bps": {"label": "Nettó bp", "digits": 1}, "ci_lo_bps": {"label": "CI alsó", "digits": 1},
    "ci_hi_bps": {"label": "CI felső", "digits": 1}, "profit_factor": {"label": "PF", "digits": 2},
    "max_dd_bps": {"label": "Max DD bp", "digits": 0}, "blocks_pos": "Blokk > 0", "assets_pos": "Asset > 0",
    "daily_mean_bps": {"label": "Napi átl. bp", "digits": 2}, "selected": "Kiválasztott",
    "econ_status": "Státusz", "econ_failed": "Bukott",
}


def build(ev: EvalResult, root: Path | None = None) -> Path:
    lb, run, strat = ev.leaderboard, ev.res.run, ev.strategies
    base = lb[lb["is_baseline"].astype(bool)].iloc[0]
    spa = ev.res.spa
    r2 = strat[strat["round"] == 2].sort_values("mean_net_bps", ascending=False)
    sel = strat[strat["strategy"] == ev.selected].iloc[0]
    ref = strat[strat["strategy"] == REF_STRATEGY].iloc[0]
    best_net = r2.iloc[0]
    any_go = (lb["go_status"].isin(["GO-jelölt", "kísérleti jelölt"])).any()
    r2_go = (strat["econ_status"] == "gazdasági jelölt").any()
    rb = ReportBuilder(
        scope="general", slug=SLUG, order=33, root=root,
        title="Impulse Scout leaderboard — 1. kör univerzum, 2. kör kereskedési szabályok (t936)",
        summary=(f"epic_073 t936: 7 modell a közös purged CV-n ({run['n_events']} esemény); univerzum SPA p {_f(spa['p_consistent'])}, "
                 f"H1–H3 Holm min. {_f(min(ev.holm.values()))}; mind NO-GO. 2. kör (12 előre regisztrált szabály a baseline score-on): "
                 f"kiválasztott {ev.selected} {_s(sel['mean_net_bps'])} bp [{_s(sel['ci_lo_bps'])}; {_s(sel['ci_hi_bps'])}], "
                 f"stratégia-SPA p {_f(ev.spa0['p_consistent'])} → {'jelölt' if r2_go else 'NO-GO'}."),
        tags=["scout", "epic_073", "leaderboard", "spa", "holm", "trading_rules"],
        source={**source_info("analysis", GENERATOR), "kind": "analysis"},
    )
    rb.add_markdown(
        "## Cél\n\n"
        "**t936 (epic_073):** az Impulse Scout leaderboard lezárása a `_doc_/methodology_doc/5300_impulse_scout.md` szerint: "
        "(1) a teljes 1. körös modell-univerzum **együttes** többszörös-tesztelés kontrollja (Hansen SPA minden modellre egyszerre, "
        "Holm a H1–H3 elsődleges hipotézisen), a go / no-go kapuk újraszámolásával; (2) a **2. kör** — a user döntése szerint "
        "(2026-11-22) nem új modelltípus, hanem **kereskedési szabály-iteráció a baseline S0 + C score-ján**, előre regisztrált, "
        "12 változatos rácson; (3) a holdout-2 próba javaslata. Kód: `src/reports/builders/scout_leaderboard.py`, "
        "`scout_bench/universe.py`, `scout_bench/rules.py`.\n\n"
        "> **Holdout-védelem:** csak a train split (2021-01 → 2026-01) purged CV-je; az OOS (2026-02 → 07) nem szerepel, a holdout-2 "
        "(2026-08-01 →) nem töltődött be. Az 1m árak a train végén vágottak.")
    chk_txt = ""
    if ev.oof_check is not None:
        chk_txt = (f" A tárolt leaderboard AUC-kal való egyezés: max. eltérés **{ev.oof_check['abs_diff'].max():.1e}** "
                   "(a modellek determinisztikusak).")
    rb.add_markdown(
        "## Protokoll\n\n"
        f"- **OOF:** mind a 7 1. körös modell OOF predikciója újraszámolva ugyanazokon a foldokon (fold-hash `{run['fold_hash']}`, "
        f"{run['n_events']} esemény, {run['n_impulse']} impulzus, {run['assets']}), tárolva: lab DB `research.\"{OOF_TABLE}\"`.{chk_txt}\n"
        f"- **Univerzum SPA (statisztikai):** a 6 nem-baseline modell egyszerre, veszteség = eseményenkénti bináris log-loss "
        f"(baseline − modell), nap-összegzett, stacionárius bootstrap (várható blokk {run['spa_mean_block']:g} nap, {run['spa_boot']} "
        "újramintavétel). Az A7 kapu (SPA p < 0,05) **csak a kiválasztott legjobb modellre** vonatkozik; a többi modell nem lehet jelölt.\n"
        "- **Holm (H1–H3 együtt):** a tárolt, N = 1000 nap-csomagos, teljes refites permutációs p-értékekből (t932 / t933 / t934).\n"
        "- **B kapu:** a felső 20% **és** a felső 10% (fold-rang) szimuláció is: nettó > 0, CI alsó > 0, hit > break-even.\n"
        "- **2. kör:** a baseline S0 + C OOF score-ja változatlan, a szabályok nem illesztenek paramétert. Közös: belépés a `t_c`-nél nyíló "
        "1m bar nyitóján (taker + csúszás), same-bar = stop, költségek `config/strategies.json` "
        f"(`{run['costs_json']}`), funding nincs. **Stratégia-SPA:** 26 stratégia (7 modell × felső 20% / 10% alap szabállyal + 12 "
        "2. körös változat), nap-összegzett nettó bp, benchmark: **nincs kereskedés (0)**, másodlagosan a baseline alap szabálya (felső 20%).")

    # ---- 1. round-1 universe
    holm_txt = ", ".join(f"{k} {_f(v)}" for k, v in sorted(ev.holm.items()))
    rb.add_markdown(
        "## 1. Az 1. kör teljes univerzuma\n\n"
        f"Baseline (S0 + C, pooled) AUC **{_f(base['auc'])}**. Univerzum SPA: legjobb modell **{spa['best_name']}**, "
        f"**p = {_f(spa['p_consistent'])}** (alsó / felső változat {_f(spa['p_lower'])} / {_f(spa['p_upper'])}); "
        f"H1–H3 együttes Holm: {holm_txt}. "
        + ("Egyik modell sem jelölt." if not any_go else "Van jelölt: lásd a táblát."))
    rb.add_table("leaderboard", _lb_view(ev), title="1. kör leaderboard (univerzum SPA, együttes Holm, újraszámolt kapuk)",
                 columns=LB_COLS)
    rb.add_chart("dauc", _dauc_fig(ev), title="ΔAUC és 95% CI modellenként", height=360)
    rb.add_table("gates", _gate_view(ev), title="Go / no-go kapuk (5300 A1–A7 + B) modellenként")
    perm_tab = pd.DataFrame([{"model": k, "perm_p": v["perm_p"], "run_id": v["run_id"], "holm_p": ev.holm.get(k)}
                             for k, v in ev.perm_source.items()])
    rb.add_table("perm_sources", perm_tab, title="Permutációs p-értékek forrása (tárolt futások) és a H1–H3 Holm",
                 columns={"model": "Modell", "perm_p": {"label": "perm. p", "digits": 4}, "run_id": "Futás",
                          "holm_p": {"label": "Holm p", "digits": 4}})

    # ---- 2. round 2
    keys_r2 = [REF_STRATEGY, *r2["strategy"].tolist()]
    rb.add_markdown(
        "## 2. A 2. kör: kereskedési szabályok a baseline score-on\n\n"
        "A 12 változat a ticketben **a futtatás előtt** rögzített (t936 Notes, „2. kör — ELŐZETES REGISZTRÁCIÓ”). Viszonyítás: az "
        f"1. kör baseline alap szabálya felső 20%-on **{_s(ref['mean_net_bps'])} bp** [{_s(ref['ci_lo_bps'])}; {_s(ref['ci_hi_bps'])}] "
        f"(n = {int(ref['n'])}). A legnagyobb átlagú 2. körös változat **{best_net['strategy']}** {_s(best_net['mean_net_bps'])} bp "
        f"[{_s(best_net['ci_lo_bps'])}; {_s(best_net['ci_hi_bps'])}]; a rögzített szabály szerint kiválasztott (legnagyobb studentizált "
        f"napi átlag) **{ev.selected}** {_s(sel['mean_net_bps'])} bp [{_s(sel['ci_lo_bps'])}; {_s(sel['ci_hi_bps'])}], blokk > 0: "
        f"{int(sel['blocks_pos'])}/6, asset > 0: {int(sel['assets_pos'])}/3.")
    show = strat[strat["round"] == 2].sort_values("mean_net_bps", ascending=False)
    show = pd.concat([strat[strat["strategy"] == REF_STRATEGY], show])
    rb.add_table("round2", show[[c for c in STRAT_COLS if c in show.columns]], title="2. kör: szabályváltozatok (+ az 1. körös referencia)",
                 columns=STRAT_COLS)
    rb.add_chart("equity_round2", _equity_fig(ev, keys_r2), title="Equity görbék (kumulált nettó bp, időrendben) — 2. kör", height=460)
    top3 = r2["strategy"].tolist()[:3]
    rb.add_chart("blocks_round2", _blocks_fig(ev, [REF_STRATEGY, *top3]),
                 title="Blokkonkénti átlagos nettó bp: referencia + a 3 legnagyobb átlagú változat", height=400)
    asset_cols = [c for c in strat.columns if c.endswith("usdt_net_bps")]
    blk_cols = [f"b{b}_net_bps" for b in range(1, 7)]
    stab = show[["strategy", *blk_cols, *asset_cols]]
    rb.add_table("stability", stab, title="Stabilitás: átlagos nettó bp blokkonként és assetenként",
                 columns={"strategy": "Stratégia", **{c: {"label": c.replace("_net_bps", "").upper(), "digits": 1}
                                                      for c in (*blk_cols, *asset_cols)}})

    # ---- 3. strategy universe SPA
    r1s = strat[strat["round"] == 1]
    rb.add_markdown(
        "## 3. Stratégia-univerzum és SPA (1. + 2. kör)\n\n"
        f"Benchmark „nincs kereskedés”: legjobb **{ev.spa0['best_name']}**, **SPA p = {_f(ev.spa0['p_consistent'])}** "
        f"(alsó / felső {_f(ev.spa0['p_lower'])} / {_f(ev.spa0['p_upper'])}). Benchmark a baseline alap szabálya (felső 20%): legjobb "
        f"**{ev.spa_ref['best_name']}**, **SPA p = {_f(ev.spa_ref['p_consistent'])}**. Az 1. kör modelljeinek szimulációja az alap "
        "szabállyal az alábbi táblában (a B kapu bemenete).")
    rb.add_table("round1_trades", r1s[[c for c in STRAT_COLS if c in r1s.columns]],
                 title="1. kör: modellek × felső 20% / 10%, alap triple barrier", columns=STRAT_COLS)
    keys_r1 = [k for k in r1s["strategy"] if k.endswith("top20")]
    rb.add_chart("equity_round1", _equity_fig(ev, keys_r1), title="Equity görbék — 1. kör modellek, felső 20%", height=460)

    # ---- interpretation
    rb.add_markdown(
        "## Értelmezés\n\n"
        f"- **Modellek (1. kör):** az univerzum SPA (p = {_f(spa['p_consistent'])}) és a H1–H3 Holm (min. {_f(min(ev.holm.values()))}) "
        "szerint egyik modell sem jobb a skalár S0 + C baseline-nál zajtól megkülönböztethetően; a ΔAUC CI-k mind tartalmazzák a nullát. "
        "Ahol javulás van (3 osztályos log-loss: LightGBM, MiniRocket, hazard), az a hamis indulás / oldalazás arányának becsléséből "
        "jön, nem az impulzus-rangsorból. **Mind NO-GO**, a baseline marad.\n"
        f"- **Szabályok (2. kör):** a kiválasztott {ev.selected} nettó {_s(sel['mean_net_bps'])} bp / jelzés, CI "
        f"[{_s(sel['ci_lo_bps'])}; {_s(sel['ci_hi_bps'])}]; a stratégia-SPA p = {_f(ev.spa0['p_consistent'])} (vs 0) / "
        f"{_f(ev.spa_ref['p_consistent'])} (vs baseline alap szabály). "
        + ("**Gazdasági jelölt van.**" if r2_go else "**Egyik változat sem teljesíti a gazdasági jelölt feltételeit: NO-GO.**")
        + "\n- **Korlátok:** funding nincs modellezve; a 2. kör a baseline score-ra épül, így a szelekciós torzítást a 26 stratégiás SPA "
        "kezeli, de a szabály-rács a t930 leíró ismereteire támaszkodott (A-sáv szűrő); a CV-szimuláció csak előszűrő, a döntő a holdout-2.")
    return rb.write()


def build_safe(ev: EvalResult, root: Path | None = None) -> Path:
    import shutil
    import tempfile

    from reports import store

    try:
        return build(ev, root)
    except PermissionError:
        tmp = Path(tempfile.mkdtemp(prefix="scout_lb_"))
        out = build(ev, tmp)
        target = store.report_dir(store.GENERAL_OWNER, SLUG, root)
        target.mkdir(parents=True, exist_ok=True)
        shutil.copytree(out, target, dirs_exist_ok=True)
        return target


# --------------------------------------------------------------------------- refresh of the round-1 reports (stage 4)


def _fmt_gate(v: Any) -> str:
    return "n/a" if v is None or (isinstance(v, float) and np.isnan(v)) else ("igen" if bool(v) else "nem")


def patch_table(tab: dict[str, Any], lb: pd.DataFrame) -> bool:
    """Update SPA / Holm / gate / status values in one report table (JSON, report format). Handles the 3 shapes used
    by the round-1 reports: metric-rows x model-columns, model-rows x metric-columns, and the gates table."""
    rows_by = lb.set_index("model")
    keys = [c["key"] for c in tab["columns"]]
    changed = False
    upd = {"spa_p", "holm_p", "go_status", "go_failed", "spa_best"}
    if keys and keys[0] == "metric":                                   # metric x model columns
        for row in tab["rows"]:
            if row[0] in upd:
                for j, k in enumerate(keys[1:], start=1):
                    if k in rows_by.index and k != sb.BASELINE_NAME:
                        v = rows_by.at[k, row[0]]
                        row[j] = None if v is None or (isinstance(v, float) and np.isnan(v)) else (
                            float(v) if row[0].endswith("_p") else v)
                        changed = True
    elif "model" in keys:                                              # model x metric columns
        mi = keys.index("model")
        for row in tab["rows"]:
            name = row[mi]
            if name not in rows_by.index or name == sb.BASELINE_NAME:
                continue
            for j, k in enumerate(keys):
                if k in upd:
                    v = rows_by.at[name, k]
                    row[j] = None if v is None or (isinstance(v, float) and np.isnan(v)) else (
                        float(v) if k.endswith("_p") else v if not isinstance(v, np.bool_) else bool(v))
                    changed = True
    elif len(keys) == 2 and keys[0].lower().startswith("felt"):           # gates table (single model)
        models = [m for m in lb["model"] if m != sb.BASELINE_NAME]
        name = tab.get("_model") or (models[0] if len(models) == 1 else None)
        if name is None:
            return False
        for row in tab["rows"]:
            g = f"gate_{row[0]}"
            if g in rows_by.columns:
                row[1] = _fmt_gate(rows_by.at[name, g])
                changed = True
    return changed


def refresh_round1_reports(ev: EvalResult, root: Path | None = None) -> list[str]:
    """Patch the SPA / Holm / gate values of the round-1 reports in place and append a dated t936 section."""
    from reports import store

    lb = ev.leaderboard
    spa = ev.res.spa
    done = []
    for slug, models in ROUND1_REPORTS.items():
        d = store.report_dir(store.GENERAL_OWNER, slug, root)
        if not (d / "manifest.json").exists():
            continue
        sub = lb[lb["model"].isin([sb.BASELINE_NAME, *models])]
        for f in sorted((d / "tables").glob("*.json")):
            tab = json.loads(f.read_text(encoding="utf-8"))
            if len(models) == 1:
                tab["_model"] = models[0]
            ch = patch_table(tab, sub)
            tab.pop("_model", None)
            if ch:
                f.write_text(json.dumps(tab, ensure_ascii=False, indent=2), encoding="utf-8")
        md = (d / "report.md").read_text(encoding="utf-8")
        marker = "## t936 frissítés"
        if marker in md:
            md = md[:md.index(marker)].rstrip() + "\n"
        lines = []
        for m in models:
            r = lb[lb["model"] == m].iloc[0]
            holm = f", Holm p (H1–H3 együtt) **{_f(r['holm_p'])}**" if bool(r["primary"]) else ""
            lines.append(f"- `{m}`: SPA-legjobb: {'igen' if bool(r['spa_best']) else 'nem'}{holm}; go / no-go **{r['go_status']}** "
                         f"(bukott: `{r['go_failed']}`).")
        md += (f"\n{marker} (2026-11-22, leaderboard-univerzum)\n\n"
               f"Az SPA és a Holm értékek a **t936** együttes kiértékeléséből származnak (`_doc_/reports/scout_leaderboard/`): a "
               f"teljes 1. körös univerzum (6 modell a baseline ellen, bináris log-loss, nap-összegzett) SPA p = **{_f(spa['p_consistent'])}**, "
               f"legjobb modell: `{spa['best_name']}`. Az A7 kapu csak a kiválasztott legjobb modellre vonatkozik (a többinél n/a, "
               "`not_spa_best`), a B kapu a felső 20% és 10% szimulációt is nézi. A táblákban a `spa_p` / `holm_p` / kapuk ennek "
               "megfelelően frissítve; a fenti szöveg korábbi, futásonkénti SPA-értékei elavultak.\n\n" + "\n".join(lines) + "\n")
        (d / "report.md").write_text(md, encoding="utf-8")
        man = json.loads((d / "manifest.json").read_text(encoding="utf-8"))
        man["updated_at"] = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        (d / "manifest.json").write_text(json.dumps(man, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        done.append(slug)
    return done


# --------------------------------------------------------------------------- CLI


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or SLUG).split("\n")[0])
    ap.add_argument("--stage", choices=["oof", "eval", "report", "refresh", "all"], default="all")
    ap.add_argument("--cache-dir", default=str(CACHE_DIR))
    ap.add_argument("--no-persist", action="store_true")
    ap.add_argument("--n-boot", type=int, default=2000)
    ap.add_argument("--spa-boot", type=int, default=5000)
    ap.add_argument("--root", default=None, help="report root (default: _doc_/reports)")
    a = ap.parse_args()
    cache = Path(a.cache_dir)
    root = Path(a.root) if a.root else None
    if a.stage in ("oof", "all"):
        stage_oof(cache, persist_lab=not a.no_persist)
    ev = None
    if a.stage in ("eval", "all"):
        ev = stage_eval(cache, persist_lab=not a.no_persist, n_boot=a.n_boot, spa_boot=a.spa_boot)
    if a.stage in ("report", "refresh", "all"):
        ev = ev or load_eval(cache)
        if a.stage in ("report", "all"):
            print(build_safe(ev, root))
        if a.stage in ("refresh", "all"):
            print(refresh_round1_reports(ev, root))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # run through the package module so pickled results reference ``reports.builders.scout_leaderboard`` (not __main__)
    from reports.builders.scout_leaderboard import main as _main

    _main()
