# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Benchmark runner: cross-validated OOF predictions, metrics, permutation / SPA, trade simulation, leaderboard persistence."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import polars as pl

from reports.builders import wave_events as we
from reports.builders.scout_bench import metrics as mt
from reports.builders.scout_bench import stats as st
from reports.builders.scout_bench import trades as tr
from reports.builders.scout_bench.folds import FoldSet, make_folds
from reports.builders.scout_bench.frame import (
    CLASSES,
    describe,
    frame_hash,
    prepare_frame,
    y_binary,
    y_class,
)
from reports.builders.scout_bench.models import ModelRegistry, ModelSpec

logger = logging.getLogger(__name__)

SEED = 20260101
SCHEMA = "research"
LEADERBOARD_TABLE = "scout_leaderboard_v1"
RUNS_TABLE = "scout_leaderboard_v1__runs"
GENERATOR = "reports.builders.scout_bench"
QUANTILES = (0.80, 0.90)             # SECONDARY top 20% / 10% view: fold-train score percentiles (5300 v1 wording)
TOPQ = {"20": 0.20, "10": 0.10}     # PRIMARY top-q view: rank inside each test fold (epic_073 decision 2026-11-21)
TOPQ_VIEW = "fold_rank"             # stored per row so the leaderboard semantics are explicit


@dataclass
class ModelOutput:
    name: str
    proba: np.ndarray            # (n, 3) OOF probabilities in CLASSES order
    score: np.ndarray            # (n,) binary impulse score
    thr20: np.ndarray            # (n,) per-event fold-train 80th percentile of the score
    thr10: np.ndarray            # (n,) per-event fold-train 90th percentile
    fit_seconds: float = 0.0


def cross_val(spec: ModelSpec, frame: pl.DataFrame, folds: FoldSet, prefix_data: Any = None, *, seed: int = SEED,
              thresholds: bool = True) -> ModelOutput:
    """OOF predictions of one model over the shared folds (every usable event is in exactly one test fold)."""
    n = frame.height
    proba = np.full((n, len(CLASSES)), np.nan)
    score = np.full(n, np.nan)
    thr20 = np.full(n, np.nan)
    thr10 = np.full(n, np.nan)
    t = time.perf_counter()
    for f in folds:
        train, test = frame[f.train_idx], frame[f.test_idx]
        model = spec.factory()
        model.fit(train, prefix_data, seed=seed)
        p = np.asarray(model.predict_proba(test, prefix_data), dtype=float)
        if p.shape != (test.height, len(CLASSES)) or not np.isfinite(p).all():
            raise ValueError(f"{spec.name}: predict_proba must return a finite ({test.height}, {len(CLASSES)}) array")
        proba[f.test_idx] = p / p.sum(axis=1, keepdims=True)
        score[f.test_idx] = model.score(proba[f.test_idx])
        if thresholds:
            s_tr = model.score(np.asarray(model.predict_proba(train, prefix_data), dtype=float))
            thr20[f.test_idx], thr10[f.test_idx] = np.quantile(s_tr, QUANTILES)
    return ModelOutput(spec.name, proba, score, thr20, thr10, time.perf_counter() - t)


# --------------------------------------------------------------------------- permutation


def permutation_null(frame: pl.DataFrame, folds: FoldSet, base: ModelSpec, specs: list[ModelSpec], bundles: st.DayBundles,
                     *, n_perm: int, seed: int = SEED, prefix_data: Any = None) -> dict[str, np.ndarray]:
    """Null of the pooled dAUC of every spec and of the baseline AUC: day-bundle label permutation, FULL refit of the
    baseline and of every model on the same folds in every permutation."""
    null = {s.name: np.zeros(n_perm) for s in specs}
    null["_auc_base"] = np.zeros(n_perm)
    t = time.perf_counter()
    for i in range(n_perm):
        rng = np.random.default_rng([seed, i])
        fp = st.permute_frame_labels(frame, st.permutation_source(frame.height, bundles, rng))
        yp = y_binary(fp)
        a = mt.auc(yp, cross_val(base, fp, folds, prefix_data, seed=seed, thresholds=False).score)
        null["_auc_base"][i] = a
        for s in specs:
            null[s.name][i] = mt.auc(yp, cross_val(s, fp, folds, prefix_data, seed=seed, thresholds=False).score) - a
        if (i + 1) % 100 == 0:
            logger.info("permutation %d/%d (%.0f s)", i + 1, n_perm, time.perf_counter() - t)
    return null


# --------------------------------------------------------------------------- evaluation


def _model_metrics(frame: pl.DataFrame, out: ModelOutput, base: ModelOutput, clusters: st.DayClusters, *, n_boot: int,
                   seed: int) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    y, cls = y_binary(frame), y_class(frame)
    blocks, assets = frame["cv_block"].to_numpy(), frame["asset_id"].to_numpy()
    is_base = out.name == base.name
    m: dict[str, Any] = {
        "auc": mt.auc(y, out.score), "base_auc": mt.auc(y, base.score),
        "logloss3": mt.logloss_3class(cls, out.proba), "base_logloss3": mt.logloss_3class(cls, base.proba),
        "logloss_bin": mt.logloss_binary(y, out.score), "base_rate": float(y.mean()),
    }
    m["delta_auc"] = m["auc"] - m["base_auc"]
    m["delta_logloss3"] = m["logloss3"] - m["base_logloss3"]
    if is_base:
        m["ci_lo"] = m["ci_hi"] = float("nan")
    else:
        m["ci_lo"], m["ci_hi"], _ = st.cluster_bootstrap(
            lambda idx: mt.auc(y[idx], out.score[idx]) - mt.auc(y[idx], base.score[idx]), clusters, n_boot=n_boot, seed=seed)
    m["topq_view"] = TOPQ_VIEW
    for q, frac in TOPQ.items():
        # primary: rank-based top-q inside every test fold (fold = CV block), pooled over the folds
        t = mt.top_q_mask(y, mt.fold_rank_select(out.score, blocks, frac))
        b = mt.top_q_mask(y, mt.fold_rank_select(base.score, blocks, frac))
        m.update({f"n_top{q}": t["n"], f"prec_top{q}": t["precision"], f"lift_top{q}": t["lift"],
                  f"base_prec_top{q}": b["precision"]})
    for q, thr, bthr in (("20", out.thr20, base.thr20), ("10", out.thr10, base.thr10)):
        # secondary: fold-train percentile threshold (the number of signals per fold is not fixed)
        t = mt.top_q(y, out.score, thr)
        b = mt.top_q(y, base.score, bthr)
        m.update({f"n_top{q}_trthr": t["n"], f"prec_top{q}_trthr": t["precision"], f"lift_top{q}_trthr": t["lift"],
                  f"base_prec_top{q}_trthr": b["precision"]})
    blk = mt.breakdown_auc(y, out.score, base.score, blocks)
    ast = mt.breakdown_auc(y, out.score, base.score, assets)
    m["blocks_pos"] = int((blk["delta_auc"] > 0).sum())
    m["n_blocks"] = int(len(blk))
    pb, pbt, prow = [], [], []
    sel_m = mt.fold_rank_select(out.score, blocks, TOPQ["20"])
    sel_b = mt.fold_rank_select(base.score, blocks, TOPQ["20"])
    for b in sorted(set(blocks.tolist())):                                   # top-20% precision per block vs baseline
        bs = blocks == b
        pm = mt.top_q_mask(y[bs], sel_m[bs])["precision"]
        pbase = mt.top_q_mask(y[bs], sel_b[bs])["precision"]
        pb.append(pm > pbase)
        pbt.append(mt.top_q(y[bs], out.score[bs], out.thr20[bs])["precision"]
                   > mt.top_q(y[bs], base.score[bs], base.thr20[bs])["precision"])
        prow.append({"block": int(b) + 1, "n": int(bs.sum()), "n_top20": int(sel_m[bs].sum()),
                     "prec_top20": pm, "base_prec_top20": pbase, "base_rate": float(y[bs].mean())})
    m["blocks_prec_better"] = int(sum(pb))
    m["blocks_prec_better_trthr"] = int(sum(pbt))
    bb = mt.best_block_removed(y, out.score, base.score, blocks)
    m["best_block"] = None if bb["best_block"] is None else int(bb["best_block"]) + 1
    m["dauc_wo_best_block_mean"], m["dauc_wo_best_block_pooled"] = bb["mean_wo_best"], bb["pooled_wo_best"]
    d_assets = ast["delta_auc"].to_numpy()
    m["assets_nonneg"] = int((d_assets >= 0).sum())
    m["n_assets"] = int(len(ast))
    m["asset_min_dauc"] = float(np.nanmin(d_assets)) if d_assets.size else float("nan")
    cal_tab, cal = mt.calibration(y, out.score)
    m["cal_slope"], m["cal_intercept"], m["brier"] = cal["slope"], cal["intercept"], cal["brier"]
    tabs = {"blocks": blk.assign(block=lambda d: d["group"].astype(int) + 1).drop(columns="group"),
            "assets": ast.rename(columns={"group": "asset_id"}), "calibration": cal_tab}
    cal_rows = []
    for a in sorted(set(assets.tolist())):
        sel = assets == a
        _, cs = mt.calibration(y[sel], out.score[sel])
        cal_rows.append({"asset_id": a, "n": int(sel.sum()), "slope": cs["slope"], "intercept": cs["intercept"],
                         "mean_pred": cs["mean_pred"], "obs_rate": cs["obs_rate"]})
    tabs["calibration_asset"] = pd.DataFrame(cal_rows)
    tabs["topq_blocks"] = pd.DataFrame(prow)
    return m, tabs


# --------------------------------------------------------------------------- go / no-go (5300 A + B)

#: 5300 minimum event counts (pooled / per asset / per block)
MIN_EVENTS = {"pooled": 1500, "pooled_impulse": 300, "asset": 300, "asset_impulse": 50, "block": 100}
GATE_KEYS = ("gate_min_events", "gate_dauc", "gate_holm", "gate_logloss", "gate_top20", "gate_stability", "gate_spa",
             "gate_econ")


def min_events_ok(frame: pl.DataFrame) -> bool:
    """5300 go-criterion A1 on the benchmark frame."""
    y = y_binary(frame)
    if frame.height < MIN_EVENTS["pooled"] or y.sum() < MIN_EVENTS["pooled_impulse"]:
        return False
    for a in set(frame["asset_id"].to_list()):
        sel = (frame["asset_id"] == a).to_numpy()
        if sel.sum() < MIN_EVENTS["asset"] or y[sel].sum() < MIN_EVENTS["asset_impulse"]:
            return False
    blk = frame["cv_block"].to_numpy()
    return all((blk == b).sum() >= MIN_EVENTS["block"] for b in set(blk.tolist()))


def _econ_ok(row: dict[str, Any], q: str) -> bool | None:
    """Gate B on one signal set (``q`` = "20" / "10"): net > 0, CI lower > 0, hit rate above break-even."""
    if f"trade{q}_mean_net_bps" not in row:
        return None

    def f(k: str) -> float:
        v = row.get(k)
        return float("nan") if v is None else float(v)

    return bool(f(f"trade{q}_mean_net_bps") > 0 and f(f"trade{q}_ci_lo_bps") > 0
                and f(f"trade{q}_hit_rate") > f(f"trade{q}_break_even_hit"))


def go_no_go(row: dict[str, Any], *, min_events: bool) -> dict[str, Any]:
    """5300 go / no-go gates of one non-baseline leaderboard row; every top-q condition uses the PRIMARY (fold-rank)
    view. ``gate_*`` = True / False (None = not applicable: Holm for exploratory models, economics without bars).

    * ``gate_spa`` (5300 A7): the SPA p applies to the SELECTED best model of the universe only (``spa_best``); for
      every other model it is None (not applicable) and the model cannot be a candidate (``go_failed`` lists
      ``not_spa_best``) — t936.
    * ``gate_econ`` (5300 B): the top 20% AND the top 10% signal sets must both pass (t936).
    """

    def f(k: str) -> float:
        v = row.get(k)
        return float("nan") if v is None else float(v)

    n_assets = int(row.get("n_assets") or 0)
    is_best = bool(row.get("spa_best"))
    e20, e10 = _econ_ok(row, "20"), _econ_ok(row, "10")
    g: dict[str, Any] = {
        "gate_min_events": bool(min_events),
        "gate_dauc": bool(f("delta_auc") >= 0.02 and f("ci_lo") > 0),
        "gate_holm": bool(f("holm_p") < 0.01) if row.get("primary") else None,
        "gate_logloss": bool(f("delta_logloss3") < 0),
        "gate_top20": bool(f("prec_top20") > f("base_prec_top20") and int(row.get("blocks_prec_better") or 0) >= 4
                           and f("lift_top20") > 1),
        "gate_stability": bool(int(row.get("blocks_pos") or 0) >= 4 and f("dauc_wo_best_block_mean") > 0.01
                               and int(row.get("assets_nonneg") or 0) >= int(np.ceil(2 * n_assets / 3))
                               and f("asset_min_dauc") >= -0.02),
        "gate_spa": bool(f("spa_p") < 0.05) if is_best else None,
        "gate_econ": None if e20 is None else bool(e20 and (e10 is None or e10)),
    }
    need = ["gate_min_events", "gate_dauc", "gate_logloss", "gate_top20", "gate_stability", "gate_spa"]
    stat_ok = all(g[k] is True for k in need) and g["gate_holm"] is not False
    if stat_ok and g["gate_econ"] is True:
        g["go_status"] = "GO-jelölt" if row.get("primary") else "kísérleti jelölt"
    else:
        g["go_status"] = "NO-GO"
    failed = [k.removeprefix("gate_") for k in (*need, "gate_holm", "gate_econ") if g[k] is False]
    if g["gate_spa"] is None:
        failed.append("not_spa_best")
    g["go_failed"] = ",".join(failed)
    return g


@dataclass
class BenchResult:
    leaderboard: pd.DataFrame
    run: dict[str, Any]
    frame: pl.DataFrame
    folds: FoldSet
    outputs: dict[str, ModelOutput]
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)       # key: "<table>:<model>"
    trades: pd.DataFrame | None = None
    perm_null: dict[str, np.ndarray] = field(default_factory=dict)
    spa: dict[str, Any] | None = None


def run_benchmark(frame: pl.DataFrame, registry: ModelRegistry, bars: dict[str, we.MinuteData] | None = None, *,
                  prefix_data: Any = None, seed: int = SEED, n_perm: int = 0, n_boot: int = 2000, spa_boot: int = 5000,
                  spa_mean_block: float = 5.0, costs: tr.Costs | None = None, bounds: list[datetime] | None = None,
                  prepared: bool = False, note: str = "") -> BenchResult:
    """Run every registered model (baseline included) on the shared folds and score it against the baseline.

    * ``n_perm > 0``: day-bundle permutation (full refit) for the primaries H1-H3 (Holm over them) and for the baseline
      AUC (sanity); ``0`` skips the test (``perm_p`` NaN).
    * the SPA runs over ALL non-baseline models of the registry (the model universe).
    * ``bars``: 1m grids per asset; without them the trade simulation is skipped.
    """
    t_start = time.perf_counter()
    fr = frame if prepared else prepare_frame(frame, bounds)
    folds = make_folds(fr, bounds)
    base_spec = registry.baseline
    others = registry.specs(include_baseline=False)
    outs = {s.name: cross_val(s, fr, folds, prefix_data, seed=seed) for s in registry.specs()}
    base = outs[base_spec.name]
    clusters = st.day_clusters(fr["t0"].to_numpy())
    y = y_binary(fr)

    perm_p: dict[str, float] = {}
    holm_p: dict[str, float] = {}
    null: dict[str, np.ndarray] = {}
    if n_perm > 0:
        primaries = [s for s in others if s.primary]
        bundles = st.day_bundles(fr["t0"].to_numpy(), fr["asset_id"].to_numpy())
        null = permutation_null(fr, folds, base_spec, primaries, bundles, n_perm=n_perm, seed=seed, prefix_data=prefix_data)
        perm_p[base_spec.name] = st.perm_p(mt.auc(y, base.score), null["_auc_base"])
        for s in primaries:
            perm_p[s.name] = st.perm_p(mt.auc(y, outs[s.name].score) - mt.auc(y, base.score), null[s.name])
        holm_p = st.holm({s.name: perm_p[s.name] for s in primaries})

    spa = None
    if others:
        lb = mt.logloss_binary_each(y, base.score)
        d = np.column_stack([lb - mt.logloss_binary_each(y, outs[s.name].score) for s in others])
        spa = st.spa_test(st.day_sums(d, clusters.day_of, clusters.n_days), n_boot=spa_boot, mean_block=spa_mean_block, seed=seed)
        spa["best_name"] = others[spa["best"]].name

    # trades: simulated once per event, the signal sets are model specific
    all_trades = None
    costs = costs or tr.load_costs()
    if bars:
        all_trades = tr.simulate(fr, bars, costs)
    t0 = fr["t0"].to_numpy()
    blocks = fr["cv_block"].to_numpy()
    min_ok = min_events_ok(fr)

    rows, tables = [], {}
    for s in registry.specs():
        o = outs[s.name]
        m, tabs = _model_metrics(fr, o, base, clusters, n_boot=n_boot, seed=seed)
        for k, v in tabs.items():
            tables[f"{k}:{s.name}"] = v
        row: dict[str, Any] = {
            "model": s.name, "family": s.family, "primary": s.primary, "hypothesis": s.hypothesis or "",
            "is_baseline": s.is_baseline, "round": s.round, "config_hash": s.config_hash, "fit_seconds": o.fit_seconds, **m,
            "perm_p": perm_p.get(s.name, float("nan")), "holm_p": holm_p.get(s.name, float("nan")),
            "perm_stat": "auc" if s.is_baseline else "delta_auc",
            "spa_p": float("nan") if spa is None or s.is_baseline else spa["p_consistent"],
            "spa_best": bool(spa is not None and not s.is_baseline and spa["best_name"] == s.name),
        }
        if all_trades is not None:
            sels = {"20": mt.fold_rank_select(o.score, blocks, TOPQ["20"]),              # primary: fold rank
                    "10": mt.fold_rank_select(o.score, blocks, TOPQ["10"]),
                    "20t": o.score >= o.thr20, "10t": o.score >= o.thr10,                 # secondary: train threshold
                    "all": np.ones(fr.height, bool)}
            for q, sig in sels.items():
                sub = all_trades[all_trades["row_id"].isin(np.flatnonzero(sig))]
                ss = tr.summarise(sub, t0[sub["row_id"].to_numpy()], n_boot=n_boot, seed=seed)
                for k, v in ss.items():
                    row[f"trade{q}_{k}"] = v
                tables[f"trades{q}:{s.name}"] = pd.DataFrame([ss])
        row.update(go_no_go(row, min_events=min_ok) if not s.is_baseline
                   else dict.fromkeys(GATE_KEYS) | {"go_status": "baseline", "go_failed": ""})
        rows.append(row)
    lb_df = pd.DataFrame(rows)

    created = datetime.now().replace(microsecond=0)
    spec_hashes = {s.name: s.config_hash for s in registry.specs()}
    run_hash = hashlib.sha256(json.dumps({"folds": folds.hash, "data": frame_hash(fr), "specs": spec_hashes, "seed": seed,
                                          "n_perm": n_perm, "n_boot": n_boot, "spa_boot": spa_boot,
                                          "costs": costs.to_json()}, sort_keys=True).encode()).hexdigest()
    run = {
        "run_id": run_hash[:12], "run_hash": run_hash, "created_at": created, "generator": GENERATOR, "note": note,
        "seed": seed, "n_perm": n_perm, "n_boot": n_boot, "spa_boot": spa_boot, "spa_mean_block": spa_mean_block,
        "fold_hash": folds.hash, "data_hash": frame_hash(fr), "embargo_min": folds.embargo_min,
        "cv_bounds": json.dumps([b.date().isoformat() for b in folds.bounds]),
        "n_events": fr.height, "n_impulse": int(y.sum()), "assets": ",".join(describe(fr)["assets"]),
        "cell_id": str(fr["cell_id"][0]) if fr.height else "", "costs_json": costs.to_json(),
        "spec_hashes": json.dumps(spec_hashes, sort_keys=True),
        "spa_p": float("nan") if spa is None else spa["p_consistent"],
        "spa_best": "" if spa is None else spa["best_name"], "git_sha": _git_sha(), "seconds": time.perf_counter() - t_start,
    }
    lb_df.insert(0, "run_id", run["run_id"])
    lb_df.insert(1, "created_at", created)
    return BenchResult(lb_df, run, fr, folds, outs, tables, all_trades, null, spa)


def _git_sha() -> str | None:
    from reports.builders._common import git_sha

    return git_sha()


# --------------------------------------------------------------------------- persistence (lab DB)


_INT_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", '"NULL"'}


def _add_missing_columns(con: Any, table: str) -> None:
    """Schema evolution: add the columns of the staged ``_stage`` relation that ``table`` does not have yet, and widen an
    integer / NULL-typed column to DOUBLE when the staged value is DOUBLE (e.g. ``spa_p`` first written as all-NULL)."""
    have = {r[0]: r[1] for r in con.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
        [SCHEMA, table]).fetchall()}
    for name, typ, *_ in con.execute("DESCRIBE _stage").fetchall():
        if name not in have:
            con.execute(f'ALTER TABLE {SCHEMA}."{table}" ADD COLUMN "{name}" {typ}')
        elif typ == "DOUBLE" and have[name] in _INT_TYPES:
            con.execute(f'ALTER TABLE {SCHEMA}."{table}" ALTER COLUMN "{name}" TYPE DOUBLE')


def persist(res: BenchResult, asset_id: str | None = None, *, tries: int = 20, delay: float = 3.0,
            lab_path: str | None = None, models: list[str] | tuple[str, ...] | None = None) -> dict[str, Any]:
    """Write this run to ``research."scout_leaderboard_v1"`` (+ ``__runs``) of the lab DB (never the live DB).

    * ``models=None``: every row of the run, replacing earlier rows of the same ``run_id``.
    * ``models=[...]``: ONLY those models' rows, replacing earlier rows of the same model names (delete + insert by
      model id), so agents benchmarking different models in parallel never clobber each other's rows. Every tried
      variant has its own name (``_r1`` / ``_r2`` ...), so the leaderboard still lists all of them (5300).

    New columns are added to an existing table (schema evolution); the write is one transaction; retries while another
    process holds the lab DB lock.
    """
    import duckdb

    import utils

    path = lab_path or utils.lab_db_path(asset_id)
    lbp = res.leaderboard
    if models is not None:
        unknown = set(models) - set(lbp["model"])
        if unknown:
            raise KeyError(f"models not in this run: {sorted(unknown)}")
        lbp = lbp[lbp["model"].isin(list(models))]
    lb = pl.from_pandas(lbp)
    # float fields (NaN included) are typed Float64 explicitly: an all-NULL value must not create an INTEGER column
    float_keys = {k: pl.Float64 for k, v in res.run.items() if isinstance(v, float)}
    runs = pl.DataFrame([{k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in res.run.items()}],
                        schema_overrides=float_keys)
    last: Exception | None = None
    for attempt in range(tries):
        try:
            con = duckdb.connect(path)
        except duckdb.Error as exc:
            last = exc
            logger.warning("lab db locked/unavailable (attempt %d/%d): %s", attempt + 1, tries, exc)
            time.sleep(delay)
            continue
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            con.execute("BEGIN TRANSACTION")
            for table, df in ((LEADERBOARD_TABLE, lb), (RUNS_TABLE, runs)):
                arrow = df.to_arrow()  # noqa: F841
                con.execute("CREATE OR REPLACE TEMP TABLE _stage AS SELECT * FROM arrow")
                con.execute(f'CREATE TABLE IF NOT EXISTS {SCHEMA}."{table}" AS SELECT * FROM _stage WHERE 1 = 0')
                _add_missing_columns(con, table)
                if table == LEADERBOARD_TABLE and models is not None:
                    con.execute(f'DELETE FROM {SCHEMA}."{table}" WHERE model IN (SELECT model FROM _stage)')
                else:
                    con.execute(f'DELETE FROM {SCHEMA}."{table}" WHERE run_id = ?', [res.run["run_id"]])
                con.execute(f'INSERT INTO {SCHEMA}."{table}" BY NAME SELECT * FROM _stage')
            con.execute("COMMIT")
            return {"lab_path": path, "leaderboard": f'{SCHEMA}."{LEADERBOARD_TABLE}"', "runs": f'{SCHEMA}."{RUNS_TABLE}"',
                    "run_id": res.run["run_id"], "attempts": attempt + 1}
        finally:
            con.close()
    raise RuntimeError(f"lab db write failed after {tries} tries: {last}")
