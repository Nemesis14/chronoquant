"""Null distributions of the whole epic_079 pipeline (t974).

Two nulls, both re-running the *complete* nested procedure (preprocessing, inner-CV config selection, fallback, ensemble), so the
distribution of "best of all learners" already contains the selection bias:

* ``perm``    — time-shift permutation of the labels: every label vector is circularly shifted in time by a random offset (30 ... 400 days)
  *within its scale*, the same offset for all scales, so the label structure (prevalence drift, serial dependence, cross-scale
  agreement) is kept while the link to the features is cut.  Cheap (no price re-simulation).
* ``placebo`` — the sign-placebo of epic_077 (|r| kept, fair random signs): price -> candidates -> features -> models, all re-run.
  Volume / flow / funding stay real but are decoupled from the synthetic direction.  Tests that the *machinery* (zigzag mechanics, offset,
  features built from a random-sign price) does not manufacture skill.

``uv run python -m reports.builders.cand_v2_null --mode perm --n 40 --targets big,fr_h1``
"""

from __future__ import annotations

import argparse
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import numpy as np
import polars as pl

from reports.builders import cand_pred_features as cf
from reports.builders import cand_v2_features as fe
from reports.builders import cand_v2_model as md
from reports.builders import cand_v2_sample as sm
from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an
from reports.builders.move_census_placebo import placebo_logp

logger = logging.getLogger(__name__)
TABLE_NULL = "cand_v2_null"
_G: dict[str, Any] = {}


def shifted_labels(data: md.Data, target: str, rng: np.random.Generator, span_days: tuple[int, int] = (30, 400)) -> np.ndarray:
    """Labels of ``target`` circularly shifted in time by a common random offset, within each scale."""
    y = data.y[target]
    out = np.full_like(y, np.nan)
    shift = int(rng.integers(span_days[0], span_days[1] + 1)) * 1440
    lo, hi = int(data.conf_idx.min()), int(data.conf_idx.max()) + 1
    for s in np.unique(data.scale):
        rows = np.nonzero(data.scale == s)[0]
        order = rows[np.argsort(data.conf_idx[rows])]
        t = data.conf_idx[order]
        tgt = lo + (t - lo + shift) % (hi - lo)
        j = np.clip(np.searchsorted(t, tgt), 0, t.size - 1)
        out[order] = y[order[j]]
    return out


def stats_rows(data: md.Data, oof: pl.DataFrame, rep: int, kind_label: str) -> list[dict[str, Any]]:
    rows = []
    for (target, lrn), g in oof.group_by(["target", "learner"], maintain_order=True):
        idx = g["row"].to_numpy()
        r = md.eval_pred(md.KIND[target], g["y"].to_numpy(), g["shift"].to_numpy(), g["base"].to_numpy(), data.w[idx])
        rows.append({"null": kind_label, "rep": rep, "target": target, "learner": lrn, **r})
    return rows


def _init_perm(asset_id: str) -> None:
    _G["data"] = md.make_data(mc.load_table(asset_id, fe.TABLE_FEATURES))


def _one_perm(job: tuple[int, int, tuple[str, ...]]) -> list[dict[str, Any]]:
    rep, seed, targets = job
    data: md.Data = _G["data"]
    rows: list[dict[str, Any]] = []
    for t in targets:
        rng = np.random.default_rng(seed * 100 + md.TARGETS.index(t))
        yo = shifted_labels(data, t, rng)
        o, _ = md.run_target(data, t, n_jobs=1, y_override=yo, seed=seed)
        rows += stats_rows(data, o, rep, "perm")
    return rows


def _init_placebo(asset_id: str) -> None:
    _G["s"] = cf.load_series(asset_id)
    p = mc.CensusParams()
    _G["p"] = p
    _G["win"] = mc.window_bounds(mc.load_minutes(asset_id), p)


def _one_placebo(job: tuple[int, int, tuple[str, ...]]) -> list[dict[str, Any]]:
    rep, seed, targets = job
    s0: cf.Series = _G["s"]
    p: mc.CensusParams = _G["p"]
    i0, i1 = _G["win"]
    lp = placebo_logp(s0.logp, "sign", np.random.default_rng(seed))
    s = cf.Series(s0.m0, lp, s0.vol, s0.quote, s0.tbuy, s0.trades, s0.fund_t, s0.fund_r)
    c = sm.build_candidates(s, p, i0, i1)
    c = c.with_columns(*[pl.from_epoch((pl.col(x) + s.m0) * 60, time_unit="s").cast(pl.Datetime("ms")).alias(n)
                         for x, n in (("conf_idx", "conf_time"), ("low_idx", "low_time"))])
    c = sm.add_targets(sm.add_clusters(c))
    tab = fe.build_features(s, p, i0, i1, c)
    data = md.make_data(tab)
    rows: list[dict[str, Any]] = []
    for t in targets:
        o, _ = md.run_target(data, t, n_jobs=1, seed=seed)
        rows += stats_rows(data, o, rep, "placebo")
    return rows


def run(mode: str, asset_id: str, n: int, targets: tuple[str, ...], workers: int, seed0: int = 20261210) -> pl.DataFrame:
    jobs = [(rep, seed0 + rep, targets) for rep in range(n)]
    init, fn = (_init_perm, _one_perm) if mode == "perm" else (_init_placebo, _one_placebo)
    out: list[dict[str, Any]] = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=workers, initializer=init, initargs=(asset_id,)) as ex:
        for i, rows in enumerate(ex.map(fn, jobs, chunksize=1)):
            out += rows
            logger.info("%s %d / %d (%.0fs)", mode, i + 1, n, time.time() - t0)
    return pl.DataFrame(out)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--mode", choices=("perm", "placebo"), default="perm")
    ap.add_argument("--n", type=int, default=40)
    ap.add_argument("--targets", default="big,fr_h1")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--table", default=None)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    df = run(a.mode, a.asset_id, a.n, tuple(a.targets.split(",")), a.workers)
    an._write(a.asset_id, {a.table or f"{TABLE_NULL}_{a.mode}": df})
    print(df.group_by("target", "learner").agg(pl.col("gain").mean().alias("gain"), pl.col("ic").mean().alias("ic")))


if __name__ == "__main__":
    main()
