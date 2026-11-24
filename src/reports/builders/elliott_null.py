"""Sign-placebo null of the Elliott test (epic_080 / t982): the whole chain (sigma, zigzag, windows, rules, outcomes) is re-run on random-sign
prices (|r| kept, volume real).  Statistic per replicate: the excess hit rate over ``p_rw`` and the difference to the control cohort, per variant,
and their maximum over the 16 variants.

``uv run python -m reports.builders.elliott_null --n 40 --workers 4``
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
from reports.builders import elliott_test as et
from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an
from reports.builders.move_census_placebo import placebo_logp

logger = logging.getLogger(__name__)
TABLE_NULL = "elliott_null"
_G: dict[str, Any] = {}


def _init(asset_id: str) -> None:
    _G["s"] = cf.load_series(asset_id)
    p = mc.CensusParams()
    _G["p"] = p
    _G["win"] = mc.window_bounds(mc.load_minutes(asset_id), p)


def _one(job: tuple[int, int]) -> pl.DataFrame:
    rep, seed = job
    s: cf.Series = _G["s"]
    i0, i1 = _G["win"]
    lp = placebo_logp(s.logp, "sign", np.random.default_rng(seed))
    ev = et.build_events(lp, s.vol, s.m0, _G["p"], i0, i1)
    return et.variant_stats(ev, n_boot=0).select("family", "target", "stop", "level", "n", "hit", "p_rw", "excess", "delta", "net_bps").with_columns(
        pl.lit(rep).alias("rep"))


def run(asset_id: str, n: int, workers: int, seed0: int = 20261224) -> pl.DataFrame:
    out = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=workers, initializer=_init, initargs=(asset_id,)) as ex:
        for i, d in enumerate(ex.map(_one, [(r, seed0 + r) for r in range(n)], chunksize=1)):
            out.append(d)
            logger.info("placebo %d / %d (%.0fs)", i + 1, n, time.time() - t0)
    return pl.concat(out)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--n", type=int, default=40)
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    df = run(a.asset_id, a.n, a.workers)
    an._write(a.asset_id, {TABLE_NULL: df})
    print(df.group_by("rep").agg(pl.col("excess").max().alias("max_excess"), pl.col("delta").max().alias("max_delta")).select(
        pl.col("max_excess").mean(), pl.col("max_delta").mean()))


if __name__ == "__main__":
    main()
