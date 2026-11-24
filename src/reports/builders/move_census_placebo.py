"""Placebo null of the large-move census (epic_077 / t951) — library + runner.

Same detector, same grid, same window as :mod:`reports.builders.move_census`, run on synthetic 1m series:

* ``sign``  — ``r'_t = |r_t| * s_t`` with iid fair signs ``s_t``: the 1m volatility envelope (clustering, intraday
  seasonality, jumps) is kept *exactly*, every directional dependence (trend persistence, mean reversion, leverage) is
  destroyed. Answers: "is there directional structure beyond a random walk with the same volatility path?"
* ``block`` — circular-free block bootstrap of the 1m returns with 7-day blocks (day-aligned) drawn with replacement:
  intraday and intra-week structure (incl. direction) and volatility clustering up to a week are kept, everything
  longer is shuffled. Answers: "is the multi-day structure more than a re-arrangement of weeks?"

``uv run python -m reports.builders.move_census_placebo --asset-id btcusdt --n 200``
"""

from __future__ import annotations

import argparse
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import duckdb
import numpy as np
import polars as pl

from reports.builders import move_census as mc

logger = logging.getLogger(__name__)

KINDS = ("sign", "block")
TABLE_PLACEBO = f"move_census_{mc.VERSION}_placebo"
BLOCK_DAYS = 7

_G: dict[str, Any] = {}


def placebo_logp(logp: np.ndarray, kind: str, rng: np.random.Generator, block_days: int = BLOCK_DAYS) -> np.ndarray:
    """One synthetic log-price path with the same length and start as ``logp``."""
    r = np.diff(logp)
    n = r.size
    if kind == "sign":
        r2 = np.abs(r) * rng.choice(np.array([-1.0, 1.0]), size=n)
    elif kind == "block":
        length = block_days * 1440
        n_blocks = -(-n // length)
        starts = rng.integers(0, (n - length) // 1440 + 1, n_blocks) * 1440
        r2 = np.concatenate([r[s:s + length] for s in starts])[:n]
    else:
        raise ValueError(f"unknown placebo kind {kind!r}")
    return np.concatenate(([logp[0]], logp[0] + np.cumsum(r2)))


def _init(asset_id: str, p_json: str) -> None:

    m = mc.load_minutes(asset_id)
    _G["logp"] = np.log(m.close)
    _G["m0"] = m.m0
    p = mc.CensusParams.from_json(p_json)
    _G["p"] = p
    _G["win"] = mc.window_bounds(m, p)


def _one(job: tuple[str, int, int]) -> list[dict[str, Any]]:
    kind, rep, seed = job
    rng = np.random.default_rng(seed)
    lp = placebo_logp(_G["logp"], kind, rng)
    i0, i1 = _G["win"]
    rows, _, scale_rows = mc.run_grid(lp, _G["m0"], _G["p"], i0, i1, with_moves=False)
    vr = {r["scale_min"]: r["vr"] for r in scale_rows}
    return [{"kind": kind, "rep": rep, **r, "vr": vr[r["scale_min"]]} for r in rows]


def run_placebo(asset_id: str, n: int, p: mc.CensusParams | None = None, *, kinds: tuple[str, ...] = KINDS,
                seed0: int = 20261124, workers: int = 4) -> pl.DataFrame:
    """``n`` realisations per kind. Deterministic: seed = seed0 + 1000 * kind_index + rep."""
    p = p or mc.CensusParams()
    jobs = [(kind, rep, seed0 + 1000 * ki + rep) for ki, kind in enumerate(kinds) for rep in range(n)]
    out: list[dict[str, Any]] = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=workers, initializer=_init, initargs=(asset_id, p.to_json())) as ex:
        for i, rows in enumerate(ex.map(_one, jobs, chunksize=2)):
            out.extend(rows)
            if (i + 1) % 20 == 0:
                logger.info("placebo %d / %d (%.0fs)", i + 1, len(jobs), time.time() - t0)
    return pl.DataFrame(out)


def summarize(real: pl.DataFrame, plc: pl.DataFrame, stats: tuple[str, ...] = ("n_big", "share_big", "median_dur_min",
                                                                            "er_med", "mdd_ratio_med", "vr")) -> pl.DataFrame:
    """Real vs placebo per (kind, scale, k, stat): placebo mean / 2.5 / 97.5 percentile, ratio and two-sided empirical p."""
    rows = []
    for kind in plc["kind"].unique().sort():
        for (sc, k), g in plc.filter(pl.col("kind") == kind).group_by(["scale_min", "k"], maintain_order=True):
            rr = real.filter((pl.col("scale_min") == sc) & (pl.col("k") == k))
            for st in stats:
                x = g[st].drop_nulls().to_numpy()
                obs = float(rr[st][0])
                if x.size == 0 or not np.isfinite(obs):
                    continue
                mu = float(x.mean())
                p_two = (1 + float(np.sum(np.abs(x - mu) >= abs(obs - mu)))) / (x.size + 1)
                rows.append({"kind": kind, "scale_min": sc, "k": k, "stat": st, "real": obs, "plc_mean": mu,
                             "plc_p025": float(np.percentile(x, 2.5)), "plc_p975": float(np.percentile(x, 97.5)),
                             "plc_sd": float(x.std(ddof=1)) if x.size > 1 else float("nan"),
                             "ratio": obs / mu if mu else float("nan"), "p_two": p_two, "n_rep": int(x.size)})
    return pl.DataFrame(rows).sort(["kind", "stat", "k", "scale_min"])


def persist(plc: pl.DataFrame, asset_id: str, *, tries: int = 20, delay: float = 3.0) -> None:
    import utils

    path = utils.lab_db_path(asset_id)
    last: Exception | None = None
    for attempt in range(tries):
        try:
            con = duckdb.connect(path)
        except duckdb.Error as exc:
            last = exc
            logger.warning("lab db locked (attempt %d/%d): %s", attempt + 1, tries, exc)
            time.sleep(delay)
            continue
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {mc.SCHEMA}")
            arr = plc.to_arrow()  # noqa: F841
            con.execute(f'CREATE OR REPLACE TABLE {mc.SCHEMA}."{TABLE_PLACEBO}" AS SELECT * FROM arr')
            return
        finally:
            con.close()
    raise RuntimeError(f"lab db write failed: {last}")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    plc = run_placebo(a.asset_id, a.n, workers=a.workers)
    persist(plc, a.asset_id)
    logger.info("placebo rows=%d written to %s.%s", plc.height, mc.SCHEMA, TABLE_PLACEBO)


if __name__ == "__main__":
    main()
