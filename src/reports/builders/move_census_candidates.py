"""Denominator of the census (epic_077 / t953): causal early-impulse *candidates*, their base rate and R-multiple edge.

A **candidate** is a confirmed low pivot of the sigma-normalised zigzag of :mod:`reports.builders.move_census` at
(scale T, size k). It is known at ``confirm_idx`` (the first bar that shows a rebound of ``rho * k * sigma_T``); nothing after
that bar is used for its features. Hindsight enters only in the *labels*.

Causal state at the confirmation bar:

* ``chain`` — number of consecutive *higher lows* ending at this low (0 = a fresh low, i.e. the Scout-like "1-2" start;
  1 = after one impulse-correction cycle; >= 2 = later). Bucket ``cbucket`` = min(chain, 2).
* ``retr``  — depth of the correction just finished, vs the impulse before it.
* ``vol_ratio`` — mean volume of that correction / mean volume of the impulse before it.
* ``risk_z`` — entry risk ``(x0 - L) / sigma_T`` with ``x0`` the close of the confirmation bar, ``L`` the pivot low.

Labels (hindsight):

* ``big`` — the up-leg that starts at the pivot ends with ``net >= k * sigma_T`` (the census definition).
* first-touch R-multiples: entry ``x0`` at the confirmation bar, stop = the pivot low ``L`` (structural), target ``+n R``
  with ``R = x0 - L``, ``n in {1, 2, 3}``, close based, no time limit; unresolved at the window end = censored (dropped).
  A martingale gives ``P(+nR) = 1 / (n + 1)`` and ``E[R] = 0`` before costs.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from numba import njit

from reports.builders import move_census as mc

N_TARGETS = (1, 2, 3)
COST_BPS = 13.0          # taker in + taker out (5 + 5) + slippage in + out (1.5 + 1.5), config/strategies.json costs
E_INV = float(np.exp(-1.0))   # random-walk P(big | candidate): P(max rises 0.5k more before a 0.5k drawdown) = 1 - exp(-1) fails


@njit(cache=True)
def _first_touch(logp: np.ndarray, conf: np.ndarray, low: np.ndarray, r0: np.ndarray, i1: int, nt: int):
    """First touch of the stop (``logp <= low``) vs targets ``x0 + n * r0`` (n = 1..nt) per candidate.

    Returns ``(outcome[m, nt], bars[m])``: outcome 1 = target hit first, -1 = stop first, 0 = unresolved; ``bars`` = bars until
    the stop / last target (unresolved: -1)."""
    m = conf.size
    out = np.zeros((m, nt), np.int8)
    bars = np.full(m, -1, np.int64)
    for j in range(m):
        x0 = logp[conf[j]]
        stop = logp[low[j]]
        done = 0
        stopped = False
        i = conf[j] + 1
        while i <= i1 and not stopped and done < nt:
            x = logp[i]
            if x <= stop:
                stopped = True
                for q in range(done, nt):
                    out[j, q] = -1
                bars[j] = i - conf[j]
            else:
                while done < nt and x >= x0 + (done + 1) * r0[j]:
                    out[j, done] = 1
                    done += 1
                if done == nt:
                    bars[j] = i - conf[j]
            i += 1
    return out, bars


def candidates_cell(logp: np.ndarray, volume_csum: np.ndarray, sigma: np.ndarray, scale: int, k: float, p: mc.CensusParams,
                    i0: int, i1: int) -> pl.DataFrame:
    """All completed candidates of one (scale, k) cell."""
    piv, typ, cnf = mc._zigzag(logp, sigma, k, p.rho, i0, i1)
    n = piv.size
    if n < 3:
        return pl.DataFrame()
    jl = np.nonzero((typ[:-1] == -1) & (typ[1:] == 1))[0]          # low pivot followed by its high (completed leg)
    low_i, high_i, conf_i, hconf_i = piv[jl], piv[jl + 1], cnf[jl], cnf[jl + 1]
    sig = sigma[low_i]
    net = logp[high_i] - logp[low_i]
    all_lows = np.nonzero(typ == -1)[0]
    chain_all = np.zeros(all_lows.size, np.int64)
    lv = logp[piv[all_lows]]
    for q in range(1, all_lows.size):
        chain_all[q] = chain_all[q - 1] + 1 if lv[q] > lv[q - 1] else 0
    chain = chain_all[np.searchsorted(all_lows, jl)]
    has_prev = jl >= 2
    jp = np.where(has_prev, jl, 2)
    h_prev, l_prev = piv[jp - 1], piv[jp - 2]
    up_prev = logp[h_prev] - logp[l_prev]
    down_prev = logp[h_prev] - logp[low_i]
    retr = np.where(has_prev, down_prev / np.where(up_prev > 0, up_prev, np.nan), np.nan)

    def mean_vol(a: np.ndarray, b: np.ndarray) -> np.ndarray:
        return (volume_csum[b + 1] - volume_csum[a]) / (b - a + 1)

    vr = np.where(has_prev, mean_vol(h_prev, low_i) / np.maximum(mean_vol(l_prev, h_prev), 1e-12), np.nan)
    x0 = logp[conf_i]
    r0 = x0 - logp[low_i]
    outcome, bars = _first_touch(logp, conf_i, low_i, r0, i1, len(N_TARGETS))
    r_simple = np.expm1(r0)
    return pl.DataFrame({
        "scale_min": np.full(jl.size, scale, np.int32), "k": np.full(jl.size, k),
        "low_idx": low_i, "conf_idx": conf_i, "end_idx": high_i, "end_conf_idx": hconf_i,
        "chain": chain, "cbucket": np.minimum(chain, 2), "retr": retr, "vol_ratio": vr,
        "prev_up_z": np.where(has_prev, up_prev / sig, np.nan), "risk_z": r0 / sig, "final_z": net / sig,
        "big": (net / sig >= k), "res1": outcome[:, 0], "res2": outcome[:, 1], "res3": outcome[:, 2],
        "resolve_bars": bars, "cost_r": (COST_BPS * 1e-4) / r_simple,
    })


def candidates_grid(logp: np.ndarray, volume: np.ndarray, p: mc.CensusParams, i0: int, i1: int) -> pl.DataFrame:
    csum = np.concatenate(([0.0], np.cumsum(np.nan_to_num(volume))))
    parts = []
    for scale in p.scales:
        sigma = mc.trailing_sigma(logp, scale, p.sigma_win)
        for k in p.ks:
            d = candidates_cell(logp, csum, sigma, scale, k, p, i0, i1)
            if d.height:
                parts.append(d)
    return pl.concat(parts)


def with_returns(c: pl.DataFrame) -> pl.DataFrame:
    """Add the realised R of each target (``+n`` if hit, ``-1`` if stopped, null if censored) and the net-of-cost R."""
    cols = []
    for n in N_TARGETS:
        r = pl.col(f"res{n}")
        gross = pl.when(r == 1).then(float(n)).when(r == -1).then(-1.0).otherwise(None)
        cols += [gross.alias(f"R{n}"), (gross - pl.col("cost_r")).alias(f"Rnet{n}")]
    return c.with_columns(cols)


def summarize(c: pl.DataFrame, by: tuple[str, ...] = ("scale_min", "k", "cbucket")) -> pl.DataFrame:
    """Base rate and edge per group (``cbucket = -1`` rows pool the buckets)."""
    c = with_returns(c)
    pooled = c.with_columns(pl.lit(-1).alias("cbucket"))
    d = pl.concat([c.with_columns(pl.col("cbucket").cast(pl.Int32)), pooled.with_columns(pl.col("cbucket").cast(pl.Int32))])
    aggs = [pl.len().alias("n"), pl.col("big").mean().alias("p_big"), pl.col("final_z").median().alias("final_z_med"),
            pl.col("risk_z").median().alias("risk_z_med"), pl.col("cost_r").median().alias("cost_r_med")]
    for n in N_TARGETS:
        aggs += [pl.col(f"R{n}").count().alias(f"n_res{n}"), (pl.col(f"res{n}") == 1).sum().alias(f"hit{n}"),
                 pl.col(f"R{n}").mean().alias(f"ER{n}"), pl.col(f"Rnet{n}").mean().alias(f"ERnet{n}")]
    out = d.group_by(list(by)).agg(aggs)
    for n in N_TARGETS:
        out = out.with_columns((pl.col(f"hit{n}") / pl.col(f"n_res{n}")).alias(f"p_hit{n}"))
    return out.sort(list(by))


def cluster_ci(day: np.ndarray, x: np.ndarray, *, n_boot: int = 1000, seed: int = 7) -> tuple[float, float, float]:
    """Mean of ``x`` with a day-cluster bootstrap 95% CI (candidates of the same UTC day resampled together)."""
    ok = np.isfinite(x)
    day, x = day[ok], x[ok]
    if x.size == 0:
        return float("nan"), float("nan"), float("nan")
    u, inv = np.unique(day, return_inverse=True)
    s = np.bincount(inv, weights=x)
    cnt = np.bincount(inv).astype(np.float64)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, u.size, (n_boot, u.size))
    est = s[idx].sum(1) / cnt[idx].sum(1)
    return float(x.mean()), float(np.percentile(est, 2.5)), float(np.percentile(est, 97.5))


# --------------------------------------------------------------------------------------------------------------
# placebo + comparison
# --------------------------------------------------------------------------------------------------------------

CAND_STATS = ("p_big", "p_hit1", "p_hit2", "p_hit3", "ER1", "ER2", "ER3", "ERnet1", "ERnet2", "ERnet3")
_C: dict = {}


def _c_init(asset_id: str, p_json: str) -> None:

    m = mc.load_minutes(asset_id)
    _C.update(logp=np.log(m.close), vol=m.volume, m0=m.m0)
    _C["p"] = mc.CensusParams.from_json(p_json)
    _C["win"] = mc.window_bounds(m, _C["p"])


def _c_one(job: tuple[str, int, int]) -> pl.DataFrame:
    from reports.builders.move_census_placebo import placebo_logp

    kind, rep, seed = job
    lp = placebo_logp(_C["logp"], kind, np.random.default_rng(seed))
    i0, i1 = _C["win"]
    s = summarize(candidates_grid(lp, _C["vol"], _C["p"], i0, i1))
    return s.select(["scale_min", "k", "cbucket", "n", *CAND_STATS]).with_columns(
        pl.lit(kind).alias("kind"), pl.lit(rep).alias("rep"))


def run_placebo_candidates(asset_id: str, n: int, p: mc.CensusParams | None = None, *,
                           kinds: tuple[str, ...] = ("sign", "block"), seed0: int = 20261126, workers: int = 4) -> pl.DataFrame:
    import logging
    import time
    from concurrent.futures import ProcessPoolExecutor

    log = logging.getLogger(__name__)
    p = p or mc.CensusParams()
    jobs = [(kind, rep, seed0 + 1000 * ki + rep) for ki, kind in enumerate(kinds) for rep in range(n)]
    parts, t0 = [], time.time()
    with ProcessPoolExecutor(max_workers=workers, initializer=_c_init, initargs=(asset_id, p.to_json())) as ex:
        for i, df in enumerate(ex.map(_c_one, jobs, chunksize=1)):
            parts.append(df)
            if (i + 1) % 10 == 0:
                log.info("candidate placebo %d / %d (%.0fs)", i + 1, len(jobs), time.time() - t0)
    return pl.concat(parts)


def compare_cands(real: pl.DataFrame, plc: pl.DataFrame, stats: tuple[str, ...] = CAND_STATS) -> pl.DataFrame:
    """Real vs placebo per (kind, scale, k, cbucket, stat): placebo mean / 2.5 / 97.5 % and two-sided empirical p."""
    rows = []
    for kind in plc["kind"].unique().sort():
        pk = plc.filter(pl.col("kind") == kind)
        for key, g in pk.group_by(["scale_min", "k", "cbucket"], maintain_order=True):
            r = real.filter((pl.col("scale_min") == key[0]) & (pl.col("k") == key[1]) & (pl.col("cbucket") == key[2]))
            if r.height == 0:
                continue
            for st in stats:
                x = g[st].drop_nulls().drop_nans().to_numpy()
                obs = r[st][0]
                if x.size < 5 or obs is None or not np.isfinite(obs):
                    continue
                mu = float(x.mean())
                rows.append({"kind": kind, "scale_min": key[0], "k": key[1], "cbucket": key[2], "stat": st, "real": float(obs),
                             "plc_mean": mu, "plc_p025": float(np.percentile(x, 2.5)), "plc_p975": float(np.percentile(x, 97.5)),
                             "p_two": (1 + float(np.sum(np.abs(x - mu) >= abs(obs - mu)))) / (x.size + 1), "n_rep": int(x.size),
                             "n_real": int(r["n"][0])})
    return pl.DataFrame(rows)


# --------------------------------------------------------------------------------------------------------------
# runner
# --------------------------------------------------------------------------------------------------------------

TABLE_CAND = f"move_census_{mc.VERSION}_cand"
TABLE_CAND_SUM = f"move_census_{mc.VERSION}_cand_sum"
TABLE_CAND_PLC = f"move_census_{mc.VERSION}_cand_placebo"


def main(argv: list[str] | None = None) -> None:
    import argparse
    import logging

    from reports.builders.move_census_anatomy import _write

    ap = argparse.ArgumentParser(description="t953 candidates: --stage real | placebo")
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--stage", choices=("real", "placebo"), default="real")
    ap.add_argument("--n", type=int, default=40)
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    p = mc.CensusParams()
    if a.stage == "real":
        m = mc.load_minutes(a.asset_id)
        i0, i1 = mc.window_bounds(m, p)
        logp = np.log(m.close)
        cand = candidates_grid(logp, m.volume, p, i0, i1)
        # drift-removed variant of the real path (BTC drifted up; a martingale baseline has no drift)
        r = np.diff(logp)
        lp_dm = np.concatenate(([logp[0]], logp[0] + np.cumsum(r - r.mean())))
        cand_dm = candidates_grid(lp_dm, m.volume, p, i0, i1)
        def with_times(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns(
                *[pl.from_epoch((pl.col(c) + m.m0) * 60, time_unit="s").cast(pl.Datetime("ms")).alias(n)
                  for c, n in (("conf_idx", "conf_time"), ("low_idx", "low_time"), ("end_idx", "end_time"))])

        summ = pl.concat([summarize(cand).with_columns(pl.lit("real").alias("variant")),
                          summarize(cand_dm).with_columns(pl.lit("real_demeaned").alias("variant"))])
        _write(a.asset_id, {TABLE_CAND: with_times(cand), TABLE_CAND_SUM: summ})
        log.info("candidates=%d (demeaned %d)", cand.height, cand_dm.height)
    else:
        plc = run_placebo_candidates(a.asset_id, a.n, p, workers=a.workers)
        _write(a.asset_id, {TABLE_CAND_PLC: plc})
        log.info("candidate placebo rows=%d", plc.height)


if __name__ == "__main__":
    main()
