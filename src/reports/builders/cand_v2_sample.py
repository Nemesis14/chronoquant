"""Sample set of epic_079 / t970: all-scale candidates, clusters / weights, causal exclusions, analytic offset, targets.

A candidate is a confirmed low pivot of the sigma-normalised zigzag (epic_077 / t953) at scale T, size k = 4.  Scales
T = 15 ... 960 min are pooled (the structure was shown to be scale-free, epic_077); features are scale-free (sigma / T units).

* **Offset** — random-walk probability that a candidate with rebound ``risk_z`` (sigma units at the confirmation bar) becomes a
  big move: ``p_rw = exp(-(k - risk_z) / (rho * k))`` (P(max rises d before a drawdown D) = exp(-d / D), d = k - risk_z,
  D = rho * k).  It removes the mechanical part of the target; models are judged on what they add *beyond* it.
* **Clusters** — the same low is seen by several scales.  Candidates whose low pivots lie within ``max(3, 0.1 * T_small)`` minutes
  form one cluster; weight = 1 / cluster size, so a market situation counts once.  ``n_eff`` = number of clusters.
* **Causal exclusions** (known at the confirmation bar, applied to train and test alike): ``flash`` (V-shaped, the rebound took
  <= max(2, T/20) bars), ``gap`` (> 2% zero-volume minutes in the last 2T bars) and ``trivial`` (rebound already >= k - 0.2 sigma,
  the label is ~certain).
* **Targets**: ``y_big`` (hindsight leg >= k sigma), ``y_hit1`` (+1R before the structural stop; censored = null),
  ``fr_h05/h1/h2`` (forward return after the confirmation, in sigma_T, horizons 0.5T / 1T / 2T).

``uv run python -m reports.builders.cand_v2_sample``
"""

from __future__ import annotations

import argparse
import logging

import numpy as np
import polars as pl

from reports.builders import cand_pred_features as cf
from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an
from reports.builders import move_census_candidates as cd

logger = logging.getLogger(__name__)

SCALES = (15, 30, 60, 120, 240, 480, 960)
K = 4.0
HORIZONS = (("h05", 0.5), ("h1", 1.0), ("h2", 2.0))
TABLE_SAMPLE = "cand_v2_sample"
TABLE_SAMPLE_STATS = "cand_v2_sample_stats"
TABLE_POWER = "cand_v2_power"


def offset_prob(risk_z: np.ndarray, k: float = K, rho: float = 0.5) -> np.ndarray:
    """Random-walk P(big | rebound = risk_z sigma): exp(-(k - risk_z) / (rho * k)), capped to (0.01, 0.99)."""
    return np.clip(np.exp(-(k - risk_z) / (rho * k)), 0.01, 0.99)


def build_candidates(s: cf.Series, p: mc.CensusParams, i0: int, i1: int, scales: tuple[int, ...] = SCALES, k: float = K) -> pl.DataFrame:
    csum = np.concatenate(([0.0], np.cumsum(s.vol)))
    parts = []
    for T in scales:
        sigma = mc.trailing_sigma(s.logp, T, p.sigma_win)
        d = cd.candidates_cell(s.logp, csum, sigma, T, k, p, i0, i1)
        if d.height == 0:
            continue
        cfi = d["conf_idx"].to_numpy()
        sg = sigma[cfi]
        fr = {}
        for name, mult in HORIZONS:
            h = max(int(round(mult * T)), 1)
            end = cfi + h
            ok = end <= i1
            v = np.full(cfi.size, np.nan)
            v[ok] = (s.logp[end[ok]] - s.logp[cfi[ok]]) / sg[ok]
            fr[f"fr_{name}"] = v
        h2 = int(round(2 * T))
        zero = np.concatenate(([0], np.cumsum((s.vol == 0).astype(np.int64))))
        a = np.maximum(cfi - 2 * T + 1, 0)
        gap_share = (zero[cfi + 1] - zero[a]) / (cfi - a + 1)
        low = d["low_idx"].to_numpy()
        d = d.with_columns(
            pl.Series("sigma_conf", sg),
            pl.Series("flash", (cfi - low) <= max(2, T // 20)),
            pl.Series("gap", gap_share > 0.02),
            pl.Series("label_idx", np.maximum.reduce([d["end_conf_idx"].to_numpy(), cfi + np.maximum(d["resolve_bars"].to_numpy(), 0),
                                                     cfi + h2])),
            *[pl.Series(n, v) for n, v in fr.items()])
        parts.append(d)
        logger.info("T=%d candidates=%d", T, d.height)
    return pl.concat(parts)


def add_clusters(c: pl.DataFrame) -> pl.DataFrame:
    """Cluster id and weight: candidates of different scales whose low pivots coincide (within max(3, 0.1 * T_small) bars)."""
    low = c["low_idx"].to_numpy()
    sc = c["scale_min"].to_numpy()
    order = np.argsort(low, kind="stable")
    parent = np.arange(c.height)

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in zip(order[:-1], order[1:], strict=True):
        tol = max(3, 0.1 * min(sc[a], sc[b]))
        if sc[a] != sc[b] and low[b] - low[a] <= tol:
            ra, rb = find(int(a)), find(int(b))
            if ra != rb:
                parent[rb] = ra
    root = np.array([find(i) for i in range(c.height)])
    _, cid, cnt = np.unique(root, return_inverse=True, return_counts=True)
    return c.with_columns(pl.Series("cluster", cid), pl.Series("w", 1.0 / cnt[cid]))


def add_targets(c: pl.DataFrame) -> pl.DataFrame:
    return c.with_columns(
        pl.col("big").cast(pl.Int8).alias("y_big"),
        pl.when(pl.col("res1") == 1).then(1).when(pl.col("res1") == -1).then(0).otherwise(None).cast(pl.Int8).alias("y_hit1"),
        pl.Series("p_rw", offset_prob(c["risk_z"].to_numpy())),
        (pl.col("risk_z") >= K - 0.2).alias("trivial"),
        pl.col("conf_time").dt.year().alias("year"))


def _mean(x: pl.Series) -> float:
    return float(np.mean(x.drop_nulls().to_numpy()))


def _wmean(x: pl.Series, w: pl.Series) -> float:
    return float(np.sum(x.to_numpy() * w.to_numpy()) / np.sum(w.to_numpy()))


def sample_stats(c: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for T, g in c.group_by("scale_min", maintain_order=True):
        rows.append({"scale_min": T[0], "n": g.height, "n_clean": int(((~g["flash"]) & (~g["gap"]) & (~g["trivial"])).sum()), "flash": int(g["flash"].sum()),
                     "gap": int(g["gap"].sum()), "trivial": int(g["trivial"].sum()), "p_big": _mean(g["y_big"]), "p_rw_mean": _mean(g["p_rw"]),
                     "p_hit1": _mean(g["y_hit1"]), "n_clusters": int(g["cluster"].n_unique()),
                     "singleton_share": float(np.mean((g["w"] == 1.0).to_numpy()))})
    return pl.DataFrame(rows).sort("scale_min")


def offset_calibration(c: pl.DataFrame, bins: tuple[float, ...] = (1.4, 1.8, 2.0, 2.2, 2.5, 3.0, 4.0, 99.0)) -> pl.DataFrame:
    """Mean of ``y_big`` vs the offset per ``risk_z`` bin (clean candidates) — is the analytic baseline calibrated?"""
    d = c.filter((~pl.col("flash")) & (~pl.col("gap")))
    rz = d["risk_z"].to_numpy()
    rows = []
    for lo, hi in zip(bins[:-1], bins[1:], strict=True):
        m = (rz >= lo) & (rz < hi)
        if m.sum() < 30:
            continue
        g = d.filter(pl.Series(m))
        rows.append({"risk_z_lo": lo, "risk_z_hi": hi, "n": int(m.sum()), "p_big": _mean(g["y_big"]), "p_rw": _mean(g["p_rw"]),
                     "w_p_big": _wmean(g["y_big"], g["w"])})
    return pl.DataFrame(rows)


def power_table(c: pl.DataFrame, test_year_from: int = 2023) -> pl.DataFrame:
    """Hanley-McNeil-style AUC standard error and the minimum detectable effect (alpha 0.05 two-sided, power 0.8) for the test
    window, with the effective sample = clusters (weights sum) of the clean candidates, per target."""
    d = c.filter((~pl.col("flash")) & (~pl.col("gap")) & (~pl.col("trivial")) & (pl.col("year") >= test_year_from) & (pl.col("year") <= 2025))
    rows = []
    for t in ("y_big", "y_hit1"):
        x = d.filter(pl.col(t).is_not_null())
        n_eff = float(np.sum(x["w"].to_numpy()))
        pr = _wmean(x[t], x["w"])
        n1, n0 = n_eff * pr, n_eff * (1 - pr)
        se = float(np.sqrt((n1 + n0 + 1) / (12 * n1 * n0)))
        rows.append({"target": t, "n_rows": x.height, "n_eff": n_eff, "prevalence": pr, "auc_se_null": se, "mde_auc_80pct": 2.8 * se})
    return pl.DataFrame(rows)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    p = mc.CensusParams()
    s = cf.load_series(a.asset_id)
    i0, i1 = mc.window_bounds(mc.load_minutes(a.asset_id), p)
    c = build_candidates(s, p, i0, i1)
    c = c.with_columns(*[pl.from_epoch((pl.col(x) + s.m0) * 60, time_unit="s").cast(pl.Datetime("ms")).alias(n)
                         for x, n in (("conf_idx", "conf_time"), ("low_idx", "low_time"))])
    c = add_targets(add_clusters(c))
    st, cal, pw = sample_stats(c), offset_calibration(c), power_table(c)
    an._write(a.asset_id, {TABLE_SAMPLE: c, TABLE_SAMPLE_STATS: st, "cand_v2_offset_calibration": cal, TABLE_POWER: pw})
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=200):
        print(st)
        print(cal)
        print(pw)
        print("rows", c.height, "clusters", c["cluster"].n_unique(), "sum w", round(float(c["w"].sum()), 1))


if __name__ == "__main__":
    main()
