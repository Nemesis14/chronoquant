"""Elliott-wave test on BTC 1m (epic_080): can the 3rd / 5th wave be estimated from rule-based early setups?

Pivots come from the sigma-normalised zigzag of epic_077 (scales T = 15 ... 960 min, k in {3, 4}, rho = 0.5).  Every window of five
consecutive confirmed pivots ``low-high-low-high-low`` (p0 ... p4) is a candidate; the entry is the close of the confirmation bar of p4 (causal).

* family **A** (``p3 < p1``): nested 1-2-1-2, the next move is the 3rd wave.  Hard rules ``p2 > p0``, ``p4 > p2``.
* family **B** (``p3 > p1``): 1-2-3-4 impulse, the next move is the 5th wave.  Hard rules ``p2 > p0``, ``p4 > p2``, ``p4 > p1``, ``W3 >= W1``.
* cohort ``fib`` = hard rules + Fibonacci retracement bands, ``hard`` = hard rules only, ``control`` = windows that fail the hard rules
  (same pivots, same formulas, no Elliott structure).

Outcome: first touch of the target or the stop on the 1m closes after the entry (time limit 60 * T bars).  Benchmark for a driftless random
walk: ``p_rw = D / (U + D)`` with D = log(entry / stop), U = log(target / entry).  Statistics are on resolved events (hit or stop).

``uv run python -m reports.builders.elliott_test`` (real run) / ``reports.builders.elliott_null`` (placebo null).
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

import numpy as np
import polars as pl
from numba import njit

from reports.builders import cand_pred_features as cf
from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an

logger = logging.getLogger(__name__)

SCALES = (15, 30, 60, 120, 240, 480, 960)
KS = (3.0, 4.0)
RHO = 0.5
HMAX = 60
COST_BPS = 13.0
FIB2 = (0.382, 0.786)
FIB4_A = (0.382, 0.786)
FIB4_B = (0.236, 0.5)
EXT_A = 1.618
GAP_SHARE = 0.02
TABLE_EVENTS = "elliott_events"
TABLE_SUMMARY = "elliott_summary"
TABLE_SLICES = "elliott_slices"
TABLE_COUNTS = "elliott_counts"
FAMILIES = ("A", "B")
TARGETS = ("t1", "t2")
STOPS = ("tight", "wide")


@dataclass
class Windows:
    """Five-pivot windows of one (scale, k) cell; ``p`` holds the pivot bar indices p0 ... p4, ``conf`` the confirmation bar of p4."""

    p: np.ndarray            # (5, n)
    conf: np.ndarray         # (n,)


def zigzag_windows(logp: np.ndarray, sigma: np.ndarray, k: float, i0: int, i1: int, rho: float = RHO) -> Windows:
    piv, typ, cnf = mc._zigzag(logp, sigma, k, rho, i0, i1)
    idx = np.nonzero(typ == -1)[0]
    idx = idx[idx >= 4]
    return Windows(np.stack([piv[idx - 4 + j] for j in range(5)]), cnf[idx])


def classify(lp: np.ndarray) -> dict[str, np.ndarray]:
    """Family, cohort and ratios of windows given their pivot log-prices ``lp`` (5, n)."""
    lp0, lp1, lp2, lp3, lp4 = lp
    fam_a = lp3 < lp1
    w1 = lp1 - lp0
    w3 = lp3 - lp2
    ok = (lp2 > lp0) & (lp4 > lp2)
    hard = np.where(fam_a, ok, ok & (lp4 > lp1) & (w3 >= w1))
    with np.errstate(divide="ignore", invalid="ignore"):
        r2 = (lp1 - lp2) / w1
        r4 = (lp3 - lp4) / w3
    lo4 = np.where(fam_a, FIB4_A[0], FIB4_B[0])
    hi4 = np.where(fam_a, FIB4_A[1], FIB4_B[1])
    fib = hard & (r2 >= FIB2[0]) & (r2 <= FIB2[1]) & (r4 >= lo4) & (r4 <= hi4)
    cohort = np.where(fib, "fib", np.where(hard, "hard", "control"))
    return {"fam_a": fam_a, "cohort": cohort, "r2": r2, "r4": r4, "w3w1": w3 / w1}


def targets_stops(lp: np.ndarray, fam_a: np.ndarray) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    lp0, lp1, lp2, lp3, lp4 = lp
    tg = {"t1": np.where(fam_a, lp2 + EXT_A * (lp1 - lp0), lp4 + (lp1 - lp0)), "t2": np.where(fam_a, lp1, lp3)}
    st = {"tight": lp4, "wide": lp2}
    return tg, st


@njit(cache=True)
def _touch(logp: np.ndarray, start: np.ndarray, stop: np.ndarray, tgt: np.ndarray, hmax: int, i1: int):
    n = start.size
    code = np.zeros(n, np.int8)
    ext = np.zeros(n, np.int64)
    for j in range(n):
        end = start[j] + hmax
        full = end <= i1
        if not full:
            end = i1
        code[j] = 0 if full else 2
        ext[j] = end
        for i in range(start[j] + 1, end + 1):
            x = logp[i]
            if x <= stop[j]:
                code[j] = -1
                ext[j] = i
                break
            if x >= tgt[j]:
                code[j] = 1
                ext[j] = i
                break
    return code, ext


def cell_events(logp: np.ndarray, zero_csum: np.ndarray, sigma: np.ndarray, T: int, k: float, i0: int, i1: int) -> pl.DataFrame:
    """All (window x target x stop) events of one (scale, k) cell."""
    w = zigzag_windows(logp, sigma, k, i0, i1)
    if w.conf.size == 0:
        return pl.DataFrame()
    a = np.maximum(w.conf - 2 * T + 1, 0)
    gap = (zero_csum[w.conf + 1] - zero_csum[a]) / (w.conf - a + 1) > GAP_SHARE
    keep = ~gap
    p, conf = w.p[:, keep], w.conf[keep]
    lp = logp[p]
    c = logp[conf]
    cl = classify(lp)
    tg, st = targets_stops(lp, cl["fam_a"])
    rows = []
    for tn in TARGETS:
        for sn in STOPS:
            U, D = tg[tn] - c, c - st[sn]
            ok = (U > 0) & (D > 0)
            if not ok.any():
                continue
            code, ext = _touch(logp, conf[ok], st[sn][ok], tg[tn][ok], HMAX * T, i1)
            res = np.where(code == 1, tg[tn][ok] - c[ok], np.where(code == -1, st[sn][ok] - c[ok], logp[ext] - c[ok]))
            rows.append(pl.DataFrame({
                "scale": T, "k": k, "family": np.where(cl["fam_a"][ok], "A", "B"), "cohort": cl["cohort"][ok], "target": tn, "stop": sn,
                "conf_idx": conf[ok], "U": U[ok], "D": D[ok], "p_rw": D[ok] / (U[ok] + D[ok]), "code": code, "net_bps": res * 1e4 - COST_BPS,
                "r2": cl["r2"][ok], "r4": cl["r4"][ok], "w3w1": cl["w3w1"][ok]}))
    return pl.concat(rows) if rows else pl.DataFrame()


def build_events(logp: np.ndarray, vol: np.ndarray, m0: int, p: mc.CensusParams, i0: int, i1: int,
                 scales: tuple[int, ...] = SCALES, ks: tuple[float, ...] = KS) -> pl.DataFrame:
    zc = np.concatenate(([0], np.cumsum((vol == 0).astype(np.int64))))
    parts = []
    for T in scales:
        sigma = mc.trailing_sigma(logp, T, p.sigma_win)
        for k in ks:
            d = cell_events(logp, zc, sigma, T, k, i0, i1)
            if d.height:
                parts.append(d)
                logger.info("T=%d k=%.0f events=%d", T, k, d.height)
    ev = pl.concat(parts)
    day = (ev["conf_idx"].to_numpy() + m0) // 1440
    year = (day.astype("datetime64[D]").astype("datetime64[Y]").astype(int) + 1970)
    return ev.with_columns(pl.Series("day", day), pl.Series("year", year))


# ---------------------------------------------------------------------------------------------------------------------
# statistics
# ---------------------------------------------------------------------------------------------------------------------

def _day_sums(day: np.ndarray, x: np.ndarray, days: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    idx = np.searchsorted(days, day)
    return np.bincount(idx, x, minlength=days.size), np.bincount(idx, minlength=days.size).astype(float)


def boot_excess(day_e: np.ndarray, x_e: np.ndarray, day_c: np.ndarray, x_c: np.ndarray, n_boot: int = 1000, seed: int = 7
                ) -> dict[str, float]:
    """Day-cluster bootstrap of the mean of ``x_e`` and of the difference ``mean(x_e) - mean(x_c)``."""
    days = np.union1d(day_e, day_c)
    se, ne = _day_sums(day_e, x_e, days)
    sc, nc = _day_sums(day_c, x_c, days)
    rng = np.random.default_rng(seed)
    nd = days.size
    e_b = np.full(max(n_boot, 1), np.nan)
    d_b = np.full(max(n_boot, 1), np.nan)
    for b in range(n_boot):
        pick = rng.integers(0, nd, nd)
        me = se[pick].sum() / max(ne[pick].sum(), 1.0)
        mc_ = sc[pick].sum() / max(nc[pick].sum(), 1.0)
        e_b[b] = me
        d_b[b] = me - mc_
    me0 = float(se.sum() / ne.sum()) if ne.sum() else float("nan")
    mc0 = float(sc.sum() / nc.sum()) if nc.sum() else float("nan")
    return {"excess": me0, "ex_lo": float(np.percentile(e_b, 2.5)), "ex_hi": float(np.percentile(e_b, 97.5)),
            "delta": me0 - mc0, "d_lo": float(np.percentile(d_b, 2.5)), "d_hi": float(np.percentile(d_b, 97.5)),
            "se": float(e_b.std())}


def _mean(x: pl.Series) -> float:
    return float(np.mean(x.to_numpy()))


def variant_stats(ev: pl.DataFrame, n_boot: int = 1000) -> pl.DataFrame:
    """One row per (family, target, stop, level): resolved Elliott events vs ``p_rw`` and vs the control cohort."""
    rows = []
    res = ev.filter(pl.col("code").is_in([1, -1])).with_columns((pl.col("code") == 1).cast(pl.Float64).alias("hit"))
    for (fam, tn, sn), g in res.group_by("family", "target", "stop", maintain_order=True):
        ctrl = g.filter(pl.col("cohort") == "control")
        xc = ctrl["hit"].to_numpy() - ctrl["p_rw"].to_numpy()
        for level, cohorts in (("hard", ("fib", "hard")), ("fib", ("fib",))):
            e = g.filter(pl.col("cohort").is_in(cohorts))
            if e.height < 20 or ctrl.height < 20:
                continue
            xe = e["hit"].to_numpy() - e["p_rw"].to_numpy()
            b = boot_excess(e["day"].to_numpy(), xe, ctrl["day"].to_numpy(), xc, n_boot)
            tot = ev.filter((pl.col("family") == fam) & (pl.col("target") == tn) & (pl.col("stop") == sn) & pl.col("cohort").is_in(cohorts))
            rows.append({"family": fam, "target": tn, "stop": sn, "level": level, "n": e.height, "n_days": int(np.unique(e["day"].to_numpy()).size),
                         "timeout_share": float(np.mean((tot["code"] == 0).to_numpy())) if tot.height else float("nan"),
                         "hit": _mean(e["hit"]), "p_rw": _mean(e["p_rw"]), "n_ctrl": ctrl.height,
                         "hit_ctrl": _mean(ctrl["hit"]), "p_rw_ctrl": _mean(ctrl["p_rw"]),
                         "net_bps": _mean(e["net_bps"]), "net_bps_ctrl": _mean(ctrl["net_bps"]),
                         "mde": 2.8 * b["se"], **b})
    return pl.DataFrame(rows)


def slice_stats(ev: pl.DataFrame, key: str, family_target: tuple[tuple[str, str], ...] = (("A", "t1"), ("B", "t1")), stop: str = "tight"
                ) -> pl.DataFrame:
    """Excess over ``p_rw`` of the hard-rule Elliott events per ``key`` (scale / year), primary target and tight stop."""
    rows = []
    res = ev.filter(pl.col("code").is_in([1, -1]) & (pl.col("stop") == stop))
    for fam, tn in family_target:
        g = res.filter((pl.col("family") == fam) & (pl.col("target") == tn))
        for cohorts, name in ((("fib", "hard"), "hard"), (("fib",), "fib"), (("control",), "control")):
            for val, h in g.filter(pl.col("cohort").is_in(cohorts)).group_by(key, maintain_order=True):
                x = (h["code"].to_numpy() == 1).astype(float) - h["p_rw"].to_numpy()
                rows.append({"family": fam, "target": tn, "level": name, key: val[0], "n": h.height, "excess": float(x.mean()),
                             "net_bps": _mean(h["net_bps"])})
    return pl.DataFrame(rows).sort("family", "level", key)


def count_table(ev: pl.DataFrame) -> pl.DataFrame:
    """Setup frequency: windows per (scale, family, cohort) counted once (primary target, tight stop)."""
    d = ev.filter((pl.col("target") == "t1") & (pl.col("stop") == "tight"))
    return (d.group_by("scale", "family", "cohort")
            .agg(pl.len().alias("n"), (pl.col("code") == 1).sum().alias("hit"), (pl.col("code") == -1).sum().alias("stopped"),
                 (pl.col("code") == 0).sum().alias("timeout"), (pl.col("code") == 2).sum().alias("censored"),
                 pl.col("p_rw").mean().alias("p_rw"))
            .sort("scale", "family", "cohort"))


def lookahead_check(logp: np.ndarray, vol: np.ndarray, p: mc.CensusParams, i0: int, i1: int, T: int = 60, k: float = 4.0, cut_frac: float = 0.5
                    ) -> bool:
    """Windows with a confirmation bar <= cut are identical on the truncated and the full series."""
    sigma = mc.trailing_sigma(logp, T, p.sigma_win)
    cut = i0 + int((i1 - i0) * cut_frac)
    full = zigzag_windows(logp, sigma, k, i0, i1)
    part = zigzag_windows(logp[:cut + 1], sigma[:cut + 1], k, i0, cut)
    m = full.conf <= cut
    return bool(np.array_equal(full.p[:, m], part.p) and np.array_equal(full.conf[m], part.conf))


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    p = mc.CensusParams()
    s = cf.load_series(a.asset_id)
    i0, i1 = mc.window_bounds(mc.load_minutes(a.asset_id), p)
    logger.info("look-ahead check: %s", lookahead_check(s.logp, s.vol, p, i0, i1))
    ev = build_events(s.logp, s.vol, s.m0, p, i0, i1)
    smry = variant_stats(ev)
    an._write(a.asset_id, {TABLE_EVENTS: ev, TABLE_SUMMARY: smry, TABLE_COUNTS: count_table(ev),
                           TABLE_SLICES: pl.concat([slice_stats(ev, "scale").with_columns(pl.lit("scale").alias("by"), pl.col("scale").cast(pl.Int64).alias("val")).drop("scale"),
                                                    slice_stats(ev, "year").with_columns(pl.lit("year").alias("by"), pl.col("year").cast(pl.Int64).alias("val")).drop("year")])})
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=250):
        print(count_table(ev))
        print(smry.select("family", "target", "stop", "level", "n", "hit", "p_rw", "excess", "ex_lo", "ex_hi", "delta", "d_lo", "d_hi", "net_bps",
                          "net_bps_ctrl", "mde"))


if __name__ == "__main__":
    main()
