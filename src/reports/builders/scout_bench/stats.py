"""Day-clustered inference of the Impulse Scout benchmark (5300 "Asset-közi függőség", "Többszörös tesztelés").

* UTC-day clusters: one day = ALL assets' events together (simultaneous events are one resampling unit).
* day-bundle permutation: whole day label bundles move between days with the same per-asset event structure
  (fallback: same total event count); the label columns (class, impulse flag, tau) move together.
* Holm correction over the pre-registered primaries.
* Hansen SPA (2005) on day-summed loss differences with the stationary bootstrap (Politis-Romano 1994).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl

from reports.builders.scout_bench.frame import LABEL_COLS

# --------------------------------------------------------------------------- day clusters + bootstrap


@dataclass
class DayClusters:
    day_of: np.ndarray                 # event position -> day code
    n_days: int
    members: list[np.ndarray]          # day code -> event positions


def day_clusters(t0: np.ndarray) -> DayClusters:
    days = np.asarray(t0).astype("datetime64[D]")
    _, code = np.unique(days, return_inverse=True)
    n = int(code.max()) + 1 if code.size else 0
    order = np.argsort(code, kind="stable")
    starts = np.flatnonzero(np.r_[True, code[order][1:] != code[order][:-1]]) if code.size else np.array([], int)
    ends = np.r_[starts[1:], code.size]
    return DayClusters(code, n, [order[s:e] for s, e in zip(starts, ends, strict=True)])


def cluster_bootstrap(stat: Callable[[np.ndarray], float], clusters: DayClusters, *, n_boot: int = 2000,
                      seed: int = 20260101, salt: int = 999_001) -> tuple[float, float, np.ndarray]:
    """Percentile 95% CI of ``stat(row_idx)`` under day resampling (days drawn with replacement). NaN draws dropped."""
    rng = np.random.default_rng([seed, salt])
    vals = []
    for _ in range(n_boot):
        pick = rng.integers(0, clusters.n_days, clusters.n_days)
        idx = np.concatenate([clusters.members[d] for d in pick])
        v = stat(idx)
        if np.isfinite(v):
            vals.append(v)
    arr = np.array(vals)
    if arr.size == 0:
        return float("nan"), float("nan"), arr
    return float(np.quantile(arr, 0.025)), float(np.quantile(arr, 0.975)), arr


# --------------------------------------------------------------------------- day-bundle permutation


@dataclass
class DayBundles:
    """Permutation units: ``groups`` = list of ``(n_days_in_group, events_per_day)`` event-position arrays; days in a
    group are exchangeable. ``fixed`` = events of days that have no exchangeable partner (labels stay)."""

    groups: list[np.ndarray]
    n_days: int
    n_fixed_days: int
    n_structure_groups: int            # groups formed by the exact per-asset structure
    n_total_groups: int                # groups formed by the total-count fallback


def day_bundles(t0: np.ndarray, asset: np.ndarray) -> DayBundles:
    """Group the days by their per-asset event counts; leftover singleton days are grouped by total count."""
    cl = day_clusters(t0)
    assets = np.unique(asset)
    keyed: dict[tuple[int, ...], list[np.ndarray]] = {}
    for pos in cl.members:
        pos = pos[np.lexsort((np.asarray(t0)[pos], asset[pos]))]     # (asset, time) order inside the day
        key = tuple(int((asset[pos] == a).sum()) for a in assets)
        keyed.setdefault(key, []).append(pos)
    groups: list[np.ndarray] = []
    leftovers: dict[int, list[np.ndarray]] = {}
    n_struct = 0
    for days in keyed.values():
        if len(days) >= 2:
            groups.append(np.stack(days))
            n_struct += 1
        else:
            leftovers.setdefault(days[0].size, []).append(days[0])
    n_total = n_fixed = 0
    for days in leftovers.values():
        if len(days) >= 2:
            groups.append(np.stack(days))
            n_total += 1
        else:
            n_fixed += 1
    return DayBundles(groups, cl.n_days, n_fixed, n_struct, n_total)


def permutation_source(n: int, bundles: DayBundles, rng: np.random.Generator) -> np.ndarray:
    """``src`` such that ``labels_perm = labels[src]``: each exchangeable day receives another day's whole label vector."""
    src = np.arange(n)
    for g in bundles.groups:
        src[g] = g[rng.permutation(g.shape[0])]
    return src


def permute_frame_labels(frame: pl.DataFrame, src: np.ndarray, cols: tuple[str, ...] = LABEL_COLS) -> pl.DataFrame:
    """Frame with the label columns moved by ``src`` (features, keys, times, trade levels stay)."""
    return frame.with_columns([pl.Series(c, frame[c].to_numpy()[src]) for c in cols if c in frame.columns])


def perm_p(observed: float, null: np.ndarray) -> float:
    """``(1 + #{null >= observed}) / (N + 1)`` (5300)."""
    return float((1 + np.sum(null >= observed)) / (null.size + 1))


def holm(pvals: dict[str, float]) -> dict[str, float]:
    """Holm step-down adjusted p-values (monotone, capped at 1)."""
    names = sorted(pvals, key=lambda k: pvals[k])
    m = len(names)
    out: dict[str, float] = {}
    run = 0.0
    for i, k in enumerate(names):
        run = max(run, min(1.0, (m - i) * pvals[k]))
        out[k] = run
    return out


# --------------------------------------------------------------------------- SPA


def stationary_indices(n: int, n_boot: int, mean_block: float, rng: np.random.Generator) -> np.ndarray:
    """Stationary-bootstrap index matrix ``(n_boot, n)`` (geometric blocks, circular)."""
    p = 1.0 / max(mean_block, 1.0)
    starts = rng.integers(0, n, (n_boot, n))
    new = rng.random((n_boot, n)) < p
    new[:, 0] = True
    pos = np.arange(n)[None, :]
    last = np.maximum.accumulate(np.where(new, pos, 0), axis=1)
    base = np.take_along_axis(starts, last, axis=1)
    return (base + pos - last) % n


def day_sums(values: np.ndarray, day_of: np.ndarray, n_days: int) -> np.ndarray:
    """Sum of per-event values (n,) or (n, K) per day -> (n_days,) / (n_days, K), days in chronological code order."""
    v = np.asarray(values, dtype=float)
    out = np.zeros((n_days,) + v.shape[1:])
    np.add.at(out, day_of, v)
    return out


def spa_test(d: np.ndarray, *, n_boot: int = 5000, mean_block: float = 5.0, seed: int = 20260101) -> dict[str, Any]:
    """Hansen SPA. ``d`` = ``(T, K)`` day-aggregated loss differences ``loss_baseline - loss_model_k``
    (positive = model better). Returns the consistent / lower / upper p-values and the best model's index."""
    d = np.asarray(d, dtype=float)
    if d.ndim == 1:
        d = d[:, None]
    t, k = d.shape
    rng = np.random.default_rng([seed, 31_337])
    dbar = d.mean(axis=0)
    idx = stationary_indices(t, n_boot, mean_block, rng)
    boot = np.empty((n_boot, k))
    for s in range(0, n_boot, 250):
        boot[s:s + 250] = d[idx[s:s + 250]].mean(axis=1)
    omega = np.sqrt(t) * boot.std(axis=0, ddof=1)
    omega = np.where(omega > 1e-12, omega, np.inf)                  # a degenerate model can never be the best
    tstat = max(0.0, float(np.max(np.sqrt(t) * dbar / omega)))
    loglog = np.sqrt(2 * np.log(np.log(max(t, 3))))
    mu_c = np.where(np.sqrt(t) * dbar / omega <= -loglog, dbar, 0.0)
    mu_l = np.minimum(dbar, 0.0)
    out: dict[str, Any] = {"t_stat": tstat, "n_days": t, "n_models": k, "best": int(np.argmax(np.sqrt(t) * dbar / omega)),
                           "dbar": dbar}
    for name, mu in (("p_consistent", mu_c), ("p_lower", mu_l), ("p_upper", np.zeros(k))):
        z = np.sqrt(t) * ((boot - dbar) + mu) / omega
        tb = np.maximum(z.max(axis=1), 0.0)
        # ``>=`` (not ``>``): with T = max(0, .) the statistic is 0 when no model beats the baseline, and every bootstrap
        # draw is >= 0, so p = 1 (Hansen 2005 convention); a strict ``>`` gave a spuriously small p in that case.
        out[name] = float(np.mean(tb >= tstat))
    return out
