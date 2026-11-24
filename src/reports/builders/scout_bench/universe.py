# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""Leaderboard-universe inference of the Impulse Scout benchmark (epic_073 / t936; 5300 "Többszörös tesztelés").

The per-model runs (t932-t935) each saw only a part of the model universe. This module evaluates the WHOLE universe
from stored / recomputed OOF scores on the shared folds:

* :func:`logloss_spa` — Hansen SPA of every non-baseline model against the baseline on the day-summed binary log-loss
  difference (the statistical leaderboard SPA of 5300);
* :func:`joint_holm` — Holm over the pre-registered primaries H1-H3 (their permutation p-values);
* :func:`apply_universe` — writes the universe SPA p / best model and the joint Holm p into leaderboard rows and
  re-derives the go / no-go gates (``gate_spa`` only for the selected best model, gate B on top 20% AND top 10%);
* :func:`event_pnl` / :func:`pnl_spa` — per-event net returns -> day sums -> SPA of a set of trading strategies against
  a benchmark (no trade = 0, or another strategy).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

from reports.builders.scout_bench import metrics as mt
from reports.builders.scout_bench import stats as st
from reports.builders.scout_bench.runner import go_no_go


def logloss_spa(y: np.ndarray, base_score: np.ndarray, scores: Mapping[str, np.ndarray], t0: np.ndarray, *,
                n_boot: int = 5000, mean_block: float = 5.0, seed: int = 20260101) -> dict[str, Any]:
    """SPA over ``scores`` (model -> OOF binary score, aligned with ``y``) vs the baseline: loss difference
    ``logloss(base) - logloss(model)`` per event, summed per UTC day. Returns :func:`stats.spa_test` + ``best_name``,
    ``names`` and the per-model mean daily difference ``dbar`` (positive = model better)."""
    names = list(scores)
    if not names:
        raise ValueError("empty model universe")
    cl = st.day_clusters(t0)
    lb = mt.logloss_binary_each(y, base_score)
    d = np.column_stack([lb - mt.logloss_binary_each(y, scores[n]) for n in names])
    out = st.spa_test(st.day_sums(d, cl.day_of, cl.n_days), n_boot=n_boot, mean_block=mean_block, seed=seed)
    out["names"] = names
    out["best_name"] = names[out["best"]]
    out["dbar_by_model"] = dict(zip(names, map(float, out["dbar"]), strict=True))
    return out


def joint_holm(perm_p: Mapping[str, float]) -> dict[str, float]:
    """Holm over the given primaries; a missing / NaN p-value counts as 1 (a test that did not run cannot reject)."""
    clean = {k: (1.0 if v is None or not np.isfinite(v) else float(v)) for k, v in perm_p.items()}
    return st.holm(clean) if clean else {}


def apply_universe(lb: pd.DataFrame, *, spa_p: float, spa_best: str, holm_p: Mapping[str, float], min_events: bool
                   ) -> pd.DataFrame:
    """Copy of the leaderboard rows with the universe SPA p (same value on every non-baseline row), ``spa_best`` (only
    the selected best model), the joint Holm p (primaries) and the re-derived gates / go status."""
    out = lb.copy()
    for col, default in (("spa_best", False), ("go_status", ""), ("go_failed", "")):
        if col not in out.columns:
            out[col] = default
    out["spa_best"] = out["spa_best"].astype(object)
    out["go_status"] = out["go_status"].astype(object)
    out["go_failed"] = out["go_failed"].astype(object)
    for i in out.index:
        if bool(out.at[i, "is_baseline"]):
            out.at[i, "spa_p"], out.at[i, "spa_best"] = float("nan"), False
            continue
        name = out.at[i, "model"]
        out.at[i, "spa_p"] = float(spa_p)
        out.at[i, "spa_best"] = bool(name == spa_best)
        if bool(out.at[i, "primary"]):
            out.at[i, "holm_p"] = float(holm_p.get(name, float("nan")))
        for k, v in go_no_go(out.loc[i].to_dict(), min_events=min_events).items():
            if k not in out.columns:
                out[k] = None
                out[k] = out[k].astype(object)
            out.at[i, k] = v
    return out


def event_pnl(n: int, trades: pd.DataFrame, rows: np.ndarray | None = None, col: str = "net_bps") -> np.ndarray:
    """Per-event net return vector of length ``n`` (frame order): the trade's ``col`` on the signalled rows (``rows`` =
    boolean mask or positions; ``None`` = every traded row), 0 elsewhere (no trade, or an invalid window)."""
    v = np.zeros(n)
    t = trades
    if rows is not None:
        pos = np.flatnonzero(rows) if np.asarray(rows).dtype == bool else np.asarray(rows, dtype=np.int64)
        t = trades[trades["row_id"].isin(pos)]
    v[t["row_id"].to_numpy(dtype=np.int64)] = t[col].to_numpy(dtype=float)
    return v


def pnl_spa(pnl: Mapping[str, np.ndarray], t0: np.ndarray, *, benchmark: np.ndarray | None = None, n_boot: int = 5000,
            mean_block: float = 5.0, seed: int = 20260101) -> dict[str, Any]:
    """SPA of trading strategies: ``pnl`` = strategy -> per-event net (frame order, 0 = no trade); performance =
    day-summed net minus the benchmark's (``None`` = no trade, i.e. 0). Positive = strategy better."""
    names = list(pnl)
    cl = st.day_clusters(t0)
    daily = st.day_sums(np.column_stack([pnl[k] for k in names]), cl.day_of, cl.n_days)
    if benchmark is not None:
        daily = daily - st.day_sums(benchmark, cl.day_of, cl.n_days)[:, None]
    out = st.spa_test(daily, n_boot=n_boot, mean_block=mean_block, seed=seed)
    out["names"] = names
    out["best_name"] = names[out["best"]]
    return out


def studentized_best(pnl: Mapping[str, np.ndarray], t0: np.ndarray, among: Sequence[str], *, n_boot: int = 5000,
                     mean_block: float = 5.0, seed: int = 20260101) -> str:
    """The strategy among ``among`` with the largest studentized mean daily net (the SPA's argmax restricted to them)."""
    res = pnl_spa({k: pnl[k] for k in among}, t0, n_boot=n_boot, mean_block=mean_block, seed=seed)
    return str(res["best_name"])


__all__ = ["apply_universe", "event_pnl", "joint_holm", "logloss_spa", "pnl_spa", "studentized_best"]
