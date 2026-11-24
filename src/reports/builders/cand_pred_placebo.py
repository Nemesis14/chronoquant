"""Placebo check of the candidate-predictor study (epic_078 / t963).

Runs the *whole* pipeline — price path -> zigzag -> candidates -> features -> out-of-time models — on synthetic prices with
the sign-placebo of epic_077 (``|r_t|`` kept, fair random signs): volatility path, volume, taker flow, trade counts and
funding stay real, every directional dependence is destroyed.  The real-data metrics of t962 are then compared with the
placebo distribution: if a group "works" on the placebo too, the effect is geometry / mechanics, not market information.

``uv run python -m reports.builders.cand_pred_placebo --n 60``
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
from reports.builders import cand_pred_model as cm
from reports.builders import move_census as mc
from reports.builders import move_census_anatomy as an
from reports.builders import move_census_candidates as cd
from reports.builders.move_census_placebo import placebo_logp

logger = logging.getLogger(__name__)

TABLE_PLACEBO = "cand_pred_v1_placebo"
TABLE_PLACEBO_UNI = "cand_pred_v1_placebo_univar"
TABLE_COMPARE = "cand_pred_v1_compare"
TABLE_COMPARE_UNI = "cand_pred_v1_compare_univar"
_G: dict[str, Any] = {}


def candidates_for(s: cf.Series, p: mc.CensusParams, i0: int, i1: int) -> pl.DataFrame:
    """The candidate table (T in SCALES, k = K) of a series, with the time columns the feature builder expects."""
    csum = np.concatenate(([0.0], np.cumsum(s.vol)))
    parts = []
    for T in cf.SCALES:
        sigma = mc.trailing_sigma(s.logp, T, p.sigma_win)
        d = cd.candidates_cell(s.logp, csum, sigma, T, cf.K, p, i0, i1)
        if d.height:
            parts.append(d)
    c = pl.concat(parts)
    return c.with_columns(*[pl.from_epoch((pl.col(a) + s.m0) * 60, time_unit="s").cast(pl.Datetime("ms")).alias(b)
                            for a, b in (("conf_idx", "conf_time"), ("low_idx", "low_time"))])


def quick_metrics(df: pl.DataFrame, oof: pl.DataFrame, target: str) -> pl.DataFrame:
    """AUC / lift / AUC gain over ``base`` per model on the pooled out-of-fold rows (no CIs)."""
    yy = df[f"y_{target}"].to_numpy()
    base = oof.filter(pl.col("model") == "base").sort(["test_year", "row"])
    rows = base["row"].to_numpy()
    y = yy[rows].astype(int)
    a0 = cm.auc(y, base["p"].to_numpy())
    out = []
    for m in oof["model"].unique().sort().to_list():
        p = oof.filter(pl.col("model") == m).sort(["test_year", "row"])["p"].to_numpy()
        a = cm.auc(y, p)
        out.append({"target": target, "model": m, "auc": a, "dauc": a - a0, "lift10": cm.lift_top(y, p), "base_rate": float(y.mean())})
    return pl.DataFrame(out)


def _init(asset_id: str, p_json: str) -> None:
    s = cf.load_series(asset_id)
    p = mc.CensusParams.from_json(p_json)
    m = mc.load_minutes(asset_id)
    _G.update(s=s, p=p, win=mc.window_bounds(m, p))


def _one(job: tuple[int, int, bool]) -> dict[str, Any]:
    rep, seed, with_greedy = job
    s0: cf.Series = _G["s"]
    p: mc.CensusParams = _G["p"]
    i0, i1 = _G["win"]
    lp = placebo_logp(s0.logp, "sign", np.random.default_rng(seed))
    s = cf.Series(s0.m0, lp, s0.vol, s0.quote, s0.tbuy, s0.trades, s0.fund_t, s0.fund_r)
    cand = candidates_for(s, p, i0, i1)
    tab = cf.build_features(s, p, i0, i1, cand)
    res = cm.evaluate(tab, with_ci=False, with_greedy=with_greedy, with_gbm=True)
    d = cm.add_targets(tab)
    mets = pl.concat([quick_metrics(d, res[cm.TABLE_OOF].filter(pl.col("target") == t), t) for t in cm.TARGETS])
    sel = res.get(cm.TABLE_SELECTED)
    return {"rep": rep, "metrics": mets.with_columns(pl.lit(rep).alias("rep")),
            "univar": res[cm.TABLE_UNIVAR].with_columns(pl.lit(rep).alias("rep")),
            "selected": None if sel is None else sel.with_columns(pl.lit(rep).alias("rep")), "n_cand": tab.height}


def run_placebo(asset_id: str, n: int, p: mc.CensusParams | None = None, *, seed0: int = 20261201, workers: int = 4,
                with_greedy: bool = True) -> dict[str, pl.DataFrame]:
    p = p or mc.CensusParams()
    jobs = [(rep, seed0 + rep, with_greedy) for rep in range(n)]
    parts: dict[str, list[pl.DataFrame]] = {"metrics": [], "univar": [], "selected": []}
    ncand = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=workers, initializer=_init, initargs=(asset_id, p.to_json())) as ex:
        for i, r in enumerate(ex.map(_one, jobs, chunksize=1)):
            parts["metrics"].append(r["metrics"])
            parts["univar"].append(r["univar"])
            if r["selected"] is not None:
                parts["selected"].append(r["selected"])
            ncand.append(r["n_cand"])
            logger.info("placebo %d / %d (%.0fs)", i + 1, n, time.time() - t0)
    out = {k: pl.concat(v) for k, v in parts.items() if v}
    out["n_cand"] = pl.DataFrame({"rep": list(range(n)), "n": ncand})
    return out


def compare(real_metrics: pl.DataFrame, plc: pl.DataFrame, stats: tuple[str, ...] = ("auc", "dauc", "lift10")) -> pl.DataFrame:
    """Real vs placebo per (target, model, stat): placebo mean / 2.5 / 97.5 %, two-sided and one-sided (real >= placebo) p."""
    rows = []
    for (t, m), g in plc.group_by(["target", "model"], maintain_order=True):
        r = real_metrics.filter((pl.col("target") == t) & (pl.col("model") == m))
        if r.height == 0:
            continue
        for st in stats:
            x = g[st].drop_nulls().drop_nans().to_numpy()
            obs = float(r[st][0])
            if x.size < 5:
                continue
            mu = float(x.mean())
            rows.append({"target": t, "model": m, "stat": st, "real": obs, "plc_mean": mu,
                         "plc_p025": float(np.percentile(x, 2.5)), "plc_p975": float(np.percentile(x, 97.5)),
                         "p_two": (1 + float(np.sum(np.abs(x - mu) >= abs(obs - mu)))) / (x.size + 1),
                         "p_up": (1 + float(np.sum(x >= obs))) / (x.size + 1), "n_rep": int(x.size)})
    return pl.DataFrame(rows)


def compare_univar(real: pl.DataFrame, plc: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for (t, f), g in plc.group_by(["target", "feature"], maintain_order=True):
        r = real.filter((pl.col("target") == t) & (pl.col("feature") == f))
        x = g["auc"].drop_nulls().drop_nans().to_numpy()
        if r.height == 0 or x.size < 5:
            continue
        obs = float(r["auc"][0])
        mu = float(x.mean())
        rows.append({"target": t, "feature": f, "group": r["group"][0], "real_auc": obs, "plc_mean": mu,
                     "plc_p025": float(np.percentile(x, 2.5)), "plc_p975": float(np.percentile(x, 97.5)),
                     "p_two": (1 + float(np.sum(np.abs(x - mu) >= abs(obs - mu)))) / (x.size + 1), "n_rep": int(x.size)})
    return pl.DataFrame(rows).sort(["target", "p_two"])


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset-id", default="btcusdt")
    ap.add_argument("--n", type=int, default=60)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--no-greedy", action="store_true")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    plc = run_placebo(a.asset_id, a.n, workers=a.workers, with_greedy=not a.no_greedy)
    real_m = mc.load_table(a.asset_id, cm.TABLE_METRICS)
    real_u = mc.load_table(a.asset_id, cm.TABLE_UNIVAR)
    cmp_m = compare(real_m, plc["metrics"])
    cmp_u = compare_univar(real_u, plc["univar"])
    tabs = {TABLE_PLACEBO: plc["metrics"], TABLE_PLACEBO_UNI: plc["univar"], TABLE_COMPARE: cmp_m, TABLE_COMPARE_UNI: cmp_u}
    if "selected" in plc:
        tabs["cand_pred_v1_placebo_selected"] = plc["selected"]
    an._write(a.asset_id, tabs)
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=220):
        print(cmp_m.filter(pl.col("stat") == "auc").with_columns(pl.col(pl.Float64).round(3)))
        print(cmp_u.head(12).with_columns(pl.col(pl.Float64).round(3)))


if __name__ == "__main__":
    main()
