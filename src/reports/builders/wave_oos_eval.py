# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false, reportGeneralTypeIssues=false, reportReturnType=false
"""Descriptive, post-hoc, ONE-SHOT out-of-sample evaluation of the t920 models A / B (epic_072 / t928).

NOT a gate result: the 5200 v2 OOS sign check needs >= 100 unique OOS t0 events (there are 75), so it does not run; the user
explicitly asked to *see* what happened on the OOS period (2026-02-01 -> 2026-07-31). Holdout-2 (>= 2026-08-01) is never read:
the 1m prices are loaded with an SQL ``WHERE open_time < holdout2_start`` and the events with ``split = 'oos'`` / ``'train'``.

PRE-REGISTRATION (fixed before any OOS outcome was looked at; the same code path as t920):

* Primary cell ``W4h_H8h``; secondary ``W4h_H4h`` (same protocol, not part of the t920 decision).
* Model A = ``S0 + C`` (6 scalars, standardised with the TRAIN mean / sd), Model B = A + AE latent (16 dim); L2 logistic regression,
  fixed ``C = 0.1`` (the t920 learner). Both are refitted ONCE on ALL train events of the cell (AE, render axis, standardisation
  fitted on train only), then applied to the OOS events. No tuning, no second look.
* Train comparison column = the t920 out-of-fold (6 purged blocks) predictions of the usable train events (same fold fits).
* Decision rules fixed from the TRAIN OOF scores: "signals an impulse" = score >= train-OOF 80th percentile (top 20%); also the
  90th percentile (top 10%). Score buckets = train-OOF quintile cutoffs (20/40/60/80%) applied to the OOS scores.
* Reported: class counts / shares, AUC (+ day-block bootstrap 95% CI, 2000 resamples), per-quintile rates, hits / precision / lift /
  recall at both thresholds, forward log return close[t_c] -> close[t_c + H] per signal group.

Run::

    uv run --group research python -m reports.builders.wave_oos_eval --out <dir> [--cache-dir <dir>]   (cwd: repo root)
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from reports.builders import wave_events as we
from reports.builders import wave_image_model as wm
from reports.builders import wave_predictiveness_gate as wg

logger = logging.getLogger(__name__)

CLASSES = we.CLASS_ORDER_V2
CLASS_HU = we.CLASS_HU_V2
TOP_FRACS = (0.20, 0.10)
N_BOOT = 2000
MODELS = ("A", "B")


# --------------------------------------------------------------------------- guarded loading


def ohlcv_sql() -> str:
    """1m OHLCV query capped strictly below the holdout-2 start (a bound parameter)."""
    return "SELECT open_time, open, high, low, close, volume FROM live.ohlcv WHERE open_time < ? ORDER BY open_time"


def load_1m_capped(end: datetime, asset_id: str | None = None, *, tries: int = 10, delay: float = 3.0) -> pl.DataFrame | None:
    """1m OHLCV with ``open_time < end``, read-only; later rows (holdout-2) never leave the database."""
    import duckdb

    from data_handling.store.readonly_query import open_readonly_connection

    for attempt in range(tries):
        try:
            con = open_readonly_connection(asset_id)
        except duckdb.Error as exc:
            logger.warning("live db locked/unavailable (attempt %d/%d): %s", attempt + 1, tries, exc)
            if attempt == tries - 1:
                return None
            time.sleep(delay)
            continue
        try:
            return con.execute(ohlcv_sql(), [end]).pl()
        finally:
            con.close()
    return None


def assert_no_holdout(df: pl.DataFrame, holdout_start: datetime) -> None:
    """Raise if a holdout-2 event / price row is present."""
    if "split" in df.columns and (df["split"] == "holdout2").any():
        raise ValueError("holdout-2 events present")
    col = "label_end" if "label_end" in df.columns else "open_time"
    if df.height and df[col].max() >= holdout_start and col == "open_time":
        raise ValueError("price rows at / after the holdout-2 start present")
    if col == "label_end" and df.height and df[col].max() > holdout_start:
        raise ValueError("event interval reaches into holdout-2")


# --------------------------------------------------------------------------- fit on all train, predict OOS


def fit_predict_oos(ev_tr: pl.DataFrame, ev_oos: pl.DataFrame, pre_tr: wm.Prefixes, pre_oos: wm.Prefixes, *,
                    cfg: wm.AEConfig, seed: int = wm.SEED, c: float = wg.LR_C) -> dict[str, np.ndarray]:
    """Fit A and B once on every train event (AE / render axis / standardisation train-only) and score the OOS events."""
    emb = wm.fit_embedding(ev_tr, pre_tr, cfg=cfg, seed=seed)
    z_tr, z_oos = wm.embed(emb, pre_tr), wm.embed(emb, pre_oos)
    x_tr = ev_tr.select(list(wg.SCALAR_COLS)).to_numpy().astype(float)
    x_te = ev_oos.select(list(wg.SCALAR_COLS)).to_numpy().astype(float)
    if np.isnan(x_tr).any() or np.isnan(x_te).any():
        raise ValueError("NaN scalar features")
    mu, sd = x_tr.mean(axis=0), x_tr.std(axis=0)
    sd = np.where(sd > 1e-12, sd, 1.0)
    xa_tr, xa_te = (x_tr - mu) / sd, (x_te - mu) / sd
    y = ev_tr["label_impulse"].to_numpy().astype(float)
    return {"A": wg._lr(xa_tr, y, xa_te, c),
            "B": wg._lr(np.hstack([xa_tr, z_tr]), y, np.hstack([xa_te, z_oos]), c)}


# --------------------------------------------------------------------------- metrics


def class_table(cls: np.ndarray) -> dict[str, Any]:
    n = int(cls.size)
    return {"n": n, **{k: int((cls == k).sum()) for k in CLASSES}, **{f"{k}_share": float((cls == k).mean()) for k in CLASSES}}


def auc_ci(y: np.ndarray, scores: dict[str, np.ndarray], t0: np.ndarray, *, n_boot: int = N_BOOT, seed: int = wm.SEED) -> dict[str, Any]:
    """AUC of each model + day-block bootstrap 95% CI (also for B - A)."""
    blocks = wg.day_blocks(t0)
    day_idx = [np.flatnonzero(blocks.day_of == d) for d in range(blocks.n_days)]
    rng = np.random.default_rng([seed, 928_001])
    vals: dict[str, list[float]] = {"A": [], "B": [], "delta": []}
    for _ in range(n_boot):
        idx = np.concatenate([day_idx[d] for d in rng.integers(0, blocks.n_days, blocks.n_days)])
        yb = y[idx]
        if yb.min() == yb.max():
            continue
        a, b = wg.auc(yb, scores["A"][idx]), wg.auc(yb, scores["B"][idx])
        vals["A"].append(a)
        vals["B"].append(b)
        vals["delta"].append(b - a)
    out: dict[str, Any] = {"n_boot_valid": len(vals["A"])}
    for k, v in vals.items():
        lo, hi = np.quantile(v, [0.025, 0.975])
        out[k] = {"lo": float(lo), "hi": float(hi)}
    out["A"]["auc"], out["B"]["auc"] = wg.auc(y, scores["A"]), wg.auc(y, scores["B"])
    out["delta"]["auc"] = out["B"]["auc"] - out["A"]["auc"]
    return out


def quintile_table(cls: np.ndarray, score: np.ndarray, cutoffs: np.ndarray) -> list[dict[str, Any]]:
    bucket = np.searchsorted(cutoffs, score, side="right")           # score >= cutoff -> higher bucket
    rows = []
    for q in range(5):
        m = bucket == q
        n = int(m.sum())
        row = {"q": q + 1, "n": n, "n_impulse": int((cls[m] == "impulse").sum())}
        for k in CLASSES:
            row[f"{k}_rate"] = float((cls[m] == k).mean()) if n else float("nan")
        rows.append(row)
    return rows


def threshold_table(cls: np.ndarray, score: np.ndarray, thr: float, ret: np.ndarray) -> dict[str, Any]:
    sig = score >= thr
    n_imp = int((cls == "impulse").sum())
    base = n_imp / cls.size
    ns = int(sig.sum())
    hits = int((cls[sig] == "impulse").sum())
    out: dict[str, Any] = {"threshold": float(thr), "n_signals": ns, "hits": hits,
                           "precision": hits / ns if ns else float("nan"),
                           "lift": (hits / ns) / base if ns and base else float("nan"),
                           "recall": hits / n_imp if n_imp else float("nan"),
                           "false_start": int((cls[sig] == "false_start").sum()), "sideways": int((cls[sig] == "sideways").sum()),
                           "ret_signal_median": float(np.nanmedian(ret[sig])) if ns else float("nan"),
                           "ret_signal_mean": float(np.nanmean(ret[sig])) if ns else float("nan"),
                           "ret_rest_median": float(np.nanmedian(ret[~sig])) if (~sig).any() else float("nan"),
                           "ret_rest_mean": float(np.nanmean(ret[~sig])) if (~sig).any() else float("nan")}
    return out


def forward_returns(m: we.MinuteData, ev: pl.DataFrame) -> np.ndarray:
    """log(close[t_c + H] / close[t_c]); ``close_tc`` is stored in the table, the end close is read from the 1m grid."""
    end_min = ev["label_end"].to_numpy().astype("datetime64[m]").astype("int64")
    idx = end_min - m.m0 - 1                                  # close of the bar closing at label_end (see wave_events.event_row)
    if (idx < 0).any() or (idx >= m.n).any():
        raise IndexError("label_end outside the loaded 1m window")
    return np.log(m.close[idx] / ev["close_tc"].to_numpy().astype(float))


def evaluate(ev: pl.DataFrame, scores: dict[str, np.ndarray], ret: np.ndarray, oof_scores: dict[str, np.ndarray]) -> dict[str, Any]:
    """One group's tables; thresholds / cutoffs come from the TRAIN OOF scores (``oof_scores``)."""
    cls = ev["label_class"].to_numpy()
    y = (cls == "impulse").astype(float)
    res: dict[str, Any] = {"classes": class_table(cls), "base_rate": float(y.mean()), "ret_all_median": float(np.nanmedian(ret)),
                           "ret_all_mean": float(np.nanmean(ret))}
    res["auc"] = auc_ci(y, scores, ev["t0"].to_numpy().astype("datetime64[us]"))
    res["quintiles"], res["thresholds"] = {}, {}
    for k in MODELS:
        cut = np.quantile(oof_scores[k], [0.2, 0.4, 0.6, 0.8])
        res["quintiles"][k] = quintile_table(cls, scores[k], cut)
        res["thresholds"][k] = {f"top{int(f * 100)}": threshold_table(cls, scores[k], float(np.quantile(oof_scores[k], 1 - f)), ret)
                                for f in TOP_FRACS}
    return res


# --------------------------------------------------------------------------- orchestration


def run_cell(cell: str, df_1m: pl.DataFrame, ev_train: pl.DataFrame, ev_oos: pl.DataFrame, p: we.WaveParams, *, cfg: wm.AEConfig,
             cache_dir: Path | None, seed: int = wm.SEED) -> dict[str, Any]:
    t_start = time.perf_counter()
    _, _, h_min = next(c for c in p.cells if c[0] == cell)
    ev_tr = ev_train.filter((pl.col("cell_id") == cell) & (pl.col("split") == "train")).sort("t0")
    ev_te = ev_oos.filter((pl.col("cell_id") == cell) & (pl.col("split") == "oos")).sort("t0")
    assert_no_holdout(ev_te, datetime.fromisoformat(p.holdout2_start))
    mg = we.to_minute_grid(df_1m)
    b15 = we.to_bars15(mg, p)
    pre_tr, pre_te = wm.build_prefixes(b15, ev_tr), wm.build_prefixes(b15, ev_te)

    # train OOF (t920 code path, per-fold fits on fold-train only; cached)
    use = wg.usable_events(ev_tr)
    oos0 = datetime.fromisoformat(p.oos_start)
    bounds = we.cv_block_bounds(p)
    key = wg._cache_key(ev_tr, cfg, seed, h_min)
    cpath = (cache_dir / f"{wg.SLUG}_{cell}_folds.npz") if cache_dir else None
    folds = wg.load_folds(cpath, key) if cpath else None
    if folds is None:
        pre_fit = wm.build_prefixes(we.to_bars15(we.to_minute_grid(df_1m.filter(pl.col("open_time") < oos0)), p), ev_tr)
        folds = wg.prepare_folds(ev_tr, pre_fit, bounds, h_min, cfg=cfg, seed=seed)
        if cpath:
            wg.save_folds(folds, cpath, key)
    y_use = use["label_impulse"].to_numpy().astype(float)
    oof_raw = wg.oof_predictions(folds, y_use, ("ae",))
    oof = {"A": oof_raw["A"], "B": oof_raw["ae"]}

    sc = fit_predict_oos(ev_tr, ev_te, pre_tr, pre_te, cfg=cfg, seed=seed)
    out = {"cell": cell, "n_train_all": ev_tr.height, "n_train_usable": use.height,
           "train_oof": evaluate(use, oof, forward_returns(mg, use), oof),
           "oos": evaluate(ev_te, sc, forward_returns(mg, ev_te), oof),
           "oos_t0_first": str(ev_te["t0"].min()), "oos_t0_last": str(ev_te["t0"].max()),
           "oos_label_end_max": str(ev_te["label_end"].max()), "seconds": time.perf_counter() - t_start}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    ap.add_argument("--asset-id", default=None)
    ap.add_argument("--out", required=True)
    ap.add_argument("--cache-dir", default=None)
    ap.add_argument("--cells", nargs="*", default=["W4h_H8h", "W4h_H4h"])
    args = ap.parse_args()
    p = we.params_v2()
    ho0 = datetime.fromisoformat(p.holdout2_start)
    ev_train = we.load_events(args.asset_id, split="train", version="v2")
    ev_oos = we.load_events(args.asset_id, split="oos", version="v2")
    df_1m = load_1m_capped(ho0, args.asset_id)
    if df_1m is None:
        raise SystemExit("live db unavailable")
    assert_no_holdout(df_1m, ho0)
    assert_no_holdout(ev_oos, ho0)
    out = {c: run_cell(c, df_1m, ev_train, ev_oos, p, cfg=wm.AEConfig(),
                       cache_dir=Path(args.cache_dir) if args.cache_dir else None) for c in args.cells}
    Path(args.out).mkdir(parents=True, exist_ok=True)
    (Path(args.out) / "oos_eval.json").write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
    print(Path(args.out) / "oos_eval.json")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
