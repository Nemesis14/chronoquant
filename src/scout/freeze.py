"""Freeze the Impulse Scout baseline (epic_074 / t940) into ``artifacts/scout_s0c_r1/``.

``uv run python -m scout.freeze [--out DIR] [--overwrite]``

Fits the S0 + C logit (:func:`reports.builders.scout_bench.baseline_spec`, C = 0.1) ONCE on the full prepared **train**
frame of t930 (``research."scout_events_v1"``, a15, ``W4h_H8h``, pooled SOL + BTC + ETH; holdout guard), recomputes its
pooled train OOF score on the shared hash-fixed folds (top-20% / 10% thresholds = 80th / 90th percentile, 5300) and
writes:

* ``model.pkl`` — the fitted :class:`reports.builders.scout_bench.ScalarLogit` (sklearn; audit / cross-check);
* ``train_oof.parquet`` — ``event_id, asset_id, t0, cv_block, label_class, oof_score`` of the train frame;
* ``manifest.json`` — features, standardisation + coefficients (the runtime scores from these, no pickle needed),
  thresholds, event definition (params json), rules, costs, train range, frame / fold hash, OOF AUC, the runtime anchor
  and live start, the sha256 of every file and the ``freeze_hash``.

Nothing from OOS / holdout-2 enters the fit (the loader reads ``split = 'train'`` only; :func:`guard_no_holdout`).
"""

from __future__ import annotations

import argparse
import json
import logging
import pickle
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from reports.builders import scout_bench as sb
from reports.builders import scout_events as se
from reports.builders import wave_events as we
from reports.builders.scout_bench.frame import frame_hash, y_binary
from reports.builders.scout_bench.trades import load_costs
from scout import runtime as rt

logger = logging.getLogger(__name__)

SEED = 20260101
#: the live forward log starts here (epic_074: backfill from the beginning of Nov 2026); earlier events are not resolved
LIVE_START = datetime(2026, 11, 1)
#: 7 day sigma window + 1 day greedy warm-up before the holdout-2 start (t938 look-back); fixed detection start
DETECT_ANCHOR = datetime.fromisoformat(we.WaveParams().holdout2_start) - timedelta(days=8)


@dataclass
class Baseline:
    """The fitted baseline + its train OOF score distribution (lifted from t938 ``scout_live_score``)."""

    model: sb.ScalarLogit
    oof_score: np.ndarray
    thr20: float
    thr10: float
    oof_auc: float
    n_train: int
    n_impulse: int
    frame: pl.DataFrame
    folds: sb.FoldSet


def fit_baseline(frame: pl.DataFrame | None = None, *, seed: int = SEED) -> Baseline:
    """S0 + C logit on the full prepared train frame (t930 a15, pooled) + its pooled train OOF score (shared folds)."""
    fr = sb.prepare_frame(frame if frame is not None else sb.load_scout_events())
    sb.guard_no_holdout(fr)
    spec = sb.baseline_spec()
    assert spec.factory is sb.ScalarLogit
    model = sb.ScalarLogit()
    model.fit(fr, None, seed=seed)
    folds = sb.make_folds(fr)
    oof = sb.cross_val(spec, fr, folds, seed=seed, thresholds=False)
    y = y_binary(fr)
    t20, t10 = np.quantile(oof.score, [0.80, 0.90])
    return Baseline(model, oof.score, float(t20), float(t10), sb.auc(y, oof.score), fr.height, int(y.sum()), fr, folds)


def _iso(x: Any) -> str:
    return x.isoformat() if isinstance(x, datetime) else str(x)


def build_manifest(base: Baseline, files: dict[str, str], *, created_at: datetime | None = None) -> dict[str, Any]:
    m: sb.ScalarLogit = base.model
    fr = base.frame
    spec = sb.baseline_spec()
    p = rt.detection_params(se.PRIMARY_VARIANT, sb.PRIMARY_CELL)
    cell = p.cells[0]
    rest = fr.filter(pl.col("label_class") != "impulse")
    q = float(np.mean(rest["label_class"].to_numpy() == "false_start"))
    costs = load_costs()
    man: dict[str, Any] = {
        "model_id": rt.MODEL_ID,
        "display_name": "Impulse Scout S0+C logit r1 (kísérleti, NO-GO)",
        "description": "Impulse Scout baseline (5300): S0 + C skalár logit, W4h_H8h, a15, pooled SOL+BTC+ETH train. "
                       "Kísérleti – nem kereskedési jel; nem része a quant modellnek.",
        "status": "experimental_no_go",
        "family": "scout",
        "generator": "scout.freeze",
        "created_at": (created_at or datetime.now(UTC)).replace(microsecond=0).isoformat(),
        "event_definition": {
            "version": "5300 / 5200 v2", "variant": se.PRIMARY_VARIANT, "min_a_sigma": se.VARIANTS[se.PRIMARY_VARIANT],
            "cell_id": cell[0], "w_min": cell[1], "h_min": cell[2], "params_json": p.to_json(),
        },
        "model": {
            "name": spec.name, "config": spec.config, "config_hash": spec.config_hash, "seed": SEED,
            "features": list(m.cols), "mu": [float(v) for v in m.mu], "sd": [float(v) for v in m.sd],
            "coef": [float(v) for v in m.clf.coef_[0]], "intercept": float(m.clf.intercept_[0]),
            "q_false_start": q, "classes": list(sb.CLASSES),
        },
        "thresholds": {"top20": base.thr20, "top10": base.thr10, "source": "train OOF score, 80th / 90th percentile"},
        "rules": [{"key": "default", "rule": sb.rules.DEFAULT_RULE.to_dict()},
                  {"key": "r2_03", "rule": rt.R2_03.to_dict()}],
        "costs": json.loads(costs.to_json()),
        "runtime": {"detect_anchor": DETECT_ANCHOR.isoformat(), "live_start": LIVE_START.isoformat(),
                    "holdout2_start": we.WaveParams().holdout2_start},
        "train": {
            "source": 'research."scout_events_v1" (split = train, a15, W4h_H8h)',
            "assets": sorted(fr["asset_id"].unique().to_list()),
            "n_events": base.n_train, "n_impulse": base.n_impulse,
            "n_by_asset": {a: int(n) for a, n in fr.group_by("asset_id").len().sort("asset_id").iter_rows()},
            "t0_min": _iso(fr["t0"].min()), "t0_max": _iso(fr["t0"].max()), "label_end_max": _iso(fr["label_end"].max()),
            "frame_hash": frame_hash(fr), "fold_hash": base.folds.hash, "n_folds": len(base.folds),
            "fold_bounds": [_iso(b) for b in base.folds.bounds], "embargo_min": base.folds.embargo_min,
            "oof_auc": base.oof_auc, "oof_file": "train_oof.parquet",
        },
        "files": files,
    }
    man["freeze_hash"] = rt.freeze_hash(man)
    return man


def write_artifact(base: Baseline, out: Path, *, overwrite: bool = False) -> dict[str, Any]:
    if (out / rt.MANIFEST).exists() and not overwrite:
        raise FileExistsError(f"{out} already holds a frozen artifact (use --overwrite only for a deliberate refreeze)")
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "model.pkl", "wb") as fh:
        pickle.dump(base.model, fh)
    fr = base.frame
    ids = fr["event_id"] if "event_id" in fr.columns else pl.Series("event_id", [
        se.event_id(a, c, t) for a, c, t in zip(fr["asset_id"], fr["cell_id"], fr["t0"], strict=True)])
    oof = pl.DataFrame({"event_id": ids, "asset_id": fr["asset_id"], "t0": fr["t0"], "cv_block": fr["cv_block"],
                        "label_class": fr["label_class"], "oof_score": base.oof_score})
    oof.write_parquet(out / "train_oof.parquet")
    files = {n: rt.file_sha256(out / n) for n in ("model.pkl", "train_oof.parquet")}
    man = build_manifest(base, files)
    (out / rt.MANIFEST).write_text(json.dumps(man, indent=2, ensure_ascii=False), encoding="utf-8")
    return man


def main() -> None:
    ap = argparse.ArgumentParser(description="Freeze the Impulse Scout S0+C baseline (t940)")
    ap.add_argument("--out", default=str(rt.ARTIFACT_DIR))
    ap.add_argument("--overwrite", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO)
    base = fit_baseline()
    man = write_artifact(base, Path(a.out), overwrite=a.overwrite)
    print(f"{man['model_id']}: n={base.n_train} (impulse {base.n_impulse}), train OOF AUC {base.oof_auc:.4f}, "
          f"top20 {base.thr20:.4f}, top10 {base.thr10:.4f}, fold hash {man['train']['fold_hash']}, "
          f"freeze hash {man['freeze_hash']} -> {a.out}")


if __name__ == "__main__":
    main()
