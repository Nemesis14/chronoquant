#!/usr/bin/env python3
# =============================================================================
# Final fit — lgbm_solusdt_l_fw60_q90_local_v4 + lgbm_solusdt_s_fw60_q10_local_v4
# =============================================================================
# One-off script: uses search_best.json params + features_search.json feature list
# to produce model.pkl, features.json, params.json for each model.
#
# TRAIN_END is 1 minute before holdout start (2025-06-10 05:07:00 in folds.json).
# =============================================================================

import json
import pickle
import sys
import time
from pathlib import Path

import lightgbm as lgb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from modeling.datasets import load_modeling_dataset

TRAIN_END = "2025-06-10 04:59:00"

MODELS = [
    {
        "model_id":    "lgbm_solusdt_l_fw60_q90_local_v4",
        "target_col":  "trg_l_fw60_q90",
        "asset_id":    "solusdt_fw60",
        "row_stride":  60,
    },
    {
        "model_id":    "lgbm_solusdt_s_fw60_q10_local_v4",
        "target_col":  "trg_s_fw60_q10",
        "asset_id":    "solusdt_fw60",
        "row_stride":  60,
    },
]


def fit_model(model_id: str, target_col: str, asset_id: str, row_stride: int) -> None:
    model_dir  = Path(f"models/{model_id}")
    search_dir = model_dir / "search"

    print(f"\n{'='*60}")
    print(f"FINAL FIT: {model_id}")
    print(f"{'='*60}")

    with open(search_dir / "features_search.json") as f:
        features = json.load(f)["features"]
    with open(search_dir / "search_best.json") as f:
        best = json.load(f)

    fold_summary = best["fold_summary"]
    mean_best_iter = sum(f["best_iteration"] for f in fold_summary) / len(fold_summary)
    n_est = round(mean_best_iter * 1.12)

    print(f"  Trial:           #{best['trial_no']}")
    print(f"  Folds in best:   {len(fold_summary)}")
    print(f"  Mean best iter:  {mean_best_iter:.1f}  ->  n_estimators={n_est}")
    print(f"  val_ll:          {best['mean_valid_ll']:.4f}")
    print(f"  gap:             {best['mean_gap']:.4f}")
    print(f"  prauc:           {best.get('mean_valid_prauc', 'N/A')}")
    print(f"  Features:        {len(features)}")
    print(f"  TRAIN_END:       {TRAIN_END}")

    final_params = {
        **best["params"],
        "objective":      "binary",
        "boosting_type":  "gbdt",
        "metric":         "binary_logloss",
        "n_estimators":   n_est,
        "subsample_freq": 1,
        "force_col_wise": True,
        "verbosity":      -1,
        "n_jobs":         4,
    }

    print("\nLoading dataset...")
    t0 = time.time()
    ds = load_modeling_dataset(
        target_col   = target_col,
        feature_cols = features,
        row_stride   = row_stride,
        asset_id     = asset_id,
        end          = TRAIN_END,
    )
    print(f"  {len(ds.X):,} rows  (open_time {ds.open_time.iloc[0]} -> {ds.open_time.iloc[-1]})")
    print(f"  Positive rate: {ds.y.mean():.4f}")
    print(f"  Load time: {time.time()-t0:.1f}s")

    print("\nFitting model...")
    t1 = time.time()
    model = lgb.LGBMClassifier(**final_params)
    model.fit(ds.X, ds.y, callbacks=[lgb.log_evaluation(-1)])
    elapsed = time.time() - t1
    print(f"  Fit complete in {elapsed:.1f}s")

    # Feature importance (top 25 by gain)
    gain = model.booster_.feature_importance(importance_type="gain")
    top25 = sorted(zip(features, gain), key=lambda x: -x[1])[:25]
    zero_gain = sum(1 for _, g in zip(features, gain) if g == 0)
    print(f"\nTop 25 features by gain:")
    for feat, value in top25:
        print(f"  {feat:<50} {value:>12.1f}")
    print(f"\n  Zero-gain features: {zero_gain} / {len(features)}")

    # Save artifacts
    model_dir.mkdir(parents=True, exist_ok=True)
    with open(model_dir / "model.pkl", "wb") as f:
        pickle.dump(model, f)
    with open(model_dir / "features.json", "w") as f:
        json.dump({"features": features, "input_features": features}, f, indent=4)
    with open(model_dir / "params.json", "w") as f:
        json.dump(final_params, f, indent=4)

    # Verify
    for name in ["model.pkl", "features.json", "params.json"]:
        fp = model_dir / name
        size = fp.stat().st_size
        print(f"  OK: {fp}  ({size:,} bytes)")

    print(f"\nDone: {model_id}")


if __name__ == "__main__":
    for cfg in MODELS:
        fit_model(**cfg)

    print("\n" + "="*60)
    print("BOTH MODELS COMPLETE")
    print("="*60)
