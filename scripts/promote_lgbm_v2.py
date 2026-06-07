#!/usr/bin/env python3
# =============================================================================
# Promote lgbm_solusdt_l_fw60_q90_local_v2 to champion
# Steps:
#   1. Train final model on all data before test period (row_stride=60)
#   2. Update config/models.json  (v2 active, stable_v1 inactive)
#   3. Update config/env.json     (solusdt_fw60 → v2)
#   4. Sync predictions to DB
#   5. Run strategy sweep and print recommended thresholds
# =============================================================================
from __future__ import annotations

import json
import os
import pickle
import sqlite3
import sys
from itertools import product
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import lightgbm as lgb
import utils
from modeling.datasets import load_modeling_dataset
from data_pipeline.sync_predictions import sync_predictions
from evaluation.backtest import build_backtest_frame, simulate_long_probability_strategy

# ── Constants ─────────────────────────────────────────────────────────────────
MODEL_ID   = "lgbm_solusdt_l_fw60_q90_local_v2"
OLD_ID     = "lgbm_solusdt_l_fw60_q90_stable_v1"
ASSET_ID   = "solusdt_fw60"
TRAIN_END  = "2025-06-04 23:59:00"
ROW_STRIDE = 60   # consistent with search stage

BEST_PARAMS = {
    "num_leaves": 7, "max_depth": 3, "min_child_samples": 324,
    "min_child_weight": 0.003273389647422092, "min_split_gain": 0.0009400693721143439,
    "reg_alpha": 0.8082246761168387, "reg_lambda": 4.977182019203682,
    "subsample": 0.8291360825892524, "colsample_bytree": 0.7930687296511203,
    "learning_rate": 0.018842713855458146, "max_bin": 127,
    "path_smooth": 0.029270729058805893, "extra_trees": False,
    "objective": "binary", "boosting_type": "gbdt",
    "metric": "binary_logloss",
    "n_estimators": 850,           # mean CV best_iter ≈ 757, +12% buffer
    "subsample_freq": 1, "force_col_wise": True,
    "verbosity": -1, "n_jobs": 4,
}

MODEL_DIR = REPO / "models" / MODEL_ID


# ─────────────────────────────────────────────────────────────────────────────
# 1. TRAIN FINAL MODEL
# ─────────────────────────────────────────────────────────────────────────────
def step1_train():
    print("\n" + "="*60)
    print("STEP 1 — Train final model")
    print("="*60)

    search_feat_file = MODEL_DIR / "search" / "features_search.json"
    with open(search_feat_file) as f:
        features = json.load(f)["features"]
    print(f"  Features: {len(features)}")

    print(f"  Loading dataset  (row_stride={ROW_STRIDE}, end={TRAIN_END})")
    ds = load_modeling_dataset(
        target_col="trg_l_fw60_q90",
        feature_cols=features,
        row_stride=ROW_STRIDE,
        asset_id=ASSET_ID,
        end=TRAIN_END,
    )
    print(f"  Dataset shape: {ds.X.shape}   y_mean={ds.y.mean():.4f}")

    print("  Fitting LightGBM …")
    model = lgb.LGBMClassifier(**BEST_PARAMS)
    model.fit(ds.X, ds.y, callbacks=[lgb.log_evaluation(-1)])
    print(f"  Done — n_estimators={model.n_estimators}")

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODEL_DIR / "model.pkl", "wb") as f:
        pickle.dump(model, f)

    features_payload = {"features": features, "input_features": features}
    with open(MODEL_DIR / "features.json", "w") as f:
        json.dump(features_payload, f, indent=4)

    with open(MODEL_DIR / "params.json", "w") as f:
        json.dump(BEST_PARAMS, f, indent=4)

    print(f"  Saved: model.pkl  features.json  params.json → {MODEL_DIR}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. UPDATE models.json
# ─────────────────────────────────────────────────────────────────────────────
def step2_update_models_json():
    print("\n" + "="*60)
    print("STEP 2 — Update config/models.json")
    print("="*60)

    path = REPO / "config" / "models.json"
    cfg = json.loads(path.read_text(encoding="utf-8"))
    models = cfg["models"]

    if OLD_ID in models:
        models[OLD_ID]["active"] = False
        print(f"  {OLD_ID} → active=false")

    if MODEL_ID in models:
        models[MODEL_ID]["active"] = True
        print(f"  {MODEL_ID} → active=true")

    path.write_text(json.dumps(cfg, indent=4, ensure_ascii=False), encoding="utf-8")
    print("  Saved config/models.json")


# ─────────────────────────────────────────────────────────────────────────────
# 3. UPDATE env.json
# ─────────────────────────────────────────────────────────────────────────────
def step3_update_env_json():
    print("\n" + "="*60)
    print("STEP 3 — Update config/env.json")
    print("="*60)

    path = REPO / "config" / "env.json"
    cfg = json.loads(path.read_text(encoding="utf-8"))
    old = cfg["runtime"]["models"].get(ASSET_ID)
    cfg["runtime"]["models"][ASSET_ID] = MODEL_ID
    path.write_text(json.dumps(cfg, indent=4, ensure_ascii=False), encoding="utf-8")
    print(f"  {ASSET_ID}: {old} → {MODEL_ID}")
    print("  Saved config/env.json")


# ─────────────────────────────────────────────────────────────────────────────
# 4. SYNC PREDICTIONS
# ─────────────────────────────────────────────────────────────────────────────
def step4_sync_predictions():
    print("\n" + "="*60)
    print("STEP 4 — Sync predictions to DB")
    print("="*60)

    # Sync from beginning of 2020 to cover the full historical range
    sync_predictions(
        start_time="2020-08-01 00:00:00",
        end_time=None,
        asset_id=ASSET_ID,
    )
    print("  Done.")


# ─────────────────────────────────────────────────────────────────────────────
# 5. STRATEGY SWEEP
# ─────────────────────────────────────────────────────────────────────────────
_ENTRY_THRESHOLDS = [0.30, 0.33, 0.36, 0.38, 0.40, 0.42, 0.45, 0.48, 0.50, 0.55]
_MAX_HOLD_MINUTES = [30, 45, 60, 90, 120]
_TAKE_PROFIT_PCTS = [0.0, 0.010, 0.015, 0.020]

SWEEP_START = "2024-01-01 00:00:00"
SWEEP_END   = "2025-12-31 23:59:00"


def step5_strategy_sweep() -> dict:
    print("\n" + "="*60)
    print("STEP 5 — Strategy threshold sweep")
    print("="*60)
    print(f"  Period: {SWEEP_START} → {SWEEP_END}")

    frame = build_backtest_frame(
        model_id=MODEL_ID,
        start=SWEEP_START,
        end=SWEEP_END,
        asset_id=ASSET_ID,
    )
    pred_range = f"[{frame['prediction'].min():.3f}, {frame['prediction'].max():.3f}]"
    print(f"  Frame: {frame.shape}  pred_range={pred_range}")

    rows = []
    combos = list(product(_ENTRY_THRESHOLDS, _MAX_HOLD_MINUTES, _TAKE_PROFIT_PCTS))
    print(f"  Sweeping {len(combos)} combinations …")

    for entry_thr, max_hold, tp_pct in combos:
        cfg = {
            "side": "long",
            "entry_threshold":         entry_thr,
            "rearm_threshold":         0.18,
            "exit_threshold":          0.10,
            "min_hold_minutes":        5,
            "max_hold_minutes":        max_hold,
            "take_profit_pct":         tp_pct,
            "stop_loss_pct":           0.0,
            "trailing_activation_pct": 0.0,
            "trailing_stop_pct":       0.0,
            "cooldown_minutes":        60,
            "fee_bps_per_side":        10.0,
            "slippage_bps_per_side":   2.0,
            "initial_equity":          10000.0,
        }
        try:
            _, _, summary = simulate_long_probability_strategy(frame, cfg)
        except Exception as e:
            continue

        if summary.get("trade_count", 0) < 15:
            continue

        rows.append({
            "entry_thr":    entry_thr,
            "max_hold":     int(max_hold),
            "tp_pct":       tp_pct,
            "trades":       int(summary["trade_count"]),
            "win_rate":     round(summary.get("win_rate") or 0, 4),
            "total_return": round(summary.get("total_return", 0), 4),
            "profit_factor":round(summary.get("profit_factor") or 0, 3),
            "max_dd":       round(summary.get("max_drawdown", 0), 4),
            "avg_hold":     round(summary.get("avg_hold_minutes") or 0, 1),
        })

    if not rows:
        print("  No valid sweep results.")
        return {}

    df = pd.DataFrame(rows)
    df["score"] = (
        df["total_return"]
        - 0.5 * df["max_dd"].abs()
        + 0.25 * (df["win_rate"] - 0.5).clip(lower=0)
    )
    df = df.sort_values("score", ascending=False).reset_index(drop=True)

    print()
    print(df.head(20).to_string(index=True))

    # Save
    out_dir = REPO / "backtests"
    out_dir.mkdir(exist_ok=True)
    df.to_csv(out_dir / f"sweep_{MODEL_ID}.csv", index=False)
    print(f"\n  Sweep CSV saved to backtests/sweep_{MODEL_ID}.csv")

    best = df.iloc[0]
    print()
    print("  RECOMMENDED:")
    print(f"    entry_threshold:  {best['entry_thr']}")
    print(f"    max_hold_minutes: {best['max_hold']}")
    print(f"    take_profit_pct:  {best['tp_pct']}")
    print(f"    win_rate:         {best['win_rate']:.2%}")
    print(f"    total_return:     {best['total_return']:.2%}")
    print(f"    profit_factor:    {best['profit_factor']:.3f}")
    print(f"    max_drawdown:     {best['max_dd']:.2%}")
    print(f"    trades:           {best['trades']}")

    return {
        "entry_threshold": float(best["entry_thr"]),
        "max_hold_minutes": int(best["max_hold"]),
        "take_profit_pct": float(best["tp_pct"]),
        "total_return": float(best["total_return"]),
        "win_rate": float(best["win_rate"]),
        "profit_factor": float(best["profit_factor"]),
        "max_drawdown": float(best["max_dd"]),
        "trades": int(best["trades"]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 6. UPDATE strategies.json
# ─────────────────────────────────────────────────────────────────────────────
def step6_update_strategies(best: dict):
    print("\n" + "="*60)
    print("STEP 6 — Update config/strategies.json")
    print("="*60)

    path = REPO / "config" / "strategies.json"
    cfg = json.loads(path.read_text(encoding="utf-8"))

    new_strategy_id = "solusdt_long_fw60_q90_local_v2"
    new_strategy = {
        "description": (
            f"LONG/FLAT strategy for SOLUSDT using lgbm_solusdt_l_fw60_q90_local_v2 (champion). "
            f"entry={best.get('entry_threshold')} max_hold={best.get('max_hold_minutes')} "
            f"tp={best.get('take_profit_pct')} — from sweep on 2024-01-01 to 2025-12-31 "
            f"({best.get('total_return',0):.2%} return, {best.get('win_rate',0):.0%} WR, "
            f"PF {best.get('profit_factor',0):.2f})."
        ),
        "asset_id":                ASSET_ID,
        "model_id":                MODEL_ID,
        "side":                    "long",
        "start":                   SWEEP_START,
        "end":                     SWEEP_END,
        "initial_equity":          10000.0,
        "entry_threshold":         best.get("entry_threshold", 0.40),
        "rearm_threshold":         0.18,
        "exit_threshold":          0.10,
        "min_hold_minutes":        5,
        "max_hold_minutes":        best.get("max_hold_minutes", 60),
        "take_profit_pct":         best.get("take_profit_pct", 0.015),
        "stop_loss_pct":           0.0,
        "trailing_activation_pct": 0.0,
        "trailing_stop_pct":       0.0,
        "cooldown_minutes":        60,
        "fee_bps_per_side":        10.0,
        "slippage_bps_per_side":   2.0,
        "output_dir":              f"backtests/{new_strategy_id}",
    }

    # Insert at front so active_strategy() picks it first for solusdt_fw60
    strategies = cfg.get("strategies", {})
    new_strategies = {new_strategy_id: new_strategy}
    new_strategies.update(strategies)
    cfg["strategies"] = new_strategies

    path.write_text(json.dumps(cfg, indent=4, ensure_ascii=False), encoding="utf-8")
    print(f"  Added {new_strategy_id} at top of strategies.json")
    print(f"  entry_threshold={new_strategy['entry_threshold']}  "
          f"max_hold={new_strategy['max_hold_minutes']}  "
          f"tp={new_strategy['take_profit_pct']}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    step1_train()
    step2_update_models_json()
    step3_update_env_json()
    step4_sync_predictions()
    best = step5_strategy_sweep()
    if best:
        step6_update_strategies(best)

    print("\n" + "="*60)
    print("PROMOTION COMPLETE")
    print("="*60)
    print(f"  Champion: {MODEL_ID}")
    print(f"  Restart the Streamlit app to pick up the new model.")
