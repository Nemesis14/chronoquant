"""
Generate a model_card.json for a trained LGBM model.

Reads CV metrics from search/search_best.json and features.json, runs a
holdout backtest, and writes models/<model_id>/model_card.json.

Usage:
    python scripts/generate_model_card.py \\
        --model-id  lgbm_solusdt_l_fw60_q90_local_v3 \\
        --side      long \\
        --holdout-start "2025-06-05 00:00:00" \\
        --holdout-end   "2026-06-05 11:26:00" \\
        --entry     0.45 \\
        --max-hold  120
"""

import argparse
import json
import sys
from pathlib import Path
from statistics import mean

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils
from modeling.evaluation.backtest import (
    build_backtest_frame,
    simulate_long_probability_strategy,
)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate model_card.json for a trained LGBM model.")
    p.add_argument("--model-id",      required=True)
    p.add_argument("--side",          required=True, choices=["long", "short"])
    p.add_argument("--holdout-start", required=True)
    p.add_argument("--holdout-end",   required=True)
    p.add_argument("--entry",         type=float, default=0.45)
    p.add_argument("--max-hold",      type=int,   default=120)
    p.add_argument("--rearm",         type=float, default=0.18)
    p.add_argument("--exit",          type=float, default=0.10)
    p.add_argument("--cooldown",      type=int,   default=60)
    p.add_argument("--asset-id",      default="solusdt_fw60")
    return p.parse_args()


# ---------------------------------------------------------------------------
# CV metrics from search_best.json
# ---------------------------------------------------------------------------

def _cv_metrics(model_dir: Path) -> dict:
    search_file = model_dir / "search" / "search_best.json"
    if not search_file.exists():
        return {}
    data = json.loads(search_file.read_text(encoding="utf-8"))
    folds = data.get("fold_summary", [])
    if not folds:
        return {
            "mean_train_prauc": data.get("mean_train_prauc"),
            "mean_valid_prauc": data.get("mean_valid_prauc"),
            "mean_train_rocauc": None,
            "mean_valid_rocauc": None,
        }
    return {
        "mean_train_prauc":  data.get("mean_train_prauc", mean(f["train_pr_auc"] for f in folds)),
        "mean_valid_prauc":  data.get("mean_valid_prauc", mean(f["valid_pr_auc"] for f in folds)),
        "mean_train_rocauc": mean(f["train_roc_auc"] for f in folds),
        "mean_valid_rocauc": mean(f["valid_roc_auc"] for f in folds),
    }


# ---------------------------------------------------------------------------
# Feature count from features.json
# ---------------------------------------------------------------------------

def _n_features(model_dir: Path) -> int:
    feat_file = model_dir / "features.json"
    if not feat_file.exists():
        return 0
    data = json.loads(feat_file.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return len(data)
    return len(data.get("input_features") or data.get("features") or [])


# ---------------------------------------------------------------------------
# Holdout backtest
# ---------------------------------------------------------------------------

def _run_holdout(args: argparse.Namespace) -> dict:
    strategy_cfg = {
        "asset_id":              args.asset_id,
        "model_id":              args.model_id,
        "side":                  args.side,
        "start":                 args.holdout_start,
        "end":                   args.holdout_end,
        "initial_equity":        10_000.0,
        "entry_threshold":       args.entry,
        "rearm_threshold":       args.rearm,
        "exit_threshold":        args.exit,
        "min_hold_minutes":      5,
        "max_hold_minutes":      args.max_hold,
        "take_profit_pct":       0.0,
        "stop_loss_pct":         0.0,
        "trailing_activation_pct": 0.0,
        "trailing_stop_pct":    0.0,
        "cooldown_minutes":      args.cooldown,
        "fee_bps_per_side":      10.0,
        "slippage_bps_per_side": 2.0,
        "output_dir":            f"backtests/_card_holdout_{args.model_id}",
    }
    print(f"Building holdout frame {args.holdout_start} -> {args.holdout_end}...")
    frame = build_backtest_frame(
        model_id=args.model_id,
        start=args.holdout_start,
        end=args.holdout_end,
        asset_id=args.asset_id,
    )
    _, _, summary = simulate_long_probability_strategy(frame, strategy_cfg)
    return summary


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------

def _fmt_return(ratio: float) -> str:
    return f"{'+' if ratio >= 0 else ''}{ratio * 100:.1f}%"


def _fmt_equity(val: float) -> str:
    return f"{val:,.2f}"


def _fmt_period(start: str, end: str) -> str:
    s = start[:10]
    e = end[:10]
    return f"{s} – {e} (holdout)"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    model_cfg = utils.load_models_config()
    model_meta = model_cfg["models"].get(args.model_id)
    if model_meta is None:
        print(f"ERROR: model_id '{args.model_id}' not found in config/models.json")
        sys.exit(1)

    model_dir = Path(utils._resolve_path(model_meta["paths"]["model_dir"]))

    print(f"Reading CV metrics from {model_dir / 'search' / 'search_best.json'}...")
    cv = _cv_metrics(model_dir)
    n_feat = _n_features(model_dir)

    print(f"Running holdout backtest (entry={args.entry}, max_hold={args.max_hold})...")
    summary = _run_holdout(args)

    trades      = summary.get("trade_count", 0)
    wins        = summary.get("winning_trades", 0)
    losses      = summary.get("losing_trades", 0)
    win_rate    = round(summary.get("win_rate", 0) * 100, 1)
    total_ret   = summary.get("total_return", 0.0)
    final_eq    = summary.get("final_equity", 10_000.0)
    max_dd      = summary.get("max_drawdown", 0.0)

    card = {
        "model_id":         args.model_id,
        "side":             args.side,
        "n_features":       n_feat,
        "train_prauc":      round(cv.get("mean_train_prauc") or 0, 3),
        "valid_prauc":      round(cv.get("mean_valid_prauc") or 0, 3),
        "train_rocauc":     round(cv.get("mean_train_rocauc") or 0, 3),
        "valid_rocauc":     round(cv.get("mean_valid_rocauc") or 0, 3),
        "holdout": {
            "period":       _fmt_period(args.holdout_start, args.holdout_end),
            "entry":        args.entry,
            "max_hold":     args.max_hold,
            "trades":       trades,
            "wins":         wins,
            "losses":       losses,
            "win_rate":     win_rate,
            "total_return": _fmt_return(total_ret),
            "final_equity": _fmt_equity(final_eq),
            "max_dd":       f"{max_dd * 100:.1f}%",
        },
    }

    out_path = model_dir / "model_card.json"
    out_path.write_text(json.dumps(card, indent=4), encoding="utf-8")
    print(f"\nmodel_card.json saved to {out_path}")
    print(f"  side={args.side}  features={n_feat}")
    print(f"  CV  train_prauc={card['train_prauc']}  valid_prauc={card['valid_prauc']}")
    print(f"  CV  train_rocauc={card['train_rocauc']}  valid_rocauc={card['valid_rocauc']}")
    print(f"  Holdout  trades={trades}  win_rate={win_rate}%  return={card['holdout']['total_return']}  max_dd={card['holdout']['max_dd']}")


if __name__ == "__main__":
    main()
