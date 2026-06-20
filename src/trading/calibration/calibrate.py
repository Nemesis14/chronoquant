"""Calibration orchestrator for single-pass strategy backtesting.

Loads sample_oos.parquet for a given model, runs a single backtest with
default parameters, and writes a strategy artifact to
artifacts/<model_id>/strategy/strategy_artifact.json.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import utils
from trading.calibration.artifacts import write_strategy_artifact
from trading.calibration.backtest import (
    load_oos_frame,
    simulate_long_probability_strategy,
    write_backtest_report,
)

logger = logging.getLogger(__name__)

# %% Default strategy parameters

# NOTE: entry_threshold, rearm_threshold, exit_threshold are compared directly
# against the raw regression score (pred_long / pred_short), which is a
# continuous MFE estimate in percentage units — NOT a probability in [0, 1].
# No sigmoid or softmax is applied anywhere in the pipeline.
# Threshold calibration should be performed via percentile-based sweep over
# the OOS score distribution rather than assuming a fixed probability scale.
_DEFAULT_STRATEGY = {
    "entry_threshold":         0.45,   # raw regression score threshold (not probability)
    "rearm_threshold":         0.18,   # raw regression score threshold
    "exit_threshold":          0.10,   # raw regression score threshold
    "min_hold_minutes":        5,
    "max_hold_minutes":        120,
    "take_profit_pct":         0.0,
    "stop_loss_pct":           0.0,
    "trailing_activation_pct": 0.0,
    "trailing_stop_pct":       0.0,
    "cooldown_minutes":        60,
    "fee_bps_per_side":        10.0,
    "slippage_bps_per_side":   2.0,
    "initial_equity":          10000.0,
}

# %% Calibration


def run_calibration(
    model_id : str,
    start    : str | None = None,
    end      : str | None = None,
) -> dict:
    """Load sample_oos.parquet, run single-pass backtest, write strategy artifact.

    The side (long/short) is determined automatically from manifest.json.
    Strategy parameters default to the module-level ``_DEFAULT_STRATEGY`` dict.

    Args:
        model_id : Artifact directory name, e.g. ``lgbm_solusdt_l_fw60_2021``.
        start    : Optional OOS start override (``YYYY-MM-DD``).
                   If None, uses the full parquet range.
        end      : Optional OOS end override (``YYYY-MM-DD``).
                   If None, uses the full parquet range.

    Returns:
        Summary dict with keys: model_id, side, oos_period, strategy, metrics,
        artifact_path.
    """
    # --- resolve side from manifest ---
    manifest_path = Path(utils._resolve_path(f"artifacts/{model_id}/manifest.json"))
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found: {manifest_path}")
    manifest     = json.loads(manifest_path.read_text(encoding="utf-8"))
    target_name  = manifest.get("target_name", "")
    if target_name == "long_mfe_fw60":
        side = "long"
    elif target_name == "short_mfe_fw60":
        side = "short"
    else:
        raise ValueError(
            f"Unknown target_name {target_name!r} in manifest for {model_id!r}"
        )

    logger.info("Calibrating model=%s  side=%s  start=%s  end=%s", model_id, side, start, end)

    # --- load OOS frame ---
    frame = load_oos_frame(model_id, start=start, end=end)

    actual_start = frame["open_time"].min().strftime("%Y-%m-%d")
    actual_end   = frame["open_time"].max().strftime("%Y-%m-%d")
    oos_period   = {"start": actual_start, "end": actual_end}

    # --- run backtest ---
    strategy_cfg       = dict(_DEFAULT_STRATEGY)
    strategy_cfg["side"] = side

    trades_df, equity_df, summary = simulate_long_probability_strategy(frame, strategy_cfg)

    logger.info(
        "Backtest complete: trades=%d  win_rate=%.2f%%  total_return=%.2f%%  max_dd=%.2f%%",
        summary.get("trade_count", 0),
        (summary.get("win_rate") or 0) * 100,
        (summary.get("total_return") or 0) * 100,
        (summary.get("max_drawdown") or 0) * 100,
    )

    # --- write artifacts ---
    out_dir = Path(utils._resolve_path(f"artifacts/{model_id}/strategy"))
    out_dir.mkdir(parents=True, exist_ok=True)

    if not trades_df.empty:
        trades_df.to_csv(out_dir / "trades.csv", index=False)
        equity_df.to_csv(out_dir / "equity_curve.csv", index=False)
        write_backtest_report(out_dir, model_id, strategy_cfg, summary, trades_df)
        logger.info("Trades and equity curve saved to %s", out_dir)

    artifact_path = write_strategy_artifact(
        model_id     = model_id,
        side         = side,
        strategy_cfg = strategy_cfg,
        summary      = summary,
        oos_period   = oos_period,
    )

    return {
        "model_id":     model_id,
        "side":         side,
        "oos_period":   oos_period,
        "strategy":     strategy_cfg,
        "metrics":      summary,
        "artifact_path": str(artifact_path),
    }
