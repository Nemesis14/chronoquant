"""Strategy artifact read/write for ChronoQuant strategy sessions.

Handles persistence of strategy_artifact.json to artifacts/{session_id}/.
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

import utils

logger = logging.getLogger(__name__)

# %%  Path helper


def _artifact_dir(session_id: str) -> Path:
    """Return the absolute artifact directory path for a session.

    Args:
        session_id: Strategy session identifier (e.g. 'strategy_2101_2605_202605').

    Returns:
        Path to artifacts/{session_id}/.
    """
    repo_root = Path(utils._resolve_path("."))
    return repo_root / "artifacts" / session_id


# %% Realized backtest outputs


def write_realized_outputs(artifact_dir: Path, trades: list[dict[str, Any]]) -> None:
    """Write realized backtest outputs to artifact_dir.

    Writes three files:
    - trades.parquet       : trade ledger with close-derived price columns
    - equity_curve.parquet : cumulative MFE equity curve per trade
    - summary.json         : headline metrics with equity_basis annotation

    Args:
        artifact_dir: Resolved Path to the artifact directory (e.g. artifacts/{session_id}/).
                      Must already exist. No path resolution is performed here.
        trades      : List of trade dicts from _simulate_strategy(), each containing
                      at minimum: entry_time, exit_time, direction, score_pct_at_entry,
                      bucket_mean_mfe, hold_minutes, exit_reason. Optional:
                      entry_price, exit_price.
    """
    artifact_dir.mkdir(parents=True, exist_ok=True)
    n = len(trades)

    # --- trades.parquet ---
    if n > 0:
        trades_df = pd.DataFrame({
            "entry_time"        : pd.to_datetime([t["entry_time"] for t in trades]),
            "exit_time"         : pd.to_datetime([t["exit_time"]  for t in trades]),
            "direction"         : [t["direction"]          for t in trades],
            "entry_price"       : pd.Series([t.get("entry_price") for t in trades], dtype="float64"),
            "exit_price"        : pd.Series([t.get("exit_price")  for t in trades], dtype="float64"),
            "hold_minutes"      : [int(t["hold_minutes"])  for t in trades],
            "exit_reason"       : [t["exit_reason"]        for t in trades],
            "score_pct_at_entry": [float(t["score_pct_at_entry"]) for t in trades],
            "bucket_mean_mfe"   : [float(t["bucket_mean_mfe"])    for t in trades],
        })
    else:
        trades_df = pd.DataFrame({
            "entry_time"        : pd.Series([], dtype="datetime64[ns]"),
            "exit_time"         : pd.Series([], dtype="datetime64[ns]"),
            "direction"         : pd.Series([], dtype="object"),
            "entry_price"       : pd.Series([], dtype="float64"),
            "exit_price"        : pd.Series([], dtype="float64"),
            "hold_minutes"      : pd.Series([], dtype="int64"),
            "exit_reason"       : pd.Series([], dtype="object"),
            "score_pct_at_entry": pd.Series([], dtype="float64"),
            "bucket_mean_mfe"   : pd.Series([], dtype="float64"),
        })

    trades_df.to_parquet(artifact_dir / "trades.parquet", index=False)
    logger.info("trades.parquet written: %d rows → %s", n, artifact_dir / "trades.parquet")

    # --- equity_curve.parquet ---
    if n > 0:
        mfes         = [float(t["bucket_mean_mfe"]) for t in trades]
        cum_mfe      = []
        running      = 0.0
        for m in mfes:
            running += m
            cum_mfe.append(running)

        equity_df = pd.DataFrame({
            "trade_index"   : list(range(n)),
            "entry_time"    : pd.to_datetime([t["entry_time"] for t in trades]),
            "bucket_mean_mfe": mfes,
            "cumulative_mfe": cum_mfe,
        })
    else:
        equity_df = pd.DataFrame({
            "trade_index"    : pd.Series([], dtype="int64"),
            "entry_time"     : pd.Series([], dtype="datetime64[ns]"),
            "bucket_mean_mfe": pd.Series([], dtype="float64"),
            "cumulative_mfe" : pd.Series([], dtype="float64"),
        })

    equity_df.to_parquet(artifact_dir / "equity_curve.parquet", index=False)
    logger.info("equity_curve.parquet written: %s", artifact_dir / "equity_curve.parquet")

    # --- summary.json ---
    if n > 0:
        mfes        = [float(t["bucket_mean_mfe"]) for t in trades]
        gross_ret   = sum(mfes)
        wins        = sum(1 for m in mfes if m > 0)
        win_rate    = wins / n
        final_eq    = 1.0 + gross_ret
        summary = {
            "initial_capital": 1.0,
            "final_equity"   : round(final_eq,   6),
            "n_trades"       : n,
            "win_rate"       : round(win_rate,    6),
            "gross_return"   : round(gross_ret,   6),
            "net_return"     : round(gross_ret,   6),
            "equity_basis"   : "mfe_proxy",
            "note"           : (
                "entry_price and exit_price reflect close at entry/exit timestamps; "
                "equity still uses bucket_mean_mfe as proxy return per trade"
            ),
        }
    else:
        summary = {
            "initial_capital": 1.0,
            "final_equity"   : 1.0,
            "n_trades"       : 0,
            "win_rate"       : None,
            "gross_return"   : 0.0,
            "net_return"     : 0.0,
            "equity_basis"   : "mfe_proxy",
            "note"           : (
                "entry_price and exit_price reflect close at entry/exit timestamps when trades exist; "
                "equity still uses bucket_mean_mfe as proxy return per trade"
            ),
        }

    with open(artifact_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
        f.write("\n")
    logger.info("summary.json written: %s", artifact_dir / "summary.json")


# %% Write / Read


def write_strategy_artifact(
    session_id     : str,
    long_model_id  : str,
    short_model_id : str,
    fit_period     : dict,      # {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}
    decision_params: dict,
    metrics        : dict,
    optuna_best    : dict,
) -> Path:
    """Write strategy_artifact.json to artifacts/{session_id}/.

    Args:
        session_id    : Strategy session identifier.
        long_model_id : Model ID for the long direction.
        short_model_id: Model ID for the short direction.
        fit_period    : Dict with 'start' and 'end' date strings for the session window.
        decision_params: Dict with rank-first decision parameters:
                        long_entry_pct, short_entry_pct, min_edge_gap,
                        min_hold_minutes, max_hold_minutes, cooldown_minutes,
                        rearm_pct, conflict_rule.
        metrics       : Performance metrics dict for the fit window.
        optuna_best   : Dict with 'value' (best objective) and 'n_trials'.

    Returns:
        Path to the written strategy_artifact.json file.
    """
    artifact = {
        "session_id"             : session_id,
        "long_model"             : long_model_id,
        "short_model"            : short_model_id,
        "signal_mode"            : "rank_first",
        "evaluation_mode"        : "same_window",
        "fit_period"             : fit_period,
        "rank_lookup_long_path"  : "rank_lookup_long.parquet",
        "rank_lookup_short_path" : "rank_lookup_short.parquet",
        "isotonic_long_path"     : "isotonic_long.pkl",
        "isotonic_short_path"    : "isotonic_short.pkl",
        "decision_params"        : decision_params,
        "optuna_best_trial"      : optuna_best,
        "metrics"                : metrics,
        "trades_path"            : "trades.parquet",
        "equity_curve_path"      : "equity_curve.parquet",
        "summary_path"           : "summary.json",
        "calibrated_at"          : datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    out_dir = _artifact_dir(session_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "strategy_artifact.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2, ensure_ascii=False)
        f.write("\n")

    logger.info("strategy_artifact.json written: %s", out_path)
    return out_path


def read_strategy_artifact(session_id: str) -> dict:
    """Read strategy_artifact.json from artifacts/{session_id}/.

    Args:
        session_id: Strategy session identifier.

    Returns:
        Parsed artifact dict. Keys follow the rank-first contract:
        session_id, long_model, short_model, signal_mode, evaluation_mode,
        fit_period, rank_lookup_long_path, rank_lookup_short_path,
        isotonic_long_path, isotonic_short_path, decision_params,
        optuna_best_trial, metrics, calibrated_at.

    Raises:
        FileNotFoundError: If the artifact file does not exist.
    """
    path = _artifact_dir(session_id) / "strategy_artifact.json"
    if not path.exists():
        raise FileNotFoundError(f"strategy_artifact.json not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)
