"""Strategy optimization using Optuna for ChronoQuant.

Runs an Optuna TPE sweep over entry/exit/cooldown parameters using a pure-Python
rank-first state machine backtest. Writes strategy_artifact.json and sweep_results.csv.
"""

import logging
import math
from pathlib import Path
from typing import Any

import optuna
import pandas as pd

import utils
from strategy.strategy.artifacts import (
    register_strategy,
    write_realized_outputs,
    write_strategy_artifact,
)

logger = logging.getLogger(__name__)

# Suppress Optuna per-trial logs
optuna.logging.set_verbosity(optuna.logging.WARNING)


# %% State machine backtest


def _simulate_strategy(
    df               : pd.DataFrame,
    long_entry_pct   : float,
    short_entry_pct  : float,
    min_edge_gap     : float,
    max_hold_minutes : int,
    cooldown_minutes : int,
    min_hold_minutes : int,
    rearm_pct        : float,
) -> list[dict[str, Any]]:
    """Run a rank-first state machine backtest over a strategy DataFrame.

    States: FLAT -> IN_LONG or IN_SHORT -> COOLDOWN -> FLAT.

    Entry uses percentile rank scores (score_pct_long / score_pct_short).
    When both signals fire simultaneously the highest edge (gap) direction wins;
    if the gap is below min_edge_gap no entry is made.

    Args:
        df              : DataFrame with columns open_time (datetime-like),
                          score_pct_long, score_pct_short,
                          bucket_mean_mfe_long, bucket_mean_mfe_short.
        long_entry_pct  : Minimum score_pct_long to enter long (0-1).
        short_entry_pct : Minimum score_pct_short to enter short (0-1).
        min_edge_gap    : Minimum edge gap required when both signals fire.
        max_hold_minutes: Minutes to hold before forced exit.
        cooldown_minutes: Minutes to stay in COOLDOWN after exit.
        min_hold_minutes: Minimum minutes held before signal-based exit is allowed.
        rearm_pct       : score_pct threshold below which the signal is considered
                          decayed / re-armed.

    Returns:
        List of trade dicts with keys: entry_time, exit_time, direction,
        score_pct_at_entry, bucket_mean_mfe, n_bars.
    """
    # --- Constants ---
    FLAT     = "FLAT"
    IN_LONG  = "IN_LONG"
    IN_SHORT = "IN_SHORT"
    COOLDOWN = "COOLDOWN"

    state           : str                   = FLAT
    entry_time      : pd.Timestamp | None   = None
    exit_time       : pd.Timestamp | None   = None
    direction       : str | None            = None
    score_pct_entry : float                 = 0.0
    mfe_at_entry    : float                 = 0.0
    entry_price     : float | None          = None
    trades          : list[dict[str, Any]]  = []

    for row in df.itertuples(index=False):
        ts        : pd.Timestamp = pd.Timestamp(row.open_time)           # type: ignore[attr-defined,assignment]
        pct_long  : float        = float(row.score_pct_long)             # type: ignore[attr-defined]
        pct_short : float        = float(row.score_pct_short)            # type: ignore[attr-defined]
        mfe_long  : float        = float(row.bucket_mean_mfe_long)       # type: ignore[attr-defined]
        mfe_short : float        = float(row.bucket_mean_mfe_short)      # type: ignore[attr-defined]
        close_raw: Any = getattr(row, "close", None)
        if close_raw is None:
            close: float | None = None
        else:
            close_value = float(close_raw)
            close = None if math.isnan(close_value) else close_value

        if state == FLAT:
            long_signal  = pct_long  >= long_entry_pct
            short_signal = pct_short >= short_entry_pct

            if long_signal and short_signal:
                edge_long  = pct_long  - pct_short
                edge_short = pct_short - pct_long
                if edge_long > edge_short and edge_long >= min_edge_gap:
                    state           = IN_LONG
                    entry_time      = ts
                    direction       = "long"
                    score_pct_entry = pct_long
                    mfe_at_entry    = mfe_long
                    entry_price     = close
                elif edge_short > edge_long and edge_short >= min_edge_gap:
                    state           = IN_SHORT
                    entry_time      = ts
                    direction       = "short"
                    score_pct_entry = pct_short
                    mfe_at_entry    = mfe_short
                    entry_price     = close
                # else: gap insufficient — stay FLAT

            elif long_signal:
                state           = IN_LONG
                entry_time      = ts
                direction       = "long"
                score_pct_entry = pct_long
                mfe_at_entry    = mfe_long
                entry_price     = close

            elif short_signal:
                state           = IN_SHORT
                entry_time      = ts
                direction       = "short"
                score_pct_entry = pct_short
                mfe_at_entry    = mfe_short
                entry_price     = close

        elif state == IN_LONG:
            elapsed = int((ts - entry_time).total_seconds() / 60)  # type: ignore[operator]

            if elapsed >= max_hold_minutes:
                exit_time = ts
                trades.append({
                    "entry_time"        : entry_time,
                    "exit_time"         : exit_time,
                    "direction"         : direction,
                    "entry_price"       : entry_price,
                    "exit_price"        : close,
                    "score_pct_at_entry": score_pct_entry,
                    "bucket_mean_mfe"   : mfe_at_entry,
                    "n_bars"            : elapsed,
                    "hold_minutes"      : elapsed,
                    "exit_reason"       : "max_hold",
                })
                state     = COOLDOWN
                direction = None

            elif pct_short >= short_entry_pct and elapsed >= min_hold_minutes:
                # opposite_edge exit
                exit_time = ts
                trades.append({
                    "entry_time"        : entry_time,
                    "exit_time"         : exit_time,
                    "direction"         : direction,
                    "entry_price"       : entry_price,
                    "exit_price"        : close,
                    "score_pct_at_entry": score_pct_entry,
                    "bucket_mean_mfe"   : mfe_at_entry,
                    "n_bars"            : elapsed,
                    "hold_minutes"      : elapsed,
                    "exit_reason"       : "opposite_edge",
                })
                state     = COOLDOWN
                direction = None

            elif pct_long < rearm_pct and elapsed >= min_hold_minutes:
                # signal_decay exit
                exit_time = ts
                trades.append({
                    "entry_time"        : entry_time,
                    "exit_time"         : exit_time,
                    "direction"         : direction,
                    "entry_price"       : entry_price,
                    "exit_price"        : close,
                    "score_pct_at_entry": score_pct_entry,
                    "bucket_mean_mfe"   : mfe_at_entry,
                    "n_bars"            : elapsed,
                    "hold_minutes"      : elapsed,
                    "exit_reason"       : "signal_decay",
                })
                state     = COOLDOWN
                direction = None

        elif state == IN_SHORT:
            elapsed = int((ts - entry_time).total_seconds() / 60)  # type: ignore[operator]

            if elapsed >= max_hold_minutes:
                exit_time = ts
                trades.append({
                    "entry_time"        : entry_time,
                    "exit_time"         : exit_time,
                    "direction"         : direction,
                    "entry_price"       : entry_price,
                    "exit_price"        : close,
                    "score_pct_at_entry": score_pct_entry,
                    "bucket_mean_mfe"   : mfe_at_entry,
                    "n_bars"            : elapsed,
                    "hold_minutes"      : elapsed,
                    "exit_reason"       : "max_hold",
                })
                state     = COOLDOWN
                direction = None

            elif pct_long >= long_entry_pct and elapsed >= min_hold_minutes:
                # opposite_edge exit
                exit_time = ts
                trades.append({
                    "entry_time"        : entry_time,
                    "exit_time"         : exit_time,
                    "direction"         : direction,
                    "entry_price"       : entry_price,
                    "exit_price"        : close,
                    "score_pct_at_entry": score_pct_entry,
                    "bucket_mean_mfe"   : mfe_at_entry,
                    "n_bars"            : elapsed,
                    "hold_minutes"      : elapsed,
                    "exit_reason"       : "opposite_edge",
                })
                state     = COOLDOWN
                direction = None

            elif pct_short < rearm_pct and elapsed >= min_hold_minutes:
                # signal_decay exit
                exit_time = ts
                trades.append({
                    "entry_time"        : entry_time,
                    "exit_time"         : exit_time,
                    "direction"         : direction,
                    "entry_price"       : entry_price,
                    "exit_price"        : close,
                    "score_pct_at_entry": score_pct_entry,
                    "bucket_mean_mfe"   : mfe_at_entry,
                    "n_bars"            : elapsed,
                    "hold_minutes"      : elapsed,
                    "exit_reason"       : "signal_decay",
                })
                state     = COOLDOWN
                direction = None

        elif state == COOLDOWN:
            elapsed_since_exit = int((ts - exit_time).total_seconds() / 60)  # type: ignore[operator]
            if elapsed_since_exit >= cooldown_minutes and pct_long <= rearm_pct and pct_short <= rearm_pct:
                state = FLAT

    return trades


# %% Objective


def _objective(
    trial   : optuna.Trial,
    eval_df : pd.DataFrame,
) -> float:
    """Optuna objective: mean bucket_mean_mfe at entry across all trades.

    Args:
        trial  : Optuna trial object.
        eval_df: DataFrame for the evaluation period with score_pct_* and
                 bucket_mean_mfe_* columns.

    Returns:
        Mean bucket_mean_mfe at entry over trades, or -inf if fewer than 50 trades.
    """
    long_entry_pct   = trial.suggest_float("long_entry_pct",   0.70, 0.99)
    short_entry_pct  = trial.suggest_float("short_entry_pct",  0.70, 0.99)
    min_edge_gap     = trial.suggest_float("min_edge_gap",     0.00, 0.30)
    max_hold_minutes = trial.suggest_int(  "max_hold_minutes",  30, 240)
    cooldown_minutes = trial.suggest_int(  "cooldown_minutes",  15, 120)
    min_hold_minutes = trial.suggest_int(  "min_hold_minutes",   3,  30)
    rearm_pct        = trial.suggest_float("rearm_pct",         0.40, 0.75)

    trades = _simulate_strategy(
        eval_df,
        long_entry_pct,
        short_entry_pct,
        min_edge_gap,
        max_hold_minutes,
        cooldown_minutes,
        min_hold_minutes,
        rearm_pct,
    )

    if len(trades) < 50:
        return float("-inf")

    mean_quality = sum(t["bucket_mean_mfe"] for t in trades) / len(trades)
    return mean_quality


# %% Metrics helper


def _compute_metrics(trades: list[dict[str, Any]]) -> dict:
    """Compute evaluation metrics from a list of trades.

    Args:
        trades: List of trade dicts from _simulate_strategy().

    Returns:
        Metrics dict with n_trades, total_return, win_rate, sharpe,
        max_drawdown, sufficient_sample.
    """
    n = len(trades)
    if n == 0:
        return {
            "n_trades"         : 0,
            "total_return"     : 0.0,
            "win_rate"         : None,
            "sharpe"           : None,
            "max_drawdown"     : None,
            "sufficient_sample": False,
        }

    mfes        = [t["bucket_mean_mfe"] for t in trades]
    total_ret   = sum(mfes)
    wins        = sum(1 for m in mfes if m > 0)
    win_rate    = wins / n

    # --- Sharpe ---
    mean_m = total_ret / n
    if n > 1:
        variance = sum((m - mean_m) ** 2 for m in mfes) / (n - 1)
        std_m    = math.sqrt(variance) if variance > 0 else 0.0
        sharpe   = (mean_m / std_m * math.sqrt(252)) if std_m > 0 else None
    else:
        sharpe = None

    # --- Max drawdown (simplified cumulative) ---
    cumulative   = 0.0
    peak         = 0.0
    max_drawdown = 0.0
    for m in mfes:
        cumulative  += m
        peak         = max(peak, cumulative)
        drawdown     = cumulative - peak
        max_drawdown = min(max_drawdown, drawdown)

    return {
        "n_trades"         : n,
        "total_return"     : round(total_ret, 6),
        "win_rate"         : round(win_rate, 4),
        "sharpe"           : round(sharpe, 4) if sharpe is not None else None,
        "max_drawdown"     : round(max_drawdown, 6),
        "sufficient_sample": n >= 50,
    }


# %% Cutoffs


def _build_cutoffs(calibrated_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Derive per-direction decile cutoff rows from the calibrated scored table.

    For each direction and decile bucket (1..10), records the raw-score lower/upper
    bound, the bucket's upper percentile, and its realized stats (bucket_mean_mfe,
    bucket_hit_rate).  Persisted to ``strat."<session>__cutoffs"``.

    Args:
        calibrated_df : Output of fit_calibration (carries score_pct_*, bucket_*,
                        bucket_mean_mfe_*, bucket_hit_rate_* and pred_*_raw cols).

    Returns:
        List of cutoff dicts (one per direction+bucket present), or empty.
    """
    rows: list[dict[str, Any]] = []
    for direction, raw_col, bucket_col, pct_col, mean_col, hr_col in (
        ("long",  "pred_long_raw",  "bucket_long",  "score_pct_long",
         "bucket_mean_mfe_long",  "bucket_hit_rate_long"),
        ("short", "pred_short_raw", "bucket_short", "score_pct_short",
         "bucket_mean_mfe_short", "bucket_hit_rate_short"),
    ):
        if bucket_col not in calibrated_df.columns:
            continue
        for bid, grp in calibrated_df.groupby(bucket_col):
            raw      = grp[raw_col].to_numpy(dtype=float)
            pct      = grp[pct_col].to_numpy(dtype=float)
            mean_arr = grp[mean_col].to_numpy(dtype=float)
            hr_arr   = grp[hr_col].to_numpy(dtype=float)
            rows.append({
                "direction"      : direction,
                "bucket_id"      : int(bid),  # type: ignore[arg-type]
                "score_raw_lower": float(raw.min()) if len(raw) else None,
                "score_raw_upper": float(raw.max()) if len(raw) else None,
                "score_pct_upper": float(pct.max()) if len(pct) else None,
                "bucket_mean_mfe": float(mean_arr[0]) if len(mean_arr) else None,
                "bucket_hit_rate": float(hr_arr[0]) if len(hr_arr) else None,
            })
    return rows


# %% Public API


def optimize_strategy(
    session_id     : str,
    long_model_id  : str,
    short_model_id : str,
    calibrated_df  : pd.DataFrame,
    start          : str,
    end            : str,
    n_trials       : int = 200,
    asset_id       : str | None = None,
) -> dict:
    """Run Optuna strategy optimization on the evaluation period.

    Takes the calibrated scored table (output of fit_calibration; must include
    score_pct_* and bucket_mean_mfe_* columns). Filters to [start, end] window.
    Runs Optuna TPE search with rank-first percentile-based entry/exit logic.
    Writes the realized backtest outputs as ``strat.*`` DuckDB tables, registers
    the session in ``reg.strategies`` + ``reg.artifacts``, and writes
    strategy_artifact.json + sweep_results.csv to artifacts/{session_id}/.

    Args:
        session_id    : Strategy session identifier.
        long_model_id : Model ID for the long direction.
        short_model_id: Model ID for the short direction.
        calibrated_df : The calibrated scored table from fit_calibration.
        start         : Optimization window start date, YYYY-MM-DD (inclusive).
        end           : Optimization window end date, YYYY-MM-DD (inclusive).
        n_trials      : Number of Optuna trials (default 200).
        asset_id      : Asset key for the lab connection; resolved if None.

    Returns:
        Dict summary with session_id, best_params, metrics, optuna_best, and
        strat_tables (the written table names).

    Raises:
        ValueError: If required rank columns are absent, or the period produces
                    no rows.
    """
    artifact_dir = Path(utils._resolve_path("artifacts")) / session_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    df = calibrated_df
    logger.info("optimize_strategy: calibrated table has %d rows", len(df))

    required_cols = (
        "score_pct_long",
        "score_pct_short",
        "bucket_mean_mfe_long",
        "bucket_mean_mfe_short",
    )
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(
                f"Column '{col}' missing from calibrated table. "
                "Run fit_calibration first."
            )

    # --- Filter to optimization window ---
    start_ts = f"{start} 00:00:00"
    end_ts   = f"{end} 23:59:59"
    open_time_str = df["open_time"].astype(str)
    mask     = (open_time_str >= start_ts) & (open_time_str <= end_ts)
    eval_df  : pd.DataFrame = df[mask].copy().reset_index(drop=True)  # type: ignore[assignment]

    if eval_df.empty:
        raise ValueError(
            f"No rows in period {start} to {end}. "
            f"Full table range: {df['open_time'].min()} to {df['open_time'].max()}"
        )
    logger.info("Optimization period rows: %d", len(eval_df))

    # --- Optuna study ---
    logger.info("Starting Optuna study: %d trials", n_trials)
    study = optuna.create_study(
        direction = "maximize",
        sampler   = optuna.samplers.TPESampler(seed=42),
    )
    study.optimize(
        lambda trial: _objective(trial, eval_df),
        n_trials = n_trials,
    )

    best_params = study.best_params
    best_value  = study.best_value
    logger.info(
        "Optuna complete — best value: %.6f  params: %s", best_value, best_params
    )

    # --- Re-simulate with best params for eval metrics ---
    best_trades = _simulate_strategy(
        eval_df,
        best_params["long_entry_pct"],
        best_params["short_entry_pct"],
        best_params["min_edge_gap"],
        best_params["max_hold_minutes"],
        best_params["cooldown_minutes"],
        best_params["min_hold_minutes"],
        best_params["rearm_pct"],
    )
    metrics = _compute_metrics(best_trades)

    # --- Write strat.* tables (trades / equity / cutoffs) ---
    cutoffs      = _build_cutoffs(df)
    strat_tables = write_realized_outputs(
        session_id = session_id,
        trades     = best_trades,
        cutoffs    = cutoffs,
        asset_id   = asset_id,
    )

    # --- Write sweep_results.csv ---
    sweep_rows = []
    for trial in study.trials:
        row = {"trial_number": trial.number, "value": trial.value}
        row.update(trial.params)
        sweep_rows.append(row)

    sweep_df   = pd.DataFrame(sweep_rows)
    sweep_path = artifact_dir / "sweep_results.csv"
    sweep_df.to_csv(sweep_path, index=False)
    logger.info("sweep_results.csv written: %s", sweep_path)

    # --- Write strategy_artifact.json ---
    optuna_best = {"value": best_value, "n_trials": n_trials}

    decision_params = {
        **best_params,
        "conflict_rule": "highest_edge",
    }

    artifact_path = write_strategy_artifact(
        session_id      = session_id,
        long_model_id   = long_model_id,
        short_model_id  = short_model_id,
        fit_period      = {"start": start, "end": end},
        decision_params = decision_params,
        metrics         = metrics,
        optuna_best     = optuna_best,
    )

    # --- Register the session in reg.strategies + reg.artifacts ---
    register_strategy(
        session_id     = session_id,
        long_model_id  = long_model_id,
        short_model_id = short_model_id,
        artifact_files = [
            ("strategy_artifact", artifact_path),
            ("isotonic_long",     artifact_dir / "isotonic_long.pkl"),
            ("isotonic_short",    artifact_dir / "isotonic_short.pkl"),
            ("rank_lookup_long",  artifact_dir / "rank_lookup_long.parquet"),
            ("rank_lookup_short", artifact_dir / "rank_lookup_short.parquet"),
        ],
        asset_id       = asset_id,
    )

    result = {
        "session_id"   : session_id,
        "best_params"  : best_params,
        "metrics"      : metrics,
        "optuna_best"  : optuna_best,
        "strat_tables" : strat_tables,
    }
    return result
