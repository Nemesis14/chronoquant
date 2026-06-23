"""Execution-aware grid search engine for ChronoQuant strategy sessions.

Replaces the Optuna-based optimizer with a deterministic grid search over
entry cutoffs, take-profit specs, and stop-loss specs.  Each (direction,
cutoff, tp_spec, sl_spec) combination is simulated with a realistic
TP/SL/timeout state machine using actual OHLCV bar high/low prices.

Grid size: 8 cutoffs × 5 TP specs × 5 SL specs × 2 directions = 200 setups.

Performance note: _simulate_direction_fast() avoids iterating over all N bars
by using np.nonzero() to find only the entry candidates (score_pct >= cutoff),
then checking each candidate's 60-bar holding window with numpy slice operations.
search_strategy() pre-extracts DataFrame columns to numpy arrays once per
direction, reusing them across all (tp_spec, sl_spec, cutoff) combinations.
"""

from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import utils
from strategy.strategy.artifacts import (
    register_strategy,
    write_realized_outputs,
    write_strategy_artifact,
)

logger = logging.getLogger(__name__)

# %% Constants

ENTRY_CUTOFFS: list[float] = [0.90, 0.92, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99]
TP_SPECS: list[str] = [
    "bucket_mean_mfe",
    "bucket_median_mfe",
    "bucket_p75_mfe",
    "0.75x_bucket_mean",
    "0.50x_bucket_mean",
]
SL_SPECS: list[str] = ["none", "0.5x_tp", "1.0x_tp", "1.5x_tp", "2.0x_tp"]

_MAX_HOLD_BARS = 60
_EMPTY = np.array([], dtype=np.intp)  # reusable sentinel for "no hits"


# %% Vectorized per-bar helpers


def _tp_lr_array(
    tp_spec: str,
    mean    : np.ndarray,
    median  : np.ndarray,
    p75     : np.ndarray,
) -> np.ndarray:
    """Return per-bar TP log-return array for tp_spec.

    Args:
        tp_spec : One of the TP_SPECS constants.
        mean    : Per-bar bucket_mean_mfe array.
        median  : Per-bar bucket_median_mfe array.
        p75     : Per-bar bucket_p75_mfe array.

    Returns:
        Float64 array of per-bar TP log-returns.

    Raises:
        ValueError: If tp_spec is not recognised.
    """
    if tp_spec == "bucket_mean_mfe":
        return mean
    if tp_spec == "bucket_median_mfe":
        return median
    if tp_spec == "bucket_p75_mfe":
        return p75
    if tp_spec == "0.75x_bucket_mean":
        return 0.75 * mean
    if tp_spec == "0.50x_bucket_mean":
        return 0.50 * mean
    raise ValueError(f"Unknown tp_spec: {tp_spec!r}")


def _sl_lr_array(sl_spec: str, tp_lr: np.ndarray) -> np.ndarray:
    """Return per-bar SL log-return array for sl_spec.

    Args:
        sl_spec : One of the SL_SPECS constants.
        tp_lr   : Per-bar TP log-return array (already resolved).

    Returns:
        Float64 array of per-bar SL log-returns (0 = no stop).

    Raises:
        ValueError: If sl_spec is not recognised.
    """
    if sl_spec == "none":
        return np.zeros(len(tp_lr), dtype=np.float64)
    if sl_spec == "0.5x_tp":
        return 0.5 * tp_lr
    if sl_spec == "1.0x_tp":
        return tp_lr.copy()
    if sl_spec == "1.5x_tp":
        return 1.5 * tp_lr
    if sl_spec == "2.0x_tp":
        return 2.0 * tp_lr
    raise ValueError(f"Unknown sl_spec: {sl_spec!r}")


# %% Scalar helpers (kept for external use and backward-compatibility)


def _resolve_tp_lr(
    tp_spec       : str,
    bucket_mean   : float,
    bucket_median : float,
    bucket_p75    : float,
) -> float:
    """Compute the take-profit log-return for a single entry bar.

    Args:
        tp_spec       : One of the TP_SPECS constants.
        bucket_mean   : Bucket mean MFE for the entry bar.
        bucket_median : Bucket median MFE for the entry bar.
        bucket_p75    : Bucket 75th-percentile MFE for the entry bar.

    Returns:
        Take-profit log-return (scalar).

    Raises:
        ValueError: If tp_spec is not recognised.
    """
    if tp_spec == "bucket_mean_mfe":
        return bucket_mean
    if tp_spec == "bucket_median_mfe":
        return bucket_median
    if tp_spec == "bucket_p75_mfe":
        return bucket_p75
    if tp_spec == "0.75x_bucket_mean":
        return 0.75 * bucket_mean
    if tp_spec == "0.50x_bucket_mean":
        return 0.50 * bucket_mean
    raise ValueError(f"Unknown tp_spec: {tp_spec!r}")


def _resolve_sl_lr(sl_spec: str, tp_lr: float) -> float:
    """Compute the stop-loss log-return given a tp_lr.

    Args:
        sl_spec : One of the SL_SPECS constants.
        tp_lr   : The resolved take-profit log-return.

    Returns:
        Stop-loss magnitude (>= 0.0).  Zero means no stop-loss.

    Raises:
        ValueError: If sl_spec is not recognised.
    """
    if sl_spec == "none":
        return 0.0
    if sl_spec == "0.5x_tp":
        return 0.5 * tp_lr
    if sl_spec == "1.0x_tp":
        return 1.0 * tp_lr
    if sl_spec == "1.5x_tp":
        return 1.5 * tp_lr
    if sl_spec == "2.0x_tp":
        return 2.0 * tp_lr
    raise ValueError(f"Unknown sl_spec: {sl_spec!r}")


# %% Fast inner simulation (pre-extracted numpy arrays)


def _simulate_direction_fast(
    direction     : str,
    entry_cutoff  : float,
    tp_spec       : str,
    sl_spec       : str,
    N             : int,
    score_pct     : np.ndarray,
    close_arr     : np.ndarray,
    high_arr      : np.ndarray,
    low_arr       : np.ndarray,
    open_time     : np.ndarray,
    mean_arr      : np.ndarray,
    tp_lr_arr     : np.ndarray,
    sl_lr_arr     : np.ndarray,
    has_ohlcv     : bool,
    max_hold_bars : int = _MAX_HOLD_BARS,
) -> list[dict[str, Any]]:
    """Simulate one setup using pre-extracted numpy arrays.

    Finds entry candidates with np.nonzero (O(N) one-time scan), then
    processes only those candidates (typically 1-10% of N).  For each
    candidate, a numpy slice of the 60-bar holding window is searched for
    the first TP/SL touch — avoiding any Python loop over the full bar series.

    TP/SL conflict on the same bar: SL wins (conservative).
    Re-entry is allowed from the bar immediately after an exit.

    Args:
        direction     : ``"long"`` or ``"short"``.
        entry_cutoff  : Minimum score_pct to enter a position.
        tp_spec       : TP log-return spec name (stored in trade dict).
        sl_spec       : SL log-return spec name.
        N             : Total number of bars in eval_df.
        score_pct     : shape (N,) — score_pct_{direction}.
        close_arr     : shape (N,) — close price.
        high_arr      : shape (N,) — bar high price.
        low_arr       : shape (N,) — bar low price.
        open_time     : shape (N,) — bar timestamp.
        mean_arr      : shape (N,) — bucket_mean_mfe_{direction}.
        tp_lr_arr     : shape (N,) — pre-computed per-bar TP log-return.
        sl_lr_arr     : shape (N,) — pre-computed per-bar SL log-return.
        has_ohlcv     : If False, all exits fall back to timeout (no TP/SL).
        max_hold_bars : Bars before forced timeout exit (default 60).

    Returns:
        List of trade dicts (same schema as _simulate_direction).
    """
    # short_mfe_fw60 = log(fw_min / close) < 0 for profitable shorts.
    # Lower score_pct_short = better short (inverted ranking vs long).
    # tp_lr_arr / sl_lr_arr for short are negative; abs() gives the magnitude.
    is_short = direction == "short"
    if is_short:
        candidates = np.nonzero(
            ((1.0 - score_pct) >= entry_cutoff) & (tp_lr_arr < 0.0)
        )[0]
    else:
        candidates = np.nonzero(
            (score_pct >= entry_cutoff) & (tp_lr_arr > 0.0)
        )[0]

    trades: list[dict[str, Any]] = []
    next_allowed = 0

    for idx in candidates:
        i = int(idx)
        if i < next_allowed:
            continue

        entry_c = float(close_arr[i])
        tp_i    = abs(float(tp_lr_arr[i]))   # magnitude: positive for both long and short
        sl_i    = abs(float(sl_lr_arr[i]))

        win_end = min(i + max_hold_bars + 1, N)
        win_len = win_end - i - 1
        if win_len <= 0:
            continue

        exit_k      = win_len - 1   # default: timeout at last available bar
        exit_reason = "timeout"

        if has_ohlcv:
            w_high = high_arr[i + 1 : win_end]
            w_low  = low_arr [i + 1 : win_end]

            if direction == "long":
                tp_hits = np.nonzero(w_high >= entry_c * math.exp(tp_i))[0]
                sl_hits = (
                    np.nonzero(w_low <= entry_c * math.exp(-sl_i))[0]
                    if sl_i > 0 else _EMPTY
                )
            else:  # short
                tp_hits = np.nonzero(w_low  <= entry_c * math.exp(-tp_i))[0]
                sl_hits = (
                    np.nonzero(w_high >= entry_c * math.exp(sl_i))[0]
                    if sl_i > 0 else _EMPTY
                )

            first_tp = int(tp_hits[0]) if len(tp_hits) else max_hold_bars
            first_sl = int(sl_hits[0]) if len(sl_hits) else max_hold_bars

            # SL wins on same bar (conservative)
            if sl_i > 0 and first_sl <= first_tp and first_sl < win_len:
                exit_k, exit_reason = first_sl, "sl"
            elif first_tp < win_len:
                exit_k, exit_reason = first_tp, "tp"

        exit_bar = i + 1 + exit_k

        if exit_reason == "tp":
            exit_price = entry_c * math.exp(tp_i if direction == "long" else -tp_i)
            fact_lr    = tp_i
        elif exit_reason == "sl":
            exit_price = entry_c * math.exp(-sl_i if direction == "long" else sl_i)
            fact_lr    = -sl_i
        else:  # timeout
            exit_price = float(close_arr[exit_bar])
            fact_lr    = (
                math.log(exit_price / entry_c)
                if direction == "long"
                else math.log(entry_c / exit_price)
            ) if entry_c > 0 and exit_price > 0 else 0.0

        trades.append({
            "entry_time"         : open_time[i],
            "exit_time"          : open_time[exit_bar],
            "direction"          : direction,
            "entry_price"        : entry_c,
            "exit_price"         : exit_price,
            "fact_log_return"    : fact_lr,
            "exit_reason"        : exit_reason,
            "tp_lr"              : tp_i,
            "sl_lr"              : sl_i,
            "entry_cutoff"       : entry_cutoff,
            "score_pct_at_entry" : float(score_pct[i]),
            "tp_spec"            : tp_spec,
            "sl_spec"            : sl_spec,
            "hold_minutes"       : exit_k + 1,
            "bucket_mean_mfe"    : float(mean_arr[i]),
        })
        next_allowed = exit_bar + 1

    return trades


# %% Public simulation interface (DataFrame wrapper for tests)


def _simulate_direction(
    df            : pd.DataFrame,
    direction     : str,
    entry_cutoff  : float,
    tp_spec       : str,
    sl_spec       : str,
    max_hold_bars : int = _MAX_HOLD_BARS,
) -> list[dict[str, Any]]:
    """Simulate a single (direction, cutoff, tp_spec, sl_spec) setup.

    DataFrame wrapper around _simulate_direction_fast — extracts numpy arrays
    from df and delegates.  search_strategy() calls _simulate_direction_fast()
    directly with pre-extracted arrays; this wrapper exists for tests and
    one-off calls.

    TP/SL conflict on the same bar (long: high >= TP_price AND low <= SL_price):
    SL wins (conservative rule).

    Args:
        df            : Calibrated scored table for the evaluation period.
                        Required columns: open_time, score_pct_{direction},
                        bucket_mean_mfe_{direction}, bucket_median_mfe_{direction},
                        bucket_p75_mfe_{direction}, high, low, close.
        direction     : ``"long"`` or ``"short"``.
        entry_cutoff  : Minimum score_pct to enter a position.
        tp_spec       : TP log-return spec (see TP_SPECS).
        sl_spec       : SL log-return spec (see SL_SPECS).
        max_hold_bars : Number of bars before timeout exit.

    Returns:
        List of trade dicts.  Each dict contains: entry_time, exit_time,
        direction, entry_price, exit_price, fact_log_return, exit_reason,
        tp_lr, sl_lr, entry_cutoff, score_pct_at_entry, tp_spec, sl_spec,
        hold_minutes, bucket_mean_mfe.
    """
    mean_arr   = df[f"bucket_mean_mfe_{direction}"].to_numpy(np.float64)
    median_arr = df[f"bucket_median_mfe_{direction}"].to_numpy(np.float64)
    p75_arr    = df[f"bucket_p75_mfe_{direction}"].to_numpy(np.float64)
    high_arr   = df["high"].to_numpy(np.float64)
    low_arr    = df["low"].to_numpy(np.float64)

    tp_lr_arr = _tp_lr_array(tp_spec, mean_arr, median_arr, p75_arr)
    sl_lr_arr = _sl_lr_array(sl_spec, tp_lr_arr)
    has_ohlcv = not (np.all(np.isnan(high_arr)) or np.all(np.isnan(low_arr)))

    return _simulate_direction_fast(
        direction     = direction,
        entry_cutoff  = entry_cutoff,
        tp_spec       = tp_spec,
        sl_spec       = sl_spec,
        N             = len(df),
        score_pct     = df[f"score_pct_{direction}"].to_numpy(np.float64),
        close_arr     = df["close"].to_numpy(np.float64),
        high_arr      = high_arr,
        low_arr       = low_arr,
        open_time     = df["open_time"].to_numpy(),
        mean_arr      = mean_arr,
        tp_lr_arr     = tp_lr_arr,
        sl_lr_arr     = sl_lr_arr,
        has_ohlcv     = has_ohlcv,
        max_hold_bars = max_hold_bars,
    )


# %% Cutoffs helper


def _build_cutoffs(calibrated_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Derive per-direction decile cutoff rows from the calibrated scored table.

    Args:
        calibrated_df : Output of fit_calibration.

    Returns:
        List of cutoff dicts (one per direction+bucket present), or empty list.
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
                "bucket_id"      : int(bid),   # type: ignore[arg-type]
                "score_raw_lower": float(raw.min()) if len(raw) else None,
                "score_raw_upper": float(raw.max()) if len(raw) else None,
                "score_pct_upper": float(pct.max()) if len(pct) else None,
                "bucket_mean_mfe": float(mean_arr[0]) if len(mean_arr) else None,
                "bucket_hit_rate": float(hr_arr[0])   if len(hr_arr)   else None,
            })
    return rows


# %% Public API


def search_strategy(
    session_id     : str,
    long_model_id  : str,
    short_model_id : str,
    calibrated_df  : pd.DataFrame,
    start          : str,
    end            : str,
    directions     : list[str] | None = None,
    asset_id       : str | None = None,
) -> dict[str, Any]:
    """Run an execution-aware grid search over entry/TP/SL parameters.

    Filters the calibrated scored table to [start, end], then iterates over
    all combinations of ENTRY_CUTOFFS × TP_SPECS × SL_SPECS × directions.
    Selects the best setup by total_fact_log_return.  Writes strat.* DuckDB
    tables, strategy_artifact.json, grid_results.csv, and registers the
    session in reg.strategies + reg.artifacts.

    Performance: numpy arrays are extracted once per direction and tp_lr /
    sl_lr arrays are pre-computed per (tp_spec, sl_spec) group, so the
    inner per-cutoff simulation only performs an O(N) np.nonzero scan plus
    O(n_candidates × 60) numpy slice comparisons.

    Args:
        session_id     : Strategy session identifier.
        long_model_id  : Model ID for the long direction.
        short_model_id : Model ID for the short direction.
        calibrated_df  : Output of fit_calibration — must include score_pct_*,
                         bucket_mean/median/p75_mfe_*, high, low, close columns.
        start          : Search window start date YYYY-MM-DD (inclusive).
        end            : Search window end date YYYY-MM-DD (inclusive).
        directions     : Directions to search; default [``"long"``, ``"short"``].
        asset_id       : Asset key for the lab connection; resolved if None.

    Returns:
        Dict with session_id, best_setup, metrics, grid_results, and
        strat_tables.

    Raises:
        ValueError: If required columns are absent or the period is empty.
    """
    if directions is None:
        directions = ["long", "short"]

    artifact_dir = Path(utils._resolve_path("artifacts")) / session_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    df = calibrated_df
    logger.info("search_strategy: calibrated table has %d rows", len(df))

    # --- Validate required columns ---
    required_cols = [
        "score_pct_long",  "score_pct_short",
        "bucket_mean_mfe_long",   "bucket_mean_mfe_short",
        "bucket_median_mfe_long", "bucket_median_mfe_short",
        "bucket_p75_mfe_long",    "bucket_p75_mfe_short",
    ]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(
                f"Column '{col}' missing from calibrated table. "
                "Run fit_calibration first."
            )

    # --- Filter to search window ---
    start_ts      = f"{start} 00:00:00"
    end_ts        = f"{end} 23:59:59"
    open_time_str = df["open_time"].astype(str)
    mask          = (open_time_str >= start_ts) & (open_time_str <= end_ts)
    eval_df: pd.DataFrame = df[mask].copy().reset_index(drop=True)  # type: ignore[assignment]

    if eval_df.empty:
        raise ValueError(
            f"No rows in period {start} to {end}. "
            f"Full table range: {df['open_time'].min()} to {df['open_time'].max()}"
        )
    logger.info("Search period rows: %d", len(eval_df))

    # --- Grid search (arrays extracted once per direction) ---
    all_results: list[dict[str, Any]] = []
    N = len(eval_df)

    for direction in directions:
        mean_col   = f"bucket_mean_mfe_{direction}"
        median_col = f"bucket_median_mfe_{direction}"
        p75_col    = f"bucket_p75_mfe_{direction}"

        score_pct  = eval_df[f"score_pct_{direction}"].to_numpy(np.float64)
        close_arr  = eval_df["close"].to_numpy(np.float64)
        high_arr   = eval_df["high"].to_numpy(np.float64)
        low_arr    = eval_df["low"].to_numpy(np.float64)
        open_time  = eval_df["open_time"].to_numpy()
        mean_arr   = eval_df[mean_col].to_numpy(np.float64)
        median_arr = eval_df[median_col].to_numpy(np.float64)
        p75_arr    = eval_df[p75_col].to_numpy(np.float64)
        has_ohlcv  = not (np.all(np.isnan(high_arr)) or np.all(np.isnan(low_arr)))

        # tp_lr computed once per tp_spec; sl_lr once per (tp_spec, sl_spec)
        for tp_spec in TP_SPECS:
            tp_lr = _tp_lr_array(tp_spec, mean_arr, median_arr, p75_arr)
            for sl_spec in SL_SPECS:
                sl_lr = _sl_lr_array(sl_spec, tp_lr)
                for entry_cutoff in ENTRY_CUTOFFS:
                    trades   = _simulate_direction_fast(
                        direction, entry_cutoff, tp_spec, sl_spec,
                        N, score_pct, close_arr, high_arr, low_arr, open_time,
                        mean_arr, tp_lr, sl_lr, has_ohlcv,
                    )
                    total_lr = sum(t["fact_log_return"] for t in trades)
                    n_trades = len(trades)
                    all_results.append({
                        "direction"             : direction,
                        "entry_cutoff"          : entry_cutoff,
                        "tp_spec"               : tp_spec,
                        "sl_spec"               : sl_spec,
                        "n_trades"              : n_trades,
                        "total_fact_log_return" : total_lr,
                        "avg_fact_log_return"   : total_lr / n_trades if n_trades else 0.0,
                        "compounded_return_pct" : (math.exp(total_lr) - 1) * 100,
                        "win_rate"              : (
                            sum(1 for t in trades if t["fact_log_return"] > 0) / n_trades
                            if n_trades else 0.0
                        ),
                        "avg_hold_minutes"      : (
                            sum(t["hold_minutes"] for t in trades) / n_trades
                            if n_trades else 0.0
                        ),
                    })

    logger.info(
        "search_strategy: evaluated %d setups across %s",
        len(all_results), directions,
    )

    # --- Best setup ---
    best_setup  = max(all_results, key=lambda r: r["total_fact_log_return"])
    best_trades = _simulate_direction(
        eval_df,
        best_setup["direction"],
        best_setup["entry_cutoff"],
        best_setup["tp_spec"],
        best_setup["sl_spec"],
    )
    logger.info(
        "Best setup: direction=%s cutoff=%.2f tp_spec=%s sl_spec=%s "
        "total_lr=%.6f n_trades=%d",
        best_setup["direction"], best_setup["entry_cutoff"],
        best_setup["tp_spec"], best_setup["sl_spec"],
        best_setup["total_fact_log_return"], best_setup["n_trades"],
    )

    # --- Metrics for the best setup ---
    total_lr = best_setup["total_fact_log_return"]
    n        = best_setup["n_trades"]
    metrics  = {
        "n_trades"              : n,
        "total_fact_log_return" : round(total_lr, 6),
        "avg_fact_log_return"   : round(total_lr / n, 6) if n else 0.0,
        "compounded_return_pct" : round((math.exp(total_lr) - 1) * 100, 4),
        "win_rate"              : round(best_setup["win_rate"], 4),
        "avg_hold_minutes"      : round(best_setup["avg_hold_minutes"], 2),
        "sufficient_sample"     : n >= 50,
    }

    # --- Write grid_results.csv ---
    grid_df   = pd.DataFrame(all_results)
    grid_path = artifact_dir / "grid_results.csv"
    grid_df.to_csv(grid_path, index=False)
    logger.info("grid_results.csv written: %s", grid_path)

    # --- Write strat.* tables ---
    cutoffs      = _build_cutoffs(df)
    strat_tables = write_realized_outputs(
        session_id   = session_id,
        trades       = best_trades,
        grid_results = all_results,
        cutoffs      = cutoffs,
        asset_id     = asset_id,
        best_setup   = best_setup,
    )

    # --- Write strategy_artifact.json ---
    decision_params = {
        "entry_cutoff"           : best_setup["entry_cutoff"],
        "tp_spec"                : best_setup["tp_spec"],
        "sl_spec"                : best_setup["sl_spec"],
        "directions"             : list(directions),
        "max_hold_minutes"       : 60,
        "same_bar_conflict_rule" : "sl_first",
    }
    search_info = {
        "search_type"        : "grid",
        "n_setups_evaluated" : len(all_results),
        "best_objective"     : "total_fact_log_return",
        "best_value"         : round(best_setup["total_fact_log_return"], 6),
    }

    artifact_path = write_strategy_artifact(
        session_id      = session_id,
        long_model_id   = long_model_id,
        short_model_id  = short_model_id,
        fit_period      = {"start": start, "end": end},
        decision_params = decision_params,
        metrics         = metrics,
        search_info     = search_info,
    )

    # --- Register the session ---
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
        asset_id = asset_id,
    )

    return {
        "session_id"   : session_id,
        "best_setup"   : best_setup,
        "metrics"      : metrics,
        "grid_results" : all_results,
        "strat_tables" : strat_tables,
    }
