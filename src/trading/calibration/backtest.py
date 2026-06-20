"""Strategy backtesting engine for calibration.

Loads OOS predictions from sample_oos.parquet and OHLCV from DuckDB,
simulates probability-threshold long/short strategies, and writes
trade/equity/report artifacts.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

import utils
from data_handling.store import duckdb_query

logger = logging.getLogger(__name__)

# %% Data loading


def load_oos_frame(
    model_id  : str,
    start     : str | None = None,
    end       : str | None = None,
) -> pd.DataFrame:
    """Load OOS predictions and join with OHLCV bars from DuckDB.

    Reads predictions from ``artifacts/<model_id>/sample_oos.parquet`` and
    determines the prediction column name and side from the model manifest.
    OHLCV bars are loaded from the asset DuckDB via ``duckdb_query.query_range``.

    Args:
        model_id : Artifact directory name, e.g. ``lgbm_solusdt_l_fw60_2021``.
        start    : Optional start timestamp (inclusive) in ``YYYY-MM-DD`` format.
                   If None, uses the earliest row in the parquet file.
        end      : Optional end timestamp (inclusive) in ``YYYY-MM-DD`` format.
                   If None, uses the latest row in the parquet file.

    Returns:
        DataFrame with columns: open_time, open, high, low, close, target, prediction.

    Raises:
        FileNotFoundError: If sample_oos.parquet or manifest.json is missing.
        ValueError: If the prediction column is not found in the parquet file.
    """
    # --- resolve artifact paths ---
    artifact_dir  = Path(utils._resolve_path(f"artifacts/{model_id}"))
    parquet_path  = artifact_dir / "sample_oos.parquet"
    manifest_path = artifact_dir / "manifest.json"

    if not parquet_path.exists():
        raise FileNotFoundError(f"sample_oos.parquet not found: {parquet_path}")
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found: {manifest_path}")

    # --- read manifest to determine side and prediction column ---
    manifest     = json.loads(manifest_path.read_text(encoding="utf-8"))
    target_name  = manifest.get("target_name", "")
    if target_name == "long_mfe_fw60":
        side     = "long"
        pred_col = "pred_long"
    elif target_name == "short_mfe_fw60":
        side     = "short"
        pred_col = "pred_short"
    else:
        raise ValueError(
            f"Unknown target_name {target_name!r} in manifest — "
            "expected 'long_mfe_fw60' or 'short_mfe_fw60'"
        )

    logger.info("Loading OOS parquet: %s  side=%s  pred_col=%s", parquet_path, side, pred_col)

    # --- load and filter parquet ---
    oos = pd.read_parquet(parquet_path)
    oos["open_time"] = pd.to_datetime(oos["open_time"])

    if pred_col not in oos.columns:
        raise ValueError(
            f"Prediction column {pred_col!r} not found in sample_oos.parquet. "
            f"Available: {oos.columns.tolist()}"
        )

    if start:
        mask = oos["open_time"] >= pd.Timestamp(start)
        oos  = oos[mask]  # type: ignore[index]
    if end:
        mask = oos["open_time"] <= pd.Timestamp(end) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
        oos  = oos[mask]  # type: ignore[index]

    if len(oos) == 0:
        raise ValueError(
            f"No OOS rows for model {model_id!r} in range [{start}, {end}]"
        )

    actual_start = oos["open_time"].min().strftime("%Y-%m-%d %H:%M:%S")
    actual_end   = oos["open_time"].max().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("OOS range: %s → %s  rows=%d", actual_start, actual_end, len(oos))

    # --- load OHLCV from DuckDB ---
    db_cfg   = utils.load_asset_config()["database"]
    db_path  = db_cfg["db_path"]

    ohlcv = duckdb_query.query_range(
        db_path, "ohlcv", actual_start, actual_end,
        columns=["open_time", "open", "high", "low", "close"],
    )
    ohlcv["open_time"] = pd.to_datetime(ohlcv["open_time"])
    for col in ["open", "high", "low", "close"]:
        ohlcv[col] = pd.to_numeric(ohlcv[col], errors="coerce")
    ohlcv = ohlcv.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

    # --- join predictions onto OHLCV ---
    oos_slim = oos[["open_time", pred_col, target_name]].rename(  # type: ignore[index]
        columns={pred_col: "prediction", target_name: "target"}
    )
    frame = ohlcv.merge(oos_slim, on="open_time", how="inner")
    frame["prediction"] = pd.to_numeric(frame["prediction"], errors="coerce")
    frame["target"]     = pd.to_numeric(frame["target"],     errors="coerce")
    frame = (
        frame
        .sort_values("open_time")
        .drop_duplicates("open_time")
        .reset_index(drop=True)
    )

    logger.info(
        "Backtest frame ready: %d rows  prediction=[%.3f, %.3f]",
        len(frame),
        frame["prediction"].min(),
        frame["prediction"].max(),
    )
    return frame


# %% Simulation


def simulate_long_probability_strategy(
    frame        : pd.DataFrame,
    strategy_cfg : dict,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Simulate a probability-threshold LONG/FLAT or SHORT/FLAT strategy.

    Dispatches to the long or short simulator based on ``strategy_cfg["side"]``.

    Args:
        frame        : DataFrame with open_time, open, high, low, close,
                       target, prediction columns.
        strategy_cfg : Strategy parameter dict.

    Returns:
        Tuple of (trades_df, equity_df, summary_dict).

    Raises:
        ValueError: If frame is empty or side is invalid.
    """
    side = strategy_cfg.get("side", "long")
    if side not in ("long", "short"):
        raise ValueError(f"Unsupported strategy side: {side!r}")
    if side == "short":
        return _simulate_short_probability_strategy(frame, strategy_cfg)
    if frame.empty:
        raise ValueError("Backtest frame is empty")

    df = frame.copy().reset_index(drop=True)
    df["open_time"] = pd.to_datetime(df["open_time"])
    times       = df["open_time"].to_numpy()
    opens       = df["open"].astype(float).to_numpy()
    highs       = df["high"].astype(float).to_numpy()
    lows        = df["low"].astype(float).to_numpy()
    closes      = df["close"].astype(float).to_numpy()
    targets     = df["target"].astype(float).to_numpy()
    predictions = df["prediction"].astype(float).to_numpy()

    entry_threshold          = float(strategy_cfg["entry_threshold"])
    rearm_threshold          = float(strategy_cfg.get("rearm_threshold", entry_threshold))
    exit_threshold           = float(strategy_cfg.get("exit_threshold", -1.0))
    min_hold_minutes         = int(strategy_cfg.get("min_hold_minutes", 0))
    max_hold_minutes         = int(strategy_cfg.get("max_hold_minutes", 240))
    take_profit_pct          = float(strategy_cfg.get("take_profit_pct", 0.0))
    stop_loss_pct            = float(strategy_cfg.get("stop_loss_pct", 0.0))
    trailing_activation_pct  = float(strategy_cfg.get("trailing_activation_pct", 0.0))
    trailing_stop_pct        = float(strategy_cfg.get("trailing_stop_pct", 0.0))
    cooldown_minutes         = int(strategy_cfg.get("cooldown_minutes", 0))
    fee                      = float(strategy_cfg.get("fee_bps_per_side", 0.0)) / 10000.0
    slippage                 = float(strategy_cfg.get("slippage_bps_per_side", 0.0)) / 10000.0

    equity       = float(strategy_cfg.get("initial_equity", 10000.0))
    trades       = []
    equity_rows  = [{"open_time": pd.Timestamp(times[0]), "equity": equity}]
    position     = None
    armed        = True
    cooldown_until = pd.Timestamp(times[0])

    for i in range(1, len(df)):
        now             = pd.Timestamp(times[i])
        prev_time       = pd.Timestamp(times[i - 1])
        prev_prediction = float(predictions[i - 1])
        prev_target     = float(targets[i - 1])

        if position is None:
            if not armed and now >= cooldown_until and prev_prediction <= rearm_threshold:
                armed = True

            if armed and now >= cooldown_until and prev_prediction >= entry_threshold:
                entry_raw   = float(opens[i])
                entry_price = entry_raw * (1.0 + slippage)
                position = {
                    "entry_time":           now,
                    "entry_signal_time":    prev_time,
                    "entry_price":          entry_price,
                    "entry_raw_price":      entry_raw,
                    "entry_prediction":     prev_prediction,
                    "entry_target":         prev_target if pd.notna(prev_target) else None,
                    "highest_price":        entry_price,
                    "trailing_stop_price":  None,
                }
                armed = False

        if position is None:
            continue

        exit_raw    = None
        exit_reason = None
        entry_price = position["entry_price"]
        hold_minutes = int((now - position["entry_time"]).total_seconds() // 60)
        high  = float(highs[i])
        low   = float(lows[i])
        close = float(closes[i])

        hard_stop   = entry_price * (1.0 - stop_loss_pct) if stop_loss_pct > 0 else None
        take_profit = entry_price * (1.0 + take_profit_pct) if take_profit_pct > 0 else None

        if hard_stop is not None and low <= hard_stop:
            exit_raw    = hard_stop
            exit_reason = "stop_loss"
        elif take_profit is not None and high >= take_profit:
            exit_raw    = take_profit
            exit_reason = "take_profit"
        else:
            position["highest_price"] = max(position["highest_price"], high)
            if (
                trailing_stop_pct > 0
                and trailing_activation_pct > 0
                and position["highest_price"] >= entry_price * (1.0 + trailing_activation_pct)
            ):
                candidate = position["highest_price"] * (1.0 - trailing_stop_pct)
                current   = position["trailing_stop_price"]
                position["trailing_stop_price"] = candidate if current is None else max(current, candidate)

            trailing_stop = position["trailing_stop_price"]
            if trailing_stop is not None and low <= trailing_stop:
                exit_raw    = trailing_stop
                exit_reason = "trailing_stop"
            elif hold_minutes >= max_hold_minutes:
                exit_raw    = close
                exit_reason = "max_hold"
            elif hold_minutes >= min_hold_minutes and prev_prediction <= exit_threshold:
                exit_raw    = float(opens[i])
                exit_reason = "probability_exit"

        if exit_raw is None:
            continue

        exit_price  = float(exit_raw) * (1.0 - slippage)
        net_return  = (exit_price * (1.0 - fee)) / (entry_price * (1.0 + fee)) - 1.0
        gross_return = (float(exit_raw) / position["entry_raw_price"]) - 1.0
        equity *= 1.0 + net_return
        trades.append({
            "entry_time":         position["entry_time"].strftime("%Y-%m-%d %H:%M:%S"),
            "entry_signal_time":  position["entry_signal_time"].strftime("%Y-%m-%d %H:%M:%S"),
            "exit_time":          now.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_reason":        exit_reason,
            "hold_minutes":       hold_minutes,
            "entry_price":        position["entry_price"],
            "exit_price":         exit_price,
            "entry_prediction":   position["entry_prediction"],
            "entry_target":       position["entry_target"],
            "gross_return":       gross_return,
            "net_return":         net_return,
            "equity_after":       equity,
        })
        equity_rows.append({"open_time": now, "equity": equity})
        position       = None
        cooldown_until = now + pd.Timedelta(minutes=cooldown_minutes)

    if position is not None:
        exit_time    = pd.Timestamp(times[-1])
        exit_raw     = float(closes[-1])
        exit_price   = exit_raw * (1.0 - slippage)
        net_return   = (exit_price * (1.0 - fee)) / (position["entry_price"] * (1.0 + fee)) - 1.0
        gross_return = (exit_raw / position["entry_raw_price"]) - 1.0
        equity *= 1.0 + net_return
        trades.append({
            "entry_time":        position["entry_time"].strftime("%Y-%m-%d %H:%M:%S"),
            "entry_signal_time": position["entry_signal_time"].strftime("%Y-%m-%d %H:%M:%S"),
            "exit_time":         exit_time.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_reason":       "end_of_backtest",
            "hold_minutes":      int((exit_time - position["entry_time"]).total_seconds() // 60),
            "entry_price":       position["entry_price"],
            "exit_price":        exit_price,
            "entry_prediction":  position["entry_prediction"],
            "entry_target":      position["entry_target"],
            "gross_return":      gross_return,
            "net_return":        net_return,
            "equity_after":      equity,
        })
        equity_rows.append({"open_time": exit_time, "equity": equity})

    trades_df  = pd.DataFrame(trades)
    equity_df  = pd.DataFrame(equity_rows)
    summary    = summarize_trades(
        trades_df      = trades_df,
        equity_df      = equity_df,
        initial_equity = float(strategy_cfg.get("initial_equity", 10000.0)),
        start_time     = pd.Timestamp(times[0]),   # type: ignore[arg-type]
        end_time       = pd.Timestamp(times[-1]),   # type: ignore[arg-type]
    )
    return trades_df, equity_df, summary


def _simulate_short_probability_strategy(
    frame        : pd.DataFrame,
    strategy_cfg : dict,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Simulate a probability-threshold SHORT/FLAT strategy.

    Mirror of the long simulator — P&L is positive when price falls after entry.

    Args:
        frame        : DataFrame with open_time, open, high, low, close,
                       target, prediction columns.
        strategy_cfg : Strategy parameter dict.

    Returns:
        Tuple of (trades_df, equity_df, summary_dict).

    Raises:
        ValueError: If frame is empty.
    """
    if frame.empty:
        raise ValueError("Backtest frame is empty")

    df = frame.copy().reset_index(drop=True)
    df["open_time"] = pd.to_datetime(df["open_time"])
    times       = df["open_time"].to_numpy()
    opens       = df["open"].astype(float).to_numpy()
    highs       = df["high"].astype(float).to_numpy()
    lows        = df["low"].astype(float).to_numpy()
    closes      = df["close"].astype(float).to_numpy()
    targets     = df["target"].astype(float).to_numpy()
    predictions = df["prediction"].astype(float).to_numpy()

    entry_threshold          = float(strategy_cfg["entry_threshold"])
    rearm_threshold          = float(strategy_cfg.get("rearm_threshold", entry_threshold))
    exit_threshold           = float(strategy_cfg.get("exit_threshold", -1.0))
    min_hold_minutes         = int(strategy_cfg.get("min_hold_minutes", 0))
    max_hold_minutes         = int(strategy_cfg.get("max_hold_minutes", 240))
    take_profit_pct          = float(strategy_cfg.get("take_profit_pct", 0.0))
    stop_loss_pct            = float(strategy_cfg.get("stop_loss_pct", 0.0))
    trailing_activation_pct  = float(strategy_cfg.get("trailing_activation_pct", 0.0))
    trailing_stop_pct        = float(strategy_cfg.get("trailing_stop_pct", 0.0))
    cooldown_minutes         = int(strategy_cfg.get("cooldown_minutes", 0))
    fee                      = float(strategy_cfg.get("fee_bps_per_side", 0.0)) / 10000.0
    slippage                 = float(strategy_cfg.get("slippage_bps_per_side", 0.0)) / 10000.0

    equity       = float(strategy_cfg.get("initial_equity", 10000.0))
    trades       = []
    equity_rows  = [{"open_time": pd.Timestamp(times[0]), "equity": equity}]
    position     = None
    armed        = True
    cooldown_until = pd.Timestamp(times[0])

    for i in range(1, len(df)):
        now             = pd.Timestamp(times[i])
        prev_time       = pd.Timestamp(times[i - 1])
        prev_prediction = float(predictions[i - 1])
        prev_target     = float(targets[i - 1])

        if position is None:
            if not armed and now >= cooldown_until and prev_prediction <= rearm_threshold:
                armed = True

            if armed and now >= cooldown_until and prev_prediction >= entry_threshold:
                entry_raw   = float(opens[i])
                entry_price = entry_raw * (1.0 - slippage)
                position = {
                    "entry_time":          now,
                    "entry_signal_time":   prev_time,
                    "entry_price":         entry_price,
                    "entry_raw_price":     entry_raw,
                    "entry_prediction":    prev_prediction,
                    "entry_target":        prev_target if pd.notna(prev_target) else None,
                    "lowest_price":        entry_price,
                    "trailing_cover_price": None,
                }
                armed = False

        if position is None:
            continue

        exit_raw    = None
        exit_reason = None
        entry_price = position["entry_price"]
        hold_minutes = int((now - position["entry_time"]).total_seconds() // 60)
        high  = float(highs[i])
        low   = float(lows[i])
        close = float(closes[i])

        hard_stop   = entry_price * (1.0 + stop_loss_pct) if stop_loss_pct > 0 else None
        take_profit = entry_price * (1.0 - take_profit_pct) if take_profit_pct > 0 else None

        if hard_stop is not None and high >= hard_stop:
            exit_raw    = hard_stop
            exit_reason = "stop_loss"
        elif take_profit is not None and low <= take_profit:
            exit_raw    = take_profit
            exit_reason = "take_profit"
        else:
            position["lowest_price"] = min(position["lowest_price"], low)
            if (
                trailing_stop_pct > 0
                and trailing_activation_pct > 0
                and position["lowest_price"] <= entry_price * (1.0 - trailing_activation_pct)
            ):
                candidate = position["lowest_price"] * (1.0 + trailing_stop_pct)
                current   = position["trailing_cover_price"]
                position["trailing_cover_price"] = candidate if current is None else min(current, candidate)

            trailing_cover = position["trailing_cover_price"]
            if trailing_cover is not None and high >= trailing_cover:
                exit_raw    = trailing_cover
                exit_reason = "trailing_stop"
            elif hold_minutes >= max_hold_minutes:
                exit_raw    = close
                exit_reason = "max_hold"
            elif hold_minutes >= min_hold_minutes and prev_prediction <= exit_threshold:
                exit_raw    = float(opens[i])
                exit_reason = "probability_exit"

        if exit_raw is None:
            continue

        exit_price   = float(exit_raw) * (1.0 + slippage)
        net_return   = (entry_price * (1.0 - fee)) / (exit_price * (1.0 + fee)) - 1.0
        gross_return = (position["entry_raw_price"] - float(exit_raw)) / position["entry_raw_price"]
        equity *= 1.0 + net_return
        trades.append({
            "entry_time":        position["entry_time"].strftime("%Y-%m-%d %H:%M:%S"),
            "entry_signal_time": position["entry_signal_time"].strftime("%Y-%m-%d %H:%M:%S"),
            "exit_time":         now.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_reason":       exit_reason,
            "hold_minutes":      hold_minutes,
            "entry_price":       position["entry_price"],
            "exit_price":        exit_price,
            "entry_prediction":  position["entry_prediction"],
            "entry_target":      position["entry_target"],
            "gross_return":      gross_return,
            "net_return":        net_return,
            "equity_after":      equity,
        })
        equity_rows.append({"open_time": now, "equity": equity})
        position       = None
        cooldown_until = now + pd.Timedelta(minutes=cooldown_minutes)

    if position is not None:
        exit_time    = pd.Timestamp(times[-1])
        exit_raw     = float(closes[-1])
        exit_price   = exit_raw * (1.0 + slippage)
        net_return   = (position["entry_price"] * (1.0 - fee)) / (exit_price * (1.0 + fee)) - 1.0
        gross_return = (position["entry_raw_price"] - exit_raw) / position["entry_raw_price"]
        equity *= 1.0 + net_return
        trades.append({
            "entry_time":        position["entry_time"].strftime("%Y-%m-%d %H:%M:%S"),
            "entry_signal_time": position["entry_signal_time"].strftime("%Y-%m-%d %H:%M:%S"),
            "exit_time":         exit_time.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_reason":       "end_of_backtest",
            "hold_minutes":      int((exit_time - position["entry_time"]).total_seconds() // 60),
            "entry_price":       position["entry_price"],
            "exit_price":        exit_price,
            "entry_prediction":  position["entry_prediction"],
            "entry_target":      position["entry_target"],
            "gross_return":      gross_return,
            "net_return":        net_return,
            "equity_after":      equity,
        })
        equity_rows.append({"open_time": exit_time, "equity": equity})

    trades_df  = pd.DataFrame(trades)
    equity_df  = pd.DataFrame(equity_rows)
    summary    = summarize_trades(
        trades_df      = trades_df,
        equity_df      = equity_df,
        initial_equity = float(strategy_cfg.get("initial_equity", 10000.0)),
        start_time     = pd.Timestamp(times[0]),   # type: ignore[arg-type]
        end_time       = pd.Timestamp(times[-1]),   # type: ignore[arg-type]
    )
    return trades_df, equity_df, summary


# %% Metrics


def summarize_trades(
    trades_df      : pd.DataFrame,
    equity_df      : pd.DataFrame,
    initial_equity : float,
    start_time     : pd.Timestamp,
    end_time       : pd.Timestamp,
) -> dict:
    """Calculate trade-level and equity-level backtest metrics.

    Args:
        trades_df      : DataFrame of individual trade records.
        equity_df      : DataFrame with open_time and equity columns.
        initial_equity : Starting equity value.
        start_time     : First bar timestamp.
        end_time       : Last bar timestamp.

    Returns:
        Dict of summary metrics.
    """
    if trades_df.empty:
        return {
            "trade_count":                    0,
            "initial_equity":                 initial_equity,
            "final_equity":                   initial_equity,
            "total_return":                   0.0,
            "win_rate":                       None,
            "profit_factor":                  None,
            "max_drawdown":                   0.0,
            "avg_hold_minutes":               None,
            "median_minutes_between_entries": None,
            "exposure_pct":                   0.0,
        }

    returns          = trades_df["net_return"].astype(float)
    wins             = returns[returns > 0]
    losses           = returns[returns <= 0]
    final_equity     = float(equity_df["equity"].iloc[-1])
    entry_times      = pd.to_datetime(trades_df["entry_time"])
    between_entries  = entry_times.diff().dt.total_seconds().dropna() / 60.0
    total_minutes    = max(1.0, (end_time - start_time).total_seconds() / 60.0)
    exposure_minutes = trades_df["hold_minutes"].astype(float).sum()

    equity      = equity_df["equity"].astype(float)
    running_max = equity.cummax()
    drawdown    = equity / running_max - 1.0

    return {
        "initial_equity":                 initial_equity,
        "final_equity":                   final_equity,
        "profit":                         final_equity - initial_equity,
        "total_return":                   final_equity / initial_equity - 1.0,
        "trade_count":                    int(len(trades_df)),
        "winning_trades":                 int((returns > 0).sum()),
        "losing_trades":                  int((returns <= 0).sum()),
        "win_rate":                       float((returns > 0).mean()),      # type: ignore[arg-type]
        "avg_net_return":                 float(returns.mean()),             # type: ignore[arg-type]
        "median_net_return":              float(returns.median()),           # type: ignore[arg-type]
        "best_trade":                     float(returns.max()),              # type: ignore[arg-type]
        "worst_trade":                    float(returns.min()),              # type: ignore[arg-type]
        "profit_factor":                  _profit_factor(wins, losses),     # type: ignore[arg-type]
        "max_drawdown":                   float(drawdown.min()),             # type: ignore[arg-type]
        "avg_hold_minutes":               float(trades_df["hold_minutes"].mean()),   # type: ignore[arg-type]
        "median_hold_minutes":            float(trades_df["hold_minutes"].median()), # type: ignore[arg-type]
        "avg_minutes_between_entries":    float(between_entries.mean()) if not between_entries.empty else None,
        "median_minutes_between_entries": float(between_entries.median()) if not between_entries.empty else None,
        "exposure_pct":                   float(exposure_minutes / total_minutes),
        "exit_reasons":                   trades_df["exit_reason"].value_counts().to_dict(),
    }


def _profit_factor(wins: pd.Series, losses: pd.Series) -> float | None:
    """Return gross profit divided by absolute gross loss.

    Args:
        wins   : Series of positive net returns.
        losses : Series of non-positive net returns.

    Returns:
        Ratio or None if there are no losing trades.
    """
    loss_sum = float(losses.sum())
    if loss_sum == 0:
        return None
    return float(wins.sum() / abs(loss_sum))


# %% Report


def write_backtest_report(
    output_dir   : Path,
    strategy_id  : str,
    strategy_cfg : dict,
    summary      : dict,
    trades_df    : pd.DataFrame,
) -> None:
    """Write an HTML backtest report to output_dir/report.html.

    Args:
        output_dir   : Directory where report.html will be written.
        strategy_id  : Human-readable strategy label for the report title.
        strategy_cfg : Strategy parameter dict.
        summary      : Summary metrics dict from summarize_trades().
        trades_df    : DataFrame of individual trade records.
    """
    summary_df = pd.DataFrame([summary])
    exit_df    = pd.DataFrame(
        [{"exit_reason": key, "count": value}
         for key, value in summary.get("exit_reasons", {}).items()]
    )
    recent_trades = trades_df.tail(50).copy()
    html = f"""<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>{strategy_id} backtest report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 32px; color: #222; }}
        table {{ border-collapse: collapse; margin: 12px 0 28px 0; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: right; }}
        th:first-child, td:first-child {{ text-align: left; }}
        th {{ background: #f3f5f7; }}
    </style>
</head>
<body>
    <h1>{strategy_id} backtest report</h1>
    <h2>Strategy config</h2>
    <pre>{json.dumps(strategy_cfg, indent=4)}</pre>
    <h2>Summary</h2>
    {summary_df.to_html(index=False, escape=True, float_format=lambda x: f"{x:.6f}")}
    <h2>Exit reasons</h2>
    {exit_df.to_html(index=False, escape=True) if not exit_df.empty else "<p>No trades.</p>"}
    <h2>Recent trades</h2>
    {recent_trades.to_html(index=False, escape=True, float_format=lambda x: f"{x:.6f}") if not recent_trades.empty else "<p>No trades.</p>"}
</body>
</html>
"""
    (output_dir / "report.html").write_text(html, encoding="utf-8")
