"""K=10 realistic OOS backtest with CLUSTER-SPECIFIC calibrated TP/SL targets.

Tests whether the previous fixed TP (+2%/-1%) was mismatched to each cluster's
actual MFE magnitude, causing near-unreachable targets for weaker clusters
(e.g. cluster 1's OOS short MFE is only -1.50%, well below the fixed -2% TP).

Calibration: TP_cluster[i] = |train_mfe_mean[i]| * 0.7 (70% safety margin below
the training-period average excursion), SL_cluster[i] = TP_cluster[i] / 2
(fixed 2:1 risk/reward ratio, same as the fixed-TP backtest).

Pipeline
--------
01_target_computation -> 02_window_representation -> 08_paired_ttest_classification
    -> 09_calibrated_tp_backtest (THIS SCRIPT)
"""

from __future__ import annotations

import json
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)

# Configuration
TRAIN_START = "2021-01-01"
TRAIN_END = "2025-06-30"
OOS_START = "2025-07-01"
OOS_END = "2026-06-30"

POSITION_SIZE_USD = 1000.0
TIMEOUT_HOURS = 24.0
COSTS_PCT = 0.003
SLIPPAGE_ENTRY_PCT = 0.002
SLIPPAGE_EXIT_PCT = 0.002

TP_SAFETY_MARGIN = 0.7  # TP = |train_mfe_mean| * 0.7
SL_RATIO = 0.5  # SL = TP / 2 (2:1 risk/reward)

# Corrected tradeable set (from 08_paired_ttest_classification.py, paired t-test, Bonferroni)
TRADEABLE_SETUPS = {
    3: "long",
    9: "long",
    1: "short",
    5: "short",
    6: "short",
    7: "short",
    8: "short",
}

MODEL_PATH = "artifacts/research_cluster_regime/kmeans_k10_trained_2021_2025_06.pkl"
WINDOW_REPR_PATH = "artifacts/research_cluster_regime/solusdt_window_repr.parquet"


@dataclass
class Trade:
    """Record of a single trade with cluster-specific TP/SL."""
    entry_time: datetime
    exit_time: datetime
    cluster: int
    direction: str
    tp_pct: float
    sl_pct: float
    entry_price: float
    exit_price: float
    exit_reason: str
    mfe: float
    realized_pnl: float
    pnl_pct: float
    win: bool


def _load_window_representation() -> tuple[pd.DataFrame, np.ndarray]:
    """Load windowed feature representation from Parquet."""
    artifact_path = Path(WINDOW_REPR_PATH)
    if not artifact_path.exists():
        raise FileNotFoundError(f"Window representation not found: {artifact_path}")

    df = pl.read_parquet(str(artifact_path)).to_pandas()
    df["open_time"] = pd.to_datetime(df["open_time"])

    all_cols = df.columns.tolist()
    skip_prefixes = ("open_time", "long_mfe_fw", "short_mfe_fw")
    feature_cols = [c for c in all_cols if not any(c.startswith(p) for p in skip_prefixes)]

    X = df[feature_cols].values.astype(np.float64)

    for j in range(X.shape[1]):
        col_data = X[:, j]
        mask_nan = np.isnan(col_data)
        if np.any(mask_nan):
            col_median = np.nanmedian(col_data)
            X[mask_nan, j] = col_median

    logger.info("Loaded: %d rows, %d features", len(df), len(feature_cols))
    return df, X


def _load_ohlcv_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Load OHLCV data from DuckDB."""
    db_path = Path("database/solusdt/solusdt.duckdb")
    conn = duckdb.connect(str(db_path), read_only=True)

    try:
        query = (
            "SELECT open_time, open, high, low, close FROM ohlcv "
            f"WHERE open_time >= '{start_date}' AND open_time <= '{end_date}' "
            "ORDER BY open_time"
        )
        df = conn.execute(query).df()
        df["open_time"] = pd.to_datetime(df["open_time"])
        logger.info("Loaded OHLCV: %d rows (%s to %s)", len(df), df["open_time"].min(), df["open_time"].max())
        return df
    finally:
        conn.close()


def _compute_calibrated_tp_sl(
    df_train: pd.DataFrame,
    labels_train: np.ndarray,
) -> dict[int, dict[str, float]]:
    """Compute per-cluster calibrated TP/SL from training period MFE.

    Args:
        df_train     : Training DataFrame with long_mfe_fw360, short_mfe_fw360.
        labels_train : Cluster labels for the training rows.

    Returns:
        Dict of {cluster_id: {"tp_pct": ..., "sl_pct": ..., "train_mfe_mean": ...}}
    """
    calibration = {}

    for cluster_id, direction in TRADEABLE_SETUPS.items():
        mask = labels_train == cluster_id
        target_col = "long_mfe_fw360" if direction == "long" else "short_mfe_fw360"
        mfe_values = df_train.loc[mask, target_col].dropna().values

        train_mfe_mean = float(np.mean(mfe_values)) if len(mfe_values) > 0 else 0.0
        tp_pct = abs(train_mfe_mean) * TP_SAFETY_MARGIN
        sl_pct = tp_pct * SL_RATIO

        calibration[cluster_id] = {
            "direction": direction,
            "train_n": int(np.sum(mask)),
            "train_mfe_mean": train_mfe_mean,
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
        }

        logger.info(
            "Cluster %d (%s): train_mfe_mean=%+.6f -> TP=%.4f%%, SL=%.4f%%",
            cluster_id, direction, train_mfe_mean, tp_pct * 100, sl_pct * 100
        )

    return calibration


def _simulate_trades_calibrated(
    df_ohlcv: pd.DataFrame,
    cluster_ids: np.ndarray,
    calibration: dict[int, dict[str, float]],
) -> list[Trade]:
    """Simulate trades using cluster-specific calibrated TP/SL targets.

    Args:
        df_ohlcv    : DataFrame with open_time, high, low, close.
        cluster_ids : Cluster label for each observation.
        calibration : Per-cluster TP/SL calibration dict.

    Returns:
        List of Trade objects.
    """
    if len(df_ohlcv) != len(cluster_ids):
        raise ValueError("df_ohlcv and cluster_ids must have same length")

    trades: list[Trade] = []

    df = df_ohlcv.copy()
    df["cluster"] = cluster_ids

    current_position: dict[str, Any] | None = None

    for _, row in df.iterrows():
        cluster_id = int(row["cluster"])

        if cluster_id not in calibration:
            continue

        cal = calibration[cluster_id]
        direction = cal["direction"]
        tp_pct = cal["tp_pct"]
        sl_pct = cal["sl_pct"]

        if current_position is None:
            current_position = {
                "entry_time": row["open_time"],
                "entry_price": row["close"],
                "cluster": cluster_id,
                "direction": direction,
                "tp_pct": tp_pct,
                "sl_pct": sl_pct,
                "mfe": 0.0,
                "max_high": row["high"],
                "min_low": row["low"],
            }
            continue

        # Update MFE based on direction
        if current_position["direction"] == "long":
            current_position["max_high"] = max(current_position["max_high"], row["high"])
            current_position["mfe"] = (current_position["max_high"] / current_position["entry_price"]) - 1.0
        else:
            current_position["min_low"] = min(current_position["min_low"], row["low"])
            current_position["mfe"] = (current_position["entry_price"] / current_position["min_low"]) - 1.0

        c_tp = current_position["tp_pct"]
        c_sl = current_position["sl_pct"]

        exit_signal = None
        exit_price = row["close"]

        if current_position["direction"] == "long":
            current_return = (row["close"] / current_position["entry_price"]) - 1.0

            if current_return >= c_tp:
                exit_signal = "tp"
                exit_price = current_position["entry_price"] * (1.0 + c_tp - SLIPPAGE_EXIT_PCT)
            elif row["low"] / current_position["entry_price"] - 1.0 <= -c_sl:
                exit_signal = "sl"
                exit_price = current_position["entry_price"] * (1.0 - c_sl + SLIPPAGE_EXIT_PCT)
            elif (row["open_time"] - current_position["entry_time"]).total_seconds() >= TIMEOUT_HOURS * 3600:
                exit_signal = "timeout"
                exit_price = row["close"] * (1.0 - SLIPPAGE_EXIT_PCT)

        else:  # short
            if (row["close"] / current_position["entry_price"]) <= (1.0 - c_tp):
                exit_signal = "tp"
                exit_price = current_position["entry_price"] * (1.0 - c_tp + SLIPPAGE_EXIT_PCT)
            elif row["high"] / current_position["entry_price"] - 1.0 >= c_sl:
                exit_signal = "sl"
                exit_price = current_position["entry_price"] * (1.0 + c_sl - SLIPPAGE_EXIT_PCT)
            elif (row["open_time"] - current_position["entry_time"]).total_seconds() >= TIMEOUT_HOURS * 3600:
                exit_signal = "timeout"
                exit_price = row["close"] * (1.0 + SLIPPAGE_EXIT_PCT)

        if exit_signal:
            if current_position["direction"] == "long":
                entry_price_adjusted = current_position["entry_price"] * (1.0 + SLIPPAGE_ENTRY_PCT)
                realized_return = (exit_price - entry_price_adjusted) / entry_price_adjusted
            else:
                entry_price_adjusted = current_position["entry_price"] * (1.0 - SLIPPAGE_ENTRY_PCT)
                realized_return = (entry_price_adjusted - exit_price) / entry_price_adjusted

            realized_pnl = POSITION_SIZE_USD * realized_return - (POSITION_SIZE_USD * COSTS_PCT)
            pnl_pct = (realized_pnl / POSITION_SIZE_USD) * 100.0
            win = realized_pnl > 0

            trades.append(
                Trade(
                    entry_time=current_position["entry_time"],
                    exit_time=row["open_time"],
                    cluster=current_position["cluster"],
                    direction=current_position["direction"],
                    tp_pct=c_tp,
                    sl_pct=c_sl,
                    entry_price=entry_price_adjusted,
                    exit_price=exit_price,
                    exit_reason=exit_signal,
                    mfe=current_position["mfe"],
                    realized_pnl=realized_pnl,
                    pnl_pct=pnl_pct,
                    win=win,
                )
            )
            current_position = None

    logger.info("Simulated %d trades (calibrated TP/SL)", len(trades))
    return trades


def main() -> dict[str, Any]:
    """Run the calibrated-TP backtest end to end."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    output_dir = Path("artifacts/research_cluster_regime")
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Step 1: Loading trained K-means model...")
    with open(MODEL_PATH, "rb") as f:
        kmeans = pickle.load(f)

    logger.info("Step 2: Loading window representation...")
    df, X = _load_window_representation()

    train_mask = (df["open_time"] >= TRAIN_START) & (df["open_time"] <= TRAIN_END)
    df_train = df[train_mask].reset_index(drop=True)
    X_train = X[train_mask]

    oos_mask = (df["open_time"] >= OOS_START) & (df["open_time"] <= OOS_END)
    df_oos = df[oos_mask].reset_index(drop=True)
    X_oos = X[oos_mask]

    logger.info("Train: %d rows, OOS: %d rows", len(df_train), len(df_oos))

    logger.info("Step 3: Computing calibrated per-cluster TP/SL from training MFE...")
    labels_train = kmeans.predict(X_train)
    calibration = _compute_calibrated_tp_sl(df_train, labels_train)

    logger.info("Step 4: Predicting OOS cluster assignments...")
    cluster_ids_oos = kmeans.predict(X_oos)

    logger.info("Step 5: Loading OOS OHLCV data...")
    df_ohlcv_oos = _load_ohlcv_data(OOS_START, OOS_END)

    df_merged = pd.merge(
        df_ohlcv_oos,
        df_oos[["open_time"]].reset_index().rename(columns={"index": "cluster_idx"}),
        on="open_time",
        how="inner",
    )
    if len(df_merged) != len(df_oos):
        logger.warning("OHLCV/cluster mismatch: %d vs %d rows", len(df_merged), len(df_oos))

    cluster_idx_map = {i: cluster_ids_oos[i] for i in range(len(cluster_ids_oos))}
    cluster_ids_aligned = df_merged["cluster_idx"].map(cluster_idx_map).to_numpy()

    logger.info("Step 6: Simulating trades with calibrated TP/SL...")
    trades = _simulate_trades_calibrated(
        df_merged[["open_time", "open", "high", "low", "close"]],
        cluster_ids_aligned,
        calibration,
    )

    logger.info("Step 7: Saving outputs...")
    trades_data = [
        {
            "entry_time": t.entry_time.isoformat(),
            "exit_time": t.exit_time.isoformat(),
            "cluster": t.cluster,
            "direction": t.direction,
            "tp_pct": t.tp_pct,
            "sl_pct": t.sl_pct,
            "entry_price": t.entry_price,
            "exit_price": t.exit_price,
            "exit_reason": t.exit_reason,
            "mfe": t.mfe,
            "realized_pnl": t.realized_pnl,
            "pnl_pct": t.pnl_pct,
            "win": t.win,
        }
        for t in trades
    ]
    df_trades = pd.DataFrame(trades_data)
    trades_csv_path = output_dir / "k10_calibrated_tp_backtest_2025_07_2026_06.csv"
    df_trades.to_csv(trades_csv_path, index=False)
    logger.info("Trade log saved to %s (%d trades)", trades_csv_path, len(trades))

    n_trades = len(trades)
    win_count = sum(1 for t in trades if t.win)
    win_rate_pct = (win_count / n_trades * 100.0) if n_trades > 0 else 0.0
    pnls = [t.realized_pnl for t in trades]
    avg_pnl = float(np.mean(pnls)) if pnls else 0.0
    net_pnl = float(sum(pnls))
    gross_pnl = net_pnl + (n_trades * POSITION_SIZE_USD * COSTS_PCT)

    if pnls:
        cumulative_pnl = np.cumsum(pnls)
        running_max = np.maximum.accumulate(cumulative_pnl)
        drawdown = cumulative_pnl - running_max
        max_drawdown = float(np.min(drawdown))
    else:
        max_drawdown = 0.0

    if n_trades > 1:
        returns_daily = np.array([t.pnl_pct / 100.0 for t in trades])
        std_r = np.std(returns_daily)
        sharpe_ratio = float(np.mean(returns_daily) / std_r * np.sqrt(252)) if std_r > 0 else 0.0
    else:
        sharpe_ratio = 0.0

    per_cluster_stats = {}
    for cluster_id, cal in calibration.items():
        cluster_trades = [t for t in trades if t.cluster == cluster_id]
        c_n = len(cluster_trades)
        c_wins = sum(1 for t in cluster_trades if t.win)
        c_pnls = [t.realized_pnl for t in cluster_trades]
        per_cluster_stats[str(cluster_id)] = {
            "direction": cal["direction"],
            "tp_pct": cal["tp_pct"],
            "sl_pct": cal["sl_pct"],
            "train_mfe_mean": cal["train_mfe_mean"],
            "total_trades": c_n,
            "win_count": c_wins,
            "win_rate_pct": (c_wins / c_n * 100.0) if c_n > 0 else None,
            "net_pnl": float(sum(c_pnls)) if c_pnls else 0.0,
            "avg_pnl": float(np.mean(c_pnls)) if c_pnls else None,
        }

    per_direction_stats = {}
    for direction in ("long", "short"):
        dir_trades = [t for t in trades if t.direction == direction]
        d_n = len(dir_trades)
        d_wins = sum(1 for t in dir_trades if t.win)
        d_pnls = [t.realized_pnl for t in dir_trades]
        per_direction_stats[direction] = {
            "total_trades": d_n,
            "win_count": d_wins,
            "win_rate_pct": (d_wins / d_n * 100.0) if d_n > 0 else None,
            "net_pnl": float(sum(d_pnls)) if d_pnls else 0.0,
        }

    summary = {
        "metadata": {
            "description": (
                "Calibrated-TP realistic OOS backtest with K=10 clustering (t871 follow-up). "
                "TP_cluster = |train_mfe_mean| * 0.7, SL_cluster = TP_cluster * 0.5 (2:1 R:R). "
                "Tests whether the fixed +2%/-1% TP/SL (mismatched to actual per-cluster MFE "
                "magnitude) was suppressing a real signal."
            ),
            "date": datetime.now().isoformat(),
            "train_period": [TRAIN_START, TRAIN_END],
            "oos_period": [OOS_START, OOS_END],
            "tp_safety_margin": TP_SAFETY_MARGIN,
            "sl_ratio": SL_RATIO,
            "costs_pct": COSTS_PCT,
            "slippage_entry_pct": SLIPPAGE_ENTRY_PCT,
            "slippage_exit_pct": SLIPPAGE_EXIT_PCT,
            "timeout_hours": TIMEOUT_HOURS,
            "position_size_usd": POSITION_SIZE_USD,
        },
        "calibration": calibration,
        "backtest_results": {
            "total_trades": n_trades,
            "win_count": win_count,
            "win_rate_pct": win_rate_pct,
            "avg_pnl": avg_pnl,
            "net_pnl": net_pnl,
            "gross_pnl": gross_pnl,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
        },
        "per_cluster_breakdown": per_cluster_stats,
        "per_direction_breakdown": per_direction_stats,
        "output_files": {
            "trades_csv": str(trades_csv_path),
        },
    }

    summary_json_path = output_dir / "k10_calibrated_tp_backtest_summary.json"
    with open(summary_json_path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info("Summary saved to %s", summary_json_path)

    print("\n" + "=" * 90)
    print("K=10 CALIBRATED-TP BACKTEST SUMMARY (2025-07 to 2026-06)")
    print("TP_cluster = |train_mfe_mean| x 0.7, SL_cluster = TP_cluster x 0.5")
    print("=" * 90)
    bt = summary["backtest_results"]
    print(f"Total Trades:      {bt['total_trades']}")
    print(f"Win Count:         {bt['win_count']}")
    print(f"Win Rate:          {bt['win_rate_pct']:.2f}%")
    print(f"Avg P&L:           ${bt['avg_pnl']:.2f}")
    print(f"Net P&L:           ${bt['net_pnl']:.2f}")
    print(f"Gross P&L:         ${bt['gross_pnl']:.2f}")
    print(f"Max Drawdown:      ${bt['max_drawdown']:.2f}")
    print(f"Sharpe Ratio:      {bt['sharpe_ratio']:.3f}")
    print("-" * 90)
    print("PER-CLUSTER BREAKDOWN (TP/SL calibrated per cluster):")
    for cluster_id, stats in per_cluster_stats.items():
        wr = f"{stats['win_rate_pct']:.2f}%" if stats["win_rate_pct"] is not None else "N/A"
        print(
            f"  Cluster {cluster_id} ({stats['direction']}): TP={stats['tp_pct']*100:.3f}% "
            f"SL={stats['sl_pct']*100:.3f}% | n={stats['total_trades']}, win_rate={wr}, "
            f"net_pnl=${stats['net_pnl']:.2f}"
        )
    print("-" * 90)
    print("PER-DIRECTION BREAKDOWN:")
    for direction, stats in per_direction_stats.items():
        wr = f"{stats['win_rate_pct']:.2f}%" if stats["win_rate_pct"] is not None else "N/A"
        print(f"  {direction.upper()}: n={stats['total_trades']}, win_rate={wr}, net_pnl=${stats['net_pnl']:.2f}")
    print("=" * 90)

    return summary


if __name__ == "__main__":
    main()
