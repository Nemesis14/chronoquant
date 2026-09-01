"""K=10 clustering training (2021-2025-06) + realistic OOS backtesting (2025-07-2026-06).

Performs K-means clustering with k=10 on the full training data (2021-01 — 2025-06-30),
then applies the trained model to OOS data (2025-07-01 — 2026-06-30) and simulates
realistic trading with costs and slippage.

Pipeline
--------
01_target_computation  →  02_window_representation  →  05_k10_training_oos_backtest

Data flow
---------
solusdt_window_repr.parquet (104K rows × 70 features + targets)
    →  K-means on 70-dimensional space (k=10, deterministic)
    →  Cluster labels for OOS period (2025-07 — 2026-06)
    →  Trade simulation with realistic costs/slippage
    →  k10_oos_backtest_2025_07_2026_06_realistic.csv (trade log)
    →  k10_oos_backtest_2025_07_2026_06_summary.json (summary stats)
    →  kmeans_k10_trained_2021_2025_06.pkl (trained model)
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
from sklearn.cluster import KMeans

logger = logging.getLogger(__name__)

# Configuration
RANDOM_STATE = 42
K_CLUSTERS = 10
TRAIN_START = "2021-01-01"
TRAIN_END = "2025-06-30"
OOS_START = "2025-07-01"
OOS_END = "2026-06-30"

# Trading parameters
POSITION_SIZE_USD = 1000.0
TP_PCT = 0.02  # +2% take profit
SL_PCT = -0.01  # -1% stop loss
TIMEOUT_HOURS = 24.0
COSTS_PCT = 0.003  # 0.3% total (spread 0.05% × 2 + commission 0.1%)
SLIPPAGE_ENTRY_PCT = 0.002  # 0.2% average entry slippage
SLIPPAGE_EXIT_PCT = 0.002  # 0.2% average exit slippage

# K-means parameters
INIT_METHOD = "k-means++"
MAX_ITER = 200
N_INIT = 3


@dataclass
class Trade:
    """Record of a single trade."""
    entry_time: datetime
    exit_time: datetime
    cluster: int
    direction: str  # "long" or "short"
    entry_price: float
    exit_price: float
    mfe: float  # Maximum favorable excursion in pct
    realized_pnl: float  # $
    pnl_pct: float  # %
    win: bool


def _load_window_representation(
    artifact_path: str | None = None,
) -> tuple[pd.DataFrame, np.ndarray, list[str]]:
    """Load windowed feature representation from Parquet.

    Args:
        artifact_path : Path to solusdt_window_repr.parquet. If None, uses default.

    Returns:
        Tuple of (df_full, X_features, feature_columns)
    """
    if artifact_path is None:
        artifact_path = "artifacts/research_cluster_regime/solusdt_window_repr.parquet"

    artifact_path = Path(artifact_path)
    if not artifact_path.exists():
        raise FileNotFoundError(f"Window representation not found: {artifact_path}")

    logger.info("Loading window representation from %s", artifact_path)
    df = pl.read_parquet(str(artifact_path)).to_pandas()
    df["open_time"] = pd.to_datetime(df["open_time"])

    # Identify feature columns
    all_cols = df.columns.tolist()
    skip_prefixes = ("open_time", "long_mfe_fw", "short_mfe_fw")
    feature_cols = [c for c in all_cols if not any(c.startswith(p) for p in skip_prefixes)]

    logger.info("Loaded: %d rows, %d features", len(df), len(feature_cols))

    # Extract feature matrix
    X = df[feature_cols].values.astype(np.float64)

    # Fill NaNs with column median
    for j in range(X.shape[1]):
        col_data = X[:, j]
        mask_nan = np.isnan(col_data)
        if np.any(mask_nan):
            col_median = np.nanmedian(col_data)
            X[mask_nan, j] = col_median

    return df, X, feature_cols


def _load_ohlcv_data(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Load OHLCV data from DuckDB.

    Args:
        start_date : Start date (inclusive). If None, loads all.
        end_date   : End date (inclusive). If None, loads all.

    Returns:
        DataFrame with open_time, open, high, low, close.
    """
    db_path = Path("database/solusdt/solusdt.duckdb")
    conn = duckdb.connect(str(db_path), read_only=True)

    try:
        query = "SELECT open_time, open, high, low, close FROM ohlcv"
        if start_date or end_date:
            where_parts = []
            if start_date:
                where_parts.append(f"open_time >= '{start_date}'")
            if end_date:
                where_parts.append(f"open_time <= '{end_date}'")
            query += " WHERE " + " AND ".join(where_parts)
        query += " ORDER BY open_time"

        df = conn.execute(query).df()
        df["open_time"] = pd.to_datetime(df["open_time"])
        logger.info("Loaded OHLCV: %d rows (%s to %s)",
                    len(df), df["open_time"].min(), df["open_time"].max())
        return df
    finally:
        conn.close()


def _split_by_date(
    df: pd.DataFrame,
    X: np.ndarray,
) -> tuple[tuple[pd.DataFrame, np.ndarray], tuple[pd.DataFrame, np.ndarray]]:
    """Split data into train and OOS by date.

    Args:
        df : Full DataFrame with open_time.
        X  : Feature matrix (N, D).

    Returns:
        Tuple of ((df_train, X_train), (df_oos, X_oos))
    """
    # Training: 2021-01-01 to 2025-06-30
    train_mask = (df["open_time"] >= TRAIN_START) & (df["open_time"] <= TRAIN_END)
    df_train = df[train_mask].reset_index(drop=True)
    X_train = X[train_mask]

    # OOS: 2025-07-01 to 2026-06-30
    oos_mask = (df["open_time"] >= OOS_START) & (df["open_time"] <= OOS_END)
    df_oos = df[oos_mask].reset_index(drop=True)
    X_oos = X[oos_mask]

    logger.info("Train: %d rows, OOS: %d rows", len(df_train), len(df_oos))
    return (df_train, X_train), (df_oos, X_oos)


def _fit_kmeans(X_train: np.ndarray) -> KMeans:
    """Fit K-means clustering on training data.

    Args:
        X_train : (N, D) feature matrix for training.

    Returns:
        Fitted KMeans object.
    """
    logger.info("Fitting K-means with k=%d", K_CLUSTERS)

    kmeans = KMeans(
        n_clusters=K_CLUSTERS,
        init=INIT_METHOD,
        max_iter=MAX_ITER,
        n_init=N_INIT,
        random_state=RANDOM_STATE,
        verbose=1,
    )
    kmeans.fit(X_train)
    logger.info("K-means fitted: inertia=%.2f", kmeans.inertia_)
    return kmeans


def _simulate_trades(
    df_ohlcv: pd.DataFrame,
    cluster_ids: np.ndarray,
) -> list[Trade]:
    """Simulate trades based on cluster IDs and realistic market conditions.

    Entry: Every new cluster ID triggers a $1,000 position (long).
    Exit: TP (+2%), SL (-1%), or 24h timeout at MFE.
    Costs: 0.3% per trade.
    Slippage: 0.1-0.5% (2% average per entry/exit).

    Args:
        df_ohlcv    : DataFrame with open_time, high, low, close.
        cluster_ids : Cluster label for each observation.

    Returns:
        List of Trade objects.
    """
    if len(df_ohlcv) != len(cluster_ids):
        raise ValueError("df_ohlcv and cluster_ids must have same length")

    trades = []

    # Align OHLCV with cluster IDs
    df = df_ohlcv.copy()
    df["cluster"] = cluster_ids
    df["time_index"] = np.arange(len(df))

    current_position = None

    for _, row in df.iterrows():
        cluster_id = row["cluster"]

        # Open a new position if cluster changed or no position
        if current_position is None:
            current_position = {
                "entry_time": row["open_time"],
                "entry_price": row["close"],
                "cluster": cluster_id,
                "direction": "long",
                "mfe": 0.0,
                "max_price": row["high"],
            }

        else:
            # Update MFE (max favorable excursion for long)
            current_position["max_price"] = max(current_position["max_price"], row["high"])
            mfe_return = (current_position["max_price"] / current_position["entry_price"]) - 1.0
            current_position["mfe"] = mfe_return

            # Check exit conditions
            current_return = (row["close"] / current_position["entry_price"]) - 1.0

            exit_signal = None
            exit_price = row["close"]

            # 1. Take Profit (+2%)
            if current_return >= TP_PCT:
                exit_signal = "tp"
                # Adjust for slippage on exit
                exit_price = current_position["entry_price"] * (1.0 + TP_PCT - SLIPPAGE_EXIT_PCT)

            # 2. Stop Loss (-1%)
            elif row["low"] / current_position["entry_price"] - 1.0 <= SL_PCT:
                exit_signal = "sl"
                # Hit SL, use SL price with slippage
                exit_price = current_position["entry_price"] * (1.0 + SL_PCT + SLIPPAGE_EXIT_PCT)

            # 3. Timeout (24 hours)
            elif (row["open_time"] - current_position["entry_time"]).total_seconds() >= TIMEOUT_HOURS * 3600:
                exit_signal = "timeout"
                # Exit at MFE price
                exit_price = current_position["max_price"] * (1.0 - SLIPPAGE_EXIT_PCT)

            if exit_signal:
                # Calculate P&L
                entry_price_adjusted = current_position["entry_price"] * (1.0 + SLIPPAGE_ENTRY_PCT)
                realized_return = (exit_price - entry_price_adjusted) / entry_price_adjusted
                realized_pnl = POSITION_SIZE_USD * realized_return - (POSITION_SIZE_USD * COSTS_PCT)
                pnl_pct = (realized_pnl / POSITION_SIZE_USD) * 100.0
                win = realized_pnl > 0

                trade = Trade(
                    entry_time=current_position["entry_time"],
                    exit_time=row["open_time"],
                    cluster=current_position["cluster"],
                    direction=current_position["direction"],
                    entry_price=entry_price_adjusted,
                    exit_price=exit_price,
                    mfe=current_position["mfe"],
                    realized_pnl=realized_pnl,
                    pnl_pct=pnl_pct,
                    win=win,
                )
                trades.append(trade)

                # Reset position
                current_position = None

    logger.info("Simulated %d trades", len(trades))
    return trades


def perform_k10_training_oos_backtest(
    window_repr_path: str | None = None,
    output_dir: str | None = None,
) -> dict[str, Any]:
    """Perform K=10 training and realistic OOS backtesting.

    Pipeline:
    1. Load window representation.
    2. Split into train (2021-2025-06) and OOS (2025-07-2026-06).
    3. Fit K-means with k=10 on training data.
    4. Predict cluster assignments for OOS.
    5. Load OHLCV for OOS period.
    6. Simulate trades with realistic costs/slippage.
    7. Save trade log (CSV) and summary (JSON).

    Args:
        window_repr_path : Path to solusdt_window_repr.parquet. If None, uses default.
        output_dir       : Output directory. If None, uses artifacts/research_cluster_regime/.

    Returns:
        Dictionary with backtest results and metadata.
    """
    if output_dir is None:
        output_dir = "artifacts/research_cluster_regime"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Load data
    logger.info("Step 1: Loading window representation...")
    df, X, feature_cols = _load_window_representation(window_repr_path)

    # Step 2: Split by date
    logger.info("Step 2: Splitting data...")
    (df_train, X_train), (df_oos, X_oos) = _split_by_date(df, X)

    # Step 3: Fit K-means
    logger.info("Step 3: Fitting K-means...")
    kmeans = _fit_kmeans(X_train)

    # Step 4: Predict OOS cluster assignments
    logger.info("Step 4: Predicting OOS clusters...")
    cluster_ids_oos = kmeans.predict(X_oos)

    # Step 5: Load OHLCV for OOS period
    logger.info("Step 5: Loading OHLCV data...")
    df_ohlcv_oos = _load_ohlcv_data(OOS_START, OOS_END)

    # Align OHLCV with cluster assignments (should have same timestamps)
    # Merge on open_time to ensure alignment
    df_merged = pd.merge(
        df_ohlcv_oos,
        df_oos[["open_time"]].reset_index().rename(columns={"index": "cluster_idx"}),
        on="open_time",
        how="inner"
    )

    if len(df_merged) != len(df_oos):
        logger.warning(
            "OHLCV and cluster data mismatch: %d vs %d rows. "
            "Using intersection only.",
            len(df_merged), len(df_oos)
        )

    # Get corresponding cluster IDs
    cluster_idx_map = {i: cluster_ids_oos[i] for i in range(len(cluster_ids_oos))}
    cluster_ids_aligned = df_merged["cluster_idx"].map(cluster_idx_map).values

    # Step 6: Simulate trades
    logger.info("Step 6: Simulating trades...")
    trades = _simulate_trades(df_merged[["open_time", "open", "high", "low", "close"]], cluster_ids_aligned)

    # Step 7: Save outputs
    logger.info("Step 7: Saving outputs...")

    # Save trade log as CSV
    trades_data = []
    for trade in trades:
        trades_data.append({
            "entry_time": trade.entry_time.isoformat(),
            "exit_time": trade.exit_time.isoformat(),
            "cluster": trade.cluster,
            "direction": trade.direction,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "mfe": trade.mfe,
            "realized_pnl": trade.realized_pnl,
            "pnl_pct": trade.pnl_pct,
            "win": trade.win,
        })

    df_trades = pd.DataFrame(trades_data)
    trades_csv_path = output_dir / "k10_oos_backtest_2025_07_2026_06_realistic.csv"
    df_trades.to_csv(trades_csv_path, index=False)
    logger.info("Trade log saved to %s (%d trades)", trades_csv_path, len(trades))

    # Calculate summary statistics
    n_trades = len(trades)
    win_count = sum(1 for t in trades if t.win)
    win_rate_pct = (win_count / n_trades * 100.0) if n_trades > 0 else 0.0

    pnls = [t.realized_pnl for t in trades]
    avg_pnl = np.mean(pnls) if pnls else 0.0
    net_pnl = sum(pnls)

    # Gross P&L (before costs)
    gross_pnl = net_pnl + (n_trades * POSITION_SIZE_USD * COSTS_PCT)

    # Simple max drawdown: cumulative P&L
    if pnls:
        cumulative_pnl = np.cumsum(pnls)
        running_max = np.maximum.accumulate(cumulative_pnl)
        drawdown = cumulative_pnl - running_max
        max_drawdown = np.min(drawdown) if len(drawdown) > 0 else 0.0
    else:
        max_drawdown = 0.0

    # Sharpe ratio (daily returns, assuming ~1440 bars/day for 1m data)
    if n_trades > 1:
        returns_daily = np.array([t.pnl_pct / 100.0 for t in trades])
        sharpe_ratio = np.mean(returns_daily) / np.std(returns_daily) * np.sqrt(252) if np.std(returns_daily) > 0 else 0.0
    else:
        sharpe_ratio = 0.0

    summary = {
        "metadata": {
            "description": "Realistic OOS backtest with K=10 clustering (t871)",
            "date": datetime.now().isoformat(),
            "train_period": [TRAIN_START, TRAIN_END],
            "oos_period": [OOS_START, OOS_END],
            "k_clusters": K_CLUSTERS,
            "random_state": RANDOM_STATE,
            "position_size_usd": POSITION_SIZE_USD,
            "tp_pct": TP_PCT,
            "sl_pct": SL_PCT,
            "timeout_hours": TIMEOUT_HOURS,
            "costs_pct": COSTS_PCT,
            "slippage_entry_pct": SLIPPAGE_ENTRY_PCT,
            "slippage_exit_pct": SLIPPAGE_EXIT_PCT,
        },
        "backtest_results": {
            "total_trades": int(n_trades),
            "win_count": int(win_count),
            "win_rate_pct": float(win_rate_pct),
            "avg_pnl": float(avg_pnl),
            "net_pnl": float(net_pnl),
            "gross_pnl": float(gross_pnl),
            "max_drawdown": float(max_drawdown),
            "sharpe_ratio": float(sharpe_ratio),
        },
        "output_files": {
            "trades_csv": str(trades_csv_path),
        },
    }

    # Save summary as JSON
    summary_json_path = output_dir / "k10_oos_backtest_2025_07_2026_06_summary.json"
    with open(summary_json_path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info("Summary saved to %s", summary_json_path)

    # Save trained model
    model_path = output_dir / "kmeans_k10_trained_2021_2025_06.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(kmeans, f)
    logger.info("Trained model saved to %s", model_path)

    summary["output_files"]["model"] = str(model_path)

    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Starting K=10 training and OOS backtest...")
    results = perform_k10_training_oos_backtest()
    logger.info("Backtest complete.")

    # Print summary
    print("\n" + "=" * 80)
    print("K=10 REALISTIC BACKTEST SUMMARY (2025-07 to 2026-06)")
    print("=" * 80)
    bt = results["backtest_results"]
    print(f"Total Trades:      {bt['total_trades']}")
    print(f"Win Count:         {bt['win_count']}")
    print(f"Win Rate:          {bt['win_rate_pct']:.2f}%")
    print(f"Avg P&L:           ${bt['avg_pnl']:.2f}")
    print(f"Net P&L:           ${bt['net_pnl']:.2f}")
    print(f"Gross P&L:         ${bt['gross_pnl']:.2f}")
    print(f"Max Drawdown:      ${bt['max_drawdown']:.2f}")
    print(f"Sharpe Ratio:      {bt['sharpe_ratio']:.3f}")
    print("=" * 80)
