"""Shared DB utilities for ChronoQuant analysis notebooks.

Reuses src/ functions where possible.
Import from a notebook setup cell:
    import sys
    sys.path.insert(0, str(_root / "src"))
    import analyst.db_utils as dbu
"""

import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import duckdb
import pandas as pd

import utils
from data_handling.store.duckdb_query import (
    dataset_columns,
    query_range,
    row_count,
)
from data_handling.store.duckdb_stats import collect_duckdb_stats_report


def db_path() -> str:
    cfg = utils.load_asset_config()
    return cfg["database"]["db_path"]


def connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path(), read_only=read_only)


def load_table(table: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Load a full table (or date-range slice) as pandas DataFrame."""
    return query_range(db_path(), table, start=start, end=end)


def table_stats_df() -> pd.DataFrame:
    """Return a summary DataFrame for all 4 core tables."""
    report = collect_duckdb_stats_report(db_path())
    rows = []
    for s in report.tables:
        rows.append({
            "table":    s.table,
            "status":   s.status,
            "rows":     s.row_count,
            "columns":  s.column_count,
            "min_time": s.min_open_time,
            "max_time": s.max_open_time,
        })
    return pd.DataFrame(rows)


def null_ratio_df(table: str) -> pd.DataFrame:
    """Return null ratio per column for the given table (full scan)."""
    conn = connect()
    try:
        cols = dataset_columns(db_path(), table)
        n    = row_count(db_path(), table)
        if n == 0:
            return pd.DataFrame()
        records = []
        for col in cols:
            null_n = conn.execute(
                f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NULL'
            ).fetchone()[0]
            records.append({"column": col, "null_count": null_n, "null_pct": 100.0 * null_n / n})
        return pd.DataFrame(records)
    finally:
        conn.close()


def monthly_agg(table: str, value_col: str, agg: str = "AVG") -> pd.DataFrame:
    """Monthly aggregation of a single column."""
    conn = connect()
    try:
        df = conn.execute(f"""
            SELECT
                DATE_TRUNC('month', open_time) AS month,
                {agg}("{value_col}") AS value
            FROM {table}
            GROUP BY 1
            ORDER BY 1
        """).df()
        df["month"] = pd.to_datetime(df["month"])
        return df
    finally:
        conn.close()


def yearly_agg(table: str, value_col: str, agg: str = "AVG") -> pd.DataFrame:
    """Yearly aggregation of a single column."""
    conn = connect()
    try:
        df = conn.execute(f"""
            SELECT
                YEAR(open_time) AS year,
                {agg}("{value_col}") AS value
            FROM {table}
            GROUP BY 1
            ORDER BY 1
        """).df()
        return df
    finally:
        conn.close()


def candle_validity_df() -> pd.DataFrame:
    """Return per-check violation counts for OHLCV candle geometry."""
    conn = connect()
    try:
        row = conn.execute("""
            SELECT
                SUM(CASE WHEN high < open              THEN 1 ELSE 0 END) AS high_lt_open,
                SUM(CASE WHEN high < close             THEN 1 ELSE 0 END) AS high_lt_close,
                SUM(CASE WHEN low  > open              THEN 1 ELSE 0 END) AS low_gt_open,
                SUM(CASE WHEN low  > close             THEN 1 ELSE 0 END) AS low_gt_close,
                SUM(CASE WHEN high < low               THEN 1 ELSE 0 END) AS high_lt_low,
                SUM(CASE WHEN open  <= 0               THEN 1 ELSE 0 END) AS open_zero_neg,
                SUM(CASE WHEN high  <= 0               THEN 1 ELSE 0 END) AS high_zero_neg,
                SUM(CASE WHEN low   <= 0               THEN 1 ELSE 0 END) AS low_zero_neg,
                SUM(CASE WHEN close <= 0               THEN 1 ELSE 0 END) AS close_zero_neg,
                SUM(CASE WHEN volume < 0               THEN 1 ELSE 0 END) AS volume_negative,
                SUM(CASE WHEN trades < 0               THEN 1 ELSE 0 END) AS trades_negative,
                SUM(CASE WHEN taker_buy_base > volume  THEN 1 ELSE 0 END) AS taker_exceeds_volume,
                SUM(CASE WHEN taker_buy_quote > quote_volume THEN 1 ELSE 0 END) AS taker_quote_exceeds
            FROM ohlcv
        """).fetchone()
        cols = [
            "high_lt_open", "high_lt_close", "low_gt_open", "low_gt_close",
            "high_lt_low", "open_zero_neg", "high_zero_neg", "low_zero_neg",
            "close_zero_neg", "volume_negative", "trades_negative",
            "taker_exceeds_volume", "taker_quote_exceeds",
        ]
        df = pd.DataFrame({"check": cols, "violations": list(row)})
        df["status"] = df["violations"].apply(lambda v: "✅" if v == 0 else "❌")
        return df
    finally:
        conn.close()


def ohlcv_gaps_df(limit: int = 20) -> pd.DataFrame:
    """Return top N time gaps (> 1 minute) in the OHLCV table."""
    conn = connect()
    try:
        return conn.execute(f"""
            WITH ordered AS (
                SELECT open_time,
                       LEAD(open_time) OVER (ORDER BY open_time) AS next_open_time
                FROM ohlcv
            ),
            gaps AS (
                SELECT open_time,
                       next_open_time,
                       date_diff('minute', open_time, next_open_time) AS gap_minutes
                FROM ordered
                WHERE next_open_time IS NOT NULL
            )
            SELECT * FROM gaps WHERE gap_minutes > 1 ORDER BY gap_minutes DESC LIMIT {limit}
        """).df()
    finally:
        conn.close()


def calibration_bins_df(
    df: pd.DataFrame,
    score_col: str,
    target_col: str,
    bins: list[float] | None = None,
) -> pd.DataFrame:
    """Compute calibration table: avg predicted prob vs empirical positive rate per bin."""
    if bins is None:
        bins = [0.0, 0.05, 0.10, 0.20, 0.40, 0.60, 0.80, 1.001]

    sub = df[[score_col, target_col]].dropna()
    sub = sub[sub[target_col].isin([0, 1, True, False])]
    sub["bin"] = pd.cut(sub[score_col], bins=bins, right=False, include_lowest=True)

    result = (
        sub.groupby("bin", observed=True)
        .agg(
            row_count=(score_col, "count"),
            avg_pred=(score_col, "mean"),
            empirical_rate=(target_col, "mean"),
        )
        .reset_index()
    )
    base_rate = float(sub[target_col].mean())
    result["lift"] = result["empirical_rate"] / base_rate
    result["bin"] = result["bin"].astype(str)
    return result


def roc_pr_metrics_df(
    df: pd.DataFrame,
    score_col: str,
    target_col: str,
    label: str = "",
) -> pd.DataFrame:
    """Return ROC-AUC, PR-AUC, log-loss, and Brier score for one scorer."""
    try:
        from sklearn.metrics import (
            average_precision_score,
            brier_score_loss,
            log_loss,
            roc_auc_score,
        )
    except ImportError:
        return pd.DataFrame([{"metric": "sklearn not available", "value": float("nan")}])

    sub = df[[score_col, target_col]].dropna()
    sub = sub[sub[target_col].isin([0, 1, True, False])]
    if len(sub) == 0 or sub[target_col].nunique() < 2:
        return pd.DataFrame([{"metric": "insufficient data", "value": float("nan")}])

    y_true = sub[target_col].astype(int).to_numpy()
    y_pred = sub[score_col].to_numpy()

    records = [
        {"metric": "ROC-AUC",    "value": roc_auc_score(y_true, y_pred)},
        {"metric": "PR-AUC",     "value": average_precision_score(y_true, y_pred)},
        {"metric": "Log-loss",   "value": log_loss(y_true, y_pred)},
        {"metric": "Brier",      "value": brier_score_loss(y_true, y_pred)},
        {"metric": "Positive N", "value": float(y_true.sum())},
        {"metric": "Total N",    "value": float(len(y_true))},
        {"metric": "Base rate",  "value": float(y_true.mean())},
    ]
    result = pd.DataFrame(records)
    if label:
        result.insert(0, "model", label)
    return result


def feature_groups() -> dict[str, list[str]]:
    """Return feature column names grouped by indicator family (name-pattern based)."""
    conn = connect()
    try:
        all_feat_cols: list[str] = [
            r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'feat_ohlcv_quant'"
                "   AND column_name LIKE 'feat_%'"
                " ORDER BY ordinal_position"
            ).fetchall()
        ]
    finally:
        conn.close()

    RULES: list[tuple[str, list[str]]] = [
        ("momentum_oscillators", ["rsi", "roc", "stoch", "williams_r", "cci", "adx", "macd"]),
        ("trend_ma",             ["sma_ratio", "ema_ratio", "wma_ratio", "kama_ratio", "ema_slope", "directional"]),
        ("bollinger",            ["bb_width", "bb_position"]),
        ("volatility",           ["atr", "natr", "hist_vol", "parkinson_vol", "gk_vol", "range_expansion"]),
        ("volume_flow",          ["volume_sma", "volume_ratio", "obv", "mfi", "ad_line", "cmf",
                                  "quote_volume", "trade_count", "avg_trade", "taker_buy",
                                  "vol_adj_return", "volume_confirmed", "taker_flow"]),
        ("regime_rank",          ["natr_rank", "hist_vol_rank", "bb_width_rank",
                                  "volume_rank", "quote_volume_rank", "trade_count_rank",
                                  "volume_accel", "trade_count_accel"]),
        ("returns_stats",        ["returns_log", "returns_sma", "returns_std", "returns_skew", "returns_kurt",
                                  "return_10", "return_30", "return_60",
                                  "return_z", "return_autocorr", "variance_ratio",
                                  "return_asymmetry", "pos_return_mean", "neg_return_mean",
                                  "return_momentum_delta", "rsi_delta", "roc_delta"]),
        ("price_structure",      ["hml_range", "ohlc_range", "close_position",
                                  "dist_rolling", "rolling_drawdown", "recovery_ratio",
                                  "max_drawdown", "time_since_high", "time_since_low",
                                  "higher_high", "higher_low", "lower_high", "lower_low",
                                  "swing_high", "swing_low"]),
        ("candle_patterns",      ["body_ratio", "signed_body", "upper_wick", "lower_wick",
                                  "wick_imbalance", "doji", "hammer", "shooting_star",
                                  "inside_bar", "outside_bar", "engulf",
                                  "bull_bars_ratio", "gap_open"]),
        ("time_session",         ["hour_sin", "hour_cos", "dayofweek", "weekend",
                                  "session_asia", "session_europe", "session_us",
                                  "bars_into_session", "weekly_open", "day_range", "day_open"]),
        ("ichimoku",             ["tenkan", "kijun", "senkou", "ichimoku"]),
        ("donchian",             ["donchian"]),
        ("linear_regression",    ["lr_slope", "lr_r2", "lr_residual"]),
        ("efficiency",           ["efficiency_ratio", "atr_dist", "prev_session"]),
        ("tail_risk",            ["return_asymmetry", "pos_return_mean", "neg_return_mean"]),
        ("autocorr",             ["autocorr", "variance_ratio"]),
    ]

    assigned: set[str] = set()
    groups: dict[str, list[str]] = {}

    for group_name, patterns in RULES:
        matched = []
        for col in all_feat_cols:
            if col in assigned:
                continue
            stripped = col[len("feat_"):]
            if any(stripped.startswith(p) or p in stripped for p in patterns):
                matched.append(col)
                assigned.add(col)
        if matched:
            groups[group_name] = matched

    other = [c for c in all_feat_cols if c not in assigned]
    if other:
        groups["other"] = other

    return groups
