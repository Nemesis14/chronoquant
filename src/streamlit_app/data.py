# =============================================================================
# Streamlit dashboard data access
# =============================================================================
# Purpose:
#  - Provide read-only, bounded data queries for the Streamlit dashboard
#  - Keep UI code independent from pipeline and trading execution code
# =============================================================================

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

import utils
from db.table_ops import sqlite_connect


# =============================================================================
# load_dashboard_config() -> dict
# =============================================================================
# Purpose:
#  - Return the main config values displayed by the dashboard
# =============================================================================
def load_dashboard_config() -> dict:
    db_cfg = utils.load_db_config()["database"]
    env_cfg = utils.load_env_config()
    models_cfg = utils.load_models_config()
    strategies_cfg = utils.load_strategies_config()
    strategy_id, strategy_cfg = active_strategy(strategies_cfg)
    model_id = env_cfg.get("runtime", {}).get("model_id")

    return {
        "active_env": db_cfg.get("active_env"),
        "db_path": db_cfg.get("db_path"),
        "symbol": db_cfg.get("symbol"),
        "interval": db_cfg.get("interval"),
        "tables": db_cfg.get("tables", {}),
        "runtime_model_id": model_id,
        "runtime_model": models_cfg.get("models", {}).get(model_id, {}),
        "strategy_id": strategy_id,
        "strategy": strategy_cfg,
    }


# =============================================================================
# active_strategy(strategies_cfg: dict | None = None) -> tuple[str | None, dict]
# =============================================================================
# Purpose:
#  - Resolve the first configured strategy until a trading runtime config exists
# =============================================================================
def active_strategy(strategies_cfg: dict | None = None) -> tuple[str | None, dict]:
    strategies_cfg = strategies_cfg or utils.load_strategies_config()
    strategies = strategies_cfg.get("strategies", {})
    if not strategies:
        return None, {}
    strategy_id = next(iter(strategies))
    return strategy_id, strategies[strategy_id]


# =============================================================================
# table_exists(table_name: str, db_path: str | None = None) -> bool
# =============================================================================
# Purpose:
#  - Check optional tables without failing the UI
# =============================================================================
def table_exists(table_name: str, db_path: str | None = None) -> bool:
    db_path = db_path or _db_path()
    if not db_path or not Path(db_path).exists():
        return False
    with sqlite_connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
    return row is not None


# =============================================================================
# latest_table_timestamp(table_name: str) -> str | None
# =============================================================================
# Purpose:
#  - Return the newest timestamp-like known column using rowid order
# =============================================================================
def latest_table_timestamp(table_name: str) -> str | None:
    if not table_exists(table_name):
        return None

    columns = table_columns(table_name)
    for candidate in [
        "open_time",
        "processed_at",
        "created_at",
        "snapshot_time",
        "error_time",
        "entry_time",
    ]:
        if candidate in columns:
            query = f"""
                SELECT {_quote_identifier(candidate)}
                FROM {_quote_identifier(table_name)}
                ORDER BY rowid DESC
                LIMIT 1
            """
            return _scalar(query)
    return None


# =============================================================================
# prediction_history(lookback_hours: int = 24) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Load the latest prediction window relative to the newest stored prediction
# =============================================================================
def prediction_history(lookback_hours: int = 24) -> pd.DataFrame:
    cfg = load_dashboard_config()
    table_name = cfg["tables"].get("predictions")
    if not table_name or not table_exists(table_name):
        return pd.DataFrame()

    columns = table_columns(table_name)
    live_cols = utils.live_prediction_columns()
    select_cols = [
        col
        for col in ["open_time", "close", live_cols["target"], live_cols["prediction"], live_cols["signal"]]
        if col in columns
    ]
    if "open_time" not in select_cols:
        return pd.DataFrame()

    limit = max(60, int(lookback_hours) * 60 + 5)
    ohlcv_table = cfg["tables"].get("ohlcv")
    if _has_ohlcv_columns(ohlcv_table):
        base_close = f"base.{_quote_identifier('close')}" if "close" in select_cols else "NULL"
        extra_cols = [
            f"base.{_quote_identifier(col)} AS {_quote_identifier(col)}"
            for col in select_cols
            if col not in {"open_time", "close"}
        ]
        outer_cols = [
            f"base.{_quote_identifier('open_time')} AS {_quote_identifier('open_time')}",
            f"ohlcv.{_quote_identifier('open')} AS {_quote_identifier('open')}",
            f"ohlcv.{_quote_identifier('high')} AS {_quote_identifier('high')}",
            f"ohlcv.{_quote_identifier('low')} AS {_quote_identifier('low')}",
            f"COALESCE(ohlcv.{_quote_identifier('close')}, {base_close}) AS {_quote_identifier('close')}",
            *extra_cols,
        ]
        query = f"""
            SELECT {", ".join(outer_cols)}
            FROM (
                SELECT {", ".join(_quote_identifier(col) for col in select_cols)}
                FROM {_quote_identifier(table_name)}
                ORDER BY rowid DESC
                LIMIT ?
            ) AS base
            LEFT JOIN {_quote_identifier(ohlcv_table)} AS ohlcv
                ON ohlcv.{_quote_identifier('open_time')} = base.{_quote_identifier('open_time')}
            ORDER BY base.{_quote_identifier('open_time')} ASC
        """
    else:
        query = f"""
            SELECT *
            FROM (
                SELECT {", ".join(_quote_identifier(col) for col in select_cols)}
                FROM {_quote_identifier(table_name)}
                ORDER BY rowid DESC
                LIMIT ?
            )
            ORDER BY open_time ASC
        """
    df = _coerce_prediction_frame(_read_sql(query, params=(limit,)))
    if df.empty:
        return df
    latest_ts = df["open_time"].max()
    start_ts = latest_ts - pd.Timedelta(hours=int(lookback_hours))
    return df[df["open_time"] >= start_ts].reset_index(drop=True)


# =============================================================================
# latest_prediction() -> dict | None
# =============================================================================
# Purpose:
#  - Return the newest live prediction row as a dict
# =============================================================================
def latest_prediction() -> dict | None:
    cfg = load_dashboard_config()
    table_name = cfg["tables"].get("predictions")
    if not table_name or not table_exists(table_name):
        return None

    columns = table_columns(table_name)
    live_cols = utils.live_prediction_columns()
    select_cols = [
        col
        for col in ["open_time", "close", live_cols["target"], live_cols["prediction"], live_cols["signal"]]
        if col in columns
    ]
    if "open_time" not in select_cols:
        return None

    query = f"""
        SELECT {", ".join(_quote_identifier(col) for col in select_cols)}
        FROM {_quote_identifier(table_name)}
        ORDER BY rowid DESC
        LIMIT 1
    """
    df = _read_sql(query)
    if df.empty:
        return None
    row = _coerce_prediction_frame(df).iloc[0].to_dict()
    return {key: _json_safe(value) for key, value in row.items()}


# =============================================================================
# active_position() -> dict | None
# =============================================================================
# Purpose:
#  - Return the newest open live trading position when trading tables exist
# =============================================================================
def active_position() -> dict | None:
    table_name = "trading_positions"
    if not table_exists(table_name):
        return None

    columns = table_columns(table_name)
    status_col = "status" if "status" in columns else None
    where = "WHERE status IN ('OPEN', 'LONG', 'LONG_OPEN')" if status_col else ""
    order_col = "entry_time" if "entry_time" in columns else columns[0]
    query = f"""
        SELECT *
        FROM {_quote_identifier(table_name)}
        {where}
        ORDER BY {_quote_identifier(order_col)} DESC
        LIMIT 1
    """
    df = _read_sql(query)
    if df.empty:
        return None
    return {key: _json_safe(value) for key, value in df.iloc[0].to_dict().items()}


# =============================================================================
# closed_trades(limit: int = 500) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return live closed trades, or managed backtest trades before live tables exist
# =============================================================================
def closed_trades(limit: int = 500) -> pd.DataFrame:
    table_name = "trading_positions"
    if table_exists(table_name):
        columns = table_columns(table_name)
        order_col = "exit_time" if "exit_time" in columns else columns[0]
        where = "WHERE status IN ('CLOSED', 'FLAT')" if "status" in columns else ""
        query = f"""
            SELECT *
            FROM {_quote_identifier(table_name)}
            {where}
            ORDER BY {_quote_identifier(order_col)} DESC
            LIMIT ?
        """
        return _read_sql(query, params=(int(limit),))

    path = _repo_path("backtests/lasso_long_fw240_q90_managed_v1/trades.csv")
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    return df.tail(int(limit)).sort_values("entry_time", ascending=False).reset_index(drop=True)


# =============================================================================
# equity_curve() -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return live equity snapshots, or managed backtest equity before live tables exist
# =============================================================================
def equity_curve() -> pd.DataFrame:
    table_name = "trading_equity_snapshots"
    if table_exists(table_name):
        columns = table_columns(table_name)
        time_col = "snapshot_time" if "snapshot_time" in columns else columns[0]
        query = f"""
            SELECT *
            FROM {_quote_identifier(table_name)}
            ORDER BY {_quote_identifier(time_col)} ASC
        """
        return _read_sql(query)

    path = _repo_path("backtests/lasso_long_fw240_q90_managed_v1/equity_curve.csv")
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


# =============================================================================
# backtest_summary() -> dict
# =============================================================================
# Purpose:
#  - Return the managed strategy summary before live trading tables exist
# =============================================================================
def backtest_summary() -> dict:
    path = _repo_path("backtests/lasso_long_fw240_q90_managed_v1/summary.json")
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


# =============================================================================
# recent_orders(limit: int = 200) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return recent orders when trading order tables exist
# =============================================================================
def recent_orders(limit: int = 200) -> pd.DataFrame:
    table_name = "trading_orders"
    if not table_exists(table_name):
        return pd.DataFrame()
    columns = table_columns(table_name)
    order_col = "created_at" if "created_at" in columns else columns[0]
    query = f"""
        SELECT *
        FROM {_quote_identifier(table_name)}
        ORDER BY {_quote_identifier(order_col)} DESC
        LIMIT ?
    """
    return _read_sql(query, params=(int(limit),))


# =============================================================================
# recent_errors(limit: int = 100) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return recent trading errors when the error table exists
# =============================================================================
def recent_errors(limit: int = 100) -> pd.DataFrame:
    table_name = "trading_errors"
    if not table_exists(table_name):
        return pd.DataFrame()
    columns = table_columns(table_name)
    order_col = "error_time" if "error_time" in columns else columns[0]
    query = f"""
        SELECT *
        FROM {_quote_identifier(table_name)}
        ORDER BY {_quote_identifier(order_col)} DESC
        LIMIT ?
    """
    return _read_sql(query, params=(int(limit),))


# =============================================================================
# table_health() -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return row counts and latest timestamps for core and optional trading tables
# =============================================================================
def table_health() -> pd.DataFrame:
    cfg = load_dashboard_config()
    configured_tables = cfg["tables"]
    tables = [
        ("ohlcv", configured_tables.get("ohlcv")),
        ("features", configured_tables.get("features")),
        ("predictions", configured_tables.get("predictions")),
        ("trading_signals", "trading_signals"),
        ("trading_positions", "trading_positions"),
        ("trading_orders", "trading_orders"),
        ("trading_errors", "trading_errors"),
    ]
    rows = []
    for label, table_name in tables:
        if not table_name:
            continue
        exists = table_exists(table_name)
        rows.append(
            {
                "table": label,
                "table_name": table_name,
                "exists": exists,
                "rows": table_row_count(table_name) if exists else 0,
                "latest_time": latest_table_timestamp(table_name) if exists else None,
            }
        )
    return pd.DataFrame(rows)


# =============================================================================
# table_columns(table_name: str) -> list[str]
# =============================================================================
# Purpose:
#  - Return table columns for optional UI rendering
# =============================================================================
def table_columns(table_name: str) -> list[str]:
    if not table_exists(table_name):
        return []
    with sqlite_connect(_db_path()) as conn:
        return [row[1] for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table_name)})")]


# =============================================================================
# table_row_count(table_name: str) -> int
# =============================================================================
# Purpose:
#  - Return row count for a dashboard table
# =============================================================================
def table_row_count(table_name: str) -> int:
    if not table_exists(table_name):
        return 0
    value = _scalar(f"SELECT MAX(rowid) FROM {_quote_identifier(table_name)}")
    return int(value or 0)


# =============================================================================
# _has_ohlcv_columns(table_name: str | None) -> bool
# =============================================================================
# Purpose:
#  - Check whether the configured OHLCV table can support candlestick rendering
# =============================================================================
def _has_ohlcv_columns(table_name: str | None) -> bool:
    if not table_name or not table_exists(table_name):
        return False
    columns = set(table_columns(table_name))
    return {"open_time", "open", "high", "low", "close"}.issubset(columns)


# =============================================================================
# _coerce_prediction_frame(df: pd.DataFrame) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Normalize prediction table dtypes for chart rendering
# =============================================================================
def _coerce_prediction_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["open_time"] = pd.to_datetime(out["open_time"], errors="coerce")
    for col in ["open", "high", "low", "close", "target", "prediction"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


# =============================================================================
# _read_sql(query: str, params: tuple = ()) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Execute a bounded read-only query
# =============================================================================
def _read_sql(query: str, params: tuple = ()) -> pd.DataFrame:
    with sqlite_connect(_db_path()) as conn:
        return pd.read_sql_query(query, conn, params=params)


# =============================================================================
# _scalar(query: str, params: tuple = ()) -> Any
# =============================================================================
# Purpose:
#  - Execute a scalar query
# =============================================================================
def _scalar(query: str, params: tuple = ()) -> Any:
    with sqlite_connect(_db_path()) as conn:
        row = conn.execute(query, params).fetchone()
    return row[0] if row else None


# =============================================================================
# _db_path() -> str
# =============================================================================
# Purpose:
#  - Return configured SQLite path
# =============================================================================
def _db_path() -> str:
    return utils.load_db_config()["database"]["db_path"]


# =============================================================================
# _repo_path(relative_path: str) -> Path
# =============================================================================
# Purpose:
#  - Resolve repo-relative artifacts
# =============================================================================
def _repo_path(relative_path: str) -> Path:
    return Path(utils._repo_root()) / relative_path


# =============================================================================
# _quote_identifier(value: str) -> str
# =============================================================================
# Purpose:
#  - Quote SQLite identifiers safely
# =============================================================================
def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


# =============================================================================
# _json_safe(value: Any) -> Any
# =============================================================================
# Purpose:
#  - Convert pandas timestamps/NA values for Streamlit display dictionaries
# =============================================================================
def _json_safe(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value
