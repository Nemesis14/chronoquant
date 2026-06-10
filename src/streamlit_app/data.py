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
# load_dashboard_config(asset_id: str | None = None) -> dict
# =============================================================================
# Purpose:
#  - Return the main config values displayed by the dashboard
# =============================================================================
def load_dashboard_config(asset_id: str | None = None) -> dict:
    db_cfg         = utils.load_asset_config(asset_id)["database"]
    models_cfg     = utils.load_models_config()
    strategies_cfg = utils.load_strategies_config()
    strategy_id, strategy_cfg = active_strategy(strategies_cfg, asset_id=asset_id)
    model_id = utils.live_model_id(model_cfg=models_cfg, asset_id=asset_id)

    return {
        "asset_id":         db_cfg.get("asset_id"),
        "db_path":          db_cfg.get("db_path"),
        "symbol":           db_cfg.get("symbol"),
        "interval":         db_cfg.get("interval"),
        "tables":           db_cfg.get("tables", {}),
        "features_profile": db_cfg.get("features_profile"),
        "runtime_model_id": model_id,
        "runtime_model":    models_cfg.get("models", {}).get(model_id, {}),
        "strategy_id":      strategy_id,
        "strategy":         strategy_cfg,
    }


# =============================================================================
# active_strategy(strategies_cfg: dict | None = None) -> tuple[str | None, dict]
# =============================================================================
# Purpose:
#  - Resolve the first configured strategy until a trading runtime config exists
# =============================================================================
def active_strategy(
    strategies_cfg: dict | None = None,
    asset_id: str | None = None,
) -> tuple[str | None, dict]:
    strategies_cfg = strategies_cfg or utils.load_strategies_config()
    strategies = strategies_cfg.get("strategies", {})
    if not strategies:
        return None, {}

    if asset_id:
        for sid, scfg in strategies.items():
            if scfg.get("asset_id") == asset_id:
                return sid, scfg

    for sid, scfg in strategies.items():
        if not scfg.get("asset_id"):
            return sid, scfg

    strategy_id = next(iter(strategies))
    return strategy_id, strategies[strategy_id]


# =============================================================================
# load_long_short_strategies(asset_id: str | None = None) -> tuple[dict, dict]
# =============================================================================
# Purpose:
#  - Return (long_strategy_cfg, short_strategy_cfg) for the given asset
#  - Reads thresholds from config/strategies.json (not predictions.json)
# =============================================================================
def load_long_short_strategies(
    asset_id: str | None = None,
) -> tuple[dict, dict]:
    strategies_cfg = utils.load_strategies_config()
    strategies     = strategies_cfg.get("strategies", {})
    long_cfg  = {}
    short_cfg = {}
    for scfg in strategies.values():
        if scfg.get("asset_id") != asset_id:
            continue
        side = scfg.get("side", "")
        if side == "long" and not long_cfg:
            long_cfg = scfg
        elif side == "short" and not short_cfg:
            short_cfg = scfg
    return long_cfg, short_cfg


# =============================================================================
# table_exists(...) -> bool
# =============================================================================
# Purpose:
#  - Check optional tables without failing the UI
# =============================================================================
def table_exists(
    table_name: str,
    db_path: str | None = None,
    asset_id: str | None = None,
) -> bool:
    db_path = db_path or _db_path(asset_id)
    if not db_path or not Path(db_path).exists():
        return False
    with sqlite_connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
    return row is not None


# =============================================================================
# latest_table_timestamp(table_name: str, asset_id: str | None = None) -> str | None
# =============================================================================
# Purpose:
#  - Return the newest timestamp-like known column using rowid order
# =============================================================================
def latest_table_timestamp(table_name: str, asset_id: str | None = None) -> str | None:
    if not table_exists(table_name, asset_id=asset_id):
        return None

    columns = table_columns(table_name, asset_id=asset_id)
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
            return _scalar(query, asset_id=asset_id)
    return None


# =============================================================================
# prediction_history(...) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Load the latest prediction window relative to the newest stored prediction
# =============================================================================
def prediction_history(
    lookback_hours: int = 24,
    asset_id: str | None = None,
) -> pd.DataFrame:
    cfg = load_dashboard_config(asset_id=asset_id)
    table_name = cfg["tables"].get("predictions")
    if not table_name or not table_exists(table_name, asset_id=asset_id):
        return pd.DataFrame()

    columns = table_columns(table_name, asset_id=asset_id)
    live_cols = utils.live_prediction_columns()

    models_cfg = utils.load_models_config()
    long_pred_col, short_pred_col = _resolve_long_short_pred_cols(models_cfg, asset_id, columns)

    base_cols = [live_cols["target"], live_cols["prediction"], live_cols["signal"]]
    if short_pred_col and short_pred_col not in base_cols:
        base_cols.append(short_pred_col)

    select_cols = [col for col in ["open_time", "close", *base_cols] if col in columns]
    if "open_time" not in select_cols:
        return pd.DataFrame()

    limit = max(60, int(lookback_hours) * 60 + 5)
    ohlcv_table = cfg["tables"].get("ohlcv")
    if _has_ohlcv_columns(ohlcv_table, asset_id=asset_id):
        latest_prediction_time = _scalar(
            f"SELECT MAX({_quote_identifier('open_time')}) FROM {_quote_identifier(table_name)}",
            asset_id=asset_id,
        )
        if not latest_prediction_time:
            return pd.DataFrame()

        pred_cols = [col for col in select_cols if col not in {"open_time", "close"}]
        extra_cols = [
            f"pred.{_quote_identifier(col)} AS {_quote_identifier(col)}"
            for col in pred_cols
        ]
        outer_cols = [
            f"ohlcv.{_quote_identifier('open_time')} AS {_quote_identifier('open_time')}",
            f"ohlcv.{_quote_identifier('open')} AS {_quote_identifier('open')}",
            f"ohlcv.{_quote_identifier('high')} AS {_quote_identifier('high')}",
            f"ohlcv.{_quote_identifier('low')} AS {_quote_identifier('low')}",
            f"ohlcv.{_quote_identifier('close')} AS {_quote_identifier('close')}",
            *extra_cols,
        ]
        query = f"""
            SELECT {", ".join(outer_cols)}
            FROM (
                SELECT open_time, open, high, low, close
                FROM {_quote_identifier(ohlcv_table)}
                WHERE {_quote_identifier('open_time')} <= ?
                ORDER BY {_quote_identifier('open_time')} DESC
                LIMIT ?
            ) AS ohlcv
            LEFT JOIN {_quote_identifier(table_name)} AS pred
                ON pred.{_quote_identifier('open_time')} = ohlcv.{_quote_identifier('open_time')}
            ORDER BY ohlcv.{_quote_identifier('open_time')} ASC
        """
        params = (latest_prediction_time, limit)
    else:
        query = f"""
            SELECT *
            FROM (
                SELECT {", ".join(_quote_identifier(col) for col in select_cols)}
                FROM {_quote_identifier(table_name)}
                ORDER BY {_quote_identifier('open_time')} DESC
                LIMIT ?
            )
            ORDER BY open_time ASC
        """
        params = (limit,)

    df = _coerce_prediction_frame(_read_sql(query, params=params, asset_id=asset_id))
    if df.empty:
        return df
    if short_pred_col and short_pred_col in df.columns:
        df = df.rename(columns={short_pred_col: "short_prediction"})
    latest_ts = df["open_time"].max()
    start_ts = latest_ts - pd.Timedelta(hours=int(lookback_hours))
    return df[df["open_time"] >= start_ts].reset_index(drop=True)


# =============================================================================
# latest_prediction(asset_id: str | None = None) -> dict | None
# =============================================================================
# Purpose:
#  - Return the newest live prediction row as a dict
# =============================================================================
def latest_prediction(asset_id: str | None = None) -> dict | None:
    cfg = load_dashboard_config(asset_id=asset_id)
    table_name = cfg["tables"].get("predictions")
    if not table_name or not table_exists(table_name, asset_id=asset_id):
        return None

    columns   = table_columns(table_name, asset_id=asset_id)
    live_cols = utils.live_prediction_columns()
    models_cfg = utils.load_models_config()
    _, short_pred_col = _resolve_long_short_pred_cols(models_cfg, asset_id, columns)

    base_select = [
        col
        for col in ["open_time", "close", live_cols["target"], live_cols["prediction"], live_cols["signal"]]
        if col in columns
    ]
    if short_pred_col and short_pred_col not in base_select:
        base_select.append(short_pred_col)
    if "open_time" not in base_select:
        return None

    query = f"""
        SELECT {", ".join(_quote_identifier(col) for col in base_select)}
        FROM {_quote_identifier(table_name)}
        ORDER BY {_quote_identifier('open_time')} DESC
        LIMIT 1
    """
    df = _read_sql(query, asset_id=asset_id)
    if df.empty:
        return None
    if short_pred_col and short_pred_col in df.columns:
        df = df.rename(columns={short_pred_col: "short_prediction"})
    row = _coerce_prediction_frame(df).iloc[0].to_dict()
    return {key: _json_safe(value) for key, value in row.items()}


# =============================================================================
# active_position(asset_id: str | None = None) -> dict | None
# =============================================================================
# Purpose:
#  - Return the newest open live trading position when trading tables exist
# =============================================================================
def active_position(asset_id: str | None = None) -> dict | None:
    table_name = "trading_positions"
    if not table_exists(table_name, asset_id=asset_id):
        return None

    columns = table_columns(table_name, asset_id=asset_id)
    status_col = "status" if "status" in columns else None
    where = "WHERE status IN ('OPEN', 'LONG', 'LONG_OPEN', 'SHORT', 'SHORT_OPEN')" if status_col else ""
    order_col = "entry_time" if "entry_time" in columns else columns[0]
    query = f"""
        SELECT *
        FROM {_quote_identifier(table_name)}
        {where}
        ORDER BY {_quote_identifier(order_col)} DESC
        LIMIT 1
    """
    df = _read_sql(query, asset_id=asset_id)
    if df.empty:
        return None
    return {key: _json_safe(value) for key, value in df.iloc[0].to_dict().items()}


# =============================================================================
# closed_trades(limit: int = 500, asset_id: str | None = None) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return live closed trades, or managed backtest trades before live tables exist
# =============================================================================
def closed_trades(limit: int = 500, asset_id: str | None = None) -> pd.DataFrame:
    table_name = "trading_positions"
    if table_exists(table_name, asset_id=asset_id):
        columns = table_columns(table_name, asset_id=asset_id)
        order_col = "exit_time" if "exit_time" in columns else columns[0]
        where = "WHERE status IN ('CLOSED', 'FLAT')" if "status" in columns else ""
        query = f"""
            SELECT *
            FROM {_quote_identifier(table_name)}
            {where}
            ORDER BY {_quote_identifier(order_col)} DESC
            LIMIT ?
        """
        return _read_sql(query, params=(int(limit),), asset_id=asset_id)

    path = _repo_path("backtests/solusdt_long_fw60_q90_local_v3/trades.csv")
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    return df.tail(int(limit)).sort_values("entry_time", ascending=False).reset_index(drop=True)


# =============================================================================
# equity_curve(asset_id: str | None = None) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return live equity snapshots, or managed backtest equity before live tables exist
# =============================================================================
def equity_curve(asset_id: str | None = None) -> pd.DataFrame:
    table_name = "trading_equity_snapshots"
    if table_exists(table_name, asset_id=asset_id):
        columns = table_columns(table_name, asset_id=asset_id)
        time_col = "snapshot_time" if "snapshot_time" in columns else columns[0]
        query = f"""
            SELECT *
            FROM {_quote_identifier(table_name)}
            ORDER BY {_quote_identifier(time_col)} ASC
        """
        return _read_sql(query, asset_id=asset_id)

    path = _repo_path("backtests/solusdt_long_fw60_q90_local_v3/equity_curve.csv")
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


# =============================================================================
# backtest_summary(asset_id: str | None = None) -> dict
# =============================================================================
# Purpose:
#  - Return the managed strategy summary before live trading tables exist
# =============================================================================
def backtest_summary(asset_id: str | None = None) -> dict:
    path = _repo_path("backtests/solusdt_long_fw60_q90_local_v3/summary.json")
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


# =============================================================================
# recent_orders(limit: int = 200, asset_id: str | None = None) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return recent orders when trading order tables exist
# =============================================================================
def recent_orders(limit: int = 200, asset_id: str | None = None) -> pd.DataFrame:
    table_name = "trading_orders"
    if not table_exists(table_name, asset_id=asset_id):
        return pd.DataFrame()
    columns = table_columns(table_name, asset_id=asset_id)
    order_col = "created_at" if "created_at" in columns else columns[0]
    query = f"""
        SELECT *
        FROM {_quote_identifier(table_name)}
        ORDER BY {_quote_identifier(order_col)} DESC
        LIMIT ?
    """
    return _read_sql(query, params=(int(limit),), asset_id=asset_id)


# =============================================================================
# recent_errors(limit: int = 100, asset_id: str | None = None) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return recent trading errors when the error table exists
# =============================================================================
def recent_errors(limit: int = 100, asset_id: str | None = None) -> pd.DataFrame:
    table_name = "trading_errors"
    if not table_exists(table_name, asset_id=asset_id):
        return pd.DataFrame()
    columns = table_columns(table_name, asset_id=asset_id)
    order_col = "error_time" if "error_time" in columns else columns[0]
    query = f"""
        SELECT *
        FROM {_quote_identifier(table_name)}
        ORDER BY {_quote_identifier(order_col)} DESC
        LIMIT ?
    """
    return _read_sql(query, params=(int(limit),), asset_id=asset_id)


# =============================================================================
# table_health(asset_id: str | None = None) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Return row counts and latest timestamps for core and optional trading tables
# =============================================================================
def table_health(asset_id: str | None = None) -> pd.DataFrame:
    cfg = load_dashboard_config(asset_id=asset_id)
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
        exists = table_exists(table_name, asset_id=asset_id)
        rows.append(
            {
                "table": label,
                "table_name": table_name,
                "exists": exists,
                "rows": table_row_count(table_name, asset_id=asset_id) if exists else 0,
                "latest_time": latest_table_timestamp(table_name, asset_id=asset_id) if exists else None,
            }
        )
    return pd.DataFrame(rows)


# =============================================================================
# table_columns(table_name: str, asset_id: str | None = None) -> list[str]
# =============================================================================
# Purpose:
#  - Return table columns for optional UI rendering
# =============================================================================
def table_columns(table_name: str, asset_id: str | None = None) -> list[str]:
    if not table_exists(table_name, asset_id=asset_id):
        return []
    with sqlite_connect(_db_path(asset_id)) as conn:
        return [row[1] for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table_name)})")]


# =============================================================================
# table_row_count(table_name: str, asset_id: str | None = None) -> int
# =============================================================================
# Purpose:
#  - Return row count for a dashboard table
# =============================================================================
def table_row_count(table_name: str, asset_id: str | None = None) -> int:
    if not table_exists(table_name, asset_id=asset_id):
        return 0
    value = _scalar(
        f"SELECT MAX(rowid) FROM {_quote_identifier(table_name)}",
        asset_id=asset_id,
    )
    return int(value or 0)


# =============================================================================
# _has_ohlcv_columns(table_name: str | None, asset_id: str | None = None) -> bool
# =============================================================================
# Purpose:
#  - Check whether the configured OHLCV table can support candlestick rendering
# =============================================================================
def _has_ohlcv_columns(table_name: str | None, asset_id: str | None = None) -> bool:
    if not table_name or not table_exists(table_name, asset_id=asset_id):
        return False
    columns = set(table_columns(table_name, asset_id=asset_id))
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
    for col in ["open", "high", "low", "close", "target", "prediction", "short_prediction"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _resolve_long_short_pred_cols(
    models_cfg: dict,
    asset_id: str | None,
    available_columns: list[str],
) -> tuple[str | None, str | None]:
    long_col = None
    short_col = None
    models = models_cfg.get("models", {})
    for model_id, meta in models.items():
        if meta.get("asset_id") != asset_id:
            continue
        if not meta.get("active", False):
            continue
        col = utils.prediction_col_name(model_id)
        if col not in available_columns:
            continue
        target = meta.get("target_name", "")
        if target.startswith("trg_l_"):
            long_col = col
        elif target.startswith("trg_s_"):
            short_col = col
    return long_col, short_col


# =============================================================================
# _read_sql(...) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Execute a bounded read-only query
# =============================================================================
def _read_sql(
    query: str,
    params: tuple = (),
    asset_id: str | None = None,
) -> pd.DataFrame:
    with sqlite_connect(_db_path(asset_id)) as conn:
        return pd.read_sql_query(query, conn, params=params)


# =============================================================================
# _scalar(...) -> Any
# =============================================================================
# Purpose:
#  - Execute a scalar query
# =============================================================================
def _scalar(
    query: str,
    params: tuple = (),
    asset_id: str | None = None,
) -> Any:
    with sqlite_connect(_db_path(asset_id)) as conn:
        row = conn.execute(query, params).fetchone()
    return row[0] if row else None


# =============================================================================
# _db_path(asset_id: str | None = None) -> str
# =============================================================================
# Purpose:
#  - Return configured SQLite path
# =============================================================================
def _db_path(asset_id: str | None = None) -> str:
    return utils.load_asset_config(asset_id)["database"]["db_path"]


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
