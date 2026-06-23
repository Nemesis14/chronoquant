from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

import utils
from data_handling.store.duckdb_query import latest_open_time, query_range


def load_dashboard_config(asset_id: str | None = None) -> dict:
    db_cfg         = utils.load_asset_config(asset_id)["database"]
    models_cfg     = utils.load_models_config()
    strategies_cfg = utils.load_strategies_config()
    strategy_id, strategy_cfg = active_strategy(strategies_cfg, asset_id=asset_id)
    try:
        model_id = utils.live_model_id(model_cfg=models_cfg, asset_id=asset_id)
    except ValueError:
        model_id = None

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


def active_strategy(
    strategies_cfg: dict | None = None,
    asset_id: str | None = None,
) -> tuple[str | None, dict]:
    strategies_cfg = strategies_cfg or utils.load_strategies_config()
    strategies = strategies_cfg.get("strategies", {})
    if not strategies:
        return None, {}

    if asset_id:
        # Exact match on strategy asset_id
        for sid, scfg in strategies.items():
            if scfg.get("asset_id") == asset_id:
                return sid, scfg
        # Match via asset features_profile (e.g. "solusdt" → "solusdt_fw60")
        try:
            features_profile = utils.load_asset_config(asset_id)["database"].get("features_profile")
            if features_profile:
                for sid, scfg in strategies.items():
                    if scfg.get("asset_id") == features_profile:
                        return sid, scfg
        except Exception:
            pass

    for sid, scfg in strategies.items():
        if not scfg.get("asset_id"):
            return sid, scfg

    strategy_id = next(iter(strategies))
    return strategy_id, strategies[strategy_id]


def load_long_short_strategies(
    asset_id: str | None = None,
) -> tuple[dict, dict]:
    try:
        trading_cfg = utils.load_trading_config()
        session_id  = trading_cfg.get("strategy_session_id", "")
    except Exception:
        return {}, {}
    if not session_id:
        return {}, {}
    artifact_path = Path(utils._resolve_path(f"artifacts/{session_id}/strategy_artifact.json"))
    if not artifact_path.exists():
        return {}, {}
    try:
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception:
        return {}, {}
    params       = artifact.get("decision_params", {})
    entry_cutoff = params.get("entry_cutoff")
    long_cfg     = {"entry_cutoff": entry_cutoff}
    short_cfg    = {"entry_cutoff": entry_cutoff}   # short uses inverted signal but same threshold
    return long_cfg, short_cfg


def table_exists(
    table_name: str,
    db_path: str | None = None,
    asset_id: str | None = None,
) -> bool:
    db_path = db_path or _db_path(asset_id)
    if not db_path or not Path(db_path).exists():
        return False
    conn = duckdb.connect(db_path, read_only=True)
    try:
        row = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
    finally:
        conn.close()
    return row is not None


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
                ORDER BY {_quote_identifier(candidate)} DESC
                LIMIT 1
            """
            return _scalar(query, asset_id=asset_id)
    return None


def prediction_history(
    lookback_hours: int = 24,
    asset_id: str | None = None,
) -> pd.DataFrame:
    db_cfg  = utils.load_asset_config(asset_id)["database"]
    db_path = db_cfg["db_path"]

    latest_pred_ts = latest_open_time(db_path, "predictions")
    if latest_pred_ts is None:
        return pd.DataFrame()

    end_dt    = latest_pred_ts.to_pydatetime()
    start_dt  = end_dt - timedelta(hours=int(lookback_hours) + 1)
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str   = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    pred_df = query_range(
        db_path, "predictions",
        start   = start_str,
        end     = end_str,
        columns = ["open_time", "close", "long_pred", "short_pred"],
    )
    if pred_df.empty:
        return pd.DataFrame()

    ohlcv_df = query_range(
        db_path, "ohlcv",
        start   = start_str,
        end     = end_str,
        columns = ["open_time", "open", "high", "low", "close"],
    )

    if not ohlcv_df.empty:
        df = ohlcv_df.merge(
            pred_df[["open_time", "long_pred", "short_pred"]],
            on  = "open_time",
            how = "left",
        )
    else:
        df = pred_df

    df = _coerce_prediction_frame(
        df.rename(columns={"long_pred": "long_prediction", "short_pred": "short_prediction"})
    )
    if df.empty:
        return df

    long_lookup, short_lookup = _load_rank_lookups()
    if "long_prediction" in df.columns:
        df["long_prediction"] = _apply_rank_percentile(df["long_prediction"], long_lookup)  # type: ignore[arg-type]
    if "short_prediction" in df.columns:
        df["short_prediction"] = _apply_rank_percentile(df["short_prediction"], short_lookup)  # type: ignore[arg-type]

    latest_ts = df["open_time"].max()
    start_ts  = latest_ts - pd.Timedelta(hours=int(lookback_hours))
    return df[df["open_time"] >= start_ts].reset_index(drop=True)  # type: ignore[return-value]


def latest_prediction(asset_id: str | None = None) -> dict | None:
    db_cfg  = utils.load_asset_config(asset_id)["database"]
    db_path = db_cfg["db_path"]

    latest_pred_ts = latest_open_time(db_path, "predictions")
    if latest_pred_ts is None:
        return None

    last_dt   = latest_pred_ts.to_pydatetime()
    start_dt  = last_dt - timedelta(days=1)
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str   = last_dt.strftime("%Y-%m-%d %H:%M:%S")

    pred_df = query_range(
        db_path, "predictions",
        start   = start_str,
        end     = end_str,
        columns = ["open_time", "close", "long_pred", "short_pred"],
    )
    if pred_df.empty:
        return None

    df = _coerce_prediction_frame(
        pred_df.rename(columns={"long_pred": "long_prediction", "short_pred": "short_prediction"})
    )

    long_lookup, short_lookup = _load_rank_lookups()
    if "long_prediction" in df.columns:
        df["long_prediction"] = _apply_rank_percentile(df["long_prediction"], long_lookup)  # type: ignore[arg-type]
    if "short_prediction" in df.columns:
        df["short_prediction"] = _apply_rank_percentile(df["short_prediction"], short_lookup)  # type: ignore[arg-type]

    row = df.sort_values("open_time").iloc[-1].to_dict()
    return {key: _json_safe(value) for key, value in row.items()}


def active_position(asset_id: str | None = None) -> dict | None:
    try:
        trading_cfg = utils.load_trading_config()
        db_path     = utils._resolve_path(trading_cfg["db_path"])
    except Exception:
        return None
    if not Path(db_path).exists():
        return None

    table_name = "trading_positions"
    conn = duckdb.connect(db_path, read_only=True)
    try:
        row = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()
        if row is None:
            return None
        col_rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
            [table_name],
        ).fetchall()
        columns    = [r[0] for r in col_rows]
        status_col = "status" if "status" in columns else None
        where      = "WHERE status IN ('OPEN', 'LONG', 'LONG_OPEN', 'SHORT', 'SHORT_OPEN')" if status_col else ""
        order_col  = "entry_time" if "entry_time" in columns else columns[0]
        df = conn.execute(
            f'SELECT * FROM {_quote_identifier(table_name)} {where}'
            f' ORDER BY {_quote_identifier(order_col)} DESC LIMIT 1'
        ).df()
    finally:
        conn.close()

    if df.empty:
        return None
    return {key: _json_safe(value) for key, value in df.iloc[0].to_dict().items()}


def closed_trades(limit: int = 500, asset_id: str | None = None) -> pd.DataFrame:
    # Primary source: strat."<session_id>__trades" in the lab DB (t316 output).
    session_id = _active_strategy_session_id()
    if session_id:
        df = _load_strat_table(session_id, "trades", asset_id=asset_id)
        if not df.empty:
            order_col: str = "entry_time" if "entry_time" in df.columns else str(df.columns[0])
            return (
                df.sort_values(order_col, ascending=False)
                .head(int(limit))
                .reset_index(drop=True)
            )

    # Fallback: live trading DB (positions already settled by the trading service).
    try:
        trading_db = utils._resolve_path(utils.load_trading_config()["db_path"])
    except Exception:
        trading_db = None

    if trading_db and Path(trading_db).exists():
        table_name = "trading_positions"
        conn = duckdb.connect(trading_db, read_only=True)
        try:
            row = conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table_name]
            ).fetchone()
            if row is not None:
                col_rows  = conn.execute(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_name = ? ORDER BY ordinal_position",
                    [table_name],
                ).fetchall()
                columns   = [r[0] for r in col_rows]
                order_col = "exit_time" if "exit_time" in columns else columns[0]
                where     = "WHERE status IN ('CLOSED', 'FLAT')" if "status" in columns else ""
                return conn.execute(
                    f'SELECT * FROM {_quote_identifier(table_name)} {where}'
                    f' ORDER BY {_quote_identifier(order_col)} DESC LIMIT ?',
                    [int(limit)],
                ).df()
        finally:
            conn.close()

    return pd.DataFrame()


def equity_curve(asset_id: str | None = None) -> pd.DataFrame:
    # Primary source: strat."<session_id>__equity" in the lab DB (t316 output).
    session_id = _active_strategy_session_id()
    if session_id:
        df = _load_strat_table(session_id, "equity", asset_id=asset_id)
        if not df.empty:
            order_col: str = "trade_index" if "trade_index" in df.columns else str(df.columns[0])
            return df.sort_values(order_col, ascending=True).reset_index(drop=True)

    # Fallback: live trading DB equity snapshots.
    try:
        trading_db = utils._resolve_path(utils.load_trading_config()["db_path"])
    except Exception:
        trading_db = None

    if trading_db and Path(trading_db).exists():
        table_name = "trading_equity_snapshots"
        conn = duckdb.connect(trading_db, read_only=True)
        try:
            row = conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table_name]
            ).fetchone()
            if row is not None:
                col_rows = conn.execute(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_name = ? ORDER BY ordinal_position",
                    [table_name],
                ).fetchall()
                columns  = [r[0] for r in col_rows]
                time_col = "snapshot_time" if "snapshot_time" in columns else columns[0]
                return conn.execute(
                    f'SELECT * FROM {_quote_identifier(table_name)}'
                    f' ORDER BY {_quote_identifier(time_col)} ASC'
                ).df()
        finally:
            conn.close()

    return pd.DataFrame()


def backtest_summary(asset_id: str | None = None) -> dict:
    path = _session_artifact_path("strategy_artifact.json")
    if not path or not path.exists():
        return {}
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return artifact.get("metrics", {})


def load_strategy_artifact() -> dict:
    """Return the full strategy_artifact.json for the active session, or empty dict."""
    path = _session_artifact_path("strategy_artifact.json")
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def recent_orders(limit: int = 200, asset_id: str | None = None) -> pd.DataFrame:
    table_name = "trading_orders"
    if not table_exists(table_name, asset_id=asset_id):
        return pd.DataFrame()
    columns   = table_columns(table_name, asset_id=asset_id)
    order_col = "created_at" if "created_at" in columns else columns[0]
    query = f"""
        SELECT *
        FROM {_quote_identifier(table_name)}
        ORDER BY {_quote_identifier(order_col)} DESC
        LIMIT ?
    """
    return _read_sql(query, params=(int(limit),), asset_id=asset_id)


def recent_errors(limit: int = 100, asset_id: str | None = None) -> pd.DataFrame:
    table_name = "trading_errors"
    if not table_exists(table_name, asset_id=asset_id):
        return pd.DataFrame()
    columns   = table_columns(table_name, asset_id=asset_id)
    order_col = "error_time" if "error_time" in columns else columns[0]
    query = f"""
        SELECT *
        FROM {_quote_identifier(table_name)}
        ORDER BY {_quote_identifier(order_col)} DESC
        LIMIT ?
    """
    return _read_sql(query, params=(int(limit),), asset_id=asset_id)


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
                "table":       label,
                "table_name":  table_name,
                "exists":      exists,
                "rows":        table_row_count(table_name, asset_id=asset_id) if exists else 0,
                "latest_time": latest_table_timestamp(table_name, asset_id=asset_id) if exists else None,
            }
        )
    return pd.DataFrame(rows)


def table_columns(table_name: str, asset_id: str | None = None) -> list[str]:
    if not table_exists(table_name, asset_id=asset_id):
        return []
    db_path = _db_path(asset_id)
    if not db_path:
        return []
    conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
            [table_name],
        ).fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


def table_row_count(table_name: str, asset_id: str | None = None) -> int:
    if not table_exists(table_name, asset_id=asset_id):
        return 0
    value = _scalar(
        f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}",
        asset_id=asset_id,
    )
    return int(value or 0)


def _has_ohlcv_columns(table_name: str | None, asset_id: str | None = None) -> bool:
    if not table_name or not table_exists(table_name, asset_id=asset_id):
        return False
    columns = set(table_columns(table_name, asset_id=asset_id))
    return {"open_time", "open", "high", "low", "close"}.issubset(columns)


def _coerce_prediction_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["open_time"] = pd.to_datetime(out["open_time"], errors="coerce")
    for col in ["open", "high", "low", "close", "target", "long_prediction", "short_prediction"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _resolve_long_short_pred_cols(
    models_cfg: dict,
    asset_id: str | None,
    available_columns: list[str],
) -> tuple[str | None, str | None]:
    # Stable schema: always look for long_pred / short_pred first.
    long_col  = "long_pred"  if "long_pred"  in available_columns else None
    short_col = "short_pred" if "short_pred" in available_columns else None
    return long_col, short_col


def _read_sql(
    query: str,
    params: tuple = (),
    asset_id: str | None = None,
) -> pd.DataFrame:
    db_path = _db_path(asset_id)
    if not db_path:
        return pd.DataFrame()
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute(query, list(params)).df()
    finally:
        conn.close()


def _scalar(
    query: str,
    params: tuple = (),
    asset_id: str | None = None,
) -> Any:
    db_path = _db_path(asset_id)
    if not db_path:
        return None
    conn = duckdb.connect(db_path, read_only=True)
    try:
        row = conn.execute(query, list(params)).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def _db_path(asset_id: str | None = None) -> str | None:
    path = utils.load_asset_config(asset_id)["database"].get("db_path")
    if path:
        return path
    try:
        cfg = utils.load_trading_config()
        return utils._resolve_path(cfg["db_path"])
    except Exception:
        return None


def _session_artifact_path(filename: str) -> Path | None:
    try:
        session_id = utils.load_trading_config().get("strategy_session_id", "")
    except Exception:
        return None
    if not session_id:
        return None
    return _repo_path(f"artifacts/{session_id}/{filename}")


def _active_strategy_session_id() -> str | None:
    """Return the active strategy session_id from trading.json, or None if unavailable."""
    try:
        return utils.load_trading_config().get("strategy_session_id") or None
    except Exception:
        return None


def _load_strat_table(
    session_id: str,
    kind: str,
    asset_id: str | None = None,
) -> pd.DataFrame:
    """Read strat."<session_id>__<kind>" from the lab DB via open_lab_connection.

    Returns an empty DataFrame when the table does not exist or any error occurs,
    so callers can treat absence as a graceful no-data state.

    Args:
        session_id : Strategy session identifier.
        kind       : One of ``trades`` / ``equity`` / ``cutoffs``.
        asset_id   : Asset key; uses default_asset_id if None.
    """
    fqn = f'strat."{session_id}__{kind}"'
    try:
        conn = utils.open_lab_connection(asset_id)
    except Exception:
        return pd.DataFrame()
    try:
        row = conn.execute(
            "SELECT 1 FROM information_schema.tables"
            " WHERE table_schema = 'strat' AND table_name = ?",
            [f"{session_id}__{kind}"],
        ).fetchone()
        if row is None:
            return pd.DataFrame()
        return conn.execute(f"SELECT * FROM {fqn}").df()
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def _repo_path(relative_path: str) -> Path:
    return Path(utils._repo_root()) / relative_path


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _load_rank_lookups() -> tuple[
    tuple[np.ndarray, np.ndarray] | None,
    tuple[np.ndarray, np.ndarray] | None,
]:
    try:
        trading_cfg = utils.load_trading_config()
        session_id  = trading_cfg.get("strategy_session_id", "")
    except Exception:
        return None, None
    if not session_id:
        return None, None

    artifact_path = Path(utils._resolve_path(f"artifacts/{session_id}/strategy_artifact.json"))
    if not artifact_path.exists():
        return None, None
    try:
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception:
        return None, None

    artifact_dir = artifact_path.parent

    def _load_one(filename: str) -> tuple[np.ndarray, np.ndarray] | None:
        if not filename:
            return None
        path = artifact_dir / filename
        if not path.exists():
            return None
        try:
            df = pd.read_parquet(path)
            return df["score_raw"].to_numpy(dtype=float), df["score_pct"].to_numpy(dtype=float)
        except Exception:
            return None

    return (
        _load_one(artifact.get("rank_lookup_long_path", "")),
        _load_one(artifact.get("rank_lookup_short_path", "")),
    )


def _apply_rank_percentile(
    series: pd.Series,
    lookup: tuple[np.ndarray, np.ndarray] | None,
) -> pd.Series:
    if lookup is None:
        return series
    scores_raw, scores_pct = lookup
    raw = series.to_numpy(dtype=float)
    pct = np.interp(raw, scores_raw, scores_pct).clip(0.0, 1.0)
    return pd.Series(pct, index=series.index, name=series.name)


def _json_safe(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value
