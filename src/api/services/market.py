"""Market (candles + prediction chart) and table-health reads."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import utils
from api.services import common, predictions, trades
from api.services.common import Conn

WINDOWS = {"24h": 24, "8h": 8, "4h": 4}
DEFAULT_WINDOW = "24h"
TF_MINUTES = {"1m": 1, "5m": 5, "15m": 15}
MAX_CANDLE_HOURS = 24 * 14


def config() -> dict[str, Any]:
    db = utils.load_asset_config(None)["database"]
    return {
        "asset_id": db.get("asset_id"),
        "symbol": db.get("symbol"),
        "interval": db.get("interval"),
        "windows": list(WINDOWS),
        "default_window": DEFAULT_WINDOW,
    }


def candles(conn: Conn, hours: int, tf: str) -> dict[str, Any]:
    """OHLCV resampled to ``tf``, anchored at the latest ohlcv bar."""
    end = predictions.latest_time(conn, "ohlcv")
    if end is None:
        return {"tf": tf, "from": None, "to": None, "candles": []}
    start = end - timedelta(hours=hours)
    minutes = TF_MINUTES[tf]
    ohlcv = common.quote(common.live_table("ohlcv"))
    rows = common.fetch_rows(
        conn,
        f"SELECT time_bucket(INTERVAL '{minutes} minutes', open_time) AS t,"
        " arg_min(open, open_time) AS o, max(high) AS h, min(low) AS l,"
        " arg_max(close, open_time) AS c, sum(volume) AS v"
        f" FROM live.{ohlcv} WHERE open_time >= ? AND open_time <= ? GROUP BY 1 ORDER BY 1",
        [common.sql_ts(start), common.sql_ts(end)],
    )
    return {
        "tf": tf,
        "from": common.iso_z(start),
        "to": common.iso_z(end),
        "candles": rows,
    }


def chart(conn: Conn, window: str, include_trade: bool = True) -> dict[str, Any]:
    """The main dashboard payload: bars + predictions + thresholds + active trade.

    ``include_trade=False`` skips the active-trade lookup (journal + Binance fallback) and
    returns ``active_trade: null``.
    """
    focus = WINDOWS[window]
    anchor = predictions.latest_time(conn, "predictions") or predictions.latest_time(conn, "ohlcv")
    sessions = common.active_sessions()
    thresholds = {
        "long_entry": sessions["long"].entry_cutoff,
        "short_entry": sessions["short"].entry_cutoff,
    }
    active = trades.active_position(conn) if include_trade else None
    active_trade = (
        {
            "side": active.get("side"),
            "entry_price": active.get("entry_price"),
            "stop_loss": active.get("stop_loss"),
            "take_profit": active.get("take_profit"),
        }
        if active
        else None
    )
    if anchor is None:
        return {
            "window": window, "focus_hours": focus, "from": None, "to": None, "bars": [],
            "thresholds": thresholds, "active_trade": active_trade,
        }
    start = anchor - timedelta(hours=focus)
    rows = predictions.load_rows(conn, start, anchor, base="ohlcv")
    bars = [
        {
            "t": r["t"],
            "o": predictions.num(r["open"]),
            "h": predictions.num(r["high"]),
            "l": predictions.num(r["low"]),
            "c": predictions.num(r["close"]),
            **{k: predictions.num(r[k]) for k in ("long_pct", "short_pct", "long_raw", "short_raw")},
        }
        for r in rows
    ]
    return {
        "window": window,
        "focus_hours": focus,
        "from": common.iso_z(start),
        "to": common.iso_z(anchor),
        "bars": bars,
        "thresholds": thresholds,
        "active_trade": active_trade,
    }


_TIME_COLS = ("open_time", "processed_at", "created_at", "snapshot_time", "error_time", "entry_time")


def _health_row(conn: Conn, catalog: str, label: str, name: str) -> dict[str, Any]:
    exists = common.table_exists(conn, catalog, "main", name)
    row: dict[str, Any] = {"table": label, "table_name": name, "exists": exists, "rows": 0,
                           "latest_time": None}
    if not exists:
        return row
    fq = f"{catalog}.{common.quote(name)}"
    row["rows"] = int(common.scalar(conn, f"SELECT count(*) FROM {fq}") or 0)
    cols = common.table_columns(conn, catalog, "main", name)
    for cand in _TIME_COLS:
        if cand in cols:
            row["latest_time"] = common.iso_z(
                common.scalar(conn, f"SELECT max({common.quote(cand)}) FROM {fq}")
            )
            break
    return row


def tables(conn: Conn) -> list[dict[str, Any]]:
    """Row counts / freshness of the live and journal tables (old ``table_health``)."""
    out = [
        _health_row(conn, "live", "ohlcv", common.live_table("ohlcv")),
        _health_row(conn, "live", "features", common.live_table("features")),
        _health_row(conn, "live", "predictions", common.live_table("predictions")),
    ]
    out += [
        _health_row(conn, "jrn", name, name)
        for name in ("trading_signals", "trading_positions", "trading_orders", "trading_errors")
    ]
    return out
