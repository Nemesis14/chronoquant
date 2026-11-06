"""Prediction / signal reads (port of ``ui.data.prediction_history`` / ``latest_prediction``)."""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from api.services import common
from api.services.common import Conn


def num(value: Any) -> float | None:
    """Finite float or None (NaN / Inf / None -> None)."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def latest_time(conn: Conn, table_key: str) -> datetime | None:
    """Max ``open_time`` of a live table, or None when absent/empty."""
    name = common.live_table(table_key)
    if not common.table_exists(conn, "live", "main", name):
        return None
    value = common.scalar(conn, f"SELECT max(open_time) FROM live.{common.quote(name)}")
    return value if isinstance(value, datetime) else None


def load_rows(conn: Conn, start: datetime, end: datetime, base: str = "ohlcv") -> list[dict[str, Any]]:
    """OHLCV joined with predictions over ``[start, end]``, rank-percentile columns added.

    Keys: t (ISO-Z), open, high, low, close, long_raw, short_raw, long_pct, short_pct,
    long_model_id, short_model_id.  ``base`` selects the driving table
    (``ohlcv`` = every bar, ``predictions`` = only bars that have a prediction).
    """
    ohlcv = common.live_table("ohlcv")
    preds = common.live_table("predictions")
    has_ohlcv = common.table_exists(conn, "live", "main", ohlcv)
    has_preds = common.table_exists(conn, "live", "main", preds)
    params: list[Any] = [common.sql_ts(start), common.sql_ts(end)]
    if has_ohlcv and has_preds:
        join, driver = ("LEFT", "o") if base == "ohlcv" else ("RIGHT", "p")
        sql = (
            "SELECT open_time AS t, o.open, o.high, o.low, coalesce(o.close, p.close) AS close,"
            " p.long_pred AS long_raw, p.short_pred AS short_raw,"
            " p.long_model_id, p.short_model_id"
            f" FROM live.{common.quote(ohlcv)} o"
            f" {join} JOIN live.{common.quote(preds)} p USING (open_time)"
            f" WHERE {driver}.open_time >= ? AND {driver}.open_time <= ? ORDER BY 1"
        )
    elif has_ohlcv:
        sql = (
            "SELECT open_time AS t, open, high, low, close, NULL::DOUBLE AS long_raw,"
            " NULL::DOUBLE AS short_raw, NULL::VARCHAR AS long_model_id,"
            " NULL::VARCHAR AS short_model_id"
            f" FROM live.{common.quote(ohlcv)} WHERE open_time >= ? AND open_time <= ? ORDER BY 1"
        )
    else:
        return []
    rows = common.fetch_rows(conn, sql, params)
    sessions = common.active_sessions()
    for side in ("long", "short"):
        raw = pd.Series([num(r[f"{side}_raw"]) for r in rows], dtype="float64")
        pct = common.apply_rank(raw, sessions[side].rank_lookup()).tolist()
        for r, p in zip(rows, pct, strict=True):
            r[f"{side}_pct"] = num(p)
    return rows


def history(conn: Conn, hours: int) -> list[dict[str, Any]]:
    end = latest_time(conn, "predictions")
    if end is None:
        return []
    rows = load_rows(conn, end - timedelta(hours=hours), end, base="predictions")
    keys = ("close", "long_pct", "short_pct", "long_raw", "short_raw")
    return [{"t": r["t"], **{k: num(r[k]) for k in keys}} for r in rows]


def latest(conn: Conn) -> dict[str, Any] | None:
    end = latest_time(conn, "predictions")
    if end is None:
        return None
    rows = load_rows(conn, end - timedelta(hours=1), end, base="predictions")
    if not rows:
        return None
    r = rows[-1]
    return {
        "t": r["t"],
        "close": num(r["close"]),
        "long_pct": num(r["long_pct"]),
        "short_pct": num(r["short_pct"]),
        "long_model_id": r["long_model_id"],
        "short_model_id": r["short_model_id"],
    }


def signals(conn: Conn, hours: int) -> list[dict[str, Any]]:
    """Threshold crossings (pct rises to/above the active entry cutoff) per side."""
    end = latest_time(conn, "predictions")
    if end is None:
        return []
    rows = load_rows(conn, end - timedelta(hours=hours), end, base="predictions")
    sessions = common.active_sessions()
    out: list[dict[str, Any]] = []
    for side in ("long", "short"):
        cutoff = sessions[side].entry_cutoff
        if cutoff is None:
            continue
        prev_above = False
        for r in rows:
            pct = r[f"{side}_pct"]
            above = pct is not None and pct >= cutoff
            if above and not prev_above:
                out.append({"t": r["t"], "side": side, "pct": num(pct), "price": num(r["close"])})
            prev_above = above
    out.sort(key=lambda x: x["t"] or "")
    return out
