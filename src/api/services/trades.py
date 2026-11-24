"""Trades: strategy-backtest ledger, trading journal (raw read-only SQL) and Binance fills."""

from __future__ import annotations

import logging
from typing import Any

import utils
from api.services import binance, common
from api.services.common import Conn

logger = logging.getLogger(__name__)

OPEN_STATUSES = ("OPEN", "LONG", "LONG_OPEN", "SHORT", "SHORT_OPEN")
CLOSED_STATUSES = ("CLOSED", "FLAT")

_TRADE_COLS = (
    "entry_time", "exit_time", "entry_price", "exit_price", "fact_log_return", "tp_lr",
    "sl_lr", "exit_reason", "tp_spec", "sl_spec",
)


def _strat_trades_union(conn: Conn, asset_id: str | None = None) -> str | None:
    """UNION ALL SQL over the active long+short ``__trades`` tables (None if neither exists)."""
    parts: list[str] = []
    seen: set[str] = set()
    for direction, sess in common.active_sessions(asset_id).items():
        prefix = sess.table_prefix
        if not prefix or prefix in seen:
            continue
        name = f"{prefix}__trades"
        if not common.table_exists(conn, "lab", "strat", name):
            continue
        seen.add(prefix)
        cols = common.table_columns(conn, "lab", "strat", name)
        sel = ", ".join(c if c in cols else f"NULL AS {c}" for c in _TRADE_COLS)
        side = "direction" if "direction" in cols else f"'{direction}'"
        parts.append(
            f"SELECT {sel}, lower({side}) AS side, '{prefix}' AS session_id"
            f" FROM lab.strat.{common.quote(name)}"
        )
    return " UNION ALL ".join(parts) if parts else None


def closed(
    conn: Conn, limit: int, offset: int, source: str, asset_id: str | None = None
) -> dict[str, Any]:
    """Closed trades newest first: strat ledger (default) or the trading journal."""
    if source == "strat":
        union = _strat_trades_union(conn, asset_id)
        if union:
            total = int(common.scalar(conn, f"SELECT count(*) FROM ({union})") or 0)
            items = common.fetch_rows(
                conn,
                f"SELECT * FROM ({union}) ORDER BY entry_time DESC LIMIT ? OFFSET ?",
                [limit, offset],
            )
            return {"items": items, "total": total}
    # journal source / fallback: settled positions mapped to the same keys where possible
    pos = journal_rows(
        conn, "trading_positions", "exit_time", 100000, 0, status="closed", asset_id=asset_id
    )
    mapped = [
        {
            "entry_time": r.get("entry_time"), "exit_time": r.get("exit_time"),
            "side": str(r.get("side") or "").lower() or None,
            "entry_price": r.get("entry_price"), "exit_price": r.get("exit_price"),
            "fact_log_return": None, "tp_lr": None, "sl_lr": None,
            "exit_reason": r.get("exit_reason"), "tp_spec": None, "sl_spec": None,
            "session_id": r.get("run_id"),
        }
        for r in pos["items"]
    ]
    return {"items": common.to_page(mapped, limit, offset), "total": len(mapped)}


def journal_rows(
    conn: Conn,
    table: str,
    order_col: str,
    limit: int,
    offset: int = 0,
    status: str | None = None,
    exclude: tuple[str, ...] = (),
    asset_id: str | None = None,
) -> dict[str, Any]:
    """Rows of a trading-journal table (``jrn`` catalog); missing table -> empty page.

    The journal is asset-agnostic. The default asset sees every row (backward compatible);
    any other asset only sees rows of runs (``trading_runs.asset_id``) started for it.
    """
    if not common.table_exists(conn, "jrn", "main", table):
        return {"items": [], "total": 0}
    cols = common.table_columns(conn, "jrn", "main", table)
    conds: list[str] = []
    params: list[Any] = []
    if status and "status" in cols:
        statuses = OPEN_STATUSES if status == "open" else CLOSED_STATUSES
        conds.append(f"status IN ({', '.join('?' for _ in statuses)})")
        params = list(statuses)
    if asset_id and asset_id != utils.default_asset_id():
        runs_ok = common.table_exists(conn, "jrn", "main", "trading_runs") and (
            "asset_id" in common.table_columns(conn, "jrn", "main", "trading_runs")
        )
        if table == "trading_runs" and "asset_id" in cols:
            conds.append("asset_id = ?")
            params.append(asset_id)
        elif "run_id" in cols and runs_ok:
            conds.append("run_id IN (SELECT run_id FROM jrn.main.trading_runs WHERE asset_id = ?)")
            params.append(asset_id)
        else:
            return {"items": [], "total": 0}
    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    skip = [c for c in exclude if c in cols]
    star = f"* EXCLUDE ({', '.join(common.quote(c) for c in skip)})" if skip else "*"
    order = order_col if order_col in cols else cols[0]
    fq = f"jrn.main.{common.quote(table)}"
    total = int(common.scalar(conn, f"SELECT count(*) FROM {fq} {where}", params) or 0)
    items = common.fetch_rows(
        conn,
        f"SELECT {star} FROM {fq} {where} ORDER BY {common.quote(order)} DESC LIMIT ? OFFSET ?",
        [*params, limit, offset],
    )
    return {"items": items, "total": total}


def active_position(conn: Conn, asset_id: str | None = None) -> dict[str, Any] | None:
    """Open position: newest open journal row, else the Binance futures position."""
    page = journal_rows(
        conn, "trading_positions", "entry_time", 1, status="open", asset_id=asset_id
    )
    if page["items"]:
        r = page["items"][0]
        return {
            "side": str(r.get("side") or "").upper() or None,
            "entry_time": r.get("entry_time"),
            "entry_price": r.get("entry_price"),
            "quantity": r.get("quantity"),
            "stop_loss": r.get("stop_loss") or r.get("sl_price"),
            "take_profit": r.get("take_profit") or r.get("tp_price"),
            "source": "journal",
        }
    return binance.current_position(asset_id)
