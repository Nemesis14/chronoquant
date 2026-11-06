"""Trades: closed (strat ledger), active position, trading journal, Binance fills."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query

from api import schemas
from api.deps import get_conn
from api.errors import ApiError, no_data
from api.services import binance, trades
from api.services.common import Conn

router = APIRouter(prefix="/api/trades", tags=["trades"])

Limit = Annotated[int, Query(ge=1, le=5000)]
Offset = Annotated[int, Query(ge=0)]


@router.get("/closed", response_model=schemas.ClosedTrades)
def get_closed(
    conn: Annotated[Conn, Depends(get_conn)],
    limit: Limit = 500,
    offset: Offset = 0,
    source: Literal["strat", "journal"] = "strat",
) -> dict[str, Any]:
    return trades.closed(conn, limit, offset, source)


@router.get("/active", response_model=schemas.ActivePosition)
def get_active(conn: Annotated[Conn, Depends(get_conn)]) -> dict[str, Any]:
    pos = trades.active_position(conn)
    if pos is None:
        raise no_data("no open position")
    return pos


@router.get("/journal/positions", response_model=schemas.Page)
def get_positions(
    conn: Annotated[Conn, Depends(get_conn)],
    status: Literal["open", "closed"] | None = None,
    limit: Limit = 200,
    offset: Offset = 0,
) -> dict[str, Any]:
    return trades.journal_rows(conn, "trading_positions", "entry_time", limit, offset, status)


@router.get("/journal/orders", response_model=schemas.Page)
def get_orders(
    conn: Annotated[Conn, Depends(get_conn)],
    limit: Limit = 200,
    offset: Offset = 0,
    full: bool = False,
) -> dict[str, Any]:
    exclude = () if full else ("request_json", "response_json")
    return trades.journal_rows(conn, "trading_orders", "created_at", limit, offset, exclude=exclude)


@router.get("/journal/signals", response_model=schemas.Page)
def get_signals(
    conn: Annotated[Conn, Depends(get_conn)], limit: Limit = 50, offset: Offset = 0
) -> dict[str, Any]:
    return trades.journal_rows(conn, "trading_signals", "bar_open_time", limit, offset)


@router.get("/journal/errors", response_model=schemas.Page)
def get_errors(
    conn: Annotated[Conn, Depends(get_conn)], limit: Limit = 100, offset: Offset = 0
) -> dict[str, Any]:
    return trades.journal_rows(conn, "trading_errors", "error_time", limit, offset)


@router.get("/journal/runs", response_model=schemas.Page)
def get_runs(
    conn: Annotated[Conn, Depends(get_conn)], limit: Limit = 20, offset: Offset = 0
) -> dict[str, Any]:
    return trades.journal_rows(conn, "trading_runs", "started_at", limit, offset)


@router.get("/binance", response_model=schemas.BinanceFills)
def get_binance(limit: Limit = 200) -> dict[str, Any]:
    try:
        return binance.recent_trades(limit)
    except binance.BinanceUnavailableError as exc:
        raise ApiError(503, "binance_unavailable", f"Binance unavailable: {exc}") from exc
