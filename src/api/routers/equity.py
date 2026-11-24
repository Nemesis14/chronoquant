"""Equity curves and backtest summary of the active strategy sessions."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query

from api import schemas
from api.deps import AssetDep, get_conn
from api.services import strat
from api.services.common import Conn

router = APIRouter(prefix="/api/equity", tags=["equity"])


@router.get("", response_model=schemas.EquityResponse)
def get_equity(
    conn: Annotated[Conn, Depends(get_conn)],
    asset_id: AssetDep,
    session: Annotated[str, Query(max_length=128)] = "active",
    direction: Literal["both", "long", "short"] = "both",
) -> dict[str, Any]:
    return strat.equity(conn, session, direction, asset_id)


@router.get("/summary")
def get_summary(asset_id: AssetDep) -> dict[str, Any]:
    return strat.equity_summary(asset_id)
