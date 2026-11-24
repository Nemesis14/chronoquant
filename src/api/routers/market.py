"""Market: config, candles, the main dashboard chart payload and table health."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query

from api import schemas
from api.deps import AssetDep, get_conn
from api.services import market
from api.services.common import Conn

router = APIRouter(prefix="/api/market", tags=["market"])


@router.get("/config", response_model=schemas.MarketConfig)
def get_config(asset_id: AssetDep) -> dict[str, Any]:
    return market.config(asset_id)


@router.get("/candles", response_model=schemas.CandlesResponse, response_model_by_alias=True)
def get_candles(
    conn: Annotated[Conn, Depends(get_conn)],
    hours: Annotated[int, Query(ge=1, le=market.MAX_CANDLE_HOURS)] = 24,
    tf: Literal["1m", "5m", "15m"] = "1m",
    end: Annotated[
        datetime | None, Query(description="Window end (ISO-8601, UTC if naive); default/cap: latest bar")
    ] = None,
) -> dict[str, Any]:
    return market.candles(conn, hours, tf, end)


@router.get("/chart", response_model=schemas.ChartResponse, response_model_by_alias=True)
def get_chart(
    conn: Annotated[Conn, Depends(get_conn)],
    asset_id: AssetDep,
    window: Literal["24h", "8h", "4h"] = "24h",
    include_trade: Annotated[bool, Query()] = True,
) -> dict[str, Any]:
    return market.chart(conn, window, include_trade=include_trade, asset_id=asset_id)


@router.get("/tables", response_model=list[schemas.TableHealth])
def get_tables(conn: Annotated[Conn, Depends(get_conn)]) -> list[dict[str, Any]]:
    return market.tables(conn)
