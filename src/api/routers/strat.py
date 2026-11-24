"""Strategy sessions: active cards, session list, artifact, strat tables, grid."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query

from api import schemas
from api.deps import AssetDep, get_conn
from api.services import strat
from api.services.common import Conn

router = APIRouter(prefix="/api/strat", tags=["strat"])

Limit = Annotated[int, Query(ge=1, le=5000)]
Offset = Annotated[int, Query(ge=0)]


@router.get("/active", response_model=schemas.ActiveStrategies)
def get_active(asset_id: AssetDep) -> dict[str, Any]:
    return strat.active(asset_id)


@router.get("/sessions", response_model=list[schemas.StrategySession])
def get_sessions(conn: Annotated[Conn, Depends(get_conn)]) -> list[dict[str, Any]]:
    return strat.sessions(conn)


@router.get("/{session_id}/artifact")
def get_artifact(
    session_id: str, conn: Annotated[Conn, Depends(get_conn)], asset_id: AssetDep
) -> dict[str, Any]:
    return strat.artifact(conn, session_id, asset_id)


@router.get("/{session_id}/grid", response_model=schemas.Page)
def get_grid(
    session_id: str,
    conn: Annotated[Conn, Depends(get_conn)],
    top: Annotated[int, Query(ge=1, le=1000)] = 50,
    sort: str = "total_fact_log_return",
) -> dict[str, Any]:
    return strat.grid(conn, session_id, top, sort)


@router.get("/{session_id}/{kind}", response_model=schemas.Page)
def get_table(
    session_id: str,
    kind: Literal["trades", "equity", "cutoffs", "summary"],
    conn: Annotated[Conn, Depends(get_conn)],
    limit: Limit = 1000,
    offset: Offset = 0,
) -> dict[str, Any]:
    return strat.table(conn, session_id, kind, limit, offset)
