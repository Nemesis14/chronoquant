"""Impulse Scout forward-log (epic_074 / t942): signals + latest state, read-only.

Experimental, NO-GO model (5300): not a trading signal.  Unknown asset -> 404; an asset without
the ``scout_signals`` table (e.g. bchusdt) -> 200 with ``available = false`` and no rows.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from api import schemas
from api.deps import AssetDep, get_conn
from api.services import scout
from api.services.common import Conn

router = APIRouter(prefix="/api/scout", tags=["scout"])


@router.get("/signals", response_model=schemas.ScoutSignals)
def get_signals(
    conn: Annotated[Conn, Depends(get_conn)],
    asset_id: AssetDep,
    since: Annotated[datetime | None, Query(description="Earliest t0 (ISO-8601, UTC if naive)")] = None,
    limit: Annotated[int, Query(ge=1, le=2000)] = 500,
) -> dict[str, Any]:
    """The newest ``limit`` events with ``t0 >= since``, ascending by ``t0``."""
    return {
        "asset_id": asset_id,
        "available": scout.available(conn),
        "items": scout.signals(conn, asset_id, since, limit),
    }


@router.get("/latest", response_model=schemas.ScoutLatest)
def get_latest(
    conn: Annotated[Conn, Depends(get_conn)],
    asset_id: AssetDep,
    recent: Annotated[int, Query(ge=1, le=50)] = 5,
) -> dict[str, Any]:
    """Counters, open events, the newest ``recent`` events (newest first) and the artifact meta."""
    return scout.latest(conn, asset_id, recent)
