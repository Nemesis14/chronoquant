"""Predictions: history, latest, threshold-crossing signals."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from api import schemas
from api.deps import get_conn
from api.errors import no_data
from api.services import predictions
from api.services.common import Conn

router = APIRouter(prefix="/api/predictions", tags=["predictions"])

Hours = Annotated[int, Query(ge=1, le=24 * 14)]


@router.get("/history", response_model=schemas.PredictionHistory)
def get_history(conn: Annotated[Conn, Depends(get_conn)], hours: Hours = 24) -> dict[str, Any]:
    return {"items": predictions.history(conn, hours)}


@router.get("/latest", response_model=schemas.LatestPrediction)
def get_latest(conn: Annotated[Conn, Depends(get_conn)]) -> dict[str, Any]:
    row = predictions.latest(conn)
    if row is None:
        raise no_data("no predictions yet")
    return row


@router.get("/signals", response_model=schemas.SignalList)
def get_signals(conn: Annotated[Conn, Depends(get_conn)], hours: Hours = 24) -> dict[str, Any]:
    return {"items": predictions.signals(conn, hours)}
