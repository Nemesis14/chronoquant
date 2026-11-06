"""Tail of the API / worker log files."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Query

from api import schemas
from api.services import logs

router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("/{source}", response_model=schemas.LogResponse)
def get_logs(
    source: Literal["api", "worker"],
    lines: Annotated[int, Query(ge=1, le=2000)] = 200,
) -> dict[str, Any]:
    return {"lines": logs.tail(source, lines)}
