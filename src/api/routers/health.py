"""Health and status."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from api import schemas
from api.services import common, status

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health", response_model=schemas.Health)
def health() -> dict[str, str]:
    return {"status": "ok", "time": common.now_iso()}


@router.get("/status", response_model=schemas.Status)
def get_status() -> dict[str, Any]:
    return status.status()
