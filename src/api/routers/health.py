"""Health and status."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from api import schemas
from api.deps import AssetDep
from api.services import common, status
from worker import assets

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health", response_model=schemas.Health)
def health() -> dict[str, str]:
    return {"status": "ok", "time": common.now_iso()}


@router.get("/status", response_model=schemas.Status)
def get_status(asset_id: AssetDep) -> dict[str, Any]:
    return status.status(asset_id)


@router.get("/assets", response_model=schemas.AssetList)
def get_assets() -> dict[str, Any]:
    """Configured assets with ``has_data`` / ``active`` (sync-active) flags; ``?asset=`` takes any id."""
    return assets.list_assets()
