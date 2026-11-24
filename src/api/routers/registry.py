"""Registry (read-only): active deployment view, whitelisted tables, model detail."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from api import schemas
from api.deps import AssetDep, get_conn
from api.services import registry
from api.services.common import Conn

router = APIRouter(prefix="/api/registry", tags=["registry"])


@router.get("/active", response_model=schemas.RegistryActive)
def get_active(conn: Annotated[Conn, Depends(get_conn)], asset_id: AssetDep) -> dict[str, Any]:
    return registry.active(conn, asset_id)


# Static sub-paths first: "/models/{id}" must not be swallowed by "/{table}".
@router.get("/models/{model_id}", response_model=schemas.ModelDetail)
def get_model(model_id: str, conn: Annotated[Conn, Depends(get_conn)]) -> dict[str, Any]:
    return registry.model_detail(conn, model_id)


@router.get("/models/{model_id}/features")
def get_model_features(model_id: str) -> dict[str, Any]:
    return registry.model_features(model_id)


@router.get("/{table}", response_model=schemas.Page)
def get_table(
    table: str,
    conn: Annotated[Conn, Depends(get_conn)],
    limit: Annotated[int, Query(ge=1, le=5000)] = 200,
    offset: Annotated[int, Query(ge=0)] = 0,
    filter: Annotated[str | None, Query(max_length=200)] = None,  # noqa: A002 - contract name
) -> dict[str, Any]:
    return registry.listing(conn, table, limit, offset, filter)
