"""/api/reports: manifest list, report content, lazy chart / table files (blueprint 2.11)."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Query, Request, Response

from api.services import reports as service
from reports.store import FileBlob

router = APIRouter(prefix="/api/reports", tags=["reports"])

CACHE_CONTROL = "max-age=60"


def _json_blob(request: Request, blob: FileBlob) -> Response:
    headers = {"ETag": blob.etag, "Cache-Control": CACHE_CONTROL}
    if request.headers.get("if-none-match") == blob.etag:
        return Response(status_code=304, headers=headers)
    return Response(content=blob.data, media_type="application/json", headers=headers)


@router.get("")
def list_reports(
    scope: Annotated[Literal["model", "general"] | None, Query()] = None,
    model_id: Annotated[str | None, Query(max_length=128)] = None,
) -> dict[str, Any]:
    """Manifests only; ``scope=general`` ignores ``model_id``."""
    items = service.list_manifests(scope, model_id)
    return {"items": items, "total": len(items)}


@router.get("/{owner}/{slug}")
def get_report(owner: str, slug: str) -> dict[str, Any]:
    return service.get_report(owner, slug)


@router.get("/{owner}/{slug}/charts/{chart_id}")
def get_chart(owner: str, slug: str, chart_id: str, request: Request) -> Response:
    return _json_blob(request, service.get_chart(owner, slug, chart_id))


@router.get("/{owner}/{slug}/tables/{table_id}")
def get_table(owner: str, slug: str, table_id: str, request: Request) -> Response:
    return _json_blob(request, service.get_table(owner, slug, table_id))
