"""Report service: thin wrapper over ``reports.store`` that maps store errors to API errors."""

from __future__ import annotations

from typing import Any

from api.errors import ApiError, bad_request, not_found
from reports import store


def _translate(exc: store.ReportError) -> ApiError:
    if isinstance(exc, store.InvalidReportPath):
        return bad_request(str(exc))
    return not_found(str(exc))


def list_manifests(scope: str | None, model_id: str | None) -> list[dict[str, Any]]:
    """Manifest list only (no markdown / chart / table content)."""
    return store.list_reports(scope, model_id)


def get_report(owner: str, slug: str) -> dict[str, Any]:
    """``{"manifest": {...}, "markdown": "..."}`` of one report."""
    try:
        manifest, markdown = store.read_report(owner, slug)
    except store.ReportError as exc:
        raise _translate(exc) from exc
    return {"manifest": manifest, "markdown": markdown}


def get_chart(owner: str, slug: str, chart_id: str) -> store.FileBlob:
    try:
        return store.read_chart(owner, slug, chart_id)
    except store.ReportError as exc:
        raise _translate(exc) from exc


def get_table(owner: str, slug: str, table_id: str) -> store.FileBlob:
    try:
        return store.read_table(owner, slug, table_id)
    except store.ReportError as exc:
        raise _translate(exc) from exc
