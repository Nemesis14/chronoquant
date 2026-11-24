"""Report service: thin wrapper over ``reports.store`` that maps store errors to API errors."""

from __future__ import annotations

import json
from typing import Any

from api.errors import ApiError, bad_request, not_found
from reports import store


def _translate(exc: store.ReportError) -> ApiError:
    if isinstance(exc, store.InvalidReportPath):
        return bad_request(str(exc))
    return not_found(str(exc))


def list_manifests(scope: str | None, model_id: str | None) -> list[dict[str, Any]]:
    """Manifest list only (no markdown / chart / table content).

    Model reports get an ``asset_id`` (from ``artifacts/<owner>/manifest.json``; fallback: None);
    general reports have ``asset_id = None`` (not asset-specific).
    """
    items = store.list_reports(scope, model_id)
    assets: dict[str, str | None] = {}
    for item in items:
        owner = item["owner"]
        if owner not in assets:
            assets[owner] = None if owner == "general" else _model_asset(owner)
        item["asset_id"] = assets[owner]
    return items


def _model_asset(model_id: str) -> str | None:
    path = store.repo_root() / "artifacts" / model_id / "manifest.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8")).get("asset_id")
    except (OSError, ValueError):
        return None
    return value if isinstance(value, str) else None


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
