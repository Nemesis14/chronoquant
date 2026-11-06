"""Report tools: list manifests, read a report's markdown (read-only, path-traversal safe).

Layout (blueprint 3.2): ``artifacts/<model_id>/reports/<slug>/`` (owner = model_id) and
``_doc_/reports/<slug>/`` (owner ``general``). Each report dir holds ``manifest.json`` +
``report.md`` + ``charts/*.json``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from api.services import common
from cq_mcp.common import dump

GENERAL = "general"
_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_OWNER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _owner_root(owner: str) -> Path:
    if owner != GENERAL and not _OWNER.match(owner):
        raise ValueError(f"invalid owner: {owner!r}")
    rel = "_doc_/reports" if owner == GENERAL else f"artifacts/{owner}/reports"
    return common.repo_path(rel)


def _manifest(directory: Path) -> dict[str, Any] | None:
    try:
        data = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _owners() -> list[str]:
    artifacts = common.repo_path("artifacts")
    owners = [GENERAL]
    if artifacts.is_dir():
        owners += sorted(p.parent.name for p in artifacts.glob("*/reports") if p.is_dir())
    return owners


def list_reports(owner: str | None = None) -> str:
    """Report manifests (metadata only). `owner`: a model_id or `general`; omit for all."""
    items: list[dict[str, Any]] = []
    for who in [owner] if owner else _owners():
        root = _owner_root(who)
        if not root.is_dir():
            continue
        for directory in sorted(p for p in root.iterdir() if p.is_dir()):
            man = _manifest(directory)
            if man is None:
                continue
            content = man.get("content") or {}
            items.append({
                "owner": who, "slug": directory.name, "title": man.get("title"),
                "order": man.get("order"), "summary": man.get("summary"),
                "tags": man.get("tags"), "updated_at": man.get("updated_at") or man.get("created_at"),
                "charts": [c.get("id") for c in content.get("charts") or []],
                "tables": [t.get("id") for t in content.get("tables") or []],
            })
    items.sort(key=lambda r: (r["owner"], r["order"] if isinstance(r["order"], int) else 9999, r["slug"]))
    return dump({"items": items})


def read_report(owner: str, slug: str, include_charts: bool = False) -> str:
    """Markdown of one report plus chart titles; with `include_charts` the Plotly JSON too (can be large)."""
    if not _SLUG.match(slug):
        raise ValueError(f"invalid slug: {slug!r}")
    directory = _owner_root(owner) / slug
    man = _manifest(directory)
    if man is None:
        raise FileNotFoundError(f"report not found: {owner}/{slug}")
    content = man.get("content") or {}
    base = directory.resolve()
    md_path = (directory / (content.get("markdown") or "report.md")).resolve()
    if base not in md_path.parents:
        raise ValueError("invalid markdown path in manifest")
    try:
        markdown = md_path.read_text(encoding="utf-8")
    except OSError:
        markdown = ""
    refs = content.get("charts") or []
    charts: list[dict[str, Any]] = [{"id": c.get("id"), "title": c.get("title")} for c in refs]
    if include_charts:
        for chart, ref in zip(charts, refs, strict=True):
            path = (directory / str(ref.get("file", ""))).resolve()
            if base in path.parents and path.is_file():
                try:
                    chart["figure"] = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    chart["figure"] = None
    return dump({"manifest": man, "markdown": markdown, "charts": charts})
