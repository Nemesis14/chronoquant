"""Read side of the report store: list / read with path-traversal protection.

Layout (blueprint §3.2)::

    artifacts/<model_id>/reports/<slug>/   scope = "model"   (owner = model_id)
    _doc_/reports/<slug>/                  scope = "general" (owner = "general")
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import utils
from reports.schema import GENERAL_OWNER, ID_RE, OWNER_RE, Manifest

logger = logging.getLogger(__name__)


class ReportError(Exception):
    """Base class of store errors."""


class ReportNotFound(ReportError):  # noqa: N818
    """The requested report / chart / table does not exist."""


class InvalidReportPath(ReportError):  # noqa: N818
    """Owner / slug / id failed validation (possible path traversal)."""


@dataclass(frozen=True)
class FileBlob:
    """Raw JSON file content with a cheap content-validator (mtime + size hash)."""

    data: bytes
    etag: str


def repo_root() -> Path:
    return Path(utils.repo_root())


def reports_root(owner: str, root: Path | None = None) -> Path:
    """Directory that holds the report folders of ``owner`` (validated)."""
    base = root if root is not None else repo_root()
    if owner == GENERAL_OWNER:
        return base / "_doc_" / "reports"
    if not OWNER_RE.fullmatch(owner) or owner.strip(".") == "":
        raise InvalidReportPath(f"invalid owner: {owner!r}")
    return base / "artifacts" / owner / "reports"


def report_dir(owner: str, slug: str, root: Path | None = None) -> Path:
    """Directory of one report; raises :class:`InvalidReportPath` on a bad slug / escape."""
    if not ID_RE.fullmatch(slug):
        raise InvalidReportPath(f"invalid slug: {slug!r}")
    parent = reports_root(owner, root)
    path = parent / slug
    if parent.resolve() not in path.resolve().parents:
        raise InvalidReportPath("path escapes the reports root")
    return path


def _safe_file(directory: Path, relative: str) -> Path:
    """Resolve ``relative`` inside ``directory`` or raise :class:`InvalidReportPath`."""
    candidate = (directory / relative).resolve()
    if directory.resolve() not in candidate.parents:
        raise InvalidReportPath(f"path escapes the report directory: {relative!r}")
    return candidate


def _read_manifest(directory: Path) -> Manifest:
    path = directory / "manifest.json"
    if not path.is_file():
        raise ReportNotFound(f"no manifest in {directory.name}")
    try:
        return Manifest.model_validate_json(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise ReportNotFound(f"invalid manifest in {directory.name}: {exc}") from exc


def _owner_dirs(scope: str | None, model_id: str | None, root: Path) -> list[tuple[str, Path]]:
    owners: list[tuple[str, Path]] = []
    if scope in (None, "general"):
        owners.append((GENERAL_OWNER, reports_root(GENERAL_OWNER, root)))
    if scope in (None, "model"):
        if model_id:
            if OWNER_RE.fullmatch(model_id) and model_id != GENERAL_OWNER:
                owners.append((model_id, reports_root(model_id, root)))
        else:
            artifacts = root / "artifacts"
            if artifacts.is_dir():
                owners.extend(
                    (p.name, p / "reports")
                    for p in sorted(artifacts.iterdir())
                    if p.is_dir() and OWNER_RE.fullmatch(p.name) and p.name != GENERAL_OWNER
                )
    return owners


def list_reports(
    scope: str | None = None,
    model_id: str | None = None,
    root: Path | None = None,
) -> list[dict[str, Any]]:
    """Manifest list only (no markdown / chart / table content), sorted by owner then order."""
    base = root if root is not None else repo_root()
    items: list[dict[str, Any]] = []
    for owner, parent in _owner_dirs(scope, model_id, base):
        if not parent.is_dir():
            continue
        for directory in sorted(p for p in parent.iterdir() if p.is_dir()):
            if not ID_RE.fullmatch(directory.name):
                continue
            try:
                manifest = _read_manifest(directory)
            except ReportNotFound as exc:
                logger.warning("skipping report %s: %s", directory, exc)
                continue
            item = manifest.model_dump(mode="json")
            item["id"] = directory.name
            item["owner"] = owner
            items.append(item)
    items.sort(key=lambda m: (m["owner"] != GENERAL_OWNER, m["owner"], m["order"], m["id"]))
    return items


def read_report(owner: str, slug: str, root: Path | None = None) -> tuple[dict[str, Any], str]:
    """``(manifest, markdown)`` of one report."""
    directory = report_dir(owner, slug, root)
    manifest = _read_manifest(directory)
    md_path = _safe_file(directory, manifest.content.markdown)
    markdown = md_path.read_text(encoding="utf-8") if md_path.is_file() else ""
    data = manifest.model_dump(mode="json")
    data["id"] = slug
    data["owner"] = owner
    return data, markdown


def _blob(path: Path) -> FileBlob:
    if not path.is_file():
        raise ReportNotFound(f"missing file: {path.name}")
    stat = path.stat()
    etag = hashlib.md5(f"{stat.st_mtime_ns}-{stat.st_size}".encode(), usedforsecurity=False)
    return FileBlob(data=path.read_bytes(), etag=f'"{etag.hexdigest()}"')


def _read_ref(owner: str, slug: str, kind: str, item_id: str, root: Path | None) -> FileBlob:
    if not ID_RE.fullmatch(item_id):
        raise InvalidReportPath(f"invalid {kind} id: {item_id!r}")
    directory = report_dir(owner, slug, root)
    manifest = _read_manifest(directory)
    refs = manifest.content.charts if kind == "chart" else manifest.content.tables
    ref = next((r for r in refs if r.id == item_id), None)
    if ref is None:
        raise ReportNotFound(f"{kind} {item_id!r} is not in the manifest")
    return _blob(_safe_file(directory, ref.file))


def read_chart(owner: str, slug: str, chart_id: str, root: Path | None = None) -> FileBlob:
    """Raw Plotly figure JSON of one chart (lazy endpoint)."""
    return _read_ref(owner, slug, "chart", chart_id, root)


def read_table(owner: str, slug: str, table_id: str, root: Path | None = None) -> FileBlob:
    """Raw table JSON of one table (lazy endpoint)."""
    return _read_ref(owner, slug, "table", table_id, root)


def table_json(blob: FileBlob) -> dict[str, Any]:
    """Decode a table blob (helper for MCP / tests)."""
    return json.loads(blob.data)
