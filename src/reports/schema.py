"""Pydantic models of ``manifest.json`` (schema version 1, see ``_doc_/_plans_/068_bff_ui.md`` §3.3)."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1
GENERAL_OWNER = "general"
ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"
ID_RE = re.compile(ID_PATTERN)
#: Model ids (``lgbm_solusdt_l_fw60_2101_2607``); no path separators, never ``general``.
OWNER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

MAX_TRACE_POINTS = 5000
MAX_CANDLE_POINTS = 2000
MAX_TABLE_ROWS = 2000


class ChartRef(BaseModel):
    """One chart entry of ``content.charts``."""

    model_config = ConfigDict(extra="allow")

    id: str = Field(pattern=ID_PATTERN)
    title: str
    file: str
    height: int = 420


class TableRef(BaseModel):
    """One table entry of ``content.tables``."""

    model_config = ConfigDict(extra="allow")

    id: str = Field(pattern=ID_PATTERN)
    title: str
    file: str


class Content(BaseModel):
    model_config = ConfigDict(extra="allow")

    markdown: str = "report.md"
    charts: list[ChartRef] = Field(default_factory=list)
    tables: list[TableRef] = Field(default_factory=list)


class Manifest(BaseModel):
    """Report manifest. Unknown fields are tolerated (forward compatible)."""

    model_config = ConfigDict(extra="allow")

    schema_version: int = SCHEMA_VERSION
    id: str = Field(pattern=ID_PATTERN)
    title: str
    scope: Literal["model", "general"]
    model_id: str | None = None
    created_at: str
    updated_at: str | None = None
    order: int = 100
    summary: str | None = None
    tags: list[str] = Field(default_factory=list)
    source: dict[str, Any] | None = None
    content: Content = Field(default_factory=Content)
