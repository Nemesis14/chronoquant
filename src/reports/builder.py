"""``ReportBuilder``: writes ``manifest.json`` + ``report.md`` + ``charts/*.json`` + ``tables/*.json``.

Usage (blueprint §3.4)::

    rb = ReportBuilder(scope="model", model_id=mid, slug="01_sampling", title="...", order=10)
    rb.add_markdown("## Intro\\n...")
    rb.add_chart("monthly_counts", fig, title="Monthly samples", height=420)
    rb.add_table("fold_summary", df, title="Fold summary")
    rb.write()

The output is deterministic (same input -> same bytes, except ``created_at`` / ``updated_at``)
and ``write()`` is atomic: everything is written to a temporary sibling folder, the manifest last,
then the folder is renamed over the previous version.
"""

from __future__ import annotations

import base64
import json
import math
import re
import shutil
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np

from reports import store
from reports.schema import (
    GENERAL_OWNER,
    ID_RE,
    MAX_CANDLE_POINTS,
    MAX_TABLE_ROWS,
    MAX_TRACE_POINTS,
    SCHEMA_VERSION,
    ChartRef,
    Content,
    Manifest,
    TableRef,
)

_PLACEHOLDER_RE = re.compile(r"\{\{(chart|table):([^}\s]+)\}\}")
_THIN_KEYS = ("x", "y", "z", "text", "hovertext", "customdata", "open", "high", "low", "close")
_SKIP_THIN_TYPES = {"heatmap", "surface", "contour", "pie", "sunburst", "treemap", "table"}
_CANDLE_TYPES = {"candlestick", "ohlc"}
_PCT_NAME_PARTS = ("_pct", "pct_", "_rate", "rate_")


# --------------------------------------------------------------------------- figure sanitising


def _utc_iso(value: Any) -> str:
    if isinstance(value, np.datetime64):
        value = value.astype("datetime64[us]").astype(datetime)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)
        return value.isoformat(timespec="seconds") + "Z"
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day).isoformat(timespec="seconds") + "Z"
    return str(value)


def _decode_bdata(spec: Mapping[str, Any]) -> list[Any]:
    """Plotly 6 typed-array ``{"dtype", "bdata", "shape"?}`` -> nested python list."""
    raw = base64.b64decode(spec["bdata"])
    arr = np.frombuffer(raw, dtype=np.dtype(spec["dtype"]))
    shape = spec.get("shape")
    if shape:
        dims = [int(s) for s in str(shape).split(",")] if isinstance(shape, str) else list(shape)
        arr = arr.reshape(dims)
    return arr.tolist()


def to_jsonable(obj: Any) -> Any:
    """Recursively convert to plain JSON types: ndarray -> list, NaN/Inf -> None, bdata decoded."""
    if isinstance(obj, Mapping):
        if "bdata" in obj and "dtype" in obj:
            return to_jsonable(_decode_bdata(obj))
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, np.ndarray):
        return to_jsonable(obj.tolist())
    if isinstance(obj, np.generic):
        return to_jsonable(obj.item())
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, (datetime, date, np.datetime64)):
        return _utc_iso(obj)
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj
    if hasattr(obj, "isoformat"):  # pandas NaT and friends
        try:
            return _utc_iso(obj)
        except (ValueError, TypeError):
            return None
    return str(obj)


def _thin_indices(n: int, limit: int) -> list[int]:
    """Evenly spread, deterministic subset of ``range(n)`` that keeps the first and last point."""
    return sorted({int(i) for i in np.linspace(0, n - 1, limit).round()})


def _thin_trace(trace: dict[str, Any], max_points: int) -> None:
    ttype = trace.get("type", "scatter")
    if ttype in _SKIP_THIN_TYPES:
        return
    limit = MAX_CANDLE_POINTS if ttype in _CANDLE_TYPES else max_points
    ref = trace.get("x") if isinstance(trace.get("x"), list) else trace.get("y")
    if not isinstance(ref, list) or len(ref) <= limit:
        return
    n = len(ref)
    idx = _thin_indices(n, limit)

    def pick(values: list[Any]) -> list[Any]:
        return [values[i] for i in idx]

    for key in _THIN_KEYS:
        value = trace.get(key)
        if isinstance(value, list) and len(value) == n and not (value and isinstance(value[0], list)):
            trace[key] = pick(value)
    for sub in ("marker", "line"):
        spec = trace.get(sub)
        if isinstance(spec, dict):
            for key in ("color", "size", "opacity"):
                value = spec.get(key)
                if isinstance(value, list) and len(value) == n:
                    spec[key] = pick(value)


def figure_to_json(fig: Any, *, max_points: int = MAX_TRACE_POINTS) -> dict[str, Any]:
    """Plotly ``Figure`` (or figure dict) -> JSON-safe ``{data, layout, config?}`` dict.

    Arrays become plain lists (no ``bdata``), ``layout.template`` is dropped and every trace is
    thinned to ``max_points`` (candles: 2000).
    """
    raw = fig.to_plotly_json() if hasattr(fig, "to_plotly_json") else dict(fig)
    out = to_jsonable(raw)
    layout = out.get("layout") or {}
    layout.pop("template", None)
    figure: dict[str, Any] = {"data": out.get("data") or [], "layout": layout}
    if out.get("config"):
        figure["config"] = out["config"]
    for trace in figure["data"]:
        if isinstance(trace, dict):
            _thin_trace(trace, max_points)
    return figure


# --------------------------------------------------------------------------- table conversion


def _infer_type(name: str, values: list[Any], kind: str) -> str:
    if kind == "b":
        return "bool"
    if kind in "iu":
        return "int"
    if kind == "M":
        return "datetime"
    if kind == "f":
        lowered = name.lower()
        return "pct" if any(p in lowered for p in _PCT_NAME_PARTS) else "float"
    sample = next((v for v in values if v is not None), None)
    if isinstance(sample, bool):
        return "bool"
    if isinstance(sample, int):
        return "int"
    if isinstance(sample, float):
        return "float"
    return "str"


def _as_pandas(df: Any) -> Any:
    import pandas as pd

    if isinstance(df, pd.DataFrame):
        return df
    if hasattr(df, "to_pandas"):  # polars
        return df.to_pandas()
    if isinstance(df, Sequence) and df and isinstance(df[0], Mapping):
        return pd.DataFrame(list(df))
    raise TypeError(f"unsupported table input: {type(df).__name__}")


def table_to_json(
    df: Any,
    columns: Mapping[str, Any] | None = None,
    *,
    max_rows: int = MAX_TABLE_ROWS,
) -> dict[str, Any]:
    """DataFrame -> ``{columns, rows, total_rows, truncated}``.

    ``columns`` optionally overrides per column: a label string or a dict with
    ``label`` / ``type`` / ``digits``.
    """
    frame = _as_pandas(df)
    total = len(frame)
    head = frame.head(max_rows)
    overrides = columns or {}
    specs: list[dict[str, Any]] = []
    rows_by_col: list[list[Any]] = []
    for name in head.columns:
        key = str(name)
        series = head[name]
        values = [to_jsonable(v) if v is not None and v == v else None for v in series.tolist()]
        override = overrides.get(key, {})
        if isinstance(override, str):
            override = {"label": override}
        col_type = override.get("type") or _infer_type(key, values, series.dtype.kind)
        spec: dict[str, Any] = {"key": key, "label": override.get("label", key), "type": col_type}
        if "digits" in override:
            spec["digits"] = int(override["digits"])
        elif col_type in ("float", "pct"):
            spec["digits"] = 4 if col_type == "float" else 2
        specs.append(spec)
        rows_by_col.append(values)
    rows = [list(r) for r in zip(*rows_by_col, strict=True)] if rows_by_col else []
    return {"columns": specs, "rows": rows, "total_rows": total, "truncated": total > max_rows}


# --------------------------------------------------------------------------- builder


def _dump(obj: Any, *, indent: int | None = None) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=indent, allow_nan=False)


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class ReportBuilder:
    """Collects ordered markdown / chart / table blocks and writes one report folder."""

    def __init__(
        self,
        scope: str,
        slug: str,
        title: str,
        *,
        model_id: str | None = None,
        order: int = 100,
        summary: str | None = None,
        tags: Sequence[str] = (),
        source: Mapping[str, Any] | None = None,
        root: Path | None = None,
    ) -> None:
        if scope not in ("model", "general"):
            raise ValueError(f"scope must be 'model' or 'general', got {scope!r}")
        if scope == "model" and not model_id:
            raise ValueError("model_id is required for scope='model'")
        if scope == "general":
            model_id = None
        if not ID_RE.fullmatch(slug):
            raise ValueError(f"invalid slug: {slug!r}")
        self.scope = scope
        self.model_id = model_id
        self.slug = slug
        self.title = title
        self.order = order
        self.summary = summary
        self.tags = list(tags)
        self.source = dict(source) if source else None
        self._owner = model_id if scope == "model" and model_id else GENERAL_OWNER
        self._root = root
        self._blocks: list[str] = []
        self._charts: dict[str, tuple[ChartRef, dict[str, Any]]] = {}
        self._tables: dict[str, tuple[TableRef, dict[str, Any]]] = {}

    @property
    def output_dir(self) -> Path:
        return store.report_dir(self._owner, self.slug, self._root)

    def add_markdown(self, text: str) -> ReportBuilder:
        """Append a markdown block (GFM; chart/table placeholders allowed on their own lines)."""
        self._blocks.append(text.strip("\n"))
        return self

    def add_chart(
        self,
        chart_id: str,
        fig: Any,
        *,
        title: str,
        height: int = 420,
        placeholder: bool = True,
        max_points: int = MAX_TRACE_POINTS,
    ) -> ReportBuilder:
        """Add a Plotly figure; appends the chart placeholder to the markdown unless disabled."""
        self._check_new_id(chart_id)
        ref = ChartRef(id=chart_id, title=title, file=f"charts/{chart_id}.json", height=height)
        self._charts[chart_id] = (ref, figure_to_json(fig, max_points=max_points))
        if placeholder:
            self._blocks.append("{{chart:" + chart_id + "}}")
        return self

    def add_table(
        self,
        table_id: str,
        df: Any,
        *,
        title: str,
        columns: Mapping[str, Any] | None = None,
        placeholder: bool = True,
    ) -> ReportBuilder:
        """Add a pandas/polars table (max 2000 rows); appends the table placeholder unless disabled."""
        self._check_new_id(table_id)
        ref = TableRef(id=table_id, title=title, file=f"tables/{table_id}.json")
        self._tables[table_id] = (ref, table_to_json(df, columns))
        if placeholder:
            self._blocks.append("{{table:" + table_id + "}}")
        return self

    def _check_new_id(self, item_id: str) -> None:
        if not ID_RE.fullmatch(item_id):
            raise ValueError(f"invalid chart/table id: {item_id!r}")
        if item_id in self._charts or item_id in self._tables:
            raise ValueError(f"duplicate chart/table id: {item_id!r}")

    def markdown(self) -> str:
        """The assembled ``report.md`` text."""
        return "\n\n".join(self._blocks) + "\n"

    def _validate_placeholders(self, markdown: str) -> None:
        for kind, item_id in _PLACEHOLDER_RE.findall(markdown):
            known = self._charts if kind == "chart" else self._tables
            if item_id not in known:
                raise ValueError(f"placeholder {kind}:{item_id} has no matching {kind}")

    def write(self) -> Path:
        """Write the report atomically (idempotent; replaces an existing folder of the same slug)."""
        markdown = self.markdown()
        self._validate_placeholders(markdown)
        target = self.output_dir
        parent = target.parent
        parent.mkdir(parents=True, exist_ok=True)

        now = _now_iso()
        created_at = now
        old_manifest = target / "manifest.json"
        if old_manifest.is_file():
            try:
                created_at = json.loads(old_manifest.read_text(encoding="utf-8")).get(
                    "created_at", now
                )
            except (ValueError, OSError):
                created_at = now

        manifest = Manifest.model_validate(
            {
                "schema_version": SCHEMA_VERSION,
                "id": self.slug,
                "title": self.title,
                "scope": self.scope,
                "model_id": self.model_id,
                "created_at": created_at,
                "updated_at": now,
                "order": self.order,
                "summary": self.summary,
                "tags": self.tags,
                "source": self.source,
                "content": Content(
                    markdown="report.md",
                    charts=[ref for ref, _ in self._charts.values()],
                    tables=[ref for ref, _ in self._tables.values()],
                ).model_dump(),
            }
        )

        token = uuid.uuid4().hex[:8]
        tmp = parent / f".{self.slug}.tmp-{token}"
        old = parent / f".{self.slug}.old-{token}"
        try:
            (tmp / "charts").mkdir(parents=True)
            (tmp / "tables").mkdir(parents=True)
            (tmp / "report.md").write_text(markdown, encoding="utf-8", newline="\n")
            for ref, figure in self._charts.values():
                (tmp / ref.file).write_text(_dump(figure), encoding="utf-8", newline="\n")
            for ref, table in self._tables.values():
                (tmp / ref.file).write_text(_dump(table), encoding="utf-8", newline="\n")
            # manifest last: a reader that sees a manifest sees a complete report
            (tmp / "manifest.json").write_text(
                _dump(manifest.model_dump(mode="json"), indent=2) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            if target.exists():
                target.rename(old)
            tmp.rename(target)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            shutil.rmtree(old, ignore_errors=True)
        return target
