"""Report format + builder (epic_068 / t884): markdown + Plotly JSON + table JSON, no notebooks."""

from reports.builder import ReportBuilder
from reports.schema import SCHEMA_VERSION, ChartRef, Manifest, TableRef

__all__ = ["SCHEMA_VERSION", "ChartRef", "Manifest", "ReportBuilder", "TableRef"]
