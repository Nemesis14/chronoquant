"""Shared helpers for the service layer: JSON-safe conversion, SQL helpers, strategy resolution."""

from __future__ import annotations

import json
import logging
import math
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

import utils

logger = logging.getLogger(__name__)

Conn = duckdb.DuckDBPyConnection

SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}$")
DEFAULT_LIVE_TABLES = {"ohlcv": "ohlcv", "features": "feat_ohlcv_quant", "predictions": "predictions"}


# %% Value conversion


def iso_z(value: Any) -> str | None:
    """Convert a datetime-like value (naive = UTC) to ``YYYY-MM-DDTHH:MM:SSZ``."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, str):
        try:
            value = pd.Timestamp(value)
        except (ValueError, TypeError):
            return value
    if isinstance(value, pd.Timestamp | datetime):
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def json_safe(value: Any) -> Any:
    """NaN/Inf -> None, numpy scalars -> python, timestamps -> ISO-Z, containers recursed."""
    if value is None:
        return None
    if isinstance(value, pd.Timestamp | datetime):
        return iso_z(value)
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [json_safe(v) for v in value]
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return None
    if value is pd.NaT:
        return None
    return value


def sql_ts(ts: pd.Timestamp | datetime) -> str:
    """Format a timestamp as the DB's naive-UTC ``YYYY-MM-DD HH:MM:SS`` string."""
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


# %% SQL helpers


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def fetch_rows(conn: Conn, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    """Run ``sql`` and return JSON-safe dict rows."""
    cur = conn.execute(sql, params or [])
    cols = [d[0] for d in (cur.description or [])]
    return [{c: json_safe(v) for c, v in zip(cols, row, strict=True)} for row in cur.fetchall()]


def fetch_df(conn: Conn, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
    return conn.execute(sql, params or []).df()


def scalar(conn: Conn, sql: str, params: list[Any] | None = None) -> Any:
    row = conn.execute(sql, params or []).fetchone()
    return row[0] if row else None


def table_exists(conn: Conn, catalog: str, schema: str, name: str) -> bool:
    """True if ``catalog.schema.name`` exists (False for an unattached catalog)."""
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables"
        " WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [catalog, schema, name],
    ).fetchone()
    return row is not None


def table_columns(conn: Conn, catalog: str, schema: str, name: str) -> list[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_catalog = ? AND table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [catalog, schema, name],
    ).fetchall()
    return [r[0] for r in rows]


def live_table(key: str) -> str:
    """Resolve a configured live table name (``ohlcv`` / ``features`` / ``predictions``)."""
    try:
        configured = utils.load_asset_config(None)["database"].get("tables", {}) or {}
    except Exception:
        logger.exception("live_table: asset config unavailable")
        configured = {}
    return str(configured.get(key) or DEFAULT_LIVE_TABLES[key])


def to_page(rows: list[Any], limit: int, offset: int) -> list[Any]:
    return rows[offset : offset + limit]


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# %% Repo paths and artifacts


def repo_path(relative: str) -> Path:
    return Path(utils.repo_root()) / relative


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON object file; ``{}`` when missing or unparsable."""
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("read_json: failed to parse %s", path)
        return {}
    return data if isinstance(data, dict) else {}


# %% Active strategy sessions


class ActiveSession:
    """Resolved active strategy for one direction."""

    def __init__(self, direction: str, config_id: str | None) -> None:
        self.direction = direction
        self.config_id = config_id
        self.artifact_dir: Path | None = repo_path(f"artifacts/{config_id}") if config_id else None
        self.artifact: dict[str, Any] = (
            read_json(self.artifact_dir / "strategy_artifact.json") if self.artifact_dir else {}
        )

    @property
    def table_prefix(self) -> str | None:
        """strat-table prefix: the artifact's own ``session_id`` (config id is a path)."""
        return self.artifact.get("session_id") or self.config_id

    @property
    def decision_params(self) -> dict[str, Any]:
        return self.artifact.get("decision_params", {}) or {}

    @property
    def entry_cutoff(self) -> float | None:
        value = self.decision_params.get("entry_cutoff")
        return float(value) if value is not None else None

    def rank_lookup(self) -> tuple[np.ndarray, np.ndarray] | None:
        filename = self.artifact.get(f"rank_lookup_{self.direction}_path", "")
        if not filename or self.artifact_dir is None:
            return None
        path = self.artifact_dir / filename
        if not path.is_file():
            return None
        try:
            df = pd.read_parquet(path)
            return df["score_raw"].to_numpy(dtype=float), df["score_pct"].to_numpy(dtype=float)
        except Exception:
            logger.exception("rank_lookup: failed to read %s", path)
            return None


def active_sessions(asset_id: str | None = None) -> dict[str, ActiveSession]:
    """Active long / short strategy sessions of an asset (``config/trading.json``, default asset if None)."""
    try:
        ids = utils.strategy_session_ids(asset_id)
    except Exception:
        logger.exception("active_sessions: trading config unavailable")
        ids = {"long": None, "short": None}
    return {
        "long": ActiveSession("long", ids.get("long")),
        "short": ActiveSession("short", ids.get("short")),
    }


def apply_rank(series: pd.Series, lookup: tuple[np.ndarray, np.ndarray] | None) -> pd.Series:
    """Raw score -> rank percentile (identity when no lookup)."""
    return utils.apply_rank_percentile(series, lookup)  # type: ignore[arg-type]
