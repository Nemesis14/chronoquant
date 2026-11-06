"""Registry (``reg`` catalog) and model-artifact reads — whitelisted tables, never free SQL."""

from __future__ import annotations

import logging
from typing import Any

import utils
from api.errors import bad_request, not_found
from api.services import common
from api.services.common import Conn
from reports import store as report_store

logger = logging.getLogger(__name__)

TABLES = (
    "assets", "snapshots", "feature_sets", "models", "search_runs", "strategies", "deployments",
    "artifacts",
)
_ESC = chr(92)  # backslash: LIKE escape character
_ESC_SQL = "'" + _ESC + "'"
_ORDER_CANDIDATES = ("updated_at", "created_at")
_MODEL_FILES = (
    ("manifest", "manifest.json"),
    ("model", "model.pkl"),
    ("features", "features.json"),
    ("params", "params.json"),
    ("search_best", "search/search_best.json"),
    ("strategy_artifact", "strategy/strategy_artifact.json"),
)


def listing(conn: Conn, table: str, limit: int, offset: int, text: str | None) -> dict[str, Any]:
    """Rows of a whitelisted registry table, newest first, optional substring filter."""
    if table not in TABLES:
        raise bad_request(f"table must be one of {', '.join(TABLES)}")
    if not common.table_exists(conn, "reg", "main", table):
        return {"items": [], "total": 0}
    cols = common.table_columns(conn, "reg", "main", table)
    where, params = "", []
    if text:
        where = "WHERE " + " OR ".join(
            f"CAST({common.quote(c)} AS VARCHAR) ILIKE ? ESCAPE {_ESC_SQL}" for c in cols
        )
        escaped = text.replace(_ESC, _ESC * 2).replace("%", _ESC + "%").replace("_", _ESC + "_")
        params = [f"%{escaped}%"] * len(cols)
    order = next((c for c in _ORDER_CANDIDATES if c in cols), cols[0])
    fq = f"reg.main.{common.quote(table)}"
    total = int(common.scalar(conn, f"SELECT count(*) FROM {fq} {where}", params) or 0)
    items = common.fetch_rows(
        conn,
        f"SELECT * FROM {fq} {where} ORDER BY {common.quote(order)} DESC LIMIT ? OFFSET ?",
        [*params, limit, offset],
    )
    return {"items": items, "total": total}


def active(conn: Conn) -> dict[str, Any]:
    """Live models (from the newest prediction stamp), strategy sessions, pending deployments."""
    live: dict[str, Any] = {"long": None, "short": None}
    preds = common.live_table("predictions")
    if common.table_exists(conn, "live", "main", preds):
        row = conn.execute(
            "SELECT long_model_id, short_model_id"
            f" FROM live.{common.quote(preds)} ORDER BY open_time DESC LIMIT 1"
        ).fetchone()
        if row:
            live = {"long": row[0], "short": row[1]}
    sessions = common.active_sessions()
    pending: list[dict[str, Any]] = []
    if common.table_exists(conn, "reg", "main", "deployments"):
        pending = common.fetch_rows(
            conn,
            "SELECT * FROM reg.main.deployments WHERE status = 'pending' ORDER BY created_at DESC",
        )
    return {
        "live_models": live,
        "strategy_sessions": {s: sess.table_prefix for s, sess in sessions.items()},
        "pending_deployments": pending,
    }


def _model_dir(model_id: str):  # noqa: ANN202 — Path
    if not common.SAFE_ID.match(model_id):
        raise bad_request(f"invalid model_id: {model_id!r}")
    return common.repo_path(f"artifacts/{model_id}")


def _model_reports(model_id: str) -> list[dict[str, Any]]:
    """Report manifests of the model (empty on any reader problem; the list endpoint is authoritative)."""
    try:
        return report_store.list_reports("model", model_id)
    except report_store.ReportError:
        return []


def model_detail(conn: Conn, model_id: str) -> dict[str, Any]:
    directory = _model_dir(model_id)
    row: dict[str, Any] | None = None
    if common.table_exists(conn, "reg", "main", "models"):
        rows = common.fetch_rows(conn, "SELECT * FROM reg.main.models WHERE model_id = ?", [model_id])
        row = rows[0] if rows else None
    if row is None and not directory.is_dir():
        raise not_found(f"model not found: {model_id}")
    features = common.read_json(directory / "features.json")
    return {
        "registry": row,
        "manifest": common.json_safe(common.read_json(directory / "manifest.json")),
        "params": common.json_safe(common.read_json(directory / "params.json")),
        "search_best": common.json_safe(common.read_json(directory / "search" / "search_best.json")),
        "feature_count": len(features.get("features", []) or []),
        "artifacts": [
            {"kind": kind, "path": f"artifacts/{model_id}/{rel}", "exists": (directory / rel).is_file()}
            for kind, rel in _MODEL_FILES
        ],
        "reports": _model_reports(model_id),
    }


def model_features(model_id: str) -> dict[str, Any]:
    data = common.read_json(_model_dir(model_id) / "features.json")
    if not data:
        raise not_found(f"features.json not found for {model_id}")
    return common.json_safe(data)


def live_asset() -> dict[str, Any]:
    db = utils.load_asset_config(None)["database"]
    return {"asset_id": db.get("asset_id"), "symbol": db.get("symbol"), "interval": db.get("interval")}
