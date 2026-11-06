"""Strategy sessions: artifact cards, strat.* tables, grid results, equity."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from api.errors import bad_request, not_found
from api.services import common
from api.services.common import Conn

logger = logging.getLogger(__name__)

TABLE_KINDS = ("trades", "equity", "cutoffs", "summary", "grid_results")
GRID_SORT_COLUMNS = (
    "total_fact_log_return", "net_log_return", "avg_fact_log_return", "compounded_return_pct",
    "win_rate", "n_trades", "entry_cutoff",
)


def _metrics(artifact: dict[str, Any]) -> dict[str, Any]:
    m = artifact.get("metrics", {}) or {}
    return {
        "n_trades": m.get("n_trades"),
        "win_rate": m.get("win_rate"),
        "total_fact_log_return": m.get("total_fact_log_return"),
        "compounded_return_pct": m.get("compounded_return_pct"),
        "avg_hold_minutes": m.get("avg_hold_minutes"),
    }


def _card(sess: common.ActiveSession) -> dict[str, Any] | None:
    if not sess.artifact:
        return None
    p = sess.decision_params
    return {
        "session_id": sess.table_prefix,
        "fit_period": sess.artifact.get("fit_period"),
        "entry_cutoff": p.get("entry_cutoff"),
        "tp_spec": p.get("tp_spec"),
        "sl_spec": p.get("sl_spec"),
        "max_hold_minutes": p.get("max_hold_minutes"),
        "metrics": _metrics(sess.artifact),
    }


def active() -> dict[str, Any]:
    """Cards for the active long / short strategies (``None`` when the artifact is missing)."""
    sessions = common.active_sessions()
    return {side: _card(sess) for side, sess in sessions.items()}


def equity_summary() -> dict[str, Any]:
    return {
        side: _metrics(sess.artifact) if sess.artifact else None
        for side, sess in common.active_sessions().items()
    }


def check_session_id(session_id: str) -> str:
    if not common.SAFE_ID.match(session_id):
        raise bad_request(f"invalid session_id: {session_id!r}")
    return session_id


def require_table(conn: Conn, session_id: str, kind: str) -> str:
    """Return the quoted fully-qualified ``strat`` table or raise 404."""
    check_session_id(session_id)
    name = f"{session_id}__{kind}"
    if not common.table_exists(conn, "lab", "strat", name):
        raise not_found(f"strat table not found: {name}")
    return f"lab.strat.{common.quote(name)}"


def sessions(conn: Conn) -> list[dict[str, Any]]:
    """Strategy sessions with a ``strat.<id>__trades`` table, joined with ``reg.strategies``."""
    rows = common.fetch_rows(
        conn,
        "SELECT replace(table_name, '__trades', '') AS session_id"
        " FROM information_schema.tables"
        " WHERE table_catalog = 'lab' AND table_schema = 'strat' AND table_name LIKE '%\\_\\_trades'"
        " ESCAPE '\\' ORDER BY 1",
    )
    meta: dict[str, dict[str, Any]] = {}
    if common.table_exists(conn, "reg", "main", "strategies"):
        for r in common.fetch_rows(
            conn,
            "SELECT session_id, model_id_long, model_id_short, status, created_at"
            " FROM reg.main.strategies",
        ):
            meta[str(r["session_id"])] = r
    out = []
    for r in rows:
        m = meta.get(r["session_id"], {})
        out.append({
            "session_id": r["session_id"],
            "model_id_long": m.get("model_id_long"),
            "model_id_short": m.get("model_id_short"),
            "status": m.get("status"),
            "created_at": m.get("created_at"),
        })
    out.sort(key=lambda x: x["created_at"] or "", reverse=True)
    return out


def _artifact_path(conn: Conn, session_id: str) -> Path | None:
    """Locate ``strategy_artifact.json`` of a session (active config, registry, convention)."""
    for sess in common.active_sessions().values():
        if sess.table_prefix == session_id and sess.artifact_dir:
            return sess.artifact_dir / "strategy_artifact.json"
    if common.table_exists(conn, "reg", "main", "artifacts"):
        row = conn.execute(
            "SELECT path FROM reg.main.artifacts"
            " WHERE owner_id = ? AND kind = 'strategy_artifact' ORDER BY updated_at DESC LIMIT 1",
            [session_id],
        ).fetchone()
        if row and Path(str(row[0])).is_file():
            return Path(str(row[0]))
    if common.table_exists(conn, "reg", "main", "strategies"):
        row = conn.execute(
            "SELECT model_id_long FROM reg.main.strategies WHERE session_id = ? LIMIT 1",
            [session_id],
        ).fetchone()
        if row and row[0]:
            cand = common.repo_path(f"artifacts/{row[0]}/strategy/strategy_artifact.json")
            if cand.is_file():
                return cand
    cand = common.repo_path(f"artifacts/{session_id}/strategy_artifact.json")
    return cand if cand.is_file() else None


def artifact(conn: Conn, session_id: str) -> dict[str, Any]:
    check_session_id(session_id)
    path = _artifact_path(conn, session_id)
    data = common.read_json(path) if path else {}
    if not data:
        raise not_found(f"strategy artifact not found for session {session_id}")
    return common.json_safe(data)


def table(conn: Conn, session_id: str, kind: str, limit: int, offset: int) -> dict[str, Any]:
    fq = require_table(conn, session_id, kind)
    total = int(common.scalar(conn, f"SELECT count(*) FROM {fq}") or 0)
    order = ""
    cols = common.table_columns(conn, "lab", "strat", f"{session_id}__{kind}")
    for cand in ("entry_time", "trade_index", "bucket_id"):
        if cand in cols:
            order = f"ORDER BY {cand}"
            break
    items = common.fetch_rows(conn, f"SELECT * FROM {fq} {order} LIMIT ? OFFSET ?", [limit, offset])
    return {"items": items, "total": total}


def grid(conn: Conn, session_id: str, top: int, sort: str) -> dict[str, Any]:
    if sort not in GRID_SORT_COLUMNS:
        raise bad_request(f"sort must be one of {', '.join(GRID_SORT_COLUMNS)}")
    fq = require_table(conn, session_id, "grid_results")
    total = int(common.scalar(conn, f"SELECT count(*) FROM {fq}") or 0)
    items = common.fetch_rows(
        conn, f"SELECT * FROM {fq} ORDER BY {common.quote(sort)} DESC NULLS LAST LIMIT ?", [top]
    )
    return {"items": items, "total": total}


def equity(conn: Conn, session: str, direction: str) -> dict[str, Any]:
    """Equity points of the active long/short sessions (or one explicit session id)."""
    ids: dict[str, str | None]
    if session == "active":
        act = common.active_sessions()
        ids = {"long": act["long"].table_prefix, "short": act["short"].table_prefix}
        wanted = ("long", "short") if direction == "both" else (direction,)
    else:
        check_session_id(session)
        ids = {"session": session}
        wanted = ("session",)
    series: dict[str, list[dict[str, Any]]] = {}
    seen: set[str] = set()
    for key in wanted:
        sid = ids.get(key)
        if not sid or sid in seen or not common.table_exists(conn, "lab", "strat", f"{sid}__equity"):
            series[key] = []
            continue
        seen.add(sid)
        series[key] = common.fetch_rows(
            conn,
            "SELECT entry_time AS t, cumulative_fact_log_return AS cum_fact_log_return,"
            " fact_log_return"
            f" FROM lab.strat.{common.quote(sid + '__equity')} ORDER BY trade_index",
        )
    points = [
        {"t": p["t"], "cum_fact_log_return": p["cum_fact_log_return"], "direction": key}
        for key, pts in series.items()
        for p in pts
    ]
    points.sort(key=lambda p: p["t"] or "")
    return {
        "session_ids": {k: v for k, v in ids.items()},
        "points": points,
        "series": series if (direction == "both" or session != "active") else None,
        "summary": equity_summary(),
    }
