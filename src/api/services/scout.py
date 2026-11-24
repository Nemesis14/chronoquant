"""Impulse Scout forward-log reads (epic_074 / t942): ``live.scout_signals`` + artifact meta.

Read-only.  The table exists only on Scout assets (solusdt / btcusdt / ethusdt; written by the
worker's ``sync_scout``); elsewhere (bchusdt) the reads answer ``available = False`` and no rows.
The artifact meta comes from ``artifacts/<model_id>/manifest.json`` (plain JSON, the ``scout``
package is not imported -- it pulls the research code).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from api.services import common
from api.services.common import Conn
from worker import assets

TABLE = "scout_signals"
#: rule columns ``{rule}_{field}`` grouped into ``rules[rule][field]``
RULE_FIELDS: tuple[str, ...] = ("tp", "sl", "outcome", "outcome_at", "exit_price", "net_bps")
_SELECT = (
    "SELECT *, date_diff('second', t_c, scored_at) AS write_lag_s"
    f" FROM live.{TABLE} WHERE asset_id = ?"
)


def available(conn: Conn) -> bool:
    return common.table_exists(conn, "live", "main", TABLE)


def rule_keys(conn: Conn) -> list[str]:
    """Rule keys present in the table (``default``, ``r2_03`` for scout_s0c_r1), from ``*_outcome_at`` columns."""
    cols = common.table_columns(conn, "live", "main", TABLE)
    return [c[: -len("_outcome_at")] for c in cols if c.endswith("_outcome_at")]


def _naive_utc(ts: datetime) -> datetime:
    return ts.astimezone(UTC).replace(tzinfo=None) if ts.tzinfo is not None else ts


def _shape(row: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    """Group the per-rule columns of one row into ``rules = {key: {tp, sl, outcome, ...}}``."""
    out = dict(row)
    out["rules"] = {k: {f: out.pop(f"{k}_{f}", None) for f in RULE_FIELDS} for k in keys}
    return out


def _events(conn: Conn, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    keys = rule_keys(conn)
    return [_shape(r, keys) for r in common.fetch_rows(conn, sql, params)]


def signals(conn: Conn, asset_id: str, since: datetime | None = None, limit: int = 500) -> list[dict[str, Any]]:
    """The newest ``limit`` events with ``t0 >= since``, returned in ascending ``t0`` order ([] without the table)."""
    if not available(conn):
        return []
    sql = _SELECT
    params: list[Any] = [asset_id]
    if since is not None:
        sql += " AND t0 >= ?"
        params.append(_naive_utc(since))
    sql += " ORDER BY t0 DESC LIMIT ?"
    params.append(int(limit))
    return list(reversed(_events(conn, sql, params)))


def counts(conn: Conn, asset_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT count(*), count(*) FILTER (WHERE status = 'open'), count(*) FILTER (WHERE status = 'resolved'),"
        " count(*) FILTER (WHERE status = 'invalid'), count(*) FILTER (WHERE top20),"
        " count(*) FILTER (WHERE top10), count(*) FILTER (WHERE backfilled),"
        f" max(as_of), max(scored_at), min(scored_at) FROM live.{TABLE} WHERE asset_id = ?",
        [asset_id],
    ).fetchone()
    row = row or (0,) * 7 + (None,) * 3
    return {
        "total": int(row[0]), "open": int(row[1]), "resolved": int(row[2]), "invalid": int(row[3]),
        "top20": int(row[4]), "top10": int(row[5]), "backfilled": int(row[6]),
        "as_of": common.iso_z(row[7]), "last_scored_at": common.iso_z(row[8]),
        "first_live_sync_at": common.iso_z(row[9]),
    }


def meta(asset_id: str) -> dict[str, Any]:
    """Artifact meta for the UI: model id, experimental flag, thresholds, live window, rules."""
    model_id = assets.scout_model_id()
    man = assets.scout_manifest(model_id)
    runtime = man.get("runtime") or {}
    thresholds = man.get("thresholds") or {}
    train = man.get("train") or {}
    event_def = man.get("event_definition") or {}
    status = str(man.get("status") or "experimental_no_go")
    return {
        "model_id": man.get("model_id") or model_id,
        "display_name": man.get("display_name"),
        "status": status,
        # NO-GO research model (5300): the UI marks it "experimental - not a trading signal"
        "experimental": "experimental" in status or "no_go" in status,
        "scout_asset": asset_id in assets.scout_asset_ids(),
        "thresholds": {"top20": thresholds.get("top20"), "top10": thresholds.get("top10")},
        "live_start": runtime.get("live_start"),
        "detect_anchor": runtime.get("detect_anchor"),
        "holdout2_start": runtime.get("holdout2_start"),
        "cell_id": event_def.get("cell_id"),
        "h_min": event_def.get("h_min"),
        "train_assets": list(train.get("assets") or []),
        "oof_auc": train.get("oof_auc"),
        "rules": [
            {"key": r.get("key"), "name": (r.get("rule") or {}).get("name"),
             "description": (r.get("rule") or {}).get("description")}
            for r in man.get("rules") or []
        ],
        "freeze_hash": man.get("freeze_hash"),
    }


def latest(conn: Conn, asset_id: str, recent: int = 5) -> dict[str, Any]:
    """Counters, open events and the ``recent`` newest events (newest first) + artifact meta."""
    out: dict[str, Any] = {"asset_id": asset_id, "available": available(conn), "meta": meta(asset_id)}
    if not out["available"]:
        return {**out, "counts": None, "open_events": [], "recent": []}
    out["counts"] = counts(conn, asset_id)
    out["open_events"] = _events(conn, _SELECT + " AND status = 'open' ORDER BY t0", [asset_id])
    out["recent"] = _events(conn, _SELECT + " ORDER BY t0 DESC LIMIT ?", [asset_id, int(recent)])
    return out
