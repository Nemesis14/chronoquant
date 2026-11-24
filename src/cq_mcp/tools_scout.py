"""Impulse Scout tool (epic_074 / t942): read-only mirror of /api/scout/latest + /api/scout/signals."""

from __future__ import annotations

from datetime import datetime

from api import deps
from api.services import scout
from cq_mcp.common import clamp, dump, resolve_asset


def _parse_since(since: str | None) -> datetime | None:
    """ISO-8601 `since` (trailing Z allowed, naive = UTC) -> datetime; friendly ValueError if unparsable."""
    if not since:
        return None
    try:
        return datetime.fromisoformat(since.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid since: {since!r}; expected ISO-8601, e.g. 2026-11-10T00:00:00Z") from exc


def scout_signals(asset: str | None = None, since: str | None = None, limit: int = 50) -> str:
    """Impulse Scout forward-log of an asset (EXPERIMENTAL, NO-GO research model: not a trading signal): artifact meta (model_id, top20/top10 thresholds, live_start), counters, open events and the newest `limit` (1..500) events with t0 >= `since` (ISO-8601 UTC, optional), ascending by t0. Scout assets: solusdt, btcusdt, ethusdt; other assets answer available=false. `asset`: asset id (default solusdt)."""
    asset_id = resolve_asset(asset)
    since_dt = _parse_since(since)
    with deps.ro_connection(asset_id) as conn:
        out = scout.latest(conn, asset_id, recent=1)
        out.pop("recent", None)
        out["items"] = scout.signals(conn, asset_id, since_dt, clamp(limit, 1, 500))
    return dump(out)
