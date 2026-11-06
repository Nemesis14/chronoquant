"""Market-data tools: schema, read-only SQL, candles, predictions, latest signal."""

from __future__ import annotations

from typing import Any, Literal

from api import deps
from api.services import common, market, predictions
from cq_mcp.common import MAX_ROWS, clamp, dump
from data_handling.store import readonly_query


def list_tables(catalog: Literal["live", "lab", "reg"] | None = None, filter: str | None = None) -> str:  # noqa: A002
    """List database tables and columns. `catalog`: live | lab | reg; `filter`: substring of schema.table."""
    return dump(readonly_query.describe_schema(filter, catalog))


def query_sql(sql: str, limit: int = 200) -> str:
    """Run ONE read-only SELECT statement. Aliases: live. lab. (lab.strat.*) reg. Max 1000 rows."""
    try:
        result = readonly_query.run_readonly_query(sql, clamp(limit, 1, MAX_ROWS))
    except readonly_query.QueryRejectedError as exc:
        raise ValueError(f"query rejected: {exc}") from exc
    return dump(result)


def get_candles(hours: int = 24, tf: Literal["1m", "5m", "15m"] = "1m") -> str:
    """OHLCV candles of the last `hours` (1..168) resampled to `tf`."""
    with deps.ro_connection() as conn:
        return dump(market.candles(conn, clamp(hours, 1, 168), tf))


def get_predictions(hours: int = 24) -> str:
    """Model predictions (rank percentile and raw score, long and short) for the last `hours` (1..72)."""
    with deps.ro_connection() as conn:
        return dump({"items": predictions.history(conn, clamp(hours, 1, 72))})


def get_latest_signal() -> str:
    """Latest prediction plus its state relative to the active entry cutoffs."""
    with deps.ro_connection() as conn:
        latest = predictions.latest(conn)
    sessions = common.active_sessions()
    out: dict[str, Any] = {"latest": latest, "cutoffs": {}, "state": {}}
    for side in ("long", "short"):
        cutoff = sessions[side].entry_cutoff
        out["cutoffs"][side] = cutoff
        pct = (latest or {}).get(f"{side}_pct")
        out["state"][side] = (
            None if cutoff is None or pct is None else ("above_cutoff" if pct >= cutoff else "below_cutoff")
        )
    return dump(out)
