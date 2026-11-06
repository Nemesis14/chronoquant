"""Status, strategy, trades, equity, model and registry tools."""

from __future__ import annotations

from typing import Literal

from api import deps
from api.services import binance, registry, status, strat, trades
from cq_mcp.common import MAX_ROWS, clamp, dump


def get_status() -> str:
    """Data freshness, active models, worker and sync state."""
    return dump(status.status())


def get_active_strategy() -> str:
    """Active long/short strategy cards (session, cutoff, TP/SL spec, metrics)."""
    return dump(strat.active())


def get_trades(source: Literal["strat", "journal", "binance"] = "strat", limit: int = 100) -> str:
    """Trades: `strat` = backtest-ledger closed trades, `journal` = live journal positions, `binance` = exchange fills."""
    n = clamp(limit, 1, MAX_ROWS)
    if source == "binance":
        try:
            return dump(binance.recent_trades(n))
        except binance.BinanceUnavailableError as exc:
            raise RuntimeError(f"binance unavailable: {exc}") from exc
    with deps.ro_connection() as conn:
        if source == "journal":
            return dump(trades.journal_rows(conn, "trading_positions", "entry_time", n))
        return dump(trades.closed(conn, n, 0, "strat"))


def get_equity(session: str = "active", direction: Literal["both", "long", "short"] = "both") -> str:
    """Cumulative fact-log-return equity points plus the long/short summary."""
    with deps.ro_connection() as conn:
        data = strat.equity(conn, session, direction)
    points = data.get("points") or []
    if len(points) > 2000:
        data["points"] = points[:: len(points) // 2000 + 1]
        data["note"] = "points downsampled"
    data.pop("series", None)
    return dump({"equity": data, "summary": strat.equity_summary()})


def list_models(status: str | None = None) -> str:  # noqa: A002
    """Models in the registry (optionally filtered by a status substring)."""
    with deps.ro_connection() as conn:
        return dump(registry.listing(conn, "models", MAX_ROWS, 0, status))


def get_model(model_id: str) -> str:
    """Registry row, manifest, params, best search result and artifact list of one model."""
    with deps.ro_connection() as conn:
        detail = registry.model_detail(conn, model_id)
    detail.pop("reports", None)
    return dump(detail)


def get_registry(table: str, filter: str | None = None, limit: int = 100) -> str:  # noqa: A002
    """Whitelisted registry table (assets, snapshots, feature_sets, models, search_runs, strategies, deployments, artifacts)."""
    with deps.ro_connection() as conn:
        return dump(registry.listing(conn, table, clamp(limit, 1, MAX_ROWS), 0, filter))
