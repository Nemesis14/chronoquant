"""Status, strategy, trades, equity, model and registry tools."""

from __future__ import annotations

from typing import Literal

from api import deps
from api.services import binance, registry, status, strat, trades
from cq_mcp.common import MAX_ROWS, clamp, dump, resolve_asset
from worker import assets


def list_assets() -> str:
    """Configured assets (asset_id, symbol, interval, is_default, has_data = live DB exists, active = auto-synced). Every other tool takes an optional `asset` id (default: the default asset, solusdt)."""
    return dump(assets.list_assets())


def get_status(asset: str | None = None) -> str:
    """Data freshness, active models, worker and sync state of an asset (`asset`: asset id, default solusdt)."""
    return dump(status.status(resolve_asset(asset)))


def get_active_strategy(asset: str | None = None) -> str:
    """Active long/short strategy cards (session, cutoff, TP/SL spec, metrics). `asset`: asset id (default solusdt)."""
    return dump(strat.active(resolve_asset(asset)))


def get_trades(
    source: Literal["strat", "journal", "binance"] = "strat", limit: int = 100, asset: str | None = None
) -> str:
    """Trades: `strat` = backtest-ledger closed trades, `journal` = live journal positions, `binance` = exchange fills. `asset`: asset id (default solusdt; the journal is shared across assets)."""
    n = clamp(limit, 1, MAX_ROWS)
    asset_id = resolve_asset(asset)
    if source == "binance":
        try:
            return dump(binance.recent_trades(n, asset_id))
        except binance.BinanceUnavailableError as exc:
            raise RuntimeError(f"binance unavailable: {exc}") from exc
    with deps.ro_connection(asset_id) as conn:
        if source == "journal":
            return dump(trades.journal_rows(conn, "trading_positions", "entry_time", n))
        return dump(trades.closed(conn, n, 0, "strat", asset_id))


def get_equity(
    session: str = "active", direction: Literal["both", "long", "short"] = "both", asset: str | None = None
) -> str:
    """Cumulative fact-log-return equity points plus the long/short summary. `asset`: asset id (default solusdt)."""
    asset_id = resolve_asset(asset)
    with deps.ro_connection(asset_id) as conn:
        data = strat.equity(conn, session, direction, asset_id)
    points = data.get("points") or []
    if len(points) > 2000:
        data["points"] = points[:: len(points) // 2000 + 1]
        data["note"] = "points downsampled"
    data.pop("series", None)
    return dump({"equity": data, "summary": strat.equity_summary(asset_id)})


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
