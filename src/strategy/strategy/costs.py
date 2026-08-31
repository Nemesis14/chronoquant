"""Trading cost model for strategy search (epic_055 / t401, t402).

Until this module existed the grid search optimised GROSS return, which made it
systematically prefer over-trading setups: forced to pay nothing per fill, the
optimiser had no reason to stop trading. The epic_054 measurement showed how
far that goes -- the winning setup there took 1984 trades for a 2.52 bps edge
each, which at a realistic fee would have wiped the account out several times.

Three components, each in log-return units of NOTIONAL so they subtract
directly from `fact_log_return`:

* **fee** -- charged on notional, on both fills, win or lose. Entry is always
  taker (we act on a signal); the exit depends on how the position closed, so
  a take-profit that rests as a limit order pays the maker rate.
* **slippage** -- crossing the spread on a market order.
* **funding** -- only for the settlements a position actually spans. Not a flat
  rate: SOLUSDT funding has run at 2h and 4h intervals as well as 8h.

Note on units: fees are a fraction of notional while `fact_log_return` is a log
return. For the magnitudes involved (single-digit bps) the two coincide to
within rounding, so they are summed directly.
"""

import logging
from typing import Any

import pandas as pd

import utils

logger = logging.getLogger(__name__)

# Binance USDS-M futures VIP0, no effective BNB discount (verified 2026-08-25).
# Overridable from config/strategies.json -> "costs".
DEFAULT_COSTS: dict[str, Any] = {
    "maker_bps": 2.0,
    "taker_bps": 5.0,
    "slippage_bps": 1.5,
    "exit_order_type": {"tp": "maker", "sl": "taker", "timeout": "taker"},
}


def load_cost_config() -> dict[str, Any]:
    """Return the cost block from config/strategies.json, merged over defaults."""
    cfg = utils.load_strategies_config() or {}
    costs = dict(DEFAULT_COSTS)
    override = cfg.get("costs")
    if isinstance(override, dict):
        exits = dict(DEFAULT_COSTS["exit_order_type"])
        exits.update(override.get("exit_order_type", {}))
        costs.update(override)
        costs["exit_order_type"] = exits
    return costs


def round_trip_lr(exit_reason: str, costs: dict[str, Any] | None = None) -> float:
    """Fee + slippage for one completed trade, as a fraction of notional.

    Args:
        exit_reason: "tp", "sl" or "timeout" -- decides the exit order type.
        costs: Cost config; loaded from config when omitted.

    Returns:
        Positive cost to subtract from the trade's log return.
    """
    cfg = costs if costs is not None else load_cost_config()
    taker = float(cfg["taker_bps"])
    maker = float(cfg["maker_bps"])
    slip = float(cfg["slippage_bps"])

    exit_kind = cfg["exit_order_type"].get(exit_reason, "taker")
    exit_fee = maker if exit_kind == "maker" else taker
    # A resting limit exit does not cross the spread, so it pays no slippage.
    exit_slip = 0.0 if exit_kind == "maker" else slip

    return (taker + slip + exit_fee + exit_slip) / 10_000.0


def load_funding_schedule(asset_id: str | None = None) -> pd.DataFrame:
    """Return the settled funding series: one row per settlement.

    `funding_rate.funding_ts` is the moment the rate was charged and
    `funding_rate` is the rate charged then -- do not confuse it with
    `altdata_5m.funding_last`, which is the most recent PAST settlement as seen
    from a given bar. Pairing a settlement time with the previous rate shifts
    the whole series by one interval.
    """
    from data_handling.store.duckdb_store import get_connection

    db_path = utils.load_asset_config(asset_id)["database"]["db_path"]
    conn = get_connection(db_path)
    try:
        return conn.execute(
            "SELECT funding_ts, funding_rate FROM funding_rate ORDER BY funding_ts"
        ).df()
    finally:
        conn.close()


def funding_lr(
    entry_time: Any,
    exit_time: Any,
    direction: str,
    schedule: pd.DataFrame,
) -> float:
    """Funding paid (positive) or received (negative) over a position's life.

    Only settlements strictly after entry and at or before exit apply -- a
    position opened one minute after a settlement does not pay it.

    Sign convention: a positive funding rate means longs pay shorts, so a long
    position returns a positive cost and a short a negative one.
    """
    if schedule.empty:
        return 0.0

    ts = schedule["funding_ts"]
    crossed = schedule.loc[(ts > entry_time) & (ts <= exit_time), "funding_rate"]
    if crossed.empty:
        return 0.0

    total = float(crossed.sum())
    return total if direction == "long" else -total


def apply_costs(
    trades: list[dict[str, Any]],
    direction: str,
    schedule: pd.DataFrame | None = None,
    costs: dict[str, Any] | None = None,
) -> tuple[float, float, float]:
    """Total gross return, total fee/slippage and total funding for a trade list.

    Args:
        trades: Trade dicts carrying `fact_log_return`, `exit_reason`,
            `entry_time` and `exit_time`.
        direction: "long" or "short" -- decides the funding sign.
        schedule: Funding settlements; funding is skipped when None.
        costs: Cost config; loaded from config when omitted.

    Returns:
        (gross_lr, cost_lr, funding_lr) -- net is gross - cost - funding.
    """
    cfg = costs if costs is not None else load_cost_config()

    gross = 0.0
    cost = 0.0
    funding = 0.0
    for trade in trades:
        gross += float(trade["fact_log_return"])
        cost += round_trip_lr(str(trade.get("exit_reason", "timeout")), cfg)
        if schedule is not None:
            funding += funding_lr(
                trade["entry_time"], trade["exit_time"], direction, schedule
            )

    return gross, cost, funding
