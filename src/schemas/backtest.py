# =============================================================================
# src/schemas/backtest.py — Backtesting request and result schemas
# =============================================================================
# Purpose:
#  - Define typed contracts for backtest inputs, trade records, and results
#  - Used as cross-module boundary between evaluation and reporting layers
# =============================================================================

from .base import ChronoBase


class BacktestRequest(ChronoBase):
    model_id         : str
    asset_id         : str
    strategy_id      : str
    start            : str    # UTC "YYYY-MM-DD HH:MM:SS"
    end              : str
    side             : str    # "long" | "short"
    entry_threshold  : float
    max_hold_minutes : int


class TradeRecord(ChronoBase):
    entry_time   : str
    exit_time    : str | None   = None
    side         : str
    entry_price  : float
    exit_price   : float | None = None
    pnl_pct      : float | None = None
    hold_minutes : int | None   = None
    exit_reason  : str | None   = None


class BacktestResult(ChronoBase):
    strategy_id   : str
    model_id      : str
    asset_id      : str
    start         : str
    end           : str
    n_trades      : int
    win_rate      : float | None = None
    avg_pnl_pct   : float | None = None
    total_pnl_pct : float | None = None
    trades        : list[TradeRecord]
