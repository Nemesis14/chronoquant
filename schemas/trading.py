# =============================================================================
# schemas/trading.py — Live trading signal and state schemas
# =============================================================================
# Purpose:
#  - Define typed contracts for trade signals, strategy config, and runtime state
#  - Used as cross-module boundary between trading layer and dashboard/journal
# =============================================================================

from schemas.base import ChronoBase


class TradeSignal(ChronoBase):
    open_time       : str
    asset_id        : str
    model_id        : str
    side            : str    # "long" | "short"
    probability     : float
    entry_threshold : float
    triggered       : bool


class StrategyConfig(ChronoBase):
    strategy_id      : str
    model_id         : str
    asset_id         : str
    side             : str
    entry_threshold  : float
    max_hold_minutes : int


class JournalEntry(ChronoBase):
    entry_time  : str
    side        : str
    entry_price : float
    strategy_id : str
    model_id    : str


class TradingState(ChronoBase):
    asset_id       : str
    open_position  : JournalEntry | None = None
    last_signal    : TradeSignal | None  = None
    last_updated   : str | None          = None
