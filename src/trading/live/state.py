"""Trading state machine dataclass for the live trading loop.

Tracks open position details, daily risk counters, and the arm/cooldown
lifecycle used by the strategy evaluator.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

FLAT     = "FLAT"
LONG     = "LONG"
SHORT    = "SHORT"
COOLDOWN = "COOLDOWN"


# %% TradingState


@dataclass
class TradingState:
    """Mutable runtime state for one trading service run.

    Attributes:
        status         : Current state string (FLAT, LONG, SHORT, COOLDOWN).
        armed          : Whether the system is ready to enter a new position.
        cooldown_until : Earliest time the system may rearm after a trade.
        position_id    : Active position identifier (from journal).
        side           : Current position side (LONG or SHORT).
        entry_time     : UTC datetime when the current position was opened.
        entry_price    : Execution price of the current entry.
        quantity       : Position size in base asset units.
        run_id         : Identifier for the current service run.
        daily_trade_count  : Number of trades opened today.
        daily_loss_usdt    : Cumulative loss (USDT) today.
        consecutive_errors : Unbroken error count since last successful cycle.
        last_trade_date    : Date string of last trade (for daily reset).
    """

    status         : str           = FLAT
    armed          : bool          = True
    cooldown_until : datetime | None = None

    # Open position
    position_id : str | None   = None
    side        : str | None   = None
    entry_time  : datetime | None = None
    entry_price : float | None = None
    quantity    : float | None = None

    # Run context
    run_id : str | None = None

    # Risk counters (reset daily)
    daily_trade_count  : int   = 0
    daily_loss_usdt    : float = 0.0
    consecutive_errors : int   = 0
    last_trade_date    : str | None = None

    def hold_minutes(self, now: datetime | None = None) -> float:
        """Return how long the current position has been held in minutes.

        Args:
            now : Reference time (UTC). Defaults to current UTC time.

        Returns:
            Hold duration in minutes, or 0.0 if no position is open.
        """
        if self.entry_time is None:
            return 0.0
        now = now or datetime.now(UTC)
        return (now - self.entry_time).total_seconds() / 60.0

    def clear_position(self) -> None:
        """Reset all open-position fields to None."""
        self.position_id = None
        self.side        = None
        self.entry_time  = None
        self.entry_price = None
        self.quantity    = None

    def record_trade_result(self, pnl_usdt: float) -> None:
        """Update daily risk counters after a trade closes.

        Args:
            pnl_usdt : Realised PnL of the closed trade (negative = loss).
        """
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        if self.last_trade_date != today:
            self.daily_trade_count = 0
            self.daily_loss_usdt   = 0.0
            self.last_trade_date   = today
        self.daily_trade_count += 1
        if pnl_usdt < 0:
            self.daily_loss_usdt += abs(pnl_usdt)

    @classmethod
    def from_db(cls, run_id: str, open_position: dict | None) -> TradingState:
        """Reconstruct state from a persisted open position row.

        Args:
            run_id        : Current service run identifier.
            open_position : Row dict from ``journal.get_open_position``, or None.

        Returns:
            TradingState initialised from the open position (if any).
        """
        state = cls(run_id=run_id)
        if open_position:
            state.status      = open_position["side"]
            state.position_id = open_position["position_id"]
            state.side        = open_position["side"]
            entry_time        = open_position.get("entry_time")
            if entry_time:
                dt = datetime.fromisoformat(entry_time)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                state.entry_time = dt
            state.entry_price = open_position.get("entry_price")
            state.quantity    = open_position.get("quantity")
            state.armed       = False
        return state
