from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

FLAT = "FLAT"
LONG = "LONG"
SHORT = "SHORT"
COOLDOWN = "COOLDOWN"


@dataclass
class TradingState:
    status: str = FLAT
    armed: bool = True
    cooldown_until: datetime | None = None

    # Open position
    position_id: str | None = None
    side: str | None = None
    entry_time: datetime | None = None
    entry_price: float | None = None
    quantity: float | None = None

    # Run context
    run_id: str | None = None

    # Risk counters (reset daily)
    daily_trade_count: int = 0
    daily_loss_usdt: float = 0.0
    consecutive_errors: int = 0
    last_trade_date: str | None = None

    def hold_minutes(self, now: datetime | None = None) -> float:
        if self.entry_time is None:
            return 0.0
        now = now or datetime.now(UTC)
        return (now - self.entry_time).total_seconds() / 60.0

    def clear_position(self) -> None:
        self.position_id = None
        self.side = None
        self.entry_time = None
        self.entry_price = None
        self.quantity = None

    def record_trade_result(self, pnl_usdt: float) -> None:
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        if self.last_trade_date != today:
            self.daily_trade_count = 0
            self.daily_loss_usdt = 0.0
            self.last_trade_date = today
        self.daily_trade_count += 1
        if pnl_usdt < 0:
            self.daily_loss_usdt += abs(pnl_usdt)

    @classmethod
    def from_db(cls, run_id: str, open_position: dict | None) -> TradingState:
        state = cls(run_id=run_id)
        if open_position:
            state.status = open_position["side"]
            state.position_id = open_position["position_id"]
            state.side = open_position["side"]
            entry_time = open_position.get("entry_time")
            if entry_time:
                dt = datetime.fromisoformat(entry_time)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                state.entry_time = dt
            state.entry_price = open_position.get("entry_price")
            state.quantity = open_position.get("quantity")
            state.armed = False
        return state
