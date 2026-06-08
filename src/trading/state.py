from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

FLAT = "FLAT"
LONG = "LONG"
SHORT = "SHORT"
COOLDOWN = "COOLDOWN"


@dataclass
class TradingState:
    status: str = FLAT
    armed: bool = True
    cooldown_until: Optional[datetime] = None

    # Open position
    position_id: Optional[str] = None
    side: Optional[str] = None
    entry_time: Optional[datetime] = None
    entry_price: Optional[float] = None
    quantity: Optional[float] = None

    # Run context
    run_id: Optional[str] = None

    # Risk counters (reset daily)
    daily_trade_count: int = 0
    daily_loss_usdt: float = 0.0
    consecutive_errors: int = 0
    last_trade_date: Optional[str] = None

    def hold_minutes(self, now: Optional[datetime] = None) -> float:
        if self.entry_time is None:
            return 0.0
        now = now or datetime.now(timezone.utc)
        return (now - self.entry_time).total_seconds() / 60.0

    def clear_position(self) -> None:
        self.position_id = None
        self.side = None
        self.entry_time = None
        self.entry_price = None
        self.quantity = None

    def record_trade_result(self, pnl_usdt: float) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.last_trade_date != today:
            self.daily_trade_count = 0
            self.daily_loss_usdt = 0.0
            self.last_trade_date = today
        self.daily_trade_count += 1
        if pnl_usdt < 0:
            self.daily_loss_usdt += abs(pnl_usdt)

    @classmethod
    def from_db(cls, run_id: str, open_position: Optional[dict]) -> "TradingState":
        state = cls(run_id=run_id)
        if open_position:
            state.status = open_position["side"]
            state.position_id = open_position["position_id"]
            state.side = open_position["side"]
            entry_time = open_position.get("entry_time")
            if entry_time:
                dt = datetime.fromisoformat(entry_time)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                state.entry_time = dt
            state.entry_price = open_position.get("entry_price")
            state.quantity = open_position.get("quantity")
            state.armed = False
        return state
