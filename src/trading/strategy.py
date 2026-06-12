from __future__ import annotations

from datetime import UTC, datetime

from trading.state import COOLDOWN, FLAT, LONG, SHORT, TradingState

HOLD = "HOLD"
ENTER_LONG = "ENTER_LONG"
ENTER_SHORT = "ENTER_SHORT"
EXIT_LONG = "EXIT_LONG"
EXIT_SHORT = "EXIT_SHORT"


def evaluate(
    state: TradingState,
    pred_long: float,
    pred_short: float,
    long_cfg: dict,
    short_cfg: dict,
    now: datetime | None = None,
) -> tuple[str, str]:
    """
    Evaluate strategy for one closed bar. Mirrors simulate_long/short_probability_strategy
    from src/evaluation/backtest.py exactly.

    Returns (decision, reason).
    Does NOT mutate state — caller applies the result.
    """
    if now is None:
        now = datetime.now(UTC)

    if state.status == COOLDOWN:
        if state.cooldown_until and now < state.cooldown_until:
            remaining = (state.cooldown_until - now).total_seconds() / 60
            return HOLD, f"cooldown {remaining:.0f}min remaining"

        # Cooldown elapsed — check rearm (both models must cool below their threshold)
        if (pred_long <= long_cfg["rearm_threshold"]
                and pred_short <= short_cfg["rearm_threshold"]):
            return HOLD, "rearm_triggered"

        return HOLD, f"waiting_rearm long={pred_long:.3f} short={pred_short:.3f}"

    if state.status == FLAT:
        if not state.armed:
            return HOLD, "not_armed"

        # Long has priority when both trigger simultaneously
        if pred_long >= long_cfg["entry_threshold"]:
            return ENTER_LONG, f"pred_long={pred_long:.3f}>={long_cfg['entry_threshold']}"

        if pred_short >= short_cfg["entry_threshold"]:
            return ENTER_SHORT, f"pred_short={pred_short:.3f}>={short_cfg['entry_threshold']}"

        return HOLD, f"below_threshold long={pred_long:.3f} short={pred_short:.3f}"

    if state.status == LONG:
        hold_min = state.hold_minutes(now)

        if hold_min >= long_cfg["max_hold_minutes"]:
            return EXIT_LONG, f"max_hold {hold_min:.0f}min"

        if pred_short >= short_cfg["entry_threshold"]:
            return EXIT_LONG, f"opposite_signal pred_short={pred_short:.3f}"

        if (hold_min >= long_cfg["min_hold_minutes"]
                and pred_long <= long_cfg["exit_threshold"]):
            return EXIT_LONG, f"probability_exit pred_long={pred_long:.3f}"

        return HOLD, f"holding hold={hold_min:.0f}min pred_long={pred_long:.3f}"

    if state.status == SHORT:
        hold_min = state.hold_minutes(now)

        if hold_min >= short_cfg["max_hold_minutes"]:
            return EXIT_SHORT, f"max_hold {hold_min:.0f}min"

        if pred_long >= long_cfg["entry_threshold"]:
            return EXIT_SHORT, f"opposite_signal pred_long={pred_long:.3f}"

        if (hold_min >= short_cfg["min_hold_minutes"]
                and pred_short <= short_cfg["exit_threshold"]):
            return EXIT_SHORT, f"probability_exit pred_short={pred_short:.3f}"

        return HOLD, f"holding hold={hold_min:.0f}min pred_short={pred_short:.3f}"

    return HOLD, f"unknown_status={state.status}"
