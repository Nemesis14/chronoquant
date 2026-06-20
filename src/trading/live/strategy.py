"""Live trading strategy evaluator.

Implements the single-bar strategy decision function used by TradingService.
Mirrors the simulation logic in trading.calibration.backtest exactly so that
live behaviour matches calibrated backtest expectations.
"""

from __future__ import annotations

from datetime import UTC, datetime

from trading.live.state import COOLDOWN, FLAT, LONG, SHORT, TradingState

HOLD        = "HOLD"
ENTER_LONG  = "ENTER_LONG"
ENTER_SHORT = "ENTER_SHORT"
EXIT_LONG   = "EXIT_LONG"
EXIT_SHORT  = "EXIT_SHORT"


# %% Strategy evaluation


def evaluate(
    state      : TradingState,
    pred_long  : float,
    pred_short : float,
    long_cfg   : dict,
    short_cfg  : dict,
    now        : datetime | None = None,
) -> tuple[str, str]:
    """Evaluate strategy for one closed bar and return a decision.

    Does NOT mutate state — the caller applies the result.

    Args:
        state      : Current TradingState.
        pred_long  : Latest long-model prediction probability.
        pred_short : Latest short-model prediction probability.
        long_cfg   : Long strategy parameter dict (entry_threshold etc.).
        short_cfg  : Short strategy parameter dict.
        now        : Reference time (UTC). Defaults to current UTC time.

    Returns:
        Tuple of (decision, reason) where decision is one of:
        HOLD, ENTER_LONG, ENTER_SHORT, EXIT_LONG, EXIT_SHORT.
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
