"""Live trading strategy evaluator.

Implements the single-bar strategy decision function used by TradingService.
Mirrors the simulation logic in src/strategy/ backtest exactly so that
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
    state           : TradingState,
    score_pct_long  : float,
    score_pct_short : float,
    decision_params : dict,
    now             : datetime | None = None,
) -> tuple[str, str]:
    """Evaluate one closed bar without mutating state."""
    if now is None:
        now = datetime.now(UTC)

    if state.status == COOLDOWN:
        if state.cooldown_until and now < state.cooldown_until:
            remaining = (state.cooldown_until - now).total_seconds() / 60
            return HOLD, f"cooldown {remaining:.0f}min remaining"

        # Rearm check: cooldown elapsed but service has not yet set FLAT
        rearm_pct = float(decision_params["rearm_pct"])
        if score_pct_long <= rearm_pct and score_pct_short <= rearm_pct:
            return HOLD, "rearm_triggered"

        return HOLD, (
            f"waiting_rearm long_pct={score_pct_long:.3f} short_pct={score_pct_short:.3f}"
        )

    if state.status == FLAT:
        if not state.armed:
            return HOLD, "not_armed"

        long_entry_pct  = float(decision_params["long_entry_pct"])
        short_entry_pct = float(decision_params["short_entry_pct"])
        min_edge_gap    = float(decision_params["min_edge_gap"])

        long_signal  = score_pct_long  >= long_entry_pct
        short_signal = score_pct_short >= short_entry_pct

        if long_signal and short_signal:
            edge_long  = score_pct_long  - score_pct_short
            edge_short = score_pct_short - score_pct_long
            if edge_long > edge_short and edge_long >= min_edge_gap:
                return ENTER_LONG, (
                    f"highest_edge long pct={score_pct_long:.3f} edge={edge_long:.3f}"
                )
            if edge_short > edge_long and edge_short >= min_edge_gap:
                return ENTER_SHORT, (
                    f"highest_edge short pct={score_pct_short:.3f} edge={edge_short:.3f}"
                )
            return HOLD, (
                f"edge_gap_insufficient long={score_pct_long:.3f} short={score_pct_short:.3f}"
            )

        if long_signal:
            return ENTER_LONG, f"long_entry pct={score_pct_long:.3f}>={long_entry_pct:.2f}"

        if short_signal:
            return ENTER_SHORT, f"short_entry pct={score_pct_short:.3f}>={short_entry_pct:.2f}"

        return HOLD, f"below_threshold long={score_pct_long:.3f} short={score_pct_short:.3f}"

    if state.status == LONG:
        hold_min        = state.hold_minutes(now)
        max_hold        = int(decision_params["max_hold_minutes"])
        min_hold        = int(decision_params["min_hold_minutes"])
        short_entry_pct = float(decision_params["short_entry_pct"])
        rearm_pct       = float(decision_params["rearm_pct"])

        if hold_min >= max_hold:
            return EXIT_LONG, f"max_hold {hold_min:.0f}min"

        if hold_min >= min_hold and score_pct_short >= short_entry_pct:
            return EXIT_LONG, f"opposite_edge short_pct={score_pct_short:.3f}"

        if hold_min >= min_hold and score_pct_long < rearm_pct:
            return EXIT_LONG, f"signal_decay long_pct={score_pct_long:.3f}<{rearm_pct:.2f}"

        return HOLD, f"holding {hold_min:.0f}min long_pct={score_pct_long:.3f}"

    if state.status == SHORT:
        hold_min        = state.hold_minutes(now)
        max_hold        = int(decision_params["max_hold_minutes"])
        min_hold        = int(decision_params["min_hold_minutes"])
        long_entry_pct  = float(decision_params["long_entry_pct"])
        rearm_pct       = float(decision_params["rearm_pct"])

        if hold_min >= max_hold:
            return EXIT_SHORT, f"max_hold {hold_min:.0f}min"

        if hold_min >= min_hold and score_pct_long >= long_entry_pct:
            return EXIT_SHORT, f"opposite_edge long_pct={score_pct_long:.3f}"

        if hold_min >= min_hold and score_pct_short < rearm_pct:
            return EXIT_SHORT, f"signal_decay short_pct={score_pct_short:.3f}<{rearm_pct:.2f}"

        return HOLD, f"holding {hold_min:.0f}min short_pct={score_pct_short:.3f}"

    return HOLD, f"unknown_status={state.status}"
