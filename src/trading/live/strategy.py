"""Live trading strategy evaluator.

Implements the single-bar strategy decision function used by TradingService.
Mirrors the simulation logic in src/strategy/ backtest exactly so that
live behaviour matches calibrated backtest expectations.

State machine: FLAT → LONG/SHORT → FLAT.

Short ranking note: short_mfe_fw60 = log(fw_min/close) < 0, always negative.
A lower score_pct_short means a better short signal.
Entry condition for short: (1.0 - score_pct_short) >= entry_cutoff.
"""

from __future__ import annotations

from datetime import UTC, datetime

from trading.live.state import FLAT, LONG, SHORT, TradingState

HOLD        = "HOLD"
ENTER_LONG  = "ENTER_LONG"
ENTER_SHORT = "ENTER_SHORT"
EXIT_LONG   = "EXIT_LONG"
EXIT_SHORT  = "EXIT_SHORT"


# %% Strategy evaluation


def evaluate(
    state                : TradingState,
    score_pct_long       : float,
    score_pct_short      : float,
    decision_params      : dict,
    now                  : datetime | None = None,
    entry_cutoff_long    : float | None = None,
    entry_cutoff_short   : float | None = None,
) -> tuple[str, str]:
    """Evaluate one closed bar without mutating state."""
    if now is None:
        now = datetime.now(UTC)

    _base_cutoff     = float(decision_params["entry_cutoff"])
    entry_cutoff_l   = entry_cutoff_long  if entry_cutoff_long  is not None else _base_cutoff
    entry_cutoff_s   = entry_cutoff_short if entry_cutoff_short is not None else _base_cutoff
    max_hold_minutes = int(decision_params.get("max_hold_minutes", 60))

    entry_cutoff = _base_cutoff  # kept for reason strings

    if state.status == FLAT:
        long_signal  = score_pct_long >= entry_cutoff_l
        short_signal = (1.0 - score_pct_short) >= entry_cutoff_s

        if long_signal and short_signal:
            # Long has priority when both trigger
            return ENTER_LONG, (
                f"long_priority long_pct={score_pct_long:.3f}>={entry_cutoff_l:.2f}"
                f" short_inv={(1.0 - score_pct_short):.3f}>={entry_cutoff_s:.2f}"
            )

        if long_signal:
            return ENTER_LONG, (
                f"long_entry pct={score_pct_long:.3f}>={entry_cutoff_l:.2f}"
            )

        if short_signal:
            return ENTER_SHORT, (
                f"short_entry inv_pct={(1.0 - score_pct_short):.3f}>={entry_cutoff_s:.2f}"
            )

        return HOLD, (
            f"below_cutoff long={score_pct_long:.3f}<{entry_cutoff_l:.2f}"
            f" short_inv={(1.0 - score_pct_short):.3f}<{entry_cutoff_s:.2f}"
        )

    if state.status == LONG:
        hold_min = state.hold_minutes(now)
        if hold_min >= max_hold_minutes:
            return EXIT_LONG, f"max_hold {hold_min:.0f}min"
        return HOLD, f"holding {hold_min:.0f}min long_pct={score_pct_long:.3f}"

    if state.status == SHORT:
        hold_min = state.hold_minutes(now)
        if hold_min >= max_hold_minutes:
            return EXIT_SHORT, f"max_hold {hold_min:.0f}min"
        return HOLD, (
            f"holding {hold_min:.0f}min short_inv={(1.0 - score_pct_short):.3f}"
        )

    return HOLD, f"unknown_status={state.status}"
