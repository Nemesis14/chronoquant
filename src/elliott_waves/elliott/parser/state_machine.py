# =============================================================================
# OnlineStateMachine — real-time Elliott Wave state tracker
# =============================================================================
# Purpose:
#  - Process 1m bars one at a time to track wave formation live
#  - States: IDLE → WAVE2 → WAVE3_ACTIVE → WAVE3_DONE → WAVE4 → WAVE5 → ABC
#  - Emits PatternCandidate on state transitions via on_pattern callback
#  - Uses confirmed_idx as the lookahead-free barrier
# =============================================================================

from __future__ import annotations

from collections.abc import Callable
from enum import Enum

from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import PatternCandidate, Pivot, PivotKind


class SMState(str, Enum):
    IDLE             = "IDLE"
    WAVE2            = "WAVE2"
    WAVE3_ACTIVE     = "WAVE3_ACTIVE"
    WAVE3_DONE       = "WAVE3_DONE"
    WAVE4            = "WAVE4"
    WAVE5_ACTIVE     = "WAVE5_ACTIVE"
    ABC              = "ABC"
    FULL_CYCLE       = "FULL_CYCLE"


class OnlineStateMachine:
    # ==========================================================================
    # OnlineStateMachine(cfg, direction) -> None
    # ==========================================================================
    # Purpose:
    #  - direction: +1 bullish, -1 bearish
    #  - on_pattern callback: called when a pattern candidate is emitted
    # ==========================================================================
    def __init__(
        self,
        cfg:       ElliottConfig,
        direction: int = 1,
    ) -> None:
        self.cfg           = cfg
        self.direction     = direction
        self.state         = SMState.IDLE
        self.pivots:       list[Pivot] = []
        self._callbacks:   list[Callable[[PatternCandidate], None]] = []
        self._bar_idx      = 0

    def on_pattern(self, callback: Callable[[PatternCandidate], None]) -> None:
        """Register a callback for emitted pattern candidates."""
        self._callbacks.append(callback)

    def _emit(self, candidate: PatternCandidate) -> None:
        for cb in self._callbacks:
            cb(candidate)

    def _y(self, price: float) -> float:
        return self.direction * price

    def reset(self) -> None:
        self.state  = SMState.IDLE
        self.pivots = []

    # ==========================================================================
    # update(pivot) -> None
    # ==========================================================================
    # Purpose:
    #  - Feed a newly confirmed pivot to the state machine
    #  - State transitions happen on confirmed pivots only
    # ==========================================================================
    def update(self, pivot: Pivot) -> None:
        self._bar_idx = pivot.confirmed_idx
        self.pivots.append(pivot)
        self._transition()

    def _transition(self) -> None:
        cfg = self.cfg
        ps  = self.pivots
        d   = self.direction

        if self.state == SMState.IDLE:
            # -------------------------------------------------------------------------
            # Look for valid P0 (low) + P1 (high) forming Wave 1
            # -------------------------------------------------------------------------
            if len(ps) >= 2:
                p0, p1 = ps[-2], ps[-1]
                if (
                    p0.kind == (PivotKind.LOW if d > 0 else PivotKind.HIGH)
                    and p1.kind == (PivotKind.HIGH if d > 0 else PivotKind.LOW)
                    and self._y(p1.price) > self._y(p0.price)
                ):
                    self.state = SMState.WAVE2

        elif self.state == SMState.WAVE2:
            # -------------------------------------------------------------------------
            # Look for P2 (retrace < 100% of Wave 1, in Fibonacci zone)
            # -------------------------------------------------------------------------
            if len(ps) >= 3:
                p0, p1, p2 = ps[-3], ps[-2], ps[-1]
                W1 = self._y(p1.price) - self._y(p0.price)
                W2 = self._y(p1.price) - self._y(p2.price)
                if W1 > 0 and 0 < W2 < W1:
                    # P2 is a valid Wave 2 pivot
                    self.state = SMState.WAVE3_ACTIVE
                elif self._y(p2.price) <= self._y(p0.price):
                    # P2 broke P0 — invalid, reset
                    self.reset()

        elif self.state == SMState.WAVE3_DONE:
            # -------------------------------------------------------------------------
            # Look for P4 (Wave 4 completion) — simpler check
            # -------------------------------------------------------------------------
            if len(ps) >= 5:
                p1, p2, p3, p4 = ps[-4], ps[-3], ps[-2], ps[-1]
                if (
                    self._y(p4.price) > self._y(p2.price)           # Wave 4 doesn't retrace all W3
                    and self._y(p4.price) > self._y(p1.price)        # Wave 4 doesn't overlap W1
                ):
                    self.state = SMState.WAVE4
                elif self._y(p4.price) <= self._y(p2.price):
                    # Deep retrace — restart from p3 as possible new p0
                    self.reset()

        elif self.state == SMState.WAVE4:
            # -------------------------------------------------------------------------
            # Wave 5 trigger: new pivot above P3
            # -------------------------------------------------------------------------
            if len(ps) >= 6:
                p3, p5 = ps[-3], ps[-1]
                if self._y(p5.price) > self._y(p3.price):
                    self.state = SMState.WAVE5_ACTIVE
                    self._emit_wave5_candidate()

        elif self.state == SMState.WAVE5_ACTIVE:
            # -------------------------------------------------------------------------
            # ABC correction starts
            # -------------------------------------------------------------------------
            if len(ps) >= 7:
                p5, pa = ps[-2], ps[-1]
                if self._y(pa.price) < self._y(p5.price):
                    self.state = SMState.ABC

        elif self.state == SMState.ABC:
            # -------------------------------------------------------------------------
            # Look for full ABC
            # -------------------------------------------------------------------------
            if len(ps) >= 9:
                self._emit_full_cycle()
                self.state = SMState.FULL_CYCLE
                self.reset()

    def _emit_wave5_candidate(self) -> None:
        ps = self.pivots
        if len(ps) < 6:
            return
        p0, p1, p2, p3, p4, p5 = ps[-6], ps[-5], ps[-4], ps[-3], ps[-2], ps[-1]
        W1 = abs(self._y(p1.price) - self._y(p0.price))
        target = self._y(p4.price) + W1
        candidate = PatternCandidate(
            pattern_type       = "WAVE5_SETUP",
            start_idx          = p0.idx,
            end_idx            = p5.idx,
            confirmed_idx      = p5.confirmed_idx,
            pivots             = list(ps[-6:]),
            direction          = self.direction,
            degree             = ps[-1].degree,
            hard_pass          = True,
            score              = 65.0,
            invalidation_level = self._y(p4.price) / self.direction,
            target_zones       = {
                "target_0618_W1": (self._y(p4.price) + 0.618 * W1) / self.direction,
                "target_1000_W1": target / self.direction,
                "target_1618_W1": (self._y(p4.price) + 1.618 * W1) / self.direction,
            },
            diagnostics = {"state": SMState.WAVE5_ACTIVE.value},
        )
        self._emit(candidate)

    def _emit_full_cycle(self) -> None:
        ps = self.pivots
        if len(ps) < 9:
            return
        candidate = PatternCandidate(
            pattern_type       = "FULL_CYCLE_ONLINE",
            start_idx          = ps[0].idx,
            end_idx            = ps[-1].idx,
            confirmed_idx      = ps[-1].confirmed_idx,
            pivots             = list(ps),
            direction          = self.direction,
            degree             = ps[-1].degree,
            hard_pass          = True,
            score              = 60.0,
            invalidation_level = None,
            target_zones       = {},
            diagnostics        = {"state": SMState.FULL_CYCLE.value},
        )
        self._emit(candidate)

    # ==========================================================================
    # update_bar(bar_idx, close, high, low, pivots_so_far) -> None
    # ==========================================================================
    # Purpose:
    #  - Feed current bar data to check Wave3 trigger without waiting for pivot
    # ==========================================================================
    def check_wave3_trigger(
        self,
        bar_idx: int,
        close:   float,
        atr:     float,
    ) -> bool:
        """
        Returns True if Wave 3 trigger fires: close > P1 + buffer.
        Must be in WAVE3_ACTIVE state with at least P0, P1, P2 in pivots.
        """
        if self.state != SMState.WAVE3_ACTIVE:
            return False
        if len(self.pivots) < 3:
            return False
        p1 = self.pivots[-2]  # P1 = Wave 1 top
        buffer = max(self.cfg.wave3_buffer_atr * atr, 2.0 * self.cfg.tick_size)
        if self.direction > 0:
            return close > p1.price + buffer
        else:
            return close < p1.price - buffer

    @property
    def current_state(self) -> SMState:
        return self.state
