# =============================================================================
# DynamicParser — sliding-window Elliott pattern parser
# =============================================================================
# Purpose:
#  - Scan all pivot windows of configurable width for pattern candidates
#  - Limits combinatorial explosion with max_window_pivots
#  - Stores results in CandidateStore (top-K per interval)
#  - Returns candidates above min_score sorted by score descending
# =============================================================================

from __future__ import annotations

from elliott.elliott.config import ElliottConfig
from elliott.elliott.data import PatternCandidate, Pivot
from elliott.elliott.parser.candidate_store import CandidateStore
from elliott.elliott.validators.base import ValidationResult
from elliott.elliott.validators.combination import CombinationValidator
from elliott.elliott.validators.diagonal import DiagonalValidator
from elliott.elliott.validators.double_zigzag import DoubleZigZagValidator
from elliott.elliott.validators.flat import FlatValidator
from elliott.elliott.validators.full_cycle import FullCycleValidator
from elliott.elliott.validators.impulse import ImpulseValidator
from elliott.elliott.validators.triangle import TriangleValidator
from elliott.elliott.validators.zigzag_abc import ZigZagValidator

_VALIDATORS = [
    ImpulseValidator(),
    DiagonalValidator(),
    ZigZagValidator(),
    FlatValidator(),
    TriangleValidator(),
    DoubleZigZagValidator(),
    CombinationValidator(),
    FullCycleValidator(),
]

# Required pivot counts per validator class name
_REQUIRED_PIVOTS = {
    "ImpulseValidator":       6,
    "DiagonalValidator":      6,
    "ZigZagValidator":        4,
    "FlatValidator":          4,
    "TriangleValidator":      6,
    "DoubleZigZagValidator":  None,  # variable
    "CombinationValidator":   None,  # variable
    "FullCycleValidator":     None,  # variable, min 9
}


def _result_to_candidate(
    result:    ValidationResult,
    pivots:    list[Pivot],
    direction: int,
    degree:    int,
) -> PatternCandidate:
    confirmed_idx = max(p.confirmed_idx for p in pivots)
    return PatternCandidate(
        pattern_type        = result.pattern_type or "UNKNOWN",
        start_idx           = pivots[0].idx,
        end_idx             = pivots[-1].idx,
        confirmed_idx       = confirmed_idx,
        pivots              = list(pivots),
        direction           = direction,
        degree              = degree,
        hard_pass           = result.valid,
        score               = result.score,
        subpatterns         = [],
        invalidation_level  = None,
        target_zones        = {},
        diagnostics         = result.diagnostics,
    )


class DynamicParser:
    # ==========================================================================
    # DynamicParser(pivots, cfg, degree) -> list[PatternCandidate]
    # ==========================================================================
    # Purpose:
    #  - Run sliding-window parsing over all pivot sub-sequences
    #  - Returns candidates above cfg.min_score sorted by score descending
    # ==========================================================================
    def __init__(
        self,
        pivots: list[Pivot],
        cfg:    ElliottConfig,
        degree: int = 0,
    ) -> None:
        self.pivots = pivots
        self.cfg    = cfg
        self.degree = degree
        self.store  = CandidateStore(top_k=cfg.top_k_per_interval)

    def parse(self) -> list[PatternCandidate]:
        """Run full sliding-window parse; return candidates >= min_score."""
        pivots = self.pivots
        cfg    = self.cfg
        n      = len(pivots)

        for validator in _VALIDATORS:
            v_name = type(validator).__name__
            req    = _REQUIRED_PIVOTS.get(v_name)

            if req is not None:
                # Fixed-width: only scan windows of exactly req pivots
                widths = [req]
            else:
                # Variable-width: scan windows from min to max_window_pivots
                min_w = 7
                max_w = min(n, cfg.max_window_pivots)
                widths = list(range(min_w, max_w + 1))

            for width in widths:
                if width > n:
                    break
                for i in range(0, n - width + 1):
                    window = pivots[i : i + width]

                    for direction in (+1, -1):
                        result = validator.validate(window, direction, cfg)
                        if result.valid and result.score >= cfg.min_score:
                            candidate = _result_to_candidate(
                                result, window, direction, self.degree
                            )
                            self.store.add(candidate)

        return [
            c for c in self.store.all_candidates()
            if c.score >= cfg.min_score
        ]
