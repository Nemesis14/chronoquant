# =============================================================================
# Elliott Wave validator base classes and helpers
# =============================================================================
# Purpose:
#  - ValidationResult: return type for all validators
#  - PatternValidator: abstract base class
#  - eps_price, overlap_price: ATR-based tolerance helpers
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from elliott_waves.elliott.data import Pivot


@dataclass
class ValidationResult:
    valid:        bool
    pattern_type: str | None         = None
    score:        float              = 0.0
    reason:       str | None        = None
    diagnostics:  dict[str, Any]    = field(default_factory=dict)
    subpatterns:  list[Any]         = field(default_factory=list)

    @staticmethod
    def fail(reason: str) -> ValidationResult:
        return ValidationResult(valid=False, reason=reason)


class PatternValidator:
    pattern_type: str = "UNKNOWN"

    def validate(
        self,
        pivots:    list[Pivot],
        direction: int,
        cfg,
    ) -> ValidationResult:
        raise NotImplementedError


# =============================================================================
# eps_price(pivots, cfg) -> float
# =============================================================================
# Purpose:
#  - ATR-based epsilon for strict comparisons (e.g. P3 > P1 + eps)
# =============================================================================
def eps_price(pivots: list[Pivot], cfg) -> float:
    atr_vals = [p.atr for p in pivots if p.atr > 0]
    avg_atr  = sum(atr_vals) / len(atr_vals) if atr_vals else 0.0
    return max(2.0 * cfg.tick_size, cfg.eps_atr * avg_atr)


# =============================================================================
# overlap_price(pivots, cfg) -> float
# =============================================================================
# Purpose:
#  - ATR-based epsilon for Wave4/Wave1 overlap check
# =============================================================================
def overlap_price(pivots: list[Pivot], cfg) -> float:
    atr_vals = [p.atr for p in pivots if p.atr > 0]
    avg_atr  = sum(atr_vals) / len(atr_vals) if atr_vals else 0.0
    return max(2.0 * cfg.tick_size, cfg.overlap_atr * avg_atr)
