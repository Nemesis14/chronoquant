# =============================================================================
# Pivot utility helpers
# =============================================================================

from __future__ import annotations

from modeling.elliott.elliott.data import Pivot, PivotKind


def compress_alternating(pivots: list[Pivot]) -> list[Pivot]:
    """
    Enforce strictly alternating HIGH/LOW sequence.
    On consecutive same-kind pivots keeps the more extreme one.
    """
    if not pivots:
        return []
    result = [pivots[0]]
    for p in pivots[1:]:
        last = result[-1]
        if p.kind == last.kind:
            if p.kind == PivotKind.HIGH:
                if p.price > last.price:
                    result[-1] = p
            else:
                if p.price < last.price:
                    result[-1] = p
        else:
            result.append(p)
    return result
