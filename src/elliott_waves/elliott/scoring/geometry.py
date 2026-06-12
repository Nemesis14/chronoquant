# =============================================================================
# Geometric scoring helpers (channel lines, alternation, wedge)
# =============================================================================
# Purpose:
#  - channel_score: how well Wave4/5 fits the parallel channel from W2/W3
#  - alternation_score: penalty if Wave2 and Wave4 are the same type
#  - wedge_geometry_score: how well the wedge converges/diverges for diagonals
# =============================================================================

from __future__ import annotations

from elliott_waves.elliott.data import Pivot


def _line_y_at(p1: Pivot, p2: Pivot, direction: int, target_idx: int) -> float:
    """Linear interpolation: y value at target_idx on line through p1, p2."""
    x1, x2 = p1.idx, p2.idx
    y1, y2 = p1.y(direction), p2.y(direction)
    if x2 == x1:
        return y1
    return y1 + (y2 - y1) * (target_idx - x1) / (x2 - x1)


# =============================================================================
# channel_score(pivots, direction, cfg) -> float [0, 1]
# =============================================================================
# Purpose:
#  - Impulse channel: connect W1 end (P1) and W3 end (P3), draw parallel
#    from W2 end (P2) — this should contain W4
#  - Second channel: connect W2 (P2) and W4 (P4), parallel from W3 (P3)
#    — W5 should touch this upper channel
# Parameters:
#  - pivots: 6 pivots [P0..P5] already direction-transformed
#  - direction: +1 bullish, -1 bearish
#  - cfg: ElliottConfig (not directly used but kept for API consistency)
# =============================================================================
def channel_score(
    pivots:    list[Pivot],
    direction: int,
    cfg,
) -> float:
    if len(pivots) < 6:
        return 0.5

    P0, P1, P2, P3, P4, P5 = pivots[:6]

    # -------------------------------------------------------------------------
    # Wave 4 in channel (P2 parallel to P1-P3 line)
    # -------------------------------------------------------------------------
    p2_y = P2.y(direction)
    p4_y = P4.y(direction)

    channel_at_p4 = _line_y_at(P1, P3, direction, P4.idx) - (P1.y(direction) - p2_y)
    w4_dev = abs(p4_y - channel_at_p4)
    w3_len = abs(P3.y(direction) - P2.y(direction))
    ch4_score = max(0.0, 1.0 - w4_dev / w3_len) if w3_len > 0 else 0.5

    # -------------------------------------------------------------------------
    # Wave 5 in channel (P3 parallel to P2-P4 line)
    # -------------------------------------------------------------------------
    p5_y = P5.y(direction)
    p3_y = P3.y(direction)

    channel_at_p5 = _line_y_at(P2, P4, direction, P5.idx) + (p3_y - P2.y(direction))
    w5_dev = abs(p5_y - channel_at_p5)
    w5_len = abs(P5.y(direction) - P4.y(direction))
    ch5_score = max(0.0, 1.0 - w5_dev / w5_len) if w5_len > 0 else 0.5

    return (ch4_score + ch5_score) / 2.0


# =============================================================================
# alternation_score(pivots, direction, cfg) -> float [0, 1]
# =============================================================================
# Purpose:
#  - Wave 2 and Wave 4 should alternate in character (sharp vs sideways)
#  - Proxy: compare their retrace depths (different = good alternation)
# =============================================================================
def alternation_score(
    pivots:    list[Pivot],
    direction: int,
    cfg,
) -> float:
    if len(pivots) < 5:
        return 0.5

    P0, P1, P2, P3, P4 = pivots[:5]
    W1 = abs(P1.y(direction) - P0.y(direction))
    W3 = abs(P3.y(direction) - P2.y(direction))
    W2 = abs(P1.y(direction) - P2.y(direction))
    W4 = abs(P3.y(direction) - P4.y(direction))

    if W1 <= 0 or W3 <= 0:
        return 0.5

    r2 = W2 / W1
    r4 = W4 / W3

    # Good alternation: retrace depths differ significantly
    diff = abs(r2 - r4)
    return min(1.0, diff / 0.30)


# =============================================================================
# wedge_geometry_score(pivots, direction, cfg) -> float [0, 1]
# =============================================================================
# Purpose:
#  - Score how well the pattern forms a converging wedge (for diagonals)
#  - Contracting: upper and lower trendlines converge at P5
# =============================================================================
def wedge_geometry_score(
    pivots:    list[Pivot],
    direction: int,
    cfg,
) -> float:
    if len(pivots) < 6:
        return 0.0

    P0, P1, P2, P3, P4, P5 = pivots[:6]

    upper_slope = (P3.y(direction) - P1.y(direction)) / max(1, P3.idx - P1.idx)
    lower_slope = (P4.y(direction) - P2.y(direction)) / max(1, P4.idx - P2.idx)

    # Contracting wedge: upper slope < lower slope in transformed coordinates
    # (upper rising less steeply than lower)
    if upper_slope > lower_slope:
        return 0.0

    # Score proportional to convergence rate
    spread_p1 = P1.y(direction) - P0.y(direction)
    spread_p5 = P5.y(direction) - P4.y(direction)

    if spread_p1 <= 0:
        return 0.5

    contraction = 1.0 - spread_p5 / spread_p1
    return max(0.0, min(1.0, contraction))
