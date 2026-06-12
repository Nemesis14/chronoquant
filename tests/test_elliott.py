# =============================================================================
# Elliott Wave system tests
# =============================================================================
# Purpose:
#  - Unit tests for M1 pivot motor, M2 validators, M3 corrective validators
#  - Integration tests for the 1212 scanner and DynamicParser
# =============================================================================

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC  = ROOT / "src"
sys.path.insert(0, str(SRC))

from elliott_waves.elliott.config import ElliottConfig
from elliott_waves.elliott.data import PatternCandidate, Pivot, PivotKind
from elliott_waves.elliott.indicators.atr import atr14
from elliott_waves.elliott.indicators.momentum import ema, ema_slope
from elliott_waves.elliott.parser.candidate_store import CandidateStore
from elliott_waves.elliott.parser.dynamic_parser import DynamicParser
from elliott_waves.elliott.pivots.multi_degree import build_multi_degree
from elliott_waves.elliott.pivots.zigzag import detect_zigzag
from elliott_waves.elliott.scanners.setup_1212 import detect_1212
from elliott_waves.elliott.scanners.wave3 import Wave3Scanner
from elliott_waves.elliott.scoring.ratios import band_score, fib_score, retracement
from elliott_waves.elliott.validators.diagonal import DiagonalValidator
from elliott_waves.elliott.validators.flat import FlatValidator
from elliott_waves.elliott.validators.full_cycle import FullCycleValidator
from elliott_waves.elliott.validators.impulse import ImpulseValidator
from elliott_waves.elliott.validators.triangle import TriangleValidator
from elliott_waves.elliott.validators.zigzag_abc import ZigZagValidator

# =============================================================================
# Helpers
# =============================================================================

def _make_df(closes: list[float], freq: str = "min") -> pd.DataFrame:
    """Build minimal OHLCV DataFrame from close prices."""
    times = pd.date_range("2025-01-01 00:00", periods=len(closes), freq=freq)
    return pd.DataFrame({
        "open_time": times.strftime("%Y-%m-%d %H:%M:%S"),
        "open":      closes,
        "high":      [c * 1.002 for c in closes],
        "low":       [c * 0.998 for c in closes],
        "close":     closes,
        "volume":    [1000.0] * len(closes),
    })


def _make_pivot(idx, price, kind, degree=0, conf_offset=1, atr=0.5):
    conf_idx = idx + conf_offset
    return Pivot(
        idx           = idx,
        ts            = f"2025-01-01 0{idx:01d}:00:00",
        price         = price,
        kind          = kind,
        degree        = degree,
        confirmed_idx = conf_idx,
        confirmed_ts  = f"2025-01-01 0{conf_idx:01d}:00:00",
        atr           = atr,
    )


def _bullish_impulse_pivots():
    """Create a clean bullish impulse: P0 low, P1 high, P2 low, P3 high, P4 low, P5 high."""
    return [
        _make_pivot(0,  100.0, PivotKind.LOW),
        _make_pivot(10, 150.0, PivotKind.HIGH),
        _make_pivot(15, 120.0, PivotKind.LOW),    # W2 retraces 60%
        _make_pivot(30, 190.0, PivotKind.HIGH),   # W3 extends 1.4x W1
        _make_pivot(35, 155.0, PivotKind.LOW),    # W4 retraces 35% of W3
        _make_pivot(45, 210.0, PivotKind.HIGH),   # W5 smaller than W3
    ]


def _bullish_zigzag_pivots():
    """Create a bullish-impulse zigzag correction: Q0 high, Q1 low, Q2 high, Q3 low."""
    return [
        _make_pivot(0, 210.0, PivotKind.HIGH),
        _make_pivot(5, 170.0, PivotKind.LOW),    # A: 40% down
        _make_pivot(8, 195.0, PivotKind.HIGH),   # B: 62.5% retrace of A
        _make_pivot(14, 150.0, PivotKind.LOW),   # C: 112.5% of A (extends beyond A)
    ]


# =============================================================================
# M1 — ATR indicator tests
# =============================================================================

def test_atr14_positive_values():
    df  = _make_df([100.0 + i * 0.5 for i in range(50)])
    atr = atr14(df)
    assert len(atr) == 50
    assert all(a > 0 for a in atr)


def test_atr14_length_matches_df():
    df  = _make_df([100.0] * 20)
    atr = atr14(df)
    assert len(atr) == 20


# =============================================================================
# M1 — EMA / momentum tests
# =============================================================================

def test_ema_same_value_series():
    series = np.array([100.0] * 30)
    result = ema(series, period=14)
    assert all(abs(r - 100.0) < 1e-9 for r in result)


def test_ema_slope_positive_for_rising():
    series = np.array([float(i) for i in range(1, 31)])
    slope  = ema_slope(series, period=5)
    # After initial warmup, slope should be positive for rising series
    assert slope[-1] > 0


# =============================================================================
# M1 — ZigZag pivot tests
# =============================================================================

def test_detect_zigzag_returns_alternating():
    closes = [100.0, 103.0, 110.0, 107.0, 103.8, 106.0, 109.0, 106.5, 105.7, 108.0]
    df     = _make_df(closes)
    cfg    = ElliottConfig(zigzag_threshold=0.02)
    pivots = detect_zigzag(df, cfg)
    # All pivots must alternate HIGH/LOW
    for i in range(1, len(pivots)):
        assert pivots[i].kind != pivots[i - 1].kind, "Pivots must alternate"


def test_detect_zigzag_confirmed_idx_in_bounds():
    closes = [100.0, 103.0, 110.0, 107.0, 104.0, 107.0, 112.0, 108.0, 106.0, 110.0]
    df     = _make_df(closes)
    cfg    = ElliottConfig(zigzag_threshold=0.02)
    pivots = detect_zigzag(df, cfg)
    n      = len(closes)
    for p in pivots:
        assert 0 <= p.idx < n
        assert p.idx <= p.confirmed_idx < n


def test_detect_zigzag_empty_for_short_df():
    df     = _make_df([100.0, 101.0])
    cfg    = ElliottConfig()
    pivots = detect_zigzag(df, cfg)
    assert pivots == []


def test_detect_zigzag_high_before_low():
    """P0 is LOW, P1 is HIGH for rising zigzag."""
    closes = [100.0, 110.0, 104.0, 115.0, 109.0, 120.0]
    df     = _make_df(closes)
    cfg    = ElliottConfig(zigzag_threshold=0.02)
    pivots = detect_zigzag(df, cfg)
    if len(pivots) >= 2:
        assert pivots[0].kind != pivots[1].kind


# =============================================================================
# M1 — Multi-degree pivot tests
# =============================================================================

def test_build_multi_degree_returns_three_levels():
    closes = [100.0 + np.sin(i / 5.0) * 5 for i in range(200)]
    df     = _make_df(closes)
    cfg    = ElliottConfig(zigzag_threshold=0.005)
    result = build_multi_degree(df, cfg, degrees=3)
    assert set(result.keys()) == {0, 1, 2}


def test_build_multi_degree_degree0_more_pivots():
    """Higher degree = fewer pivots (stronger filter)."""
    closes = [100.0 + np.sin(i / 3.0) * 3 for i in range(300)]
    df     = _make_df(closes)
    cfg    = ElliottConfig(zigzag_threshold=0.005)
    result = build_multi_degree(df, cfg, degrees=3)
    assert len(result[0]) >= len(result[1])
    assert len(result[1]) >= len(result[2])


# =============================================================================
# M1 — Config tests
# =============================================================================

def test_config_for_timeframe_1m():
    cfg = ElliottConfig.for_timeframe("1m")
    assert cfg.timeframe == "1m"
    assert cfg.zigzag_threshold == 0.005
    assert cfg.fractal_left == 2


def test_config_for_timeframe_1h():
    cfg = ElliottConfig.for_timeframe("1h")
    assert cfg.timeframe == "1h"
    assert cfg.zigzag_threshold == 0.025


# =============================================================================
# M2 — Scoring tests
# =============================================================================

def test_band_score_exact_ideal():
    score = band_score(0.618, hard=(0.236, 0.854), ideal=[0.5, 0.618], tol=0.10)
    assert score == 1.0


def test_band_score_outside_hard():
    score = band_score(0.10, hard=(0.236, 0.854), ideal=[0.5, 0.618])
    assert score == 0.0


def test_band_score_between():
    score = band_score(0.50, hard=(0.236, 0.854), ideal=[0.5, 0.618], tol=0.10)
    assert score == 1.0


def test_retracement_basic():
    assert retracement(30.0, 50.0) == 0.6
    assert retracement(0.0, 50.0) == 0.0


def test_retracement_zero_base():
    import math
    assert math.isnan(retracement(10.0, 0.0))


def test_fib_score_perfect():
    score = fib_score(0.618, ideal_levels=[0.618, 1.618], tol=0.10)
    assert score == 1.0


def test_fib_score_far():
    score = fib_score(0.0, ideal_levels=[0.618, 1.618], tol=0.10)
    assert score < 0.1


# =============================================================================
# M2 — Impulse validator tests
# =============================================================================

def test_impulse_valid_bullish():
    pivots = _bullish_impulse_pivots()
    cfg    = ElliottConfig(eps_atr=0.01, overlap_atr=0.02, fib_tol=0.15)
    result = ImpulseValidator().validate(pivots, direction=1, cfg=cfg)
    assert result.valid, f"Expected valid impulse, got: {result.reason}"
    assert result.score > 50.0
    assert result.pattern_type == "IMPULSE"


def test_impulse_rejects_wrong_count():
    pivots = _bullish_impulse_pivots()[:4]  # only 4 pivots
    cfg    = ElliottConfig()
    result = ImpulseValidator().validate(pivots, direction=1, cfg=cfg)
    assert not result.valid


def test_impulse_rejects_wave2_100pct():
    """Wave 2 fully retracing Wave 1 should fail."""
    pivots = [
        _make_pivot(0,  100.0, PivotKind.LOW),
        _make_pivot(10, 150.0, PivotKind.HIGH),
        _make_pivot(15,  99.0, PivotKind.LOW),   # retraces past P0
        _make_pivot(25, 170.0, PivotKind.HIGH),
        _make_pivot(30, 140.0, PivotKind.LOW),
        _make_pivot(40, 190.0, PivotKind.HIGH),
    ]
    cfg    = ElliottConfig()
    result = ImpulseValidator().validate(pivots, direction=1, cfg=cfg)
    assert not result.valid


def test_impulse_rejects_wave3_shortest():
    """Wave 3 being the shortest should fail."""
    pivots = [
        _make_pivot(0,  100.0, PivotKind.LOW),
        _make_pivot(10, 150.0, PivotKind.HIGH),  # W1 = 50
        _make_pivot(15, 120.0, PivotKind.LOW),
        _make_pivot(20, 135.0, PivotKind.HIGH),  # W3 = 15 — shorter than W1 and would need W5 also shorter
        _make_pivot(25, 122.0, PivotKind.LOW),
        _make_pivot(35, 175.0, PivotKind.HIGH),  # W5 = 53 — longest, so W3 < W1 AND W3 < W5
    ]
    cfg    = ElliottConfig(shortest_tol=0.03)
    result = ImpulseValidator().validate(pivots, direction=1, cfg=cfg)
    assert not result.valid


def test_impulse_allows_truncation():
    """Truncated Wave 5 should be allowed with allow_truncation=True."""
    pivots = [
        _make_pivot(0,  100.0, PivotKind.LOW),
        _make_pivot(10, 150.0, PivotKind.HIGH),
        _make_pivot(15, 120.0, PivotKind.LOW),
        _make_pivot(25, 190.0, PivotKind.HIGH),  # W3 = 70, extended
        _make_pivot(30, 160.0, PivotKind.LOW),
        _make_pivot(38, 185.0, PivotKind.HIGH),  # W5 = 25, doesn't break P3 (truncated)
    ]
    cfg    = ElliottConfig(allow_truncation=True)
    result = ImpulseValidator().validate(pivots, direction=1, cfg=cfg)
    assert result.valid
    assert result.pattern_type == "IMPULSE_TRUNCATED"
    assert result.score < 100.0


def test_impulse_rejects_truncation_if_disabled():
    pivots = [
        _make_pivot(0,  100.0, PivotKind.LOW),
        _make_pivot(10, 150.0, PivotKind.HIGH),
        _make_pivot(15, 120.0, PivotKind.LOW),
        _make_pivot(25, 190.0, PivotKind.HIGH),
        _make_pivot(30, 160.0, PivotKind.LOW),
        _make_pivot(38, 185.0, PivotKind.HIGH),  # truncated
    ]
    cfg    = ElliottConfig(allow_truncation=False)
    result = ImpulseValidator().validate(pivots, direction=1, cfg=cfg)
    assert not result.valid


# =============================================================================
# M2 — Diagonal validator tests
# =============================================================================

def test_diagonal_valid_ending():
    """Ending diagonal: P4 overlaps P1 territory."""
    pivots = [
        _make_pivot(0,  100.0, PivotKind.LOW),
        _make_pivot(10, 140.0, PivotKind.HIGH),  # P1
        _make_pivot(15, 115.0, PivotKind.LOW),
        _make_pivot(25, 155.0, PivotKind.HIGH),
        _make_pivot(30, 135.0, PivotKind.LOW),   # P4 < P1 — overlap
        _make_pivot(38, 165.0, PivotKind.HIGH),
    ]
    cfg    = ElliottConfig(overlap_atr=0.10)
    result = DiagonalValidator(diagonal_type="ENDING").validate(pivots, direction=1, cfg=cfg)
    assert result.valid
    assert "DIAGONAL" in result.pattern_type


# =============================================================================
# M3 — ZigZag validator tests
# =============================================================================

def test_zigzag_valid():
    pivots = _bullish_zigzag_pivots()
    cfg    = ElliottConfig()
    result = ZigZagValidator().validate(pivots, direction=1, cfg=cfg)
    assert result.valid
    assert result.pattern_type == "ZIGZAG"
    assert result.score > 50.0


def test_zigzag_rejects_b_above_a_start():
    """B must not exceed start of A (Q0)."""
    pivots = [
        _make_pivot(0, 200.0, PivotKind.HIGH),
        _make_pivot(5, 170.0, PivotKind.LOW),
        _make_pivot(8, 210.0, PivotKind.HIGH),  # B > A start (Q0)
        _make_pivot(14, 150.0, PivotKind.LOW),
    ]
    cfg    = ElliottConfig()
    result = ZigZagValidator().validate(pivots, direction=1, cfg=cfg)
    assert not result.valid


def test_zigzag_rejects_c_not_beyond_a():
    """C must exceed A end (Q1 downward)."""
    pivots = [
        _make_pivot(0, 200.0, PivotKind.HIGH),
        _make_pivot(5, 170.0, PivotKind.LOW),
        _make_pivot(8, 190.0, PivotKind.HIGH),
        _make_pivot(14, 172.0, PivotKind.LOW),  # C is above A end (170)
    ]
    cfg    = ElliottConfig(eps_atr=0.0)
    result = ZigZagValidator().validate(pivots, direction=1, cfg=cfg)
    assert not result.valid


def test_zigzag_rejects_wrong_count():
    cfg    = ElliottConfig()
    result = ZigZagValidator().validate(_bullish_zigzag_pivots()[:3], direction=1, cfg=cfg)
    assert not result.valid


# =============================================================================
# M3 — Flat validator tests
# =============================================================================

def test_flat_regular_valid():
    """Regular flat: B retraces ~100% of A, C goes slightly beyond A end."""
    pivots = [
        _make_pivot(0,  200.0, PivotKind.HIGH),
        _make_pivot(5,  160.0, PivotKind.LOW),   # A = 40
        _make_pivot(10, 198.0, PivotKind.HIGH),  # B ~= A start (99% retrace)
        _make_pivot(15, 155.0, PivotKind.LOW),   # C slightly below A end
    ]
    cfg    = ElliottConfig()
    result = FlatValidator().validate(pivots, direction=1, cfg=cfg)
    assert result.valid
    assert result.pattern_type == "REGULAR_FLAT"


def test_flat_expanded_valid():
    """Expanded flat: B exceeds A start, C strongly extends past A end."""
    pivots = [
        _make_pivot(0,  200.0, PivotKind.HIGH),
        _make_pivot(5,  160.0, PivotKind.LOW),   # A = 40
        _make_pivot(10, 210.0, PivotKind.HIGH),  # B > A start (expanded)
        _make_pivot(15, 145.0, PivotKind.LOW),   # C < A end
    ]
    cfg    = ElliottConfig()
    result = FlatValidator().validate(pivots, direction=1, cfg=cfg)
    assert result.valid
    assert result.pattern_type == "EXPANDED_FLAT"


def test_flat_rejects_wrong_count():
    cfg    = ElliottConfig()
    result = FlatValidator().validate(_bullish_zigzag_pivots()[:3], direction=1, cfg=cfg)
    assert not result.valid


# =============================================================================
# M3 — Triangle validator tests
# =============================================================================

def test_triangle_contracting_valid():
    """Contracting triangle: lower lows rise, upper highs fall."""
    pivots = [
        _make_pivot(0,  200.0, PivotKind.HIGH),  # Q0
        _make_pivot(5,  160.0, PivotKind.LOW),   # Q1 A
        _make_pivot(10, 190.0, PivotKind.HIGH),  # Q2 B < Q0
        _make_pivot(15, 168.0, PivotKind.LOW),   # Q3 C > Q1 (lower high rose)
        _make_pivot(20, 183.0, PivotKind.HIGH),  # Q4 D < Q2 (upper fell)
        _make_pivot(25, 173.0, PivotKind.LOW),   # Q5 E > Q3
    ]
    cfg    = ElliottConfig()
    result = TriangleValidator().validate(pivots, direction=1, cfg=cfg)
    assert result.valid
    assert result.pattern_type == "CONTRACTING_TRIANGLE"


def test_triangle_rejects_wrong_count():
    cfg    = ElliottConfig()
    result = TriangleValidator().validate(_bullish_zigzag_pivots(), direction=1, cfg=cfg)
    assert not result.valid


# =============================================================================
# M2/M3 — Full cycle test
# =============================================================================

def test_full_cycle_valid():
    """Build a valid 9-pivot full cycle (impulse + zigzag ABC)."""
    impulse = _bullish_impulse_pivots()
    abc     = [
        _make_pivot(50, 210.0, PivotKind.HIGH),   # P5 (shared)
        _make_pivot(55, 175.0, PivotKind.LOW),    # A
        _make_pivot(60, 200.0, PivotKind.HIGH),   # B
        _make_pivot(65, 155.0, PivotKind.LOW),    # C
    ]
    # Combine: impulse P0-P5 + ABC (share P5)
    combined = impulse + abc[1:]  # 6 + 3 = 9 pivots
    cfg      = ElliottConfig(allow_truncation=True, fib_tol=0.20)
    result   = FullCycleValidator().validate(combined, direction=1, cfg=cfg)
    assert result.valid
    assert result.pattern_type == "FULL_CYCLE"


# =============================================================================
# M4 — CandidateStore tests
# =============================================================================

def test_candidate_store_top_k():
    store = CandidateStore(top_k=3)
    for score in [90.0, 80.0, 70.0, 60.0, 50.0]:
        c = PatternCandidate(
            pattern_type  = "IMPULSE",
            start_idx     = 0,
            end_idx       = 10,
            confirmed_idx = 11,
            pivots        = [],
            direction     = 1,
            degree        = 0,
            hard_pass     = True,
            score         = score,
        )
        store.add(c)

    result = store.get(0, 10, 1, 0)
    assert len(result) == 3
    assert result[0].score == 90.0


def test_candidate_store_all_candidates_sorted():
    store = CandidateStore(top_k=5)
    for i, score in enumerate([70.0, 85.0, 60.0]):
        c = PatternCandidate(
            pattern_type  = "ZIGZAG",
            start_idx     = i,
            end_idx       = i + 5,
            confirmed_idx = i + 6,
            pivots        = [],
            direction     = 1,
            degree        = 0,
            hard_pass     = True,
            score         = score,
        )
        store.add(c)

    all_c  = store.all_candidates()
    scores = [c.score for c in all_c]
    assert scores == sorted(scores, reverse=True)


# =============================================================================
# M4 — 1212 Scanner tests
# =============================================================================

def test_detect_1212_returns_dataframe():
    """1212 scanner returns a DataFrame (possibly empty) for short data."""
    closes = [100.0 + np.sin(i / 5.0) * 5 for i in range(200)]
    df     = _make_df(closes)
    cfg    = ElliottConfig(zigzag_threshold=0.005)
    result = detect_1212(df, cfg)
    assert isinstance(result, pd.DataFrame)


def test_detect_1212_columns():
    """When setups are found, required columns must be present."""
    closes = [100.0, 103.0, 110.0, 107.0, 103.8, 106.0, 109.0, 106.5, 105.7, 108.0,
              112.0, 109.5, 108.0, 110.5, 107.0, 111.5, 108.5, 107.5, 110.0, 113.0]
    df     = _make_df(closes * 10)  # repeat to get enough bars
    cfg    = ElliottConfig(zigzag_threshold=0.01)
    result = detect_1212(df, cfg)
    if not result.empty:
        for col in ["conf_time", "p0", "p1", "p2", "p3", "p4", "r_big", "r_sub", "score"]:
            assert col in result.columns, f"Missing column: {col}"


def test_detect_1212_score_in_range():
    closes = [100.0 + np.sin(i / 4.0) * 8 for i in range(300)]
    df     = _make_df(closes)
    cfg    = ElliottConfig(zigzag_threshold=0.005)
    result = detect_1212(df, cfg)
    if not result.empty:
        assert result["score"].between(0.0, 1.0).all()


# =============================================================================
# M4 — Wave3Scanner tests
# =============================================================================

def test_wave3_scanner_returns_list():
    closes = [100.0, 103.0, 110.0, 107.0, 103.8, 106.0, 109.0, 106.5, 105.7, 115.0,
              112.0, 116.0, 113.0, 117.0, 114.0, 120.0]
    df     = _make_df(closes)
    cfg    = ElliottConfig(zigzag_threshold=0.01, wave3_buffer_atr=0.10)
    pivots = detect_zigzag(df, cfg)
    result = Wave3Scanner().scan(df, cfg, pivots, direction=1)
    assert isinstance(result, list)


def test_wave3_scanner_candidates_have_required_fields():
    closes = [100.0, 103.0, 110.0, 107.0, 103.8, 106.0, 109.0, 106.5, 105.7, 115.0,
              112.0, 116.0, 113.0, 117.0, 114.0, 120.0, 117.0, 121.0]
    df     = _make_df(closes)
    cfg    = ElliottConfig(zigzag_threshold=0.01, wave3_buffer_atr=0.05)
    pivots = detect_zigzag(df, cfg)
    result = Wave3Scanner().scan(df, cfg, pivots, direction=1)
    for c in result:
        assert isinstance(c, PatternCandidate)
        assert c.hard_pass
        assert c.confirmed_idx >= 0
        assert "target_1000_W1" in c.target_zones


# =============================================================================
# M4 — DynamicParser integration tests
# =============================================================================

def test_dynamic_parser_returns_list():
    pivots = _bullish_impulse_pivots()
    # Add a zigzag correction after the impulse
    pivots += [
        _make_pivot(50, 210.0, PivotKind.HIGH),
        _make_pivot(55, 175.0, PivotKind.LOW),
        _make_pivot(60, 198.0, PivotKind.HIGH),
        _make_pivot(65, 152.0, PivotKind.LOW),
    ]
    cfg    = ElliottConfig(min_score=40.0, fib_tol=0.15)
    parser = DynamicParser(pivots, cfg, degree=0)
    result = parser.parse()
    assert isinstance(result, list)


def test_dynamic_parser_scores_in_range():
    pivots = _bullish_impulse_pivots() + [
        _make_pivot(50, 210.0, PivotKind.HIGH),
        _make_pivot(55, 175.0, PivotKind.LOW),
        _make_pivot(60, 198.0, PivotKind.HIGH),
        _make_pivot(65, 152.0, PivotKind.LOW),
    ]
    cfg    = ElliottConfig(min_score=0.0, fib_tol=0.20)
    parser = DynamicParser(pivots, cfg, degree=0)
    result = parser.parse()
    for c in result:
        assert 0.0 <= c.score <= 100.0


# =============================================================================
# M1 — Pivot.y() direction transform
# =============================================================================

def test_pivot_y_bullish():
    p = _make_pivot(0, 150.0, PivotKind.HIGH)
    assert p.y(+1) == 150.0


def test_pivot_y_bearish():
    p = _make_pivot(0, 150.0, PivotKind.HIGH)
    assert p.y(-1) == -150.0


# =============================================================================
# M1 — ElliottConfig defaults
# =============================================================================

def test_config_defaults():
    cfg = ElliottConfig()
    assert cfg.timeframe == "1m"
    assert cfg.zigzag_threshold == 0.010
    assert cfg.fib_tol == 0.10
    assert cfg.allow_truncation is True
