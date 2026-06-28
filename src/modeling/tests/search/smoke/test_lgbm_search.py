"""Smoke tests for the refactored lgbm_search module (t3).

Covers:
  - _check_patience() returns the correct bool for various trial histories
  - _select_best_trial() picks the best from a mini trial list (valid max + gap tiebreaker)
  - Both functions are importable without a real DB or model
"""

from __future__ import annotations

import pytest

from modeling.search.lgbm_search import _check_patience, _select_best_trial

pytestmark = pytest.mark.smoke


# ---------------------------------------------------------------------------
# _check_patience — smoke
# ---------------------------------------------------------------------------


def test_check_patience_returns_false_below_patience_threshold() -> None:
    """With fewer trials than patience, stopping should never be triggered."""
    trials = [{"valid_top10_lift": float(i) * 0.001} for i in range(10)]
    assert _check_patience(trials, patience=20, epsilon=0.001) is False


def test_check_patience_returns_false_when_improvement_present() -> None:
    """Patience not triggered when recent trials contain a meaningful improvement."""
    # 20 old trials with low lift, 1 recent high lift
    old    = [{"valid_top10_lift": 0.001}] * 20
    recent = [{"valid_top10_lift": 0.001}] * 19 + [{"valid_top10_lift": 0.010}]
    trials = old + recent
    assert _check_patience(trials, patience=20, epsilon=0.001) is False


def test_check_patience_returns_true_when_no_improvement() -> None:
    """Patience triggered when recent 20 trials don't improve best by epsilon."""
    # Best before is 0.005; recent 20 are all at 0.005 (delta = 0 < epsilon=0.001)
    old    = [{"valid_top10_lift": 0.005}] * 5
    recent = [{"valid_top10_lift": 0.005}] * 20
    trials = old + recent
    assert _check_patience(trials, patience=20, epsilon=0.001) is True


def test_check_patience_returns_false_for_empty_trials() -> None:
    assert _check_patience([], patience=20, epsilon=0.001) is False


def test_check_patience_handles_none_lift_without_error() -> None:
    """Trials with explicit None valid_top10_lift must not raise TypeError.

    None values are treated as -inf.  -inf - (-inf) = nan; nan < epsilon is False,
    so patience is NOT triggered — a conservative safe default.
    """
    trials = [{"valid_top10_lift": None}] * 25
    # Must not raise TypeError (regression guard for the None-handling fix)
    result = _check_patience(trials, patience=20, epsilon=0.001)
    assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# _select_best_trial — smoke
# ---------------------------------------------------------------------------


def test_select_best_trial_returns_none_for_empty_list() -> None:
    assert _select_best_trial([]) is None


def test_select_best_trial_returns_none_when_no_valid_lift() -> None:
    trials = [{"params": {}, "train_top10_lift": 0.01}]
    assert _select_best_trial(trials) is None


def test_select_best_trial_top5_pool_gap_tiebreaker() -> None:
    """Best trial is picked from the top-5 valid_top10_lift pool by smallest gap.

    With 3 trials where each has a unique gap, the trial with the best valid lift
    AND the smallest gap among the top-5 pool should win.
    """
    trials = [
        {"valid_top10_lift": 0.002, "train_valid_gap": 0.001, "params": {}},
        {"valid_top10_lift": 0.005, "train_valid_gap": 0.002, "params": {}},
        {"valid_top10_lift": 0.003, "train_valid_gap": 0.003, "params": {}},
    ]
    # Top-5 pool (all 3 trials, sorted by valid_top10_lift desc): [0.005, 0.003, 0.002]
    # Gaps are:                                                      [0.002,  0.003, 0.001]
    # Smallest gap is 0.001 → trial with valid_top10_lift=0.002 wins
    best = _select_best_trial(trials)
    assert best is not None
    assert best["train_valid_gap"] == pytest.approx(0.001)
    assert best["valid_top10_lift"] == pytest.approx(0.002)


def test_select_best_trial_uses_gap_as_tiebreaker() -> None:
    """When top-5 pool contains near-equal valid lifts, smaller gap wins."""
    # All 5 trials have valid_top10_lift = 0.005 but different gaps
    trials = [
        {"valid_top10_lift": 0.005, "train_valid_gap": 0.010, "params": {}},
        {"valid_top10_lift": 0.005, "train_valid_gap": 0.002, "params": {}},  # smallest gap
        {"valid_top10_lift": 0.005, "train_valid_gap": 0.007, "params": {}},
        {"valid_top10_lift": 0.005, "train_valid_gap": 0.005, "params": {}},
        {"valid_top10_lift": 0.005, "train_valid_gap": 0.009, "params": {}},
    ]
    best = _select_best_trial(trials)
    assert best is not None
    assert best["train_valid_gap"] == pytest.approx(0.002)


def test_select_best_trial_single_trial() -> None:
    trial = {"valid_top10_lift": 0.004, "train_valid_gap": 0.003, "params": {}}
    best  = _select_best_trial([trial])
    assert best is trial


def test_select_best_trial_top5_pool_respects_valid_lift_ordering() -> None:
    """Top-5 pool is selected by valid lift; gap applied only within that pool."""
    # trials 1-5 have high valid lift but large gap; trial 6 has low valid lift + tiny gap
    high_lift = [
        {"valid_top10_lift": 0.010, "train_valid_gap": 0.020, "params": {}} for _ in range(5)
    ]
    low_lift  = [{"valid_top10_lift": 0.001, "train_valid_gap": 0.000, "params": {}}]
    best      = _select_best_trial(high_lift + low_lift)
    assert best is not None
    # Best must come from the high-lift pool, not the low-lift outlier
    assert best["valid_top10_lift"] == pytest.approx(0.010)
