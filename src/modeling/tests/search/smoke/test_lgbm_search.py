"""Smoke tests for the refactored lgbm_search module (t3).

Covers:
  - _select_best_trial() picks the best from a mini trial list (objective min + gap tiebreaker)
  - The function is importable without a real DB or model

Note: the search objective is ``objective_score`` (lower = better), not the
legacy ``valid_top10_lift`` (higher = better) — see epic_050 bugfix notes.
"""

from __future__ import annotations

import pytest

from modeling.search.lgbm_search import _select_best_trial

pytestmark = pytest.mark.smoke


# ---------------------------------------------------------------------------
# _select_best_trial — smoke
# ---------------------------------------------------------------------------


def test_select_best_trial_returns_none_for_empty_list() -> None:
    assert _select_best_trial([]) is None


def test_select_best_trial_returns_none_when_no_valid_lift() -> None:
    trials = [{"params": {}, "train_ratio_p925": 0.01}]
    assert _select_best_trial(trials) is None


def test_select_best_trial_top5_pool_gap_tiebreaker() -> None:
    """Best trial is picked from the top-5 objective_score pool by smallest gap.

    With 3 trials where each has a unique gap, the trial with the best
    (lowest) objective AND the smallest gap among the top-5 pool should win.
    """
    trials = [
        {"objective_score": -0.002, "train_valid_gap": 0.001, "params": {}},
        {"objective_score": -0.005, "train_valid_gap": 0.002, "params": {}},
        {"objective_score": -0.003, "train_valid_gap": 0.003, "params": {}},
    ]
    # Top-5 pool (all 3 trials, sorted by objective_score ascending): [-0.005, -0.003, -0.002]
    # Gaps are:                                                       [ 0.002,  0.003,  0.001]
    # Smallest gap is 0.001 → trial with objective_score=-0.002 wins
    best = _select_best_trial(trials)
    assert best is not None
    assert best["train_valid_gap"] == pytest.approx(0.001)
    assert best["objective_score"] == pytest.approx(-0.002)


def test_select_best_trial_uses_gap_as_tiebreaker() -> None:
    """When top-5 pool contains near-equal objectives, smaller gap wins."""
    # All 5 trials have objective_score = -0.005 but different gaps
    trials = [
        {"objective_score": -0.005, "train_valid_gap": 0.010, "params": {}},
        {"objective_score": -0.005, "train_valid_gap": 0.002, "params": {}},  # smallest gap
        {"objective_score": -0.005, "train_valid_gap": 0.007, "params": {}},
        {"objective_score": -0.005, "train_valid_gap": 0.005, "params": {}},
        {"objective_score": -0.005, "train_valid_gap": 0.009, "params": {}},
    ]
    best = _select_best_trial(trials)
    assert best is not None
    assert best["train_valid_gap"] == pytest.approx(0.002)


def test_select_best_trial_single_trial() -> None:
    trial = {"objective_score": -0.004, "train_valid_gap": 0.003, "params": {}}
    best  = _select_best_trial([trial])
    assert best is trial


def test_select_best_trial_top5_pool_respects_valid_lift_ordering() -> None:
    """Top-5 pool is selected by objective_score; gap applied only within that pool."""
    # trials 1-5 have a good (low) objective but large gap; trial 6 has a
    # worse (higher) objective with a tiny gap and must be excluded from the pool.
    high_lift = [
        {"objective_score": -0.010, "train_valid_gap": 0.020, "params": {}} for _ in range(5)
    ]
    low_lift  = [{"objective_score": -0.001, "train_valid_gap": 0.000, "params": {}}]
    best      = _select_best_trial(high_lift + low_lift)
    assert best is not None
    # Best must come from the low-objective (better) pool, not the excluded outlier
    assert best["objective_score"] == pytest.approx(-0.010)
