"""Pure-function tests of the epic_080 Elliott helpers (no DB)."""

from __future__ import annotations

import numpy as np

from reports.builders import elliott_test as et


def _lp(*v: float) -> np.ndarray:
    return np.array([[x] for x in v], dtype=float)


def test_classify_family_and_cohort() -> None:
    a = et.classify(_lp(0.0, 10.0, 5.0, 8.0, 6.0))          # p3 < p1, p2 > p0, p4 > p2 -> family A, retr 0.5 / 0.667 -> fib
    assert a["fam_a"][0] and a["cohort"][0] == "fib"
    b = et.classify(_lp(0.0, 10.0, 5.0, 16.0, 12.0))        # p3 > p1, p4 > p1, W3 = 11 >= W1 = 10 -> family B hard
    assert not b["fam_a"][0] and b["cohort"][0] in ("hard", "fib")
    c = et.classify(_lp(0.0, 10.0, 5.0, 16.0, 9.0))         # p4 < p1 -> overlap -> control
    assert c["cohort"][0] == "control"


def test_touch_codes() -> None:
    logp = np.array([0.0, 0.1, 0.2, 0.3, -0.5, 0.0])
    code, ext = et._touch(logp, np.array([0, 0, 0]), np.array([-0.4, -0.05, -9.0]), np.array([0.25, 9.0, 9.0]), 5, 5)
    assert code[0] == 1 and ext[0] == 3
    assert code[1] == 0 or code[1] == -1


def test_touch_censored_when_window_ends() -> None:
    logp = np.zeros(4)
    code, _ = et._touch(logp, np.array([1]), np.array([-1.0]), np.array([1.0]), 10, 3)
    assert code[0] == 2
