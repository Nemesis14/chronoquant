"""Pure-function tests of the epic_079 sample / null helpers (no DB)."""

from __future__ import annotations

import numpy as np
import polars as pl

from reports.builders import cand_v2_sample as sm


def test_offset_prob_monotone_and_bounded() -> None:
    rz = np.array([1.0, 2.0, 3.0, 3.9])
    p = sm.offset_prob(rz)
    assert np.all(np.diff(p) > 0)
    assert p.min() >= 0.01 and p.max() <= 0.99


def test_add_clusters_groups_coinciding_lows_across_scales() -> None:
    c = pl.DataFrame({"low_idx": [100, 101, 5000], "scale_min": [15, 60, 15]})
    out = sm.add_clusters(c)
    assert out["cluster"][0] == out["cluster"][1] != out["cluster"][2]
    assert out["w"][0] == 0.5 and out["w"][2] == 1.0
