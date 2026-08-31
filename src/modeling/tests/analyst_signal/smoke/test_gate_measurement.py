"""Smoke tests for the t205 gate-measurement statistics (epic_053).

Covers the hand-rolled statistical primitives that replace scipy/sklearn
calls (rank correlation, quantile-binned MI, block permutation, and the
Fisher-z power formula) plus the panel-building join/pivot logic. Does not
touch a real database -- everything here is synthetic.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest

from modeling.analyst_signal.gate_measurement import (
    AGGREGATE_FEATURE,
    AGGREGATE_SERIES,
    FEATURE_COLS,
    SUBREDDITS,
    _block_permutation,
    _mi_bits,
    _quantile_codes,
    _rank_avg,
    build_panel,
    min_detectable_rho,
)

pytestmark = pytest.mark.smoke


# %% _rank_avg

def test_rank_avg_no_ties_matches_argsort_order() -> None:
    x = np.array([30.0, 10.0, 20.0])
    ranks = _rank_avg(x)
    assert list(ranks) == [2.0, 0.0, 1.0]


def test_rank_avg_ties_get_average_rank() -> None:
    x = np.array([5.0, 5.0, 1.0])
    ranks = _rank_avg(x)
    # 1.0 is rank 0; the two 5.0s split ranks {1, 2} -> average 1.5 each.
    assert ranks[2] == 0.0
    assert ranks[0] == pytest.approx(1.5)
    assert ranks[1] == pytest.approx(1.5)


# %% _quantile_codes

def test_quantile_codes_uniform_data_fills_all_bins() -> None:
    rng = np.random.default_rng(0)
    x = rng.uniform(size=5000)
    codes, k = _quantile_codes(x, n_bins=10)
    assert k == 10
    # Every quantile bin should get roughly n/10 rows.
    counts = np.bincount(codes, minlength=k)
    assert counts.min() > 400
    assert counts.max() < 600


def test_quantile_codes_degenerate_constant_series_collapses_to_one_bin() -> None:
    x = np.full(200, 3.0)
    codes, k = _quantile_codes(x, n_bins=10)
    assert k == 1
    assert (codes == 0).all()


# %% _mi_bits

def test_mi_bits_independent_uniform_is_near_zero() -> None:
    rng = np.random.default_rng(1)
    n = 20000
    x = rng.uniform(size=n)
    y = rng.uniform(size=n)
    cx, kx = _quantile_codes(x)
    cy, ky = _quantile_codes(y)
    mi = _mi_bits(cx, cy, kx, ky)
    assert mi < 0.01


def test_mi_bits_identical_series_is_close_to_log2_bins() -> None:
    rng = np.random.default_rng(2)
    x = rng.uniform(size=20000)
    cx, kx = _quantile_codes(x)
    mi = _mi_bits(cx, cx, kx, kx)
    assert mi == pytest.approx(np.log2(kx), abs=0.05)


# %% _block_permutation

def test_block_permutation_flags_a_real_relationship() -> None:
    rng = np.random.default_rng(3)
    n = 4000
    feat = rng.normal(size=n)
    noise = rng.normal(size=n)
    target = feat + 0.5 * noise  # strong, real positive relationship

    out = _block_permutation(
        feat, target, kind="magnitude",
        rng=np.random.default_rng(4), block_len=20, n_perm=500,
    )
    assert out["spearman_ic"] > 0.5
    assert out["ic_perm_p"] < 0.01
    assert out["mi_perm_p"] < 0.01


def test_block_permutation_null_data_is_not_flagged() -> None:
    rng = np.random.default_rng(5)
    n = 4000
    feat = rng.normal(size=n)
    target = rng.normal(size=n)  # independent of feat

    out = _block_permutation(
        feat, target, kind="direction",
        rng=np.random.default_rng(6), block_len=20, n_perm=500,
    )
    assert abs(out["spearman_ic"]) < 0.1
    assert out["ic_perm_p"] > 0.05


def test_block_permutation_drops_trailing_partial_block() -> None:
    rng = np.random.default_rng(7)
    n = 4013  # not a multiple of block_len
    feat = rng.normal(size=n)
    target = rng.normal(size=n)
    out = _block_permutation(
        feat, target, kind="direction",
        rng=np.random.default_rng(8), block_len=20, n_perm=50,
    )
    assert out["n_obs"] == (n // 20) * 20
    assert out["n_obs"] < n


# %% min_detectable_rho

def test_min_detectable_rho_shrinks_with_more_observations() -> None:
    small_n = min_detectable_rho(n=500, alpha=0.05)
    large_n = min_detectable_rho(n=50000, alpha=0.05)
    assert large_n < small_n
    assert 0 < large_n < small_n < 1


def test_min_detectable_rho_grows_with_stricter_alpha() -> None:
    lenient = min_detectable_rho(n=10000, alpha=0.05)
    strict  = min_detectable_rho(n=10000, alpha=0.05 / 100)
    assert strict > lenient


# %% build_panel

def _target_df(hours: int) -> pl.DataFrame:
    start = datetime(2024, 1, 1)
    ts = [start + timedelta(hours=i) for i in range(hours)]
    rng = np.random.default_rng(9)
    return pl.DataFrame({
        "ts_utc":                ts,
        "long_mfe_fw60":         rng.uniform(0, 0.02, hours),
        "abs_short_mfe_fw60":    rng.uniform(0, 0.02, hours),
        "abs_fw60_close_logret": rng.uniform(0, 0.02, hours),
        "fw60_close_logret":     rng.normal(0, 0.01, hours),
    })


def _features_df(hours: int) -> pl.DataFrame:
    start = datetime(2024, 1, 1)
    ts = [start + timedelta(hours=i) for i in range(hours)]
    rng = np.random.default_rng(10)
    frames = []
    for sub in SUBREDDITS:
        data = {"ts_utc": ts, "subreddit": [sub] * hours}
        for feat in FEATURE_COLS:
            data[feat] = rng.normal(size=hours)
        frames.append(pl.DataFrame(data))
    return pl.concat(frames)


def test_build_panel_wide_columns_and_all_avg() -> None:
    hours = 100
    panel = build_panel(_target_df(hours), _features_df(hours))

    assert panel.height == hours
    for sub in SUBREDDITS:
        for feat in FEATURE_COLS:
            assert f"{sub}__{feat}" in panel.columns
    agg_col = f"{AGGREGATE_SERIES}__{AGGREGATE_FEATURE}"
    assert agg_col in panel.columns

    # ALL_AVG att_z must equal the row-wise mean of the four subreddits' att_z.
    expected = panel.select(
        pl.mean_horizontal([f"{sub}__att_z" for sub in SUBREDDITS]).alias("expected")
    )["expected"]
    actual = panel[agg_col]
    max_abs_diff = float((expected - actual).abs().max())  # type: ignore[arg-type]
    assert max_abs_diff < 1e-9


def test_build_panel_inner_joins_on_overlap_only() -> None:
    target = _target_df(50)
    features = _features_df(80)  # more hours than target
    panel = build_panel(target, features)
    assert panel.height == 50
