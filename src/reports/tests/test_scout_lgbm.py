# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportOptionalMemberAccess=false
"""``reports.builders.scout_models_lgbm`` (epic_073 / t932) on synthetic data: no look-ahead past ``t_c``, spec rules,
3-class output of the model (no database)."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest

pytest.importorskip("lightgbm")

from reports.builders import scout_bench as sb  # noqa: E402
from reports.builders import scout_models_lgbm as L  # noqa: E402
from reports.builders import wave_events as we  # noqa: E402

W = B = 240
START = datetime(2023, 1, 1)


def grid(n_days: int = 12, seed: int = 0) -> we.MinuteData:
    rng = np.random.default_rng(seed)
    n = n_days * 1440
    c = 100 * np.exp(np.cumsum(rng.normal(0, 1e-3, n)))
    o = np.r_[c[0], c[:-1]]
    hi, lo = np.maximum(o, c) * 1.0005, np.minimum(o, c) * 0.9995
    return we.MinuteData(we._epoch_min(START), o, hi, lo, c, rng.uniform(1, 2, n))


def _after(m: we.MinuteData, k: int, seed: int = 9) -> we.MinuteData:
    """Copy of the grid with every bar from index ``k`` on replaced by noise."""
    rng = np.random.default_rng(seed)
    arrs = {f: getattr(m, f).copy() for f in ("open", "high", "low", "close", "volume")}
    for a in arrs.values():
        a[k:] = a[k:] * rng.uniform(0.5, 1.5, a.size - k)
    return replace(m, **arrs)


def _same(a: dict[str, float], b: dict[str, float]) -> bool:
    return a.keys() == b.keys() and all(np.isclose(a[k], b[k], equal_nan=True, rtol=0, atol=0) for k in a)


def test_event_and_context_features_ignore_bars_after_tc():
    m = grid()
    t0 = START + timedelta(days=9)
    i0 = we._epoch_min(t0) - m.m0
    tc = i0 + W - 1                                       # last bar closed at t_c
    low0, ctc = float(m.low[i0]), float(m.close[tc])
    sig = 0.01
    a = abs(float(np.log(ctc / low0))) + 0.01            # target distance A > 0
    t_c = t0 + timedelta(minutes=W)
    f1 = L.event_features(m, t0, W, B, low0, ctc, sig, a)
    c1 = L.context_features(m, t_c)
    m2 = _after(m, tc + 1)
    assert _same(L.event_features(m2, t0, W, B, low0, ctc, sig, a), f1)
    assert _same(L.context_features(m2, t_c), c1)
    assert np.isfinite(list(f1.values())).all() and np.isfinite(list(c1.values())).all()
    # and the features DO react to a change inside the prefix (sanity: the test can fail)
    assert not _same(L.event_features(_after(m, tc - 30), t0, W, B, low0, ctc, sig, a), f1)


def test_spec_is_primary_round1_and_no_label_features():
    assert L.SPEC.hypothesis == "H1" and L.SPEC.round == 1 and L.SPEC.family == "P1"
    assert L.MODEL_NAME in sb.REGISTRY                    # registered at import
    banned = {*sb.LABEL_COLS, "label_sub", "fwd_max_high", "fwd_min_low", "fwd_mfe_log", "fwd_mae_log", "mins_to_target",
              "mins_to_t0_break", "first_hit", "same_bar", "first_touch_min", "p_star"}
    assert not banned & set(L.FEATURES) and len(L.FEATURES) == len(set(L.FEATURES))
    reg = sb.new_registry()
    L.register(reg)
    L.register(reg)                                       # idempotent
    assert len(reg) == 2


def test_model_returns_three_class_probabilities():
    rng = np.random.default_rng(1)
    n = 400
    x = rng.normal(size=(n, len(L.FEATURES)))
    p = 1 / (1 + np.exp(-(x[:, 0] * 1.5 - 1.5)))
    cls = np.where(rng.random(n) < p, "impulse", np.where(rng.random(n) < 0.5, "false_start", "sideways"))
    df = pl.DataFrame({**{c: x[:, i] for i, c in enumerate(L.FEATURES)}, "label_class": cls})
    mdl = L.ScoutLGBM()
    mdl.fit(df[:300], None, seed=7)
    pr = mdl.predict_proba(df[300:], None)
    assert pr.shape == (100, 3) and np.allclose(pr.sum(axis=1), 1)
    assert sb.auc((cls[300:] == "impulse").astype(float), pr[:, 0]) > 0.6
    mdl2 = L.ScoutLGBM()
    mdl2.fit(df[:300], None, seed=7)
    assert np.array_equal(pr, mdl2.predict_proba(df[300:], None))   # deterministic
    assert set(mdl.importance().index) == set(L.FEATURES)
