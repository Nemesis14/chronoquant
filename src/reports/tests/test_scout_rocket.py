"""t933: MiniRocket / InceptionTime Scout models -- prefix causality, shapes, determinism (synthetic data, no DB)."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest

from reports.builders import scout_bench as sb
from reports.builders import scout_models_rocket as rk
from reports.builders import wave_events as we

B = W = 240
T_START = datetime(2022, 1, 3)


def _grid(n: int = 6000, seed: int = 0) -> tuple[we.MinuteData, np.ndarray]:
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 1e-3, n)))
    m = we.MinuteData(m0=we._epoch_min(T_START), open=close.copy(), high=close * 1.001, low=close * 0.999,
                      close=close, volume=rng.uniform(1, 3, n))
    return m, m.volume * rng.uniform(0.3, 0.7, n)


def _event(m: we.MinuteData, i: int) -> dict:
    t0 = T_START + timedelta(minutes=i)
    return {"event_start": t0 - timedelta(minutes=B), "t_c": t0 + timedelta(minutes=W), "low_t0": float(m.low[i]),
            "sigma_w": 0.01, "vol7d_mean_1m": 2.0}


def test_prefix_shape_and_values() -> None:
    m, tbb = _grid()
    ev = _event(m, 1000)
    x = rk.event_prefix(m, tbb, **ev)
    assert x.shape == (len(rk.CHANNELS), (B + W) // rk.PREFIX_STEP)
    assert x.dtype == np.float32 and np.isfinite(x).all()
    last_close = m.close[1000 + W - 1]                       # bar tc: last bar of the prefix (closes at t_c)
    assert x[0, -1] == pytest.approx(np.log(last_close / ev["low_t0"]) / 0.01, rel=1e-5)
    assert np.all(np.abs(x[2]) <= 0.5)


def test_prefix_uses_no_bar_at_or_after_t_c() -> None:
    m, tbb = _grid()
    i = 1000
    ev = _event(m, i)
    x = rk.event_prefix(m, tbb, **ev)
    tc_idx = i + W                                           # first bar opening at t_c (label window)
    m2, tbb2 = _grid()
    for arr in (m2.close, m2.high, m2.low, m2.volume):
        arr[tc_idx:] *= 7.0
    tbb2[tc_idx:] = 0.0
    assert np.array_equal(rk.event_prefix(m2, tbb2, **ev), x)
    m2.close[tc_idx - 1] *= 1.05                             # the last prefix bar does count
    assert not np.array_equal(rk.event_prefix(m2, tbb2, **ev), x)
    m3, tbb3 = _grid()
    m3.close[i - B - 1] *= 3.0                               # the bar before t0 - B does not
    assert np.array_equal(rk.event_prefix(m3, tbb3, **ev), x)


def test_prefix_gap_forward_fill() -> None:
    m, tbb = _grid()
    m.close[900:910] = np.nan
    m.volume[900:910] = np.nan
    assert np.isfinite(rk.event_prefix(m, tbb, **_event(m, 1000))).all()


def test_minirocket_shape_and_determinism() -> None:
    rng = np.random.default_rng(1)
    x = rng.normal(size=(40, 3, 96)).astype(np.float32)
    p1 = rk.minirocket_fit(x[:30], seed=7, num_features=840)
    p2 = rk.minirocket_fit(x[:30], seed=7, num_features=840)
    f1, f2 = rk.minirocket_transform(x, p1), rk.minirocket_transform(x, p2, chunk=7)
    assert f1.shape == (40, 840)
    assert np.array_equal(f1, f2)
    assert f1.min() >= 0 and f1.max() <= 1
    f3 = rk.minirocket_transform(x, rk.minirocket_fit(x[:30], seed=8, num_features=840))
    assert not np.array_equal(f1, f3)


def test_ridge_loo_matches_brute_force() -> None:
    rng = np.random.default_rng(2)
    z = rng.normal(size=(25, 60))
    z = (z - z.mean(0)) / z.std(0)
    y = np.eye(3)[rng.integers(0, 3, 25)]
    lam, u = np.linalg.eigh(z @ z.T)
    keep = lam > 1e-9 * lam.max()
    s = np.sqrt(lam[keep])
    u = u[:, keep]
    alpha, loo, _ = rk.ridge_loo(u, s, y, [10.0])
    for j in (0, 7, 24):                                     # exact LOO: refit without row j (centred on the rest)
        mask = np.arange(25) != j
        zt, yt = z[mask], y[mask]
        zm, ym = zt.mean(0), yt.mean(0)
        w = np.linalg.solve((zt - zm).T @ (zt - zm) + alpha * np.eye(60), (zt - zm).T @ (yt - ym))
        assert np.allclose((z[j] - zm) @ w + ym, loo[j], atol=1e-8)


def _toy_frame(n: int = 120, seed: int = 3) -> tuple[pl.DataFrame, rk.RocketPrefix]:
    rng = np.random.default_rng(seed)
    x = rng.normal(size=(n, 3, 96)).astype(np.float32)
    lab = rng.choice(np.array(sb.CLASSES), n, p=[0.3, 0.4, 0.3])
    x[lab == "impulse", 0, -20:] += 1.5                      # learnable shape signal
    return pl.DataFrame({"row_id": np.arange(n), "label_class": lab}), rk.RocketPrefix(x=x)


@pytest.mark.parametrize("factory", [
    lambda: rk.MiniRocketModel(dict(rk.ROCKET_CONFIG, num_features=840)),
    lambda: rk.InceptionModel(dict(rk.INCEPTION_CONFIG, max_epochs=3, n_filters=4, bottleneck=4)),
])
def test_model_shape_and_determinism(factory) -> None:
    pytest.importorskip("torch")
    df, pre = _toy_frame()
    tr, te = df[:90], df[90:]
    probs = []
    for _ in range(2):
        pre.cache.clear()
        m = factory()
        m.fit(tr, pre, seed=11)
        probs.append(m.predict_proba(te, pre))
    p = probs[0]
    assert p.shape == (30, 3) and np.isfinite(p).all()
    assert np.allclose(p.sum(axis=1), 1.0)
    assert np.array_equal(probs[0], probs[1])


def test_rocket_cache_is_label_free() -> None:
    df, pre = _toy_frame()
    cfg = dict(rk.ROCKET_CONFIG, num_features=840)
    tr = df[:90]
    m1 = rk.MiniRocketModel(cfg)
    m1.fit(tr, pre, seed=5)
    shuffled = tr.with_columns(pl.Series("label_class", tr["label_class"].to_numpy()[::-1]))
    m2 = rk.MiniRocketModel(cfg)
    m2.fit(shuffled, pre, seed=5)                            # same rows -> same cached basis, head refit on new labels
    assert m1.basis is m2.basis
    assert len(pre.cache) == 1
    assert not np.array_equal(m1.predict_proba(df[90:], pre), m2.predict_proba(df[90:], pre))


def test_registry_specs() -> None:
    reg = rk.registry()
    assert reg.get(rk.ROCKET_NAME).hypothesis == "H2" and reg.get(rk.ROCKET_NAME).round == 1
    assert not reg.get(rk.INCEPTION_NAME).primary
    assert reg.baseline.is_baseline


def test_build_prefix_requires_prepared_frame() -> None:
    m, tbb = _grid()
    ev = _event(m, 1000)
    df = pl.DataFrame([{**ev, "asset_id": "solusdt", "row_id": 5}])
    with pytest.raises(ValueError):
        rk.build_prefix(df, {"solusdt": m}, {"solusdt": tbb})
