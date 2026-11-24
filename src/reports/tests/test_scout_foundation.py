# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false
"""``reports.builders.scout_models_foundation`` (epic_073 / t935) on synthetic data (no production database).

Covers: token pooling ([REG] excluded), deterministic sign-fixed PCA, head shape / determinism / label-free fold PCA
cache, predictions independent of the scored rows' labels, the embedding path with a fake pipeline (channel layout,
cache round trip, shape guard), no bar at / after ``t_c`` reaches the embedding, registry specs (X2, exploratory), the
head-refit permutation null and the model-only persistence. The real Chronos-Bolt download test runs only with
``CQ_HEAVY_TESTS=1`` (~191 MB checkpoint).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import numpy as np
import polars as pl
import pytest

pytest.importorskip("sklearn")

from reports.builders import scout_bench as sb  # noqa: E402
from reports.builders import scout_models_foundation as fm  # noqa: E402
from reports.builders import scout_models_rocket as rk  # noqa: E402
from reports.builders import wave_events as we  # noqa: E402

H = 480
START, END = datetime(2022, 1, 1), datetime(2022, 7, 1)
BOUNDS = we.cv_block_bounds(we.params_v2(train_start="2022-01-01", oos_start="2022-07-01"))
DIM = 96


def make_frame(n_per_asset: int = 150, seed: int = 0) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    span = (END - START).total_seconds() / 60 - 2 * (240 + H)
    for a in ("aaausdt", "bbbusdt"):
        for _ in range(n_per_asset):
            t0 = (START + timedelta(minutes=240 + float(rng.uniform(0, span)))).replace(second=0, microsecond=0)
            x = rng.normal(size=6)
            p = 1 / (1 + np.exp(-(-1.6 + x[0])))
            cls = "impulse" if rng.random() < p else ("false_start" if rng.random() < 0.5 else "sideways")
            t_c = t0 + timedelta(minutes=240)
            rows.append({"asset_id": a, "cell_id": "W4h_H8h", "t0": t0, "t_c": t_c, "event_start": t0 - timedelta(minutes=240),
                         "label_end": t_c + timedelta(minutes=H), "split": "train", "h_min": H, "label_class": cls,
                         "tau_min": float(rng.integers(5, H)) if cls != "sideways" else float(H), "p_star": 110.0,
                         "low_t0": 95.0, "close_tc": 100.0, "sigma_w": 0.02, **dict(zip(sb.S0C_COLS, x, strict=True))})
    return pl.DataFrame(rows)


def make_embedding(fr: pl.DataFrame, seed: int = 1) -> fm.FoundationEmbedding:
    """Synthetic embedding aligned with ``row_id``: a few dims carry the impulse label, the rest noise."""
    rng = np.random.default_rng(seed)
    y = sb.frame.y_binary(fr)
    emb = rng.normal(size=(fr.height, DIM)).astype(np.float32)
    emb[:, :4] += (0.8 * y)[:, None].astype(np.float32)
    stats = rng.normal(size=(fr.height, 4))
    return fm.FoundationEmbedding(emb=emb, stats=stats, stat_names=fm.stat_names())


@pytest.fixture(scope="module")
def data() -> tuple[pl.DataFrame, fm.FoundationEmbedding]:
    fr = sb.prepare_frame(make_frame(), BOUNDS)
    return fr, make_embedding(fr)


# --------------------------------------------------------------------------- pooling / PCA


def test_pool_excludes_reg_token():
    s = np.zeros((2, 4, 3))
    s[:, :3, :] = 1.0
    s[:, 3, :] = 100.0                                 # the trailing [REG] token
    assert np.allclose(fm.pool_states(s, has_reg=True), 1.0)
    assert np.allclose(fm.pool_states(s, has_reg=False), 25.75)


def test_pca_deterministic_and_sign_fixed():
    x = np.random.default_rng(0).normal(size=(200, 30))
    b1, b2 = fm.fit_pca(x, 8), fm.fit_pca(x.copy(), 8)
    assert b1.comps.shape == (8, 30)
    assert np.array_equal(b1.comps, b2.comps)
    assert np.all(np.diff(b1.explained) <= 1e-12) and b1.explained.sum() <= 1 + 1e-12
    lead = b1.comps[np.arange(8), np.abs(b1.comps).argmax(axis=1)]
    assert np.all(lead > 0)
    z = fm.pca_transform(b1, x)
    assert z.shape == (200, 8)
    assert np.allclose(z.mean(axis=0), 0, atol=1e-10)


# --------------------------------------------------------------------------- head


@pytest.mark.parametrize("use_s0c", [False, True])
def test_head_shape_determinism(data, use_s0c):
    fr, fe = data
    tr, te = fr[:200], fr[200:]
    out = []
    for _ in range(2):
        m = fm.FoundationHead(use_s0c=use_s0c)
        m.fit(tr, fe, seed=0)
        out.append(m.predict_proba(te, fe))
    p = out[0]
    assert p.shape == (te.height, 3)
    assert np.allclose(p.sum(axis=1), 1.0) and (p >= 0).all()
    assert np.array_equal(out[0], out[1])
    n_feat = min(fm.N_PCA, DIM) + 4 + (len(sb.S0C_COLS) if use_s0c else 0)
    assert m.clf.coef_.shape == (3, n_feat)


def test_fold_pca_cache_is_label_free(data):
    fr, fe = data
    fe = fm.FoundationEmbedding(emb=fe.emb, stats=fe.stats, stat_names=fe.stat_names)
    tr = fr[:200]
    m = fm.FoundationHead()
    m.fit(tr, fe, seed=0)
    basis = m.basis
    perm = tr.with_columns(pl.Series("label_class", tr["label_class"].to_numpy()[::-1]))
    m2 = fm.FoundationHead()
    m2.fit(perm, fe, seed=0)
    assert m2.basis is basis                             # same fold-train rows -> same cached, label-free basis
    assert len(fe.cache) == 1
    fresh = fm.fit_pca(fe.emb[tr["row_id"].to_numpy()], fm.N_PCA)
    assert np.array_equal(fresh.comps, basis.comps)


def test_predictions_ignore_labels_of_scored_rows(data):
    fr, fe = data
    tr, te = fr[:200], fr[200:]
    m = fm.FoundationHead(use_s0c=True)
    m.fit(tr, fe, seed=0)
    p = m.predict_proba(te, fe)
    flipped = te.with_columns(pl.lit("impulse").alias("label_class"), pl.lit(1.0).alias("tau_min"))
    assert np.array_equal(m.predict_proba(flipped, fe), p)


def test_head_rejects_raw_prefix(data):
    fr, _ = data
    with pytest.raises(TypeError):
        fm.FoundationHead().fit(fr[:50], np.zeros((50, 3, 480)), seed=0)


# --------------------------------------------------------------------------- embedding path (fake pipeline)


class _Cfg:
    use_reg_token = True


class _Model:
    chronos_config = _Cfg()


class FakePipe:
    """Mimics ``ChronosBoltPipeline.embed``: instance norm + per-patch means as 'states' + a [REG] token."""

    model = _Model()

    def __init__(self) -> None:
        self.calls = 0

    def embed(self, ctx):
        import torch

        self.calls += 1
        x = ctx.numpy().astype(np.float64)
        loc, scale = x.mean(axis=1), x.std(axis=1) + 1e-5
        z = (x - loc[:, None]) / scale[:, None]
        patches = z.reshape(x.shape[0], -1, 16)
        states = np.repeat(patches.mean(axis=2, keepdims=True), 8, axis=2)        # (n, P, d=8)
        states = np.concatenate([states, np.full((x.shape[0], 1, 8), 99.0)], axis=1)
        return torch.as_tensor(states, dtype=torch.float32), (torch.as_tensor(loc), torch.as_tensor(scale))


def _minute_grid(n: int = 4000, seed: int = 0) -> tuple[we.MinuteData, np.ndarray]:
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 1e-3, n)))
    m = we.MinuteData(m0=we._epoch_min(datetime(2022, 1, 3)), open=close.copy(), high=close * 1.001, low=close * 0.999,
                      close=close, volume=rng.uniform(1, 3, n))
    return m, m.volume * 0.5


def _prefix(m: we.MinuteData, tbb: np.ndarray, starts: list[int]) -> np.ndarray:
    base = datetime(2022, 1, 3)
    return np.stack([rk.event_prefix(m, tbb, base + timedelta(minutes=i), base + timedelta(minutes=i + 480),
                                     float(m.low[i + 240]), 0.01, 2.0, step=fm.PREFIX_STEP) for i in starts])


def test_embed_prefix_channels_and_stats():
    pytest.importorskip("torch")
    m, tbb = _minute_grid()
    x = _prefix(m, tbb, [100, 900, 1700])
    assert x.shape == (3, 3, fm.PREFIX_LEN)
    emb, stats = fm.embed_prefix(FakePipe(), x, batch=2)
    assert emb.shape == (3, 2 * 8) and stats.shape == (3, 4)
    assert np.all(np.abs(emb) < 50)                       # the [REG] token (99) is excluded from the pool
    # loc of the close channel = mean of the sigma-normalised close prefix (amplitude kept as a scalar)
    assert np.allclose(stats[:, 0], x[:, 0, :].mean(axis=1), atol=1e-4)
    assert np.allclose(stats[:, 2], x[:, 1, :].mean(axis=1), atol=1e-4)


def test_embedding_uses_no_bar_at_or_after_t_c():
    pytest.importorskip("torch")
    m, tbb = _minute_grid()
    starts = [100, 900]
    e1, s1 = fm.embed_prefix(FakePipe(), _prefix(m, tbb, starts))
    m2, tbb2 = _minute_grid()
    tc = starts[0] + 480                                  # every bar opening at / after t_c of the first event changes
    for arr in (m2.close, m2.high, m2.low, m2.volume):
        arr[tc:] *= 5.0
    e2, s2 = fm.embed_prefix(FakePipe(), _prefix(m2, tbb2, starts[:1]))
    assert np.array_equal(e2[0], e1[0]) and np.array_equal(s2[0], s1[0])


def test_load_or_embed_cache_round_trip(tmp_path):
    pytest.importorskip("torch")
    m, tbb = _minute_grid()
    x = _prefix(m, tbb, [100, 900, 1700])
    fr = pl.DataFrame({"asset_id": ["a"] * 3, "t0": [datetime(2022, 1, 3, 4)] * 3, "t_c": [datetime(2022, 1, 3, 8)] * 3})
    pipe = FakePipe()
    fe1 = fm.load_or_embed(fr, x, cache_dir=tmp_path, pipe=pipe)
    fe2 = fm.load_or_embed(fr, x, cache_dir=tmp_path, pipe=pipe)
    assert pipe.calls == 2                                # 2 channels x 1 batch: the second call came from the cache
    assert np.array_equal(fe1.emb, fe2.emb) and np.array_equal(fe1.stats, fe2.stats)
    assert fe2.stat_names == fm.stat_names() and len(list(tmp_path.glob("emb_*.npz"))) == 1
    with pytest.raises(ValueError):
        fm.load_or_embed(fr[:2], x, cache_dir=tmp_path, pipe=pipe)


# --------------------------------------------------------------------------- registry / benchmark / persistence


def test_registry_specs_exploratory():
    r = fm.registry()
    assert [s.name for s in r.specs(include_baseline=False)] == [fm.EMB_NAME, fm.EMB_S0C_NAME]
    for s in r.specs(include_baseline=False):
        assert s.family == "X2" and s.hypothesis is None and not s.primary and s.round == 1
    assert fm.emb_spec().config_hash != fm.emb_s0c_spec().config_hash


def test_benchmark_and_head_refit_permutation(data, tmp_path):
    import duckdb

    from reports.builders import scout_models_hazard as hz

    fr, fe = data
    res = sb.run_benchmark(fr, fm.registry(), None, prefix_data=fe, prepared=True, bounds=BOUNDS, n_perm=0, n_boot=50,
                           spa_boot=100)
    assert set(res.leaderboard["model"]) == {sb.BASELINE_NAME, *fm.MODEL_NAMES}
    for m in fm.MODEL_NAMES:
        assert res.outputs[m].proba.shape == (fr.height, 3)
    null = fm.permutation_null(res.frame, res.folds, fe, n_perm=3)
    assert set(null) == {*fm.MODEL_NAMES, "_auc_base"} and all(np.isfinite(v).all() for v in null.values())
    null2 = fm.permutation_null(res.frame, res.folds, fe, n_perm=3)
    assert all(np.array_equal(null[k], null2[k]) for k in null)
    path = str(tmp_path / "lab.duckdb")
    for m in fm.MODEL_NAMES:
        hz.persist_model_rows(res, m, lab_path=path)
    con = duckdb.connect(path, read_only=True)
    rows = con.execute('SELECT model FROM research."scout_leaderboard_v1" ORDER BY model').fetchall()
    con.close()
    assert [r[0] for r in rows] == sorted(fm.MODEL_NAMES)          # the baseline row is not written


# --------------------------------------------------------------------------- heavy: real checkpoint


@pytest.mark.skipif(os.environ.get("CQ_HEAVY_TESTS") != "1", reason="downloads the ~191 MB Chronos-Bolt checkpoint")
def test_real_chronos_bolt_embedding():
    pytest.importorskip("chronos")
    pipe = fm.load_pipeline()
    m, tbb = _minute_grid()
    x = _prefix(m, tbb, [100, 900])
    e1, s1 = fm.embed_prefix(pipe, x)
    e2, _ = fm.embed_prefix(pipe, x)
    assert e1.shape == (2, 2 * fm.D_MODEL) and s1.shape == (2, 4)
    assert np.isfinite(e1).all() and np.array_equal(e1, e2)
