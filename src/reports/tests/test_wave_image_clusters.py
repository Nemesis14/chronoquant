# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false
"""``reports.builders.wave_image_model`` + ``wave_image_clusters`` on synthetic 1m data (no real database).

Needs the ``research`` dependency group (torch, tslearn): ``uv run --group research pytest src/reports/tests``; skipped otherwise.
Covers: image shape / no look-ahead beyond ``t_c``, determinism, train-only fitting (guard + spy), the fold fit never seeing
the test fold (spy + scrambled test rows), and the train-only report.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest

pytest.importorskip("torch")
pytest.importorskip("tslearn")
pytest.importorskip("sklearn")

from reports.builders import wave_events as we  # noqa: E402
from reports.builders import wave_image_clusters as wr  # noqa: E402
from reports.builders import wave_image_model as wm  # noqa: E402
from reports.schema import Manifest  # noqa: E402

START = datetime(2025, 1, 1)
DAYS = 75
CELL = "W1h_H2h"
OOS = START + timedelta(days=55)
PARAMS = we.WaveParams(
    cells=((CELL, 60, 120),), version="v2", sigma_window=1440, trend_bars=720, velocity_bars=30, cv_blocks=3,
    train_start=str((START + timedelta(days=2)).date()), oos_start=str(OOS.date()),
    holdout2_start=str((START + timedelta(days=65)).date()),
)
CFG = wm.AEConfig(epochs=3, width=4, latent=4, batch=32)


def _synthetic_1m(seed: int = 5) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    n = DAYS * 1440
    vol = 0.0008 * (1 + 0.8 * np.sin(np.arange(n) / 2900.0) ** 2)
    close = 100 * np.exp(np.cumsum(rng.normal(0, vol)))
    open_ = np.concatenate(([close[0]], close[:-1]))
    wick = np.abs(rng.normal(0, 0.0004, n)) * close
    return pl.DataFrame({
        "open_time": [START + timedelta(minutes=i) for i in range(n)],
        "open": open_, "high": np.maximum(open_, close) + wick, "low": np.minimum(open_, close) - wick,
        "close": close, "volume": rng.uniform(5, 15, n),
    })


@pytest.fixture(scope="module")
def df() -> pl.DataFrame:
    return _synthetic_1m()


@pytest.fixture(scope="module")
def ds(df: pl.DataFrame) -> we.EventDataset:
    return we.build_events(df, PARAMS)


@pytest.fixture(scope="module")
def ev(ds: we.EventDataset) -> pl.DataFrame:
    e = ds.events.filter((pl.col("cell_id") == CELL) & (pl.col("split") == "train")).sort("t0")
    assert e.height >= 40, f"synthetic data produced too few train events ({e.height})"
    return e


@pytest.fixture(scope="module")
def b15(df: pl.DataFrame) -> we.Bars15:
    return we.to_bars15(we.to_minute_grid(df), PARAMS)


@pytest.fixture(scope="module")
def pre(b15: we.Bars15, ev: pl.DataFrame) -> wm.Prefixes:
    return wm.build_prefixes(b15, ev)


@pytest.fixture(scope="module")
def bounds() -> list[datetime]:
    return we.cv_block_bounds(PARAMS)


# --------------------------------------------------------------------------- images


def test_image_shape_and_binary(pre: wm.Prefixes) -> None:
    rp = wm.fit_render(pre)
    img = wm.render_images(pre, rp)
    assert pre.n_bars == 2 * 60 // 15
    assert img.shape == (pre.n, 1, wm.IMG_H, wm.PX_PER_BAR * pre.n_bars)
    assert set(np.unique(img)) <= {0.0, 1.0}
    # every valid bar draws its high-low line (middle column of the price panel) and a volume bar
    j = pre.n_bars - 1
    assert (img[:, 0, :wm.PRICE_ROWS, wm.PX_PER_BAR * j + 1].sum(axis=1) >= 1).all()
    assert (img[:, 0, wm.PRICE_ROWS + wm.GAP_ROWS:, wm.PX_PER_BAR * j + 1].sum(axis=1) >= 1).all()
    assert (img[:, 0, wm.PRICE_ROWS:wm.PRICE_ROWS + wm.GAP_ROWS, :] == 0).all()        # gap row stays empty


def test_prefix_uses_only_bars_closed_by_tc(df: pl.DataFrame, ev: pl.DataFrame, pre: wm.Prefixes) -> None:
    """Scrambling every 1m bar at or after one event's ``t_c`` leaves the prefix arrays of all events up to it unchanged."""
    assert (ev["prefix_end"] <= ev["t_c"]).all()
    cut = ev["t_c"][ev.height // 2]
    bad = df.with_columns(
        *(pl.when(pl.col("open_time") >= cut).then(pl.col(c) * 3.0).otherwise(pl.col(c)).alias(c) for c in ("open", "high", "low", "close")),
        pl.when(pl.col("open_time") >= cut).then(pl.col("volume") * 7.0).otherwise(pl.col("volume")).alias("volume"),
    )
    pre_bad = wm.build_prefixes(we.to_bars15(we.to_minute_grid(bad), PARAMS), ev)
    upto = np.flatnonzero((ev["t_c"] <= cut).to_numpy())
    assert upto.size >= 10
    for f in ("open", "high", "low", "close", "volume"):
        np.testing.assert_array_equal(getattr(pre, f)[upto], getattr(pre_bad, f)[upto])
    assert not np.array_equal(pre.close[-1], pre_bad.close[-1])                           # the scramble is real


def test_prefix_look_ahead_guard(b15: we.Bars15, ev: pl.DataFrame) -> None:
    bad = ev.head(3).with_columns(pl.col("prefix_end") + timedelta(minutes=15))
    with pytest.raises(ValueError, match="beyond t_c"):
        wm.build_prefixes(b15, bad)


# --------------------------------------------------------------------------- determinism, train-only


def test_embedding_and_clusters_deterministic(ev: pl.DataFrame, pre: wm.Prefixes) -> None:
    e1, e2 = wm.fit_embedding(ev, pre, cfg=CFG, seed=3), wm.fit_embedding(ev, pre, cfg=CFG, seed=3)
    z1, z2 = wm.embed(e1, pre), wm.embed(e2, pre)
    np.testing.assert_array_equal(z1, z2)
    assert e1.losses == e2.losses
    m1, l1 = wm.fit_clusterer(e1, z1, 3, seed=3)
    m2, l2 = wm.fit_clusterer(e2, z2, 3, seed=3)
    np.testing.assert_array_equal(l1, l2)
    np.testing.assert_array_equal(m1.centroids, m2.centroids)
    assert m1.sizes[0] == m1.sizes.max()                                                    # cluster 0 = largest
    np.testing.assert_array_equal(wm.assign(m1, pre)[0], l1)                                # assign == fit labels
    e3 = wm.fit_embedding(ev, pre, cfg=CFG, seed=4)
    assert not np.array_equal(wm.embed(e3, pre), z1)                                        # the seed matters


def test_raw_baselines_deterministic(ev: pl.DataFrame, pre: wm.Prefixes) -> None:
    for method in ("kshape", "dtw_kmeans"):
        _, a = wm.fit_raw_clusterer(ev, pre, method, 3, seed=1)
        _, b = wm.fit_raw_clusterer(ev, pre, method, 3, seed=1)
        np.testing.assert_array_equal(a, b)
        assert a.max() <= 2 and a.size == pre.n
    paths = wm.raw_paths(pre)
    assert np.allclose(paths.mean(axis=1), 0, atol=1e-6) or pre.n_valid.min() < pre.n_bars   # z-normalised


def test_fit_refuses_non_train_rows(ds: we.EventDataset, ev: pl.DataFrame, b15: we.Bars15) -> None:
    allv = ds.events.filter(pl.col("cell_id") == CELL).sort("t0")
    assert (allv["split"] != "train").any(), "synthetic data must contain OOS / holdout rows"
    pre_all = wm.build_prefixes(b15, allv)
    with pytest.raises(ValueError, match="train"):
        wm.fit_embedding(allv, pre_all, cfg=CFG)
    with pytest.raises(ValueError, match="train"):
        wm.fit_raw_clusterer(allv, pre_all, "kshape", 3)
    emb = wm.fit_embedding(ev, wm.build_prefixes(b15, ev), cfg=CFG)
    assert emb.n_fit == ev.height
    oos = allv.filter(pl.col("split") == "oos")
    if oos.height:                                                                          # transform-only is allowed
        z = wm.embed(emb, wm.build_prefixes(b15, oos))
        assert z.shape == (oos.height, CFG.latent)


def test_embedding_misaligned_rows_rejected(ev: pl.DataFrame, pre: wm.Prefixes) -> None:
    with pytest.raises(ValueError, match="aligned"):
        wm.fit_embedding(ev.reverse(), pre, cfg=CFG)


# --------------------------------------------------------------------------- fold fit never sees the test fold


def test_fold_fit_does_not_see_test_fold(monkeypatch: pytest.MonkeyPatch, ev: pl.DataFrame, pre: wm.Prefixes,
                                         bounds: list[datetime]) -> None:
    seen: list[np.ndarray] = []
    orig = wm.fit_embedding

    def spy(events: pl.DataFrame, p: wm.Prefixes, **kw):
        seen.append(events["t0"].to_numpy().astype("datetime64[us]"))
        assert (events["split"] == "train").all()
        np.testing.assert_array_equal(p.t0, seen[-1])
        return orig(events, p, **kw)

    monkeypatch.setattr(wm, "fit_embedding", spy)
    h = 120
    for blk in range(3):
        seen.clear()
        ff = wm.fit_fold(ev, pre, blk, bounds, h, ks=(2, 3), cfg=CFG)
        assert len(seen) == 1
        fit_t0 = set(seen[0].tolist())
        test_t0 = set(ff.fold.test["t0"].to_numpy().astype("datetime64[us]").tolist())
        assert test_t0 and fit_t0 and not (fit_t0 & test_t0)
        # purge + embargo: no fit event's full interval touches the test block (+/- H)
        lo, hi = bounds[blk] - timedelta(minutes=h), bounds[blk + 1] + timedelta(minutes=h)
        assert ((ff.fold.train["event_start"] >= hi) | (ff.fold.train["label_end"] <= lo)).all()
        assert ff.emb.n_fit == ff.fold.train.height == ff.train_z.shape[0]
        assert ff.test_labels[3].size == ff.fold.test.height == ff.test_z.shape[0]


def test_fold_fit_independent_of_test_fold_content(ev: pl.DataFrame, pre: wm.Prefixes, bounds: list[datetime]) -> None:
    """Replacing the test fold's prefix data (and nothing else) must not change anything fitted on the train fold."""
    blk = 1
    base = wm.fit_fold(ev, pre, blk, bounds, 120, ks=(3,), cfg=CFG)
    test_pos = np.flatnonzero(np.isin(pre.t0, base.fold.test["t0"].to_numpy().astype("datetime64[us]")))
    assert test_pos.size > 0
    scr = pre.take(np.arange(pre.n))
    for f in ("open", "high", "low", "close", "volume"):
        a = getattr(scr, f).copy()
        a[test_pos] = a[test_pos][::-1] * 2.5 + 1.0
        setattr(scr, f, a)
    alt = wm.fit_fold(ev, scr, blk, bounds, 120, ks=(3,), cfg=CFG)
    np.testing.assert_array_equal(base.models[3].centroids, alt.models[3].centroids)
    np.testing.assert_array_equal(base.train_z, alt.train_z)
    np.testing.assert_array_equal(base.train_labels[3], alt.train_labels[3])
    assert base.emb.losses == alt.emb.losses
    assert not np.array_equal(base.test_z, alt.test_z)                                      # only the transform differs


def test_select_k_deterministic_and_sane(ev: pl.DataFrame, pre: wm.Prefixes, bounds: list[datetime]) -> None:
    a = wm.select_k(ev, pre, bounds, 120, ks=(2, 3), methods=("ae_kmeans", "kshape"), cfg=CFG, seed=2)
    b = wm.select_k(ev, pre, bounds, 120, ks=(2, 3), methods=("ae_kmeans", "kshape"), cfg=CFG, seed=2)
    s = a[0]
    assert s.equals(b[0])
    assert set(s["k"]) == {2, 3} and set(s["method"]) == {"ae_kmeans", "kshape"}
    assert (s["oof_ll"] > 0).all() and np.isfinite(s[["delta_ll", "ari", "jaccard"]].to_numpy()).all()
    assert a[1]["block"].nunique() == 3
    assert wm.best_k(s, "ae_kmeans") in (2, 3)


# --------------------------------------------------------------------------- report


def test_report_train_only(tmp_path: Path, df: pl.DataFrame, ds: we.EventDataset, monkeypatch: pytest.MonkeyPatch) -> None:
    fitted: list[pl.DataFrame] = []
    orig = wm.fit_embedding

    def spy(events: pl.DataFrame, p: wm.Prefixes, **kw):
        fitted.append(events)
        return orig(events, p, **kw)

    monkeypatch.setattr(wm, "fit_embedding", spy)
    out = wr.build(tmp_path, df_1m=df, events=ds.events, params=PARAMS, cell=CELL, ks=(2, 3), cfg=CFG, seed=1)
    assert out is not None and out.name == wr.SLUG
    assert fitted and all((e["split"] == "train").all() and (e["t0"] < OOS).all() for e in fitted)
    man = Manifest.model_validate_json((out / "manifest.json").read_text(encoding="utf-8"))
    chart_ids = {c.id for c in man.content.charts}
    table_ids = {t.id for t in man.content.tables}
    assert {"kselect_ll", "kselect_stability", "centroid_images", "medoid_images", "cluster_sizes", "cluster_shares", "cones",
            "recon", "method_rates"} <= chart_ids
    assert {"kselect", "cluster_summary", "baseline", "baseline_clusters", "runtime"} <= table_ids
    md = (out / "report.md").read_text(encoding="utf-8")
    for cid in chart_ids:
        assert "{{chart:" + cid + "}}" in md
    for tid in table_ids:
        assert "{{table:" + tid + "}}" in md
    # no timestamp at / after the OOS start anywhere in the content (the manifest's generation date excluded)
    pat = re.compile(r"(20\d\d-\d\d-\d\d)")
    for f in [out / "report.md", *(out / "charts").glob("*.json"), *(out / "tables").glob("*.json")]:
        for d in pat.findall(f.read_text(encoding="utf-8")):
            assert d < str(OOS.date()), f"{f.name}: {d} is not a train date"
    cone = json.loads((out / "charts" / "cones.json").read_text(encoding="utf-8"))
    assert all(t.get("y") for t in cone["data"] if t.get("type", "scatter") == "scatter")


def test_forward_paths_units(df: pl.DataFrame, ev: pl.DataFrame) -> None:
    m = we.to_minute_grid(df)
    h = 120
    paths = wr.forward_paths(m, ev, h)
    assert paths.shape == (ev.height, h // 15 + 1)
    np.testing.assert_allclose(paths[:, 0], 0.0)
    # the impulse level (+1) is touched on the 1m grid iff label == impulse; 15m sampling can only miss it, never invent it
    hit15 = (paths.max(axis=1) > 1.0)
    assert not (hit15 & (ev["label_class"].to_numpy() == "sideways")).any()
