# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false
"""``reports.builders.wave_predictiveness_gate`` on synthetic 1m data (no real database).

Needs the ``research`` dependency group (torch, tslearn): ``uv run --group research pytest src/reports/tests``; skipped otherwise.
Covers: fold-train-only fitting (the per-fold design data is label free and never sees the test fold), the day-block
permutation (block structure, label counts), determinism, the exact label-dependent refit, the gate logic and the report.
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
from reports.builders import wave_image_model as wm  # noqa: E402
from reports.builders import wave_predictiveness_gate as wg  # noqa: E402
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
CFG = wm.AEConfig(epochs=2, width=4, latent=4, batch=32)
EMBARGO = 120


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
def pre(df: pl.DataFrame, ev: pl.DataFrame) -> wm.Prefixes:
    return wm.build_prefixes(we.to_bars15(we.to_minute_grid(df), PARAMS), ev)


@pytest.fixture(scope="module")
def bounds() -> list[datetime]:
    return we.cv_block_bounds(PARAMS)


@pytest.fixture(scope="module")
def folds(ev: pl.DataFrame, pre: wm.Prefixes, bounds: list[datetime]) -> list[wg.FoldData]:
    return wg.prepare_folds(ev, pre, bounds, EMBARGO, cfg=CFG, seed=3)


# --------------------------------------------------------------------------- fold-train-only fitting


def test_folds_partition_usable_events_and_exclude_test_from_train(ev: pl.DataFrame, folds: list[wg.FoldData]) -> None:
    use = wg.usable_events(ev)
    seen = np.concatenate([f.test_idx for f in folds])
    assert sorted(seen.tolist()) == list(range(use.height))                      # every usable event is tested exactly once
    for f in folds:
        assert not set(f.train_idx.tolist()) & set(f.test_idx.tolist())
        assert f.xa_tr.shape[0] == f.train_idx.size and f.xa_te.shape[0] == f.test_idx.size
        np.testing.assert_allclose(f.xa_tr.mean(axis=0), 0.0, atol=1e-9)         # standardised with the fold-train mean only
        # purge / embargo: no train event's interval overlaps the test block range
        lo, hi = f.test_idx.min(), f.test_idx.max()
        te_start = use["event_start"][int(lo)]
        te_end = use["label_end"][int(hi)]
        for i in f.train_idx:
            assert not (use["event_start"][int(i)] < te_end and use["label_end"][int(i)] > te_start)


def test_design_data_is_label_free(ev: pl.DataFrame, pre: wm.Prefixes, bounds: list[datetime], folds: list[wg.FoldData]) -> None:
    """Scrambling every label column must not change a single design number (AE / k-means / scaling use no labels)."""
    lab_cols = [c for c in we.LABEL_COLS_V2 if c in ev.columns]
    scr = ev.with_columns(
        pl.col("label_impulse").reverse(), pl.col("label_class").reverse(),
        *(pl.col(c).reverse() for c in lab_cols if c not in ("label_impulse", "label_class")))
    again = wg.prepare_folds(scr, pre, bounds, EMBARGO, cfg=CFG, seed=3)
    for a, b in zip(folds, again, strict=True):
        np.testing.assert_array_equal(a.xa_tr, b.xa_tr)
        for r in a.rep_tr:
            np.testing.assert_array_equal(a.rep_tr[r], b.rep_tr[r])
            np.testing.assert_array_equal(a.rep_te[r], b.rep_te[r])


def test_fit_sees_only_fold_train_events(monkeypatch: pytest.MonkeyPatch, ev: pl.DataFrame, pre: wm.Prefixes, bounds: list[datetime]) -> None:
    """Spy on ``fit_embedding``: the t0 set it fits on is exactly the fold's train events (never the test fold)."""
    seen: list[np.ndarray] = []
    orig = wm.fit_embedding

    def spy(events: pl.DataFrame, p: wm.Prefixes, **kw):
        seen.append(events["t0"].to_numpy().astype("datetime64[us]"))
        return orig(events, p, **kw)

    monkeypatch.setattr(wm, "fit_embedding", spy)
    wg.prepare_folds(ev, pre, bounds, EMBARGO, cfg=CFG, seed=3)
    assert len(seen) == len(bounds) - 1
    for blk, t in enumerate(seen):
        fold = we.cv_fold(ev, blk, bounds, EMBARGO)
        test_t0 = set(fold.test["t0"].to_numpy().astype("datetime64[us]").tolist())
        assert not (set(t.tolist()) & test_t0)
        assert set(t.tolist()) == set(fold.train["t0"].to_numpy().astype("datetime64[us]").tolist())


def test_refuses_non_train_events(ev: pl.DataFrame, pre: wm.Prefixes, bounds: list[datetime]) -> None:
    bad = ev.with_columns(pl.when(pl.col("t0") == ev["t0"][0]).then(pl.lit("oos")).otherwise(pl.col("split")).alias("split"))
    with pytest.raises(ValueError, match="train"):
        wm.fit_embedding(bad, pre.select(bad), cfg=CFG)


# --------------------------------------------------------------------------- permutation


def test_day_block_permutation_preserves_structure() -> None:
    day0 = np.datetime64("2024-01-01T00:00", "us")
    mins = [0, 600, 1500, 3000, 3100, 4500, 5900, 7300, 8800, 8900, 10300]       # mix of 1- and 2-event days
    t0 = np.array([day0 + np.timedelta64(m, "m") for m in mins])
    y = np.array([1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 1], dtype=float)
    blk = wg.day_blocks(t0)
    assert set(blk.groups) == {1, 2}
    out = wg.permute_day_blocks(y, blk, np.random.default_rng(1))
    assert out.sum() == y.sum()
    for g in blk.groups.values():                                                # sizes preserved, day vectors move intact
        orig = {tuple(y[row]) for row in g}
        new = [tuple(out[row]) for row in g]
        assert all(v in orig for v in new)
        assert sorted(new) == sorted(tuple(y[row]) for row in g)
    # a day's events keep their within-day label vector as a unit (2-event days: vectors drawn from the original 2-event days)
    two = {tuple(y[row]) for row in blk.groups[2]}
    assert all(tuple(out[row]) in two for row in blk.groups[2])


def test_permutation_changes_labels_but_not_day_structure_on_real_events(ev: pl.DataFrame) -> None:
    use = wg.usable_events(ev)
    y = use["label_impulse"].to_numpy().astype(float)
    blk = wg.day_blocks(use["t0"].to_numpy().astype("datetime64[us]"))
    outs = [wg.permute_day_blocks(y, blk, np.random.default_rng([3, i])) for i in range(5)]
    assert all(o.sum() == y.sum() for o in outs)
    assert any(not np.array_equal(o, y) for o in outs)
    assert not np.array_equal(outs[0], outs[1])
    assert sum(g.shape[0] for g in blk.groups.values()) == blk.n_days


def test_permutation_null_deterministic_and_refits_labels(folds: list[wg.FoldData], ev: pl.DataFrame) -> None:
    use = wg.usable_events(ev)
    y = use["label_impulse"].to_numpy().astype(float)
    blk = wg.day_blocks(use["t0"].to_numpy().astype("datetime64[us]"))
    n1 = wg.permutation_null(folds, y, blk, n_perm=6, seed=11)
    n2 = wg.permutation_null(folds, y, blk, n_perm=6, seed=11)
    for k in n1:
        np.testing.assert_array_equal(n1[k], n2[k])
    n3 = wg.permutation_null(folds, y, blk, n_perm=6, seed=12)
    assert not np.array_equal(n1["ae"], n3["ae"])
    # the null equals an explicit refit with the permuted labels (every label-dependent component refitted)
    yp = wg.permute_day_blocks(y, blk, np.random.default_rng([11, 0]))
    o = wg.oof_predictions(folds, yp)
    assert np.isclose(wg.auc(yp, o["ae"]) - wg.auc(yp, o["A"]), n1["ae"][0])
    assert np.isclose(wg.auc(yp, o["A"]), n1["auc_a"][0])


def test_oof_uses_only_train_labels(folds: list[wg.FoldData], ev: pl.DataFrame) -> None:
    """Changing the labels of one test fold's events does not change that fold's OOF predictions."""
    use = wg.usable_events(ev)
    y = use["label_impulse"].to_numpy().astype(float)
    base = wg.oof_predictions(folds, y)
    f0 = folds[0]
    y2 = y.copy()
    y2[f0.test_idx] = 1.0 - y2[f0.test_idx]
    alt = wg.oof_predictions(folds, y2)
    for k in base:
        np.testing.assert_array_equal(base[k][f0.test_idx], alt[k][f0.test_idx])
    assert not np.array_equal(base["A"][folds[1].test_idx], alt["A"][folds[1].test_idx])      # other folds train on them


def test_fold_cache_roundtrip(tmp_path: Path, folds: list[wg.FoldData]) -> None:
    p = tmp_path / "f.npz"
    wg.save_folds(folds, p, "k1")
    assert wg.load_folds(p, "other") is None
    back = wg.load_folds(p, "k1")
    assert back is not None and len(back) == len(folds)
    for a, b in zip(folds, back, strict=True):
        np.testing.assert_array_equal(a.test_idx, b.test_idx)
        np.testing.assert_array_equal(a.xa_te, b.xa_te)
        np.testing.assert_array_equal(a.rep_te["ae"], b.rep_te["ae"])


# --------------------------------------------------------------------------- gate logic


def _counts(ok: bool = True) -> dict:
    return {"block_ok": ok, "pooled_ok": ok, "min_block_events": 108, "pooled_impulse": 117, "pooled_non_impulse": 601}


def test_gate_logic_in_order() -> None:
    crit, v = wg.evaluate_gate(_counts(), 720, 0.03, 0.001, 5, 6)
    assert v == "feltételes GO" and crit[4].passed is None
    assert wg.evaluate_gate(_counts(), 720, 0.019, 0.001, 5, 6)[1] == "NO-GO"
    assert wg.evaluate_gate(_counts(), 720, 0.03, 0.02, 5, 6)[1] == "NO-GO"
    assert wg.evaluate_gate(_counts(), 720, 0.03, 0.001, 3, 6)[1] == "NO-GO"
    assert wg.evaluate_gate(_counts(False), 720, 0.03, 0.001, 6, 6)[1] == "NO-GO"
    assert wg.evaluate_gate(_counts(), 400, 0.03, 0.001, 6, 6)[1] == "NO-GO"
    assert wg.perm_p(0.5, np.zeros(999)) == 1 / 1000
    assert wg.perm_p(-1.0, np.zeros(999)) == 1.0


# --------------------------------------------------------------------------- end-to-end report


def test_analyse_deterministic_and_report(tmp_path: Path, df: pl.DataFrame, ds: we.EventDataset) -> None:
    kw = dict(df_1m=df, events=ds.events, params=PARAMS, cell=CELL, cfg=CFG, seed=3, n_perm=8, n_boot=30)
    out = wg.build(tmp_path / "r1", **kw)
    out2 = wg.build(tmp_path / "r2", cache_dir=tmp_path / "cache", **kw)
    assert out is not None and out2 is not None
    m1 = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    Manifest.model_validate(m1)
    t1 = (out / "tables" / "summary.json").read_text(encoding="utf-8")
    t2 = (out2 / "tables" / "summary.json").read_text(encoding="utf-8")
    assert t1 == t2                                                              # deterministic (cached folds == fresh folds)
    out3 = wg.build(tmp_path / "r3", cache_dir=tmp_path / "cache", **kw)         # second run uses the cache
    assert out3 is not None
    assert (out3 / "tables" / "summary.json").read_text(encoding="utf-8") == t1
    text = (out / "report.md").read_text(encoding="utf-8")
    assert re.search(r"Cella-döntés: (NO-GO|feltételes GO)", text)
    dates = re.findall(r"20(?:2[6-9]|[3-9]\d)-\d{2}-\d{2}", text)
    assert all(d < "2026-02-01" or d >= "2026-11-01" for d in dates) or not dates
