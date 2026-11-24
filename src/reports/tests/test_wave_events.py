"""``reports.builders.wave_events`` + ``wave_event_dataset`` on synthetic 1m data (no real database).

Covers the 5200 validation checklist: look-ahead (truncated vs full run), label only from ``(t_c, t_c + H]``,
causal sigma, full-interval non-overlap, split purge, prefix window, lab persistence and the report (train-only).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import pytest

from reports.builders import wave_event_dataset as wr
from reports.builders import wave_events as we
from reports.schema import Manifest

START = datetime(2025, 1, 1)
DAYS = 30
PARAMS = we.WaveParams(
    cells=(("W1h_H1h", 60, 60), ("W2h_H2h", 120, 120)),
    sigma_window=1440, trend_bars=720, velocity_bars=30,
    train_start=str((START + timedelta(days=2)).date()), oos_start=str((START + timedelta(days=20)).date()),
    holdout2_start=str((START + timedelta(days=25)).date()),
)
COMPARE = (*we.FEATURE_COLS, *we.EVENT_COLS, "t_c", "event_start", "split")


def _synthetic_1m(days: int = DAYS, seed: int = 11) -> pl.DataFrame:
    """Random walk with volatility regimes (enough confirmed lows of all three label classes)."""
    rng = np.random.default_rng(seed)
    n = days * 1440
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


def _labelled_mid(ds: we.EventDataset, k: int = 6) -> list[dict]:
    ev = ds.events.filter(pl.col("label_class").is_not_null())
    assert ev.height >= k, "synthetic data produced too few events"
    idx = np.linspace(ev.height // 4, ev.height - 2, k).astype(int)
    return [ev.row(int(i), named=True) for i in idx]


def _same(a: dict, b: dict, cols: tuple[str, ...]) -> None:
    for c in cols:
        va, vb = a[c], b[c]
        if isinstance(va, float):
            assert np.isclose(va, vb, rtol=1e-9, atol=1e-12, equal_nan=True), (c, va, vb)
        else:
            assert va == vb, (c, va, vb)


def _find(ds: we.EventDataset, row: dict) -> dict:
    hit = ds.events.filter((pl.col("cell_id") == row["cell_id"]) & (pl.col("t0") == row["t0"]))
    assert hit.height == 1, f"event {row['cell_id']} {row['t0']} missing"
    return hit.row(0, named=True)


def test_dataset_has_all_classes(ds: we.EventDataset) -> None:
    assert ds.events.height > 0
    assert set(ds.events["label_class"].drop_nulls().unique()) == set(we.CLASS_ORDER)
    assert (ds.events["a_sigma"] >= PARAMS.min_a_sigma - 1e-12).all()
    assert set(ds.funnel["cell_id"]) == {c[0] for c in PARAMS.cells}


def test_lookahead_truncated_vs_full(df: pl.DataFrame, ds: we.EventDataset) -> None:
    """Data cut right after t_c (bars with close <= t_c) reproduces the event, its features and its prefix."""
    for row in _labelled_mid(ds):
        trunc = df.filter(pl.col("open_time") < row["t_c"])
        r2 = _find(we.build_events(trunc, PARAMS), row)
        _same(row, r2, tuple(c for c in COMPARE if c != "split"))
        assert r2["label_class"] is None and r2["split"] == "purged"


def test_label_only_from_horizon(df: pl.DataFrame, ds: we.EventDataset) -> None:
    """Changing bars after t_c + H changes nothing; changing (t_c, t_c + H] changes no feature."""
    for row in _labelled_mid(ds, 3):
        after = df.with_columns(
            pl.when(pl.col("open_time") >= row["label_end"]).then(pl.col(c) * 3).otherwise(pl.col(c)).alias(c)
            for c in ("open", "high", "low", "close"))
        _same(row, _find(we.build_events(after, PARAMS), row), (*COMPARE, *we.LABEL_COLS))
        inside = df.with_columns(
            pl.when((pl.col("open_time") >= row["t_c"]) & (pl.col("open_time") < row["label_end"]))
            .then(pl.col(c) * 1.5).otherwise(pl.col(c)).alias(c) for c in ("open", "high", "low", "close"))
        r3 = _find(we.build_events(inside, PARAMS), row)
        _same(row, r3, COMPARE)
        assert r3["label_class"] == "impulse"          # +50% inside the horizon must cross P*


def test_sigma_uses_only_bars_closed_before_t0(df: pl.DataFrame) -> None:
    m = we.to_minute_grid(df)
    roll = we.rolling_inputs(m, PARAMS)
    i = 5000
    ret = np.diff(np.log(m.close))                    # ret[j - 1] = log(c[j] / c[j - 1])
    win = ret[i - PARAMS.sigma_window - 1:i - 1]      # bars i - window .. i - 1
    assert np.isclose(roll.sigma_1m[i], np.std(win, ddof=1))
    assert np.isclose(roll.vol_mean[i], m.volume[i - PARAMS.sigma_window:i].mean())


def test_non_overlap_and_purge(ds: we.EventDataset) -> None:
    p = ds.params
    for cell_id, _, _ in p.cells:
        e = ds.events.filter(pl.col("cell_id") == cell_id).sort("t0")
        assert (e["event_start"].slice(1) >= e["label_end"].slice(0, e.height - 1)).all()
    bounds = {"train": (p.train_start, p.oos_start), "oos": (p.oos_start, p.holdout2_start)}
    for split, (lo, hi) in bounds.items():
        g = ds.events.filter(pl.col("split") == split)
        assert g.height > 0
        assert (g["event_start"] >= datetime.fromisoformat(lo)).all()
        assert (g["label_end"] <= datetime.fromisoformat(hi)).all()
    assert ds.events.filter(pl.col("split") != "purged")["label_class"].null_count() == 0


def test_prefix_window(df: pl.DataFrame, ds: we.EventDataset) -> None:
    b15 = we.to_bars15(we.to_minute_grid(df), PARAMS)
    ev = ds.events
    assert (ev["prefix_end"] <= ev["t_c"]).all() and (ev["prefix_start"] >= ev["event_start"]).all()
    span = (ev["prefix_end"] - ev["prefix_start"]).dt.total_minutes()
    assert (span == ev["n_prefix_15m"] * 15).all()
    for row in _labelled_mid(ds, 3):
        img = we.prefix_image(b15, row)
        assert img["close"].size == row["n_prefix_15m"]
        assert not np.isnan(np.concatenate(list(img.values()))).any()
        assert img["low"].min() >= -1e-9                # low[t0] is the window minimum


def test_feature_columns_exclude_labels() -> None:
    assert not set(we.FEATURE_COLS) & set(we.LABEL_COLS)
    assert not set(we.EVENT_COLS) & set(we.LABEL_COLS)


def test_persist_roundtrip(tmp_path: Path, ds: we.EventDataset) -> None:
    lab = tmp_path / "lab.duckdb"
    meta = we.persist(ds, lab_path=str(lab))
    con = duckdb.connect(str(lab), read_only=True)
    try:
        n = con.execute(f'SELECT count(*) FROM {we.SCHEMA}."{we.TABLE}"').fetchone()
        m = con.execute(f'SELECT content_hash, n_events FROM {we.SCHEMA}."{we.TABLE}__meta"').fetchone()
    finally:
        con.close()
    assert n is not None and n[0] == ds.events.height
    assert m is not None and m[0] == meta["content_hash"] == we.content_hash(ds.events) and m[1] == ds.events.height


def test_report_is_train_only(tmp_path: Path, df: pl.DataFrame) -> None:
    out = wr.build(root=tmp_path, df_1m=df, params=PARAMS, store=False)
    assert out is not None
    manifest = Manifest.model_validate_json((out / "manifest.json").read_text(encoding="utf-8"))
    assert manifest.scope == "general" and manifest.id == wr.SLUG
    md = (out / "report.md").read_text(encoding="utf-8")
    for ch in manifest.content.charts:
        assert "{{chart:" + ch.id + "}}" in md and (out / ch.file).exists()
    for tb in manifest.content.tables:
        assert "{{table:" + tb.id + "}}" in md and (out / tb.file).exists()
    ex = json.loads((out / "tables" / "examples.json").read_text(encoding="utf-8"))
    keys = [c["key"] for c in ex["columns"]]
    oos0 = np.datetime64(PARAMS.oos_start)
    assert ex["rows"], "no examples selected"
    for r in ex["rows"]:
        assert np.datetime64(r[keys.index("t_c")].rstrip("Z")) < oos0
    assert any(c.id.startswith("input_") for c in manifest.content.charts)


def test_select_examples_deterministic(ds: we.EventDataset) -> None:
    a = wr.select_examples(ds.events, "W1h_H1h")
    b = wr.select_examples(ds.events, "W1h_H1h")
    assert [r["t0"] for r in a] == [r["t0"] for r in b]
    assert all(r["split"] == "train" for r in a)


# --------------------------------------------------------------------------- v2 (t925): first-touch label, cv blocks

from dataclasses import replace  # noqa: E402

PARAMS_V2 = replace(PARAMS, version="v2")


@pytest.fixture(scope="module")
def ds2(df: pl.DataFrame) -> we.EventDataset:
    return we.build_events(df, PARAMS_V2)


def test_first_touch_class_rules() -> None:
    f = we.first_touch_class
    assert f(None, None) == "sideways"
    assert f(5, None) == "impulse" and f(None, 5) == "false_start"
    assert f(3, 5) == "impulse"                      # target first
    assert f(5, 3) == "false_start"                  # t0 break first, although the target is hit later
    assert f(4, 4) == "false_start"                  # same bar: conservative
    assert f(4, 4, "impulse") == "impulse"           # sensitivity tie-break


def _craft(df: pl.DataFrame, row: dict, bars: dict[int, dict[str, float]]) -> pl.DataFrame:
    """Flatten the label window (t_c, t_c + H] to close[t_c] and overwrite single bars (offset j = bar opening t_c + j)."""
    pos = int((row["t_c"] - START).total_seconds() // 60)
    n = int((row["label_end"] - row["t_c"]).total_seconds() // 60)
    out = df
    for col in ("open", "high", "low", "close"):
        arr = df[col].to_numpy().copy()
        arr[pos:pos + n] = row["close_tc"]
        for j, vals in bars.items():
            if col in vals:
                arr[pos + j] = vals[col]
        out = out.with_columns(pl.Series(col, arr))
    return out


def _label(df: pl.DataFrame, row: dict, params: we.WaveParams, bars: dict[int, dict[str, float]]) -> dict:
    return _find(we.build_events(_craft(df, row, bars), params), row)


def test_v2_first_touch_label_and_same_bar(df: pl.DataFrame, ds2: we.EventDataset) -> None:
    for row in _labelled_mid(ds2, 3):
        up, dn = row["p_star"] * 1.001, row["low_t0"] * 0.999
        flat = _label(df, row, PARAMS_V2, {})
        assert flat["label_class"] == "sideways" and not flat["same_bar"] and flat["label_impulse"] is False
        r = _label(df, row, PARAMS_V2, {5: {"high": up}})
        assert r["label_class"] == "impulse" and r["label_impulse"] is True and r["mins_to_target"] == 6
        r = _label(df, row, PARAMS_V2, {5: {"low": dn}})
        assert r["label_class"] == "false_start" and r["label_impulse"] is False
        # target at bar 5, t0 break at bar 9 -> impulse; break at bar 3, target at bar 5 -> false start
        r = _label(df, row, PARAMS_V2, {5: {"high": up}, 9: {"low": dn}})
        assert r["label_class"] == "impulse" and not r["same_bar"]
        r = _label(df, row, PARAMS_V2, {3: {"low": dn}, 5: {"high": up}})
        assert r["label_class"] == "false_start" and not r["same_bar"] and r["first_hit"] == "t0_break"
        # same bar: both levels on bar 4 -> false start (conservative), flagged; opposite tie-break -> impulse
        both = {4: {"high": up, "low": dn}}
        r = _label(df, row, PARAMS_V2, both)
        assert r["label_class"] == "false_start" and r["same_bar"] and r["first_hit"] == "same_bar"
        r = _label(df, row, replace(PARAMS_V2, tie_break="impulse"), both)
        assert r["label_class"] == "impulse" and r["same_bar"] and r["label_impulse"] is True


def test_v2_label_ignores_bars_outside_window(df: pl.DataFrame, ds2: we.EventDataset) -> None:
    """A target-crossing bar right after t_c + H or right before t_c (closed <= t_c) does not touch the label."""
    for row in _labelled_mid(ds2, 3):
        up = row["p_star"] * 1.01
        pos_tc = int((row["t_c"] - START).total_seconds() // 60)
        pos_end = int((row["label_end"] - START).total_seconds() // 60)
        base = _craft(df, row, {})
        arr = base["high"].to_numpy().copy()
        arr[pos_end] = up
        arr[pos_tc - 1] = up
        r = _find(we.build_events(base.with_columns(pl.Series("high", arr)), PARAMS_V2), row)
        assert r["label_class"] == "sideways"


def test_v2_truncated_run_reproduces_features(df: pl.DataFrame, ds2: we.EventDataset) -> None:
    for row in _labelled_mid(ds2):
        r2 = _find(we.build_events(df.filter(pl.col("open_time") < row["t_c"]), PARAMS_V2), row)
        _same(row, r2, tuple(c for c in COMPARE if c != "split"))
        assert r2["label_class"] is None and r2["same_bar"] is None


def test_v2_class_names_and_binary_target(ds2: we.EventDataset) -> None:
    ev = ds2.events.filter(pl.col("label_class").is_not_null())
    assert set(ev["label_class"].unique()) <= set(we.CLASS_ORDER_V2)
    assert ((ev["label_class"] == "impulse") == ev["label_impulse"]).all()
    assert not set(we.FEATURE_COLS) & set(we.LABEL_COLS_V2)


def test_v1_behaviour_unchanged(df: pl.DataFrame, ds: we.EventDataset, ds2: we.EventDataset, tmp_path: Path) -> None:
    assert we.WaveParams().version == "v1" and we.WaveParams().cells == we.CELLS
    assert set(ds.events.columns) == set(we._SCHEMA)           # no v2 columns in v1
    assert set(ds.events["label_class"].drop_nulls().unique()) == set(we.CLASS_ORDER)
    assert we.params_v2().cells == we.CELLS_V2 == (("W4h_H4h", 240, 240), ("W4h_H8h", 240, 480))
    # identical events and split in both versions (only the label differs)
    assert ds.events.select("cell_id", "t0", "split").equals(ds2.events.select("cell_id", "t0", "split"))
    # v1 label ignores the order: break at bar 3, target at bar 5 -> v1 impulse, v2 false start
    row = _labelled_mid(ds, 1)[0]
    bars = {3: {"low": row["low_t0"] * 0.999}, 5: {"high": row["p_star"] * 1.001}}
    assert _label(df, row, PARAMS, bars)["label_class"] == "impulse"
    assert _label(df, row, PARAMS_V2, bars)["label_class"] == "false_start"
    # persistence goes to its own version table; the v1 table is untouched by a v2 write
    lab = tmp_path / "lab.duckdb"
    we.persist(ds, lab_path=str(lab))
    we.persist(ds2, lab_path=str(lab))
    con = duckdb.connect(str(lab), read_only=True)
    try:
        n1 = con.execute('SELECT count(*) FROM research."wave_events_v1"').fetchone()
        n2 = con.execute('SELECT count(*) FROM research."wave_events_v2"').fetchone()
        cols1 = [r[0] for r in con.execute('DESCRIBE research."wave_events_v1"').fetchall()]
        v = con.execute('SELECT version FROM research."wave_events_v2__meta"').fetchone()
    finally:
        con.close()
    assert n1 is not None and n1[0] == ds.events.height and "same_bar" not in cols1
    assert n2 is not None and n2[0] == ds2.events.height and v == ("v2",)


def test_cv_blocks_assignment(ds2: we.EventDataset) -> None:
    b = we.cv_block_bounds(PARAMS_V2)
    assert len(b) == 7 and b[0] == datetime.fromisoformat(PARAMS_V2.train_start)
    assert b[-1] == datetime.fromisoformat(PARAMS_V2.oos_start)
    assert all(x < y for x, y in zip(b, b[1:], strict=False))
    ev = ds2.events
    tr, rest = ev.filter(pl.col("split") == "train"), ev.filter(pl.col("split") != "train")
    assert tr["cv_block"].null_count() == 0 and set(tr["cv_block"].unique()) <= set(range(6))
    assert rest["cv_block"].null_count() == rest.height
    for r in tr.iter_rows(named=True):
        k = r["cv_block"]
        assert b[k] <= r["t0"] < b[k + 1]
        assert r["cv_straddle"] == (r["event_start"] < b[k] or r["label_end"] > b[k + 1])


def _ev(start: str, end: str, block: int, straddle: bool = False, cls: str = "sideways") -> dict:
    return {"event_start": datetime.fromisoformat(start), "label_end": datetime.fromisoformat(end), "cv_block": block,
            "cv_straddle": straddle, "split": "train", "label_class": cls}


def test_cv_fold_purge_and_embargo() -> None:
    bounds = [datetime(2025, 1, 1) + timedelta(days=10 * k) for k in range(7)]       # block 1 = Jan 11 .. Jan 21
    h = 240                                                                           # embargo = 4h
    rows = [
        _ev("2025-01-11 03:00", "2025-01-11 15:00", 1),                               # test
        _ev("2025-01-10 22:00", "2025-01-11 06:00", 1, straddle=True),                # straddles the lower edge: no test
        _ev("2025-01-09 00:00", "2025-01-09 08:00", 0),                               # far before: kept
        _ev("2025-01-10 10:00", "2025-01-10 18:00", 0),                               # ends 6h before: kept
        _ev("2025-01-10 14:00", "2025-01-10 22:30", 0),                               # ends 1.5h before: embargoed
        _ev("2025-01-10 16:00", "2025-01-10 20:00", 0, straddle=True),                # straddler of another block: out
        _ev("2025-01-21 02:00", "2025-01-21 10:00", 2),                               # starts 2h after: embargoed
        _ev("2025-01-21 04:00", "2025-01-21 12:00", 2),                               # starts exactly H after: kept
        _ev("2025-01-25 00:00", "2025-01-25 08:00", 2),                               # far after: kept
        _ev("2025-01-20 20:00", "2025-01-20 23:00", 1),                               # test
    ]
    fold = we.cv_fold(pl.DataFrame(rows), 1, bounds, h)
    assert fold.test.height == 2 and not fold.test["cv_straddle"].any()
    kept = {(r["event_start"], r["label_end"]) for r in fold.train.iter_rows(named=True)}
    assert (datetime(2025, 1, 10, 14), datetime(2025, 1, 10, 22, 30)) not in kept
    assert (datetime(2025, 1, 21, 2), datetime(2025, 1, 21, 10)) not in kept
    assert (datetime(2025, 1, 10, 16), datetime(2025, 1, 10, 20)) not in kept
    assert (datetime(2025, 1, 21, 4), datetime(2025, 1, 21, 12)) in kept
    assert fold.n_straddle == 1 and fold.n_embargoed == 2 and fold.n_purged == 0 and fold.train.height == 4
    lo, hi, emb = bounds[1], bounds[2], timedelta(minutes=h)
    for r in fold.train.iter_rows(named=True):
        assert r["label_end"] <= lo - emb or r["event_start"] >= hi + emb
    # embargo 0 -> only the true overlap is purged; a non-straddling event overlapping the block is purged
    rows2 = [*rows, _ev("2025-01-20 22:00", "2025-01-21 02:00", 2)]
    f2 = we.cv_fold(pl.DataFrame(rows2), 1, bounds, 0)
    assert f2.n_purged == 1 and f2.n_embargoed == 0


def test_cv_fold_real_dataset_no_overlap(ds2: we.EventDataset) -> None:
    b = we.cv_block_bounds(PARAMS_V2)
    for cell_id, _, h in PARAMS_V2.cells:
        e = ds2.events.filter(pl.col("cell_id") == cell_id)
        emb = timedelta(minutes=h)
        for k in range(6):
            fold = we.cv_fold(e, k, b, h)
            assert fold.test.height + fold.train.height + fold.n_straddle + fold.n_purged + fold.n_embargoed <= \
                e.filter(pl.col("split") == "train").height
            assert (fold.test["cv_block"] == k).all()
            for r in fold.train.iter_rows(named=True):
                assert r["label_end"] <= b[k] - emb or r["event_start"] >= b[k + 1] + emb


def test_cv_summary_checks() -> None:
    bounds = [datetime(2025, 1, 1) + timedelta(days=10 * k) for k in range(4)]
    rows = [_ev(f"2025-01-{1 + 10 * k:02d} 01:00", f"2025-01-{1 + 10 * k:02d} 02:00", k, cls="impulse" if i % 2 else "sideways")
            for k in range(3) for i in range(4)]
    d, ck = we.cv_summary(pl.DataFrame(rows), bounds, min_block=4, min_pooled=6)
    assert d["n_test"].to_list() == [4, 4, 4] and ck["pooled_impulse"] == 6 and ck["pass"]
    _, ck2 = we.cv_summary(pl.DataFrame(rows), bounds, min_block=5, min_pooled=6)
    assert not ck2["block_ok"] and not ck2["pass"]
    _, ck3 = we.cv_summary(pl.DataFrame(rows), bounds, min_block=4, min_pooled=7)
    assert not ck3["pooled_ok"]


def test_v2_report_train_only(tmp_path: Path, df: pl.DataFrame) -> None:
    out = wr.build(root=tmp_path, df_1m=df, params=PARAMS_V2, store=False)
    assert out is not None
    manifest = Manifest.model_validate_json((out / "manifest.json").read_text(encoding="utf-8"))
    md = (out / "report.md").read_text(encoding="utf-8")
    for ch in manifest.content.charts:
        assert "{{chart:" + ch.id + "}}" in md and (out / ch.file).exists()
    ex = json.loads((out / "tables" / "examples.json").read_text(encoding="utf-8"))
    keys = [c["key"] for c in ex["columns"]]
    assert ex["rows"]
    for r in ex["rows"]:
        assert np.datetime64(r[keys.index("t_c")].rstrip("Z")) < np.datetime64(PARAMS_V2.oos_start)
    oos = json.loads((out / "tables" / "oos_counts.json").read_text(encoding="utf-8"))
    assert {c["key"] for c in oos["columns"]} >= {"sum_cells", "unique_t0"}
