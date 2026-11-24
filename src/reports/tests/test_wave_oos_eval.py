# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportOperatorIssue=false, reportIndexIssue=false, reportMissingImports=false, reportOptionalMemberAccess=false
"""``reports.builders.wave_oos_eval`` (t928): holdout-2 is never loaded, the OOS labels never reach the fit, the tables are consistent.

Synthetic 1m data (no real database); needs the ``research`` group (torch, tslearn).
"""

from __future__ import annotations

from datetime import datetime

import numpy as np
import polars as pl
import pytest

pytest.importorskip("torch")
pytest.importorskip("tslearn")
pytest.importorskip("sklearn")

from reports.builders import wave_events as we  # noqa: E402
from reports.builders import wave_image_model as wm  # noqa: E402
from reports.builders import wave_oos_eval as oe  # noqa: E402
from reports.tests.test_wave_predictiveness_gate import (  # noqa: E402
    CELL,
    CFG,
    PARAMS,
    _synthetic_1m,
)

HO0 = datetime.fromisoformat(PARAMS.holdout2_start)


@pytest.fixture(scope="module")
def df() -> pl.DataFrame:
    return _synthetic_1m()


@pytest.fixture(scope="module")
def ds(df: pl.DataFrame) -> we.EventDataset:
    return we.build_events(df, PARAMS)


def test_ohlcv_loader_never_returns_holdout_rows(df: pl.DataFrame, monkeypatch: pytest.MonkeyPatch) -> None:
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA live")
    con.register("src", df.to_pandas())
    con.execute("CREATE TABLE live.ohlcv AS SELECT * FROM src")
    assert df["open_time"].max() >= HO0                                           # the fixture does contain holdout-2 rows

    class _Keep:
        """Connection proxy whose close() is a no-op so the test can inspect the shared in-memory db."""

        def execute(self, *a, **k):
            return con.execute(*a, **k)

        def close(self) -> None:
            pass

    monkeypatch.setattr("data_handling.store.readonly_query.open_readonly_connection", lambda *a, **k: _Keep())
    out = oe.load_1m_capped(HO0)
    assert out is not None and out.height > 0
    assert out["open_time"].max() < HO0
    assert out.height == df.filter(pl.col("open_time") < HO0).height
    oe.assert_no_holdout(out, HO0)
    with pytest.raises(ValueError):
        oe.assert_no_holdout(df, HO0)


def test_assert_no_holdout_events(ds: we.EventDataset) -> None:
    ho = ds.events.filter(pl.col("split") == "holdout2")
    assert ho.height > 0
    with pytest.raises(ValueError):
        oe.assert_no_holdout(ho, HO0)
    oos = ds.events.filter(pl.col("split") == "oos")
    oe.assert_no_holdout(oos, HO0)


def test_oos_scores_independent_of_oos_labels_and_run_cell(df: pl.DataFrame, ds: we.EventDataset, tmp_path) -> None:
    capped = df.filter(pl.col("open_time") < HO0)
    ev_tr = ds.events.filter((pl.col("cell_id") == CELL) & (pl.col("split") == "train")).sort("t0")
    ev_te = ds.events.filter((pl.col("cell_id") == CELL) & (pl.col("split") == "oos")).sort("t0")
    assert ev_te.height >= 5
    b15 = we.to_bars15(we.to_minute_grid(capped), PARAMS)
    pre_tr, pre_te = wm.build_prefixes(b15, ev_tr), wm.build_prefixes(b15, ev_te)
    s1 = oe.fit_predict_oos(ev_tr, ev_te, pre_tr, pre_te, cfg=CFG, seed=3)
    scr = ev_te.with_columns(pl.col("label_impulse").not_(), pl.lit("sideways").alias("label_class"), pl.col("fwd_max_high") * 2)
    s2 = oe.fit_predict_oos(ev_tr, scr, pre_tr, pre_te, cfg=CFG, seed=3)
    for k in oe.MODELS:
        np.testing.assert_array_equal(s1[k], s2[k])
        assert s1[k].shape == (ev_te.height,) and ((s1[k] > 0) & (s1[k] < 1)).all()

    res = oe.run_cell(CELL, capped, ds.events, ds.events, PARAMS, cfg=CFG, cache_dir=tmp_path, seed=3)
    assert datetime.fromisoformat(res["oos_label_end_max"]) <= HO0
    o = res["oos"]
    assert o["classes"]["n"] == ev_te.height
    assert o["classes"]["impulse"] + o["classes"]["false_start"] + o["classes"]["sideways"] == ev_te.height
    for k in oe.MODELS:
        assert sum(r["n"] for r in o["quintiles"][k]) == ev_te.height
        for t in o["thresholds"][k].values():
            assert t["hits"] <= t["n_signals"] and t["false_start"] + t["sideways"] + t["hits"] == t["n_signals"]
