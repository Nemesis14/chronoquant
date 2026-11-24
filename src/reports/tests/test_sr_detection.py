"""``reports.builders.sr_detection`` on synthetic 1m data (no real database)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import plotly.graph_objects as go
import polars as pl
import pytest

from reports import store
from reports.builders import sr_detection as sr
from reports.schema import MAX_CANDLE_POINTS, Manifest

PARAMS = {
    "atr_window": 14, "pivot_n": 3, "pivot_min_prominence_atr": 1.0, "level_tolerance_atr": 0.25,
    "level_min_touches": 2, "strength_tau_frac": 1 / 3, "round_step": 10.0, "round_step_secondary": 5.0,
    "line_tolerance_atr": 0.25, "line_min_touches": 3, "line_dedup_slope_atr": 0.1, "break_atr": 0.5,
    "retest_bars": 12, "forward_bars": 24, "forward_move_atr": 1.0,
    "scales": {"s": {"bar": "1h", "lookback_days": 5}, "m": {"bar": "4h", "lookback_days": 15},
               "l": {"bar": "1d", "lookback_days": 40}},
}


def _synthetic_1m(days: int = 70, seed: int = 7) -> pl.DataFrame:
    """Range-bound price (100 <-> 110 swings with noise) that breaks out upward in the last third."""
    rng = np.random.default_rng(seed)
    n = days * 1440
    t = np.arange(n)
    base = 105 + 5 * np.sin(2 * np.pi * t / (1440 * 2.5)) + 2 * np.sin(2 * np.pi * t / (1440 * 9))
    base[2 * n // 3:] += np.linspace(0, 15, n - 2 * n // 3)
    close = base + np.cumsum(rng.normal(0, 0.02, n)) * 0.2
    open_ = np.concatenate(([close[0]], close[:-1]))
    wick = np.abs(rng.normal(0, 0.06, n))
    start = datetime(2025, 1, 1)
    return pl.DataFrame({
        "open_time": [start + timedelta(minutes=int(i)) for i in t],
        "open": open_, "high": np.maximum(open_, close) + wick, "low": np.minimum(open_, close) - wick,
        "close": close, "volume": np.ones(n),
    })


@pytest.fixture(scope="module")
def report(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("repo")
    out = sr.build(root=root, df_1m=_synthetic_1m(), params=PARAMS)
    assert out is not None
    return out


def test_manifest_and_store(report: Path) -> None:
    manifest = Manifest.model_validate_json((report / "manifest.json").read_text(encoding="utf-8"))
    assert manifest.scope == "general" and manifest.id == sr.SLUG and manifest.model_id is None
    root = report.parents[2]
    listed = store.list_reports("general", None, root)
    assert [r["id"] for r in listed] == [sr.SLUG]
    data, md = store.read_report("general", sr.SLUG, root)
    assert data["id"] == sr.SLUG
    assert "NYERS" in md and "placebo" in md
    charts = {c.id for c in manifest.content.charts}
    tables = {t.id for t in manifest.content.tables}
    assert {"overview_4h_m_l", "overview_1h_s", "event_monthly"} <= charts
    assert {"params", "scale_summary", "level_touch_dist", "line_summary", "event_counts", "event_yearly",
            "forward_raw", "active_levels_now"} <= tables
    for cid in charts:
        assert "{{chart:" + cid + "}}" in md
    for tid in tables:
        assert "{{table:" + tid + "}}" in md


def test_charts_are_plain_candles_within_limits(report: Path) -> None:
    for path in (report / "charts").glob("*.json"):
        fig = json.loads(path.read_text(encoding="utf-8"))
        assert "template" not in fig["layout"] and "bdata" not in json.dumps(fig)
        for trace in fig["data"]:
            assert trace.get("x"), f"empty trace in {path.name}"
            if trace.get("type") == "candlestick":
                assert len(trace["x"]) <= MAX_CANDLE_POINTS
                assert fig["layout"]["xaxis"]["rangeslider"]["visible"] is False


def test_tables_have_all_scales(report: Path) -> None:
    summary = json.loads((report / "tables" / "scale_summary.json").read_text(encoding="utf-8"))
    keys = [c["key"] for c in summary["columns"]]
    scales = [row[keys.index("scale")] for row in summary["rows"]]
    assert scales == ["s", "m", "l"]
    assert all(row[keys.index("pivots")] > 0 for row in summary["rows"][:2])
    fwd = json.loads((report / "tables" / "forward_raw.json").read_text(encoding="utf-8"))
    rate_cols = [c for c in fwd["columns"] if c["key"].endswith("_rate")]
    assert rate_cols and all(c["type"] == "pct" for c in rate_cols)


def test_example_selection_deterministic_and_unique() -> None:
    def ex(cat: str, scale: str, obj: str, day: int) -> sr.Example:
        return sr.Example(cat, scale, "level", obj, datetime(2025, 1, 1) + timedelta(days=day), 0, 24, 3, "x", "n", [0])

    cands: dict[str, dict[str, list[sr.Example]]] = {c: {} for c in sr.CATEGORIES}
    cands["bounce"] = {"l": [ex("bounce", "l", "A", 0)],
                       "m": [ex("bounce", "m", "B", 10), ex("bounce", "m", "C", 20)],
                       "s": [ex("bounce", "s", "D", 30)]}
    # B: object already used; F: key event 2 days after A (same episode) -> both skipped
    cands["role_flip"] = {"m": [ex("role_flip", "m", "B", 40), ex("role_flip", "m", "F", 2),
                                ex("role_flip", "m", "E", 50)]}
    picked = sr.select_examples(cands, per_category=3)
    assert [(e.category, e.obj_id) for e in picked] == [
        ("bounce", "A"), ("bounce", "B"), ("bounce", "D"), ("role_flip", "E")]
    assert picked == sr.select_examples(cands, per_category=3)


def test_segments_start_at_confirmation_and_split_on_flip() -> None:
    t0 = datetime(2025, 1, 1)
    grid = [t0 + timedelta(hours=h) for h in range(48)]
    conf = np.array([np.datetime64(t0 + timedelta(hours=10), "us")] * 2)
    piv = np.array([np.datetime64(t0 + timedelta(hours=3), "us"), np.datetime64(t0 + timedelta(hours=7), "us")])
    obj = sr._Obj("level", "s-L000001", "s", t0 + timedelta(hours=10), t0 + timedelta(hours=40),  # noqa: SLF001
                  lambda _t: 100.0, "support", [t0 + timedelta(hours=20)], conf, piv, 5 * 86400 / 3,
                  info={"price": 100.0, "tolerance": 0.1, "n_touches": 2, "is_round": True,
                        "is_round_secondary": False, "n_role_flips": 1})
    fig = go.Figure()
    n_seg = sr._add_objects(fig, [obj], grid, grid[0], grid[-1], dash="solid", label="s szint", legend_rank=0)  # noqa: SLF001
    assert n_seg >= 2  # split at the role flip (and where the decaying strength crosses a tier edge)
    traces: list[Any] = list(fig.data)  # plotly stubs mistype Figure.data
    xs = [x for tr in traces for x in tr.x if x is not None]
    assert min(xs) == t0 + timedelta(hours=10) and max(xs) == t0 + timedelta(hours=40)
    names = {tr.name for tr in traces}
    assert names == {"s szint – támasz", "s szint – ellenállás"}
