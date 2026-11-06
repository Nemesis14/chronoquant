"""Unit tests for ReportBuilder + report store (no DB, tmp dirs only)."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl
import pytest

from reports import ReportBuilder, store
from reports.builder import figure_to_json, table_to_json

MODEL = "lgbm_test_l_fw60"


def _builder(tmp_path: Path, slug: str = "01_sampling", **kw) -> ReportBuilder:
    return ReportBuilder(
        scope="model", model_id=MODEL, slug=slug, title="Sampling", order=10, root=tmp_path, **kw
    )


def _fig(n: int = 10) -> go.Figure:
    x = pd.date_range("2026-01-01", periods=n, freq="min")
    return go.Figure(go.Scatter(x=x, y=np.arange(n, dtype=float), name="y"))


def test_figure_json_plain_lists_no_template() -> None:
    fig = _fig(50)
    fig.update_layout(template="plotly_dark")
    out = figure_to_json(fig)
    assert "template" not in out["layout"]
    trace = out["data"][0]
    assert isinstance(trace["y"], list) and trace["y"][:3] == [0.0, 1.0, 2.0]
    assert "bdata" not in json.dumps(out)
    json.dumps(out, allow_nan=False)


def test_figure_nan_becomes_null() -> None:
    fig = go.Figure(go.Scatter(x=[1, 2, 3], y=[1.0, float("nan"), float("inf")]))
    assert figure_to_json(fig)["data"][0]["y"] == [1.0, None, None]


def test_figure_thinning_limits_points_keeps_ends() -> None:
    n = 20_000
    fig = go.Figure(go.Scatter(x=np.arange(n), y=np.arange(n, dtype=float)))
    trace = figure_to_json(fig)["data"][0]
    assert len(trace["x"]) == len(trace["y"]) <= 5000
    assert trace["x"][0] == 0 and trace["x"][-1] == n - 1


def test_candlestick_limit_2000_and_ohlc_aligned() -> None:
    n = 6000
    v = np.arange(n, dtype=float)
    fig = go.Figure(go.Candlestick(x=np.arange(n), open=v, high=v + 1, low=v - 1, close=v))
    t = figure_to_json(fig)["data"][0]
    assert len(t["x"]) <= 2000
    assert len(t["x"]) == len(t["open"]) == len(t["close"])


def test_table_types_and_truncation() -> None:
    df = pd.DataFrame(
        {
            "fold": [1, 2, 3],
            "hit_rate": [0.5, float("nan"), 0.7],
            "mean_x": [0.1, 0.2, 0.3],
            "month": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"]),
            "ok": [True, False, True],
            "name": ["a", "b", None],
        }
    )
    t = table_to_json(df, {"fold": "Fold"}, max_rows=2)
    types = {c["key"]: c["type"] for c in t["columns"]}
    assert types == {
        "fold": "int", "hit_rate": "pct", "mean_x": "float",
        "month": "datetime", "ok": "bool", "name": "str",
    }
    assert t["columns"][0]["label"] == "Fold"
    assert t["total_rows"] == 3 and t["truncated"] is True and len(t["rows"]) == 2
    assert t["rows"][1][1] is None  # NaN -> null
    assert t["rows"][0][3] == "2026-01-01T00:00:00Z"
    json.dumps(t, allow_nan=False)


def test_table_accepts_polars() -> None:
    t = table_to_json(pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}))
    assert t["rows"] == [[1, "x"], [2, "y"]]


def test_write_layout_manifest_and_placeholders(tmp_path: Path) -> None:
    rb = _builder(tmp_path, summary="s", tags=["sampling"], source={"kind": "pipeline_step"})
    rb.add_markdown("## Intro\ntext")
    rb.add_chart("monthly", _fig(), title="Monthly", height=300)
    rb.add_table("folds", pd.DataFrame({"a": [1]}), title="Folds")
    out = rb.write()
    assert out == tmp_path / "artifacts" / MODEL / "reports" / "01_sampling"
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 1 and manifest["id"] == "01_sampling"
    assert manifest["scope"] == "model" and manifest["model_id"] == MODEL
    assert manifest["content"]["charts"][0] == {
        "id": "monthly", "title": "Monthly", "file": "charts/monthly.json", "height": 300,
    }
    assert manifest["content"]["tables"][0]["file"] == "tables/folds.json"
    md = (out / "report.md").read_text(encoding="utf-8")
    assert "{{chart:monthly}}" in md and "{{table:folds}}" in md
    assert (out / "charts" / "monthly.json").is_file()
    assert (out / "tables" / "folds.json").is_file()


def test_write_is_idempotent_and_deterministic(tmp_path: Path) -> None:
    def build() -> Path:
        rb = _builder(tmp_path)
        rb.add_markdown("hi").add_chart("c", _fig(), title="C")
        rb.add_table("t", pd.DataFrame({"a": [1.5]}), title="T")
        return rb.write()

    out = build()
    first = {p.name: p.read_bytes() for p in [out / "report.md", out / "charts" / "c.json",
                                              out / "tables" / "t.json"]}
    created = json.loads((out / "manifest.json").read_text(encoding="utf-8"))["created_at"]
    out = build()
    second = {p.name: p.read_bytes() for p in [out / "report.md", out / "charts" / "c.json",
                                               out / "tables" / "t.json"]}
    assert first == second
    assert json.loads((out / "manifest.json").read_text(encoding="utf-8"))["created_at"] == created
    parent = out.parent
    assert [p.name for p in parent.iterdir()] == ["01_sampling"]  # no tmp / old leftovers


def test_rewrite_removes_stale_files(tmp_path: Path) -> None:
    rb = _builder(tmp_path)
    rb.add_chart("old", _fig(), title="Old")
    out = rb.write()
    rb2 = _builder(tmp_path)
    rb2.add_markdown("x")
    rb2.write()
    assert not (out / "charts" / "old.json").exists()


def test_builder_validation(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        ReportBuilder(scope="model", slug="a", title="t", root=tmp_path)  # model_id missing
    with pytest.raises(ValueError):
        ReportBuilder(scope="general", slug="../x", title="t", root=tmp_path)
    rb = _builder(tmp_path)
    rb.add_chart("c", _fig(), title="C")
    with pytest.raises(ValueError):
        rb.add_chart("c", _fig(), title="dup")
    with pytest.raises(ValueError):
        rb.add_table("Bad Id", pd.DataFrame({"a": [1]}), title="x")
    rb.add_markdown("{{chart:missing}}")
    with pytest.raises(ValueError):
        rb.write()


def test_general_scope_path(tmp_path: Path) -> None:
    rb = ReportBuilder(scope="general", slug="eda", title="EDA", root=tmp_path)
    rb.add_markdown("x")
    assert rb.write() == tmp_path / "_doc_" / "reports" / "eda"


# ---------------------------------------------------------------- store


def _seed(tmp_path: Path) -> None:
    for slug, order in (("02_b", 20), ("01_a", 10)):
        rb = ReportBuilder(
            scope="model", model_id=MODEL, slug=slug, title=slug, order=order, root=tmp_path
        )
        rb.add_markdown(f"# {slug}")
        rb.add_chart("c1", _fig(), title="C1")
        rb.add_table("t1", pd.DataFrame({"a": [1]}), title="T1")
        rb.write()
    g = ReportBuilder(scope="general", slug="eda", title="EDA", order=5, root=tmp_path)
    g.add_markdown("general")
    g.write()


def test_list_filters_and_order(tmp_path: Path) -> None:
    _seed(tmp_path)
    allr = store.list_reports(root=tmp_path)
    assert [(r["owner"], r["id"]) for r in allr] == [
        ("general", "eda"), (MODEL, "01_a"), (MODEL, "02_b"),
    ]
    assert [r["id"] for r in store.list_reports("model", MODEL, tmp_path)] == ["01_a", "02_b"]
    assert [r["id"] for r in store.list_reports("general", root=tmp_path)] == ["eda"]
    assert store.list_reports("model", "other_model", tmp_path) == []
    # manifest list carries no content bodies
    assert "markdown" not in allr[0] and "rows" not in json.dumps(allr)


def test_list_skips_broken_manifest(tmp_path: Path) -> None:
    _seed(tmp_path)
    bad = tmp_path / "artifacts" / MODEL / "reports" / "03_bad"
    bad.mkdir()
    (bad / "manifest.json").write_text("{nope", encoding="utf-8")
    assert [r["id"] for r in store.list_reports("model", MODEL, tmp_path)] == ["01_a", "02_b"]


def test_read_report_chart_table(tmp_path: Path) -> None:
    _seed(tmp_path)
    manifest, md = store.read_report(MODEL, "01_a", tmp_path)
    assert manifest["owner"] == MODEL and md.startswith("# 01_a")
    chart = store.read_chart(MODEL, "01_a", "c1", tmp_path)
    assert "data" in json.loads(chart.data) and chart.etag.startswith('"')
    table = store.table_json(store.read_table(MODEL, "01_a", "t1", tmp_path))
    assert table["rows"] == [[1]]


def test_store_missing(tmp_path: Path) -> None:
    _seed(tmp_path)
    with pytest.raises(store.ReportNotFound):
        store.read_report(MODEL, "99_none", tmp_path)
    with pytest.raises(store.ReportNotFound):
        store.read_chart(MODEL, "01_a", "nope", tmp_path)


@pytest.mark.parametrize(
    ("owner", "slug"),
    [
        ("..", "01_a"),
        (MODEL, ".."),
        (MODEL, "../01_a"),
        ("../artifacts/" + MODEL, "01_a"),
        ("a/b", "01_a"),
        (MODEL, "01_a/../../x"),
        ("", "01_a"),
    ],
)
def test_store_rejects_traversal(tmp_path: Path, owner: str, slug: str) -> None:
    _seed(tmp_path)
    with pytest.raises(store.ReportError):
        store.read_report(owner, slug, tmp_path)


def test_chart_id_traversal_and_manifest_file_escape(tmp_path: Path) -> None:
    _seed(tmp_path)
    with pytest.raises(store.InvalidReportPath):
        store.read_chart(MODEL, "01_a", "../manifest", tmp_path)
    # a tampered manifest pointing outside the report folder is refused
    mpath = tmp_path / "artifacts" / MODEL / "reports" / "01_a" / "manifest.json"
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    manifest["content"]["charts"][0]["file"] = "../../../../manifest.json"
    mpath.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(store.InvalidReportPath):
        store.read_chart(MODEL, "01_a", "c1", tmp_path)
