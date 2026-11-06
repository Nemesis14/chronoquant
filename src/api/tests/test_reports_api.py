"""/api/reports: manifest list, lazy content, caching headers, path-traversal protection."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import pytest
from fastapi.testclient import TestClient

from reports import ReportBuilder

MODEL = "lgbm_test_l"


@pytest.fixture
def seeded(env: Path) -> Path:
    for slug, order in (("02_b", 20), ("01_a", 10)):
        rb = ReportBuilder(scope="model", model_id=MODEL, slug=slug, title=f"T {slug}", order=order,
                           summary="sum", root=env)
        rb.add_markdown(f"## {slug}")
        rb.add_chart("c1", go.Figure(go.Scatter(x=[1, 2], y=[3, 4])), title="C1")
        rb.add_table("t1", pd.DataFrame({"a": [1, 2]}), title="T1")
        rb.write()
    g = ReportBuilder(scope="general", slug="eda", title="EDA", order=1, root=env)
    g.add_markdown("general text")
    g.write()
    return env


def test_list_manifests_only(client: TestClient, seeded: Path) -> None:
    body = client.get("/api/reports").json()
    assert body["total"] == 3
    assert [(i["owner"], i["id"]) for i in body["items"]] == [
        ("general", "eda"), (MODEL, "01_a"), (MODEL, "02_b"),
    ]
    raw = json.dumps(body)
    assert "markdown" not in body["items"][0]
    assert "rows" not in raw and '"data"' not in raw  # no table / chart payloads in the list
    assert body["items"][1]["content"]["charts"][0]["id"] == "c1"


def test_list_filters(client: TestClient, seeded: Path) -> None:
    ids = lambda r: [i["id"] for i in r.json()["items"]]  # noqa: E731
    assert ids(client.get("/api/reports", params={"scope": "general"})) == ["eda"]
    assert ids(client.get("/api/reports", params={"scope": "model", "model_id": MODEL})) == [
        "01_a", "02_b",
    ]
    assert ids(client.get("/api/reports", params={"scope": "model", "model_id": "nope"})) == []
    assert client.get("/api/reports", params={"scope": "bogus"}).status_code == 422


def test_list_empty_when_no_reports(client: TestClient) -> None:
    assert client.get("/api/reports").json() == {"items": [], "total": 0}


def test_get_report_and_lazy_files(client: TestClient, seeded: Path) -> None:
    body = client.get(f"/api/reports/{MODEL}/01_a").json()
    assert body["manifest"]["owner"] == MODEL
    assert body["markdown"].startswith("## 01_a") and "{{chart:c1}}" in body["markdown"]

    chart = client.get(f"/api/reports/{MODEL}/01_a/charts/c1")
    assert chart.status_code == 200 and "data" in chart.json()
    assert chart.headers["cache-control"] == "max-age=60"
    etag = chart.headers["etag"]
    cached = client.get(f"/api/reports/{MODEL}/01_a/charts/c1", headers={"If-None-Match": etag})
    assert cached.status_code == 304

    table = client.get(f"/api/reports/{MODEL}/01_a/tables/t1").json()
    assert table["rows"] == [[1], [2]] and table["columns"][0]["key"] == "a"

    assert client.get("/api/reports/general/eda").json()["markdown"].startswith("general text")


def test_not_found_uses_error_format(client: TestClient, seeded: Path) -> None:
    for url in (
        f"/api/reports/{MODEL}/99_none",
        f"/api/reports/{MODEL}/01_a/charts/zzz",
        f"/api/reports/{MODEL}/01_a/tables/zzz",
        "/api/reports/nomodel/01_a",
    ):
        r = client.get(url)
        assert r.status_code == 404 and r.json()["code"] == "not_found", url


@pytest.mark.parametrize(
    "url",
    [
        "/api/reports/lgbm_test_l/01_a/charts/..%2F..%2Fmanifest",
        "/api/reports/lgbm_test_l/%2e%2e%2f01_a",
        "/api/reports/..%2Fartifacts/01_a",
        "/api/reports/%2e%2e/01_a",
        "/api/reports/lgbm_test_l/01_a/tables/..%5Cmanifest",
    ],
)
def test_path_traversal_never_serves_files(client: TestClient, seeded: Path, url: str) -> None:
    r = client.get(url)
    assert r.status_code in (400, 404, 422)
    assert "schema_version" not in r.text and "token" not in r.text


def test_upper_case_and_dot_slug_rejected(client: TestClient, seeded: Path) -> None:
    r = client.get(f"/api/reports/{MODEL}/01_A")
    assert r.status_code == 400 and r.json()["code"] == "bad_request"
