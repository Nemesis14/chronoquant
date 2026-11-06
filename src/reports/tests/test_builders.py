"""Model-report builders on synthetic data (tmp DuckDB / tmp artifact dir, no real database)."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

import utils
from reports import store
from reports.builders import feature_engineering, hyperparameter_search, sampling
from reports.builders.feature_groups import GROUP_MAP, assign_group

MODEL_L = "lgbm_test_l_fw60"
MODEL_S = "lgbm_test_s_fw60"


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    lab, live = tmp_path / "lab.duckdb", tmp_path / "live.duckdb"
    rng = np.random.default_rng(0)
    times = pd.date_range("2025-01-01", periods=24 * 120, freq="h")
    for model, col in ((MODEL_L, "long_mfe_fw60"), (MODEL_S, "short_mfe_fw60")):
        frame = pd.DataFrame({
            "open_time": times,
            col: rng.normal(0.002 if col.startswith("long") else -0.002, 0.01, len(times)),
            "split": (np.arange(len(times)) >= int(len(times) * 0.75)).astype(int),
        })
        with duckdb.connect(str(lab)) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS model")
            con.register("frame", frame)
            con.execute(f'CREATE TABLE model."{model}__sample" AS SELECT * FROM frame')
    with duckdb.connect(str(live)) as con:
        con.execute(
            "CREATE TABLE ohlcv AS SELECT TIMESTAMP '2025-01-01 00:00:00' + to_hours(i) AS open_time,"
            " 100 + i * 0.01 AS close FROM range(2880) t(i)"
        )
    asset = {"database": {"asset_id": "solusdt", "db_path": str(live)}}
    monkeypatch.setattr(utils, "load_asset_config", lambda asset_id=None: asset)
    monkeypatch.setattr(utils, "lab_db_path", lambda asset_id=None: str(lab))
    monkeypatch.setattr(utils, "repo_root", lambda: str(tmp_path))
    models = {"models": {
        MODEL_L: {"artifact_dir": str(tmp_path / "artifacts" / MODEL_L), "target_name": "long_mfe_fw60"},
        MODEL_S: {"artifact_dir": str(tmp_path / "artifacts" / MODEL_S), "target_name": "short_mfe_fw60"},
    }}
    monkeypatch.setattr(utils, "load_models_config", lambda: models)
    monkeypatch.setattr("reports.builders._common.git_sha", lambda: "abc1234")
    return tmp_path


@pytest.mark.parametrize("model", [MODEL_L, MODEL_S])
def test_sampling_report(env: Path, model: str) -> None:
    out = sampling.build(model, root=env)
    assert out == env / "artifacts" / model / "reports" / "01_sampling"
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    chart_ids = {c["id"] for c in manifest["content"]["charts"]}
    assert {"monthly_target", "distribution_kde", "distribution_box", "top10_monthly_trend"} <= chart_ids
    assert any(c.startswith("top10_valid_") for c in chart_ids)
    assert manifest["source"]["git_sha"] == "abc1234" and manifest["order"] == 10
    md = (out / "report.md").read_text(encoding="utf-8")
    for cid in chart_ids:
        assert "{{chart:" + cid + "}}" in md
    # every chart is plain-list Plotly JSON with no template and <= 5000 points per trace
    for cid in chart_ids:
        fig = json.loads((out / "charts" / f"{cid}.json").read_text(encoding="utf-8"))
        assert "template" not in fig["layout"] and "bdata" not in json.dumps(fig)
        for trace in fig["data"]:
            assert len(trace.get("x", [])) <= 5000
    listed = store.list_reports("model", model, env)
    assert [r["id"] for r in listed] == ["01_sampling"]


def test_hyperparameter_search_report(env: Path) -> None:
    search = env / "artifacts" / MODEL_L / "search"
    (search / "trial_curves").mkdir(parents=True)
    trials = [
        {"trial_no": i, "params": {"learning_rate": 0.01 * (i + 1), "num_leaves": 10 + i},
         "objective_score": -1.0 - i * 0.01, "valid_ratio_p925": 1.0 + i * 0.01,
         "train_ratio_p925": 1.2 + i * 0.01, "train_valid_gap": 0.2, "spearman_rho": 0.3,
         "decile_monotonicity": 0.8, "valid_rmse": 0.01, "best_iteration": 100, "elapsed_s": 3.0}
        for i in range(1, 21)
    ]
    (search / "search_trials.jsonl").write_text("\n".join(json.dumps(t) for t in trials), encoding="utf-8")
    (search / "search_best.json").write_text(json.dumps({**trials[-1], "params": trials[-1]["params"]}),
                                             encoding="utf-8")
    (search / "trial_curves" / "trial_0020.json").write_text(
        json.dumps({"train": {"quantile": [1.0, 0.8, 0.6]}, "valid": {"quantile": [1.1, 0.9, 0.8]}}),
        encoding="utf-8")
    out = hyperparameter_search.build(MODEL_L, root=env)
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert {c["id"] for c in manifest["content"]["charts"]} >= {
        "trial_progress", "overfitting_view", "param_effect", "learning_curve"}
    assert {t["id"] for t in manifest["content"]["tables"]} == {"best_summary", "top10_trials", "best_params"}
    assert "Pruning report hiányzik" in (out / "report.md").read_text(encoding="utf-8")


def test_bin_feature_monotone_signal() -> None:
    rng = np.random.default_rng(1)
    x = rng.normal(size=5000)
    y = 0.5 * x + rng.normal(scale=0.1, size=5000)
    table, iv = feature_engineering.bin_feature(x, y)
    assert 2 <= len(table) <= feature_engineering.MAX_BINS
    assert table["Count"].sum() == 5000
    assert table["Mean"].is_monotonic_increasing
    assert iv > 0.1
    _, iv_noise = feature_engineering.bin_feature(x, rng.normal(scale=0.1, size=5000))
    assert iv_noise < iv


def test_feature_set_dict_schema() -> None:
    result = feature_engineering.FeatureAnalysis(
        model_id=MODEL_L, snapshot_id="snap", target="long_mfe_fw60", n_train_rows=10,
        min_open_time="a", max_open_time="b", feat_cols=["feat_rsi_14"], quality_dropped=[],
        iv={}, corr={}, selected=["feat_rsi_14"], top10=["feat_rsi_14"], top1_per_group=["feat_rsi_14"],
        dropped_mi={"feat_x": "mi <= 0.001"},
    )
    payload = feature_engineering.feature_set_dict(result)
    assert set(payload) == {
        "run_id", "asset_id", "model_id", "target_col", "created_at", "target_cols", "selected",
        "top10", "top1_per_group", "dropped", "review", "provenance", "thresholds"}
    assert payload["selected"] == ["feat_rsi_14"] and payload["dropped"][0]["col"] == "feat_x"
    assert payload["thresholds"]["mi_threshold"] == 0.001


def test_group_map_moved_intact() -> None:
    assert len(GROUP_MAP) == 121
    assert assign_group("feat_rsi_delta_10") == "Accel" and assign_group("feat_rsi_14") == "Momentum"
    assert assign_group("feat_unknown_zzz") == "Other"
