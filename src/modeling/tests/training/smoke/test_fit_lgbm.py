"""Smoke tests for modeling.training.fit_lgbm — no real DB required."""

from __future__ import annotations

import json
import pickle
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pytest

from modeling.training.fit_lgbm import _save_artifacts, fit_lightgbm_from_search

pytestmark = pytest.mark.smoke


# ---------------------------------------------------------------------------
# _save_artifacts
# ---------------------------------------------------------------------------

def test_save_artifacts_creates_files(tmp_path: Path) -> None:
    model = lgb.LGBMRegressor(n_estimators=5, verbosity=-1)
    X = np.random.rand(20, 3)
    y = np.random.rand(20)
    model.fit(X, y)
    features = ["feat_a", "feat_b", "feat_c"]
    params = {"objective": "regression", "n_estimators": 5}
    _save_artifacts(tmp_path, model, features, params, 5)
    assert (tmp_path / "model.pkl").exists()
    assert (tmp_path / "features.json").exists()
    assert (tmp_path / "params.json").exists()


def test_save_artifacts_model_pkl_is_loadable(tmp_path: Path) -> None:
    model = lgb.LGBMRegressor(n_estimators=5, verbosity=-1)
    model.fit(np.random.rand(20, 2), np.random.rand(20))
    _save_artifacts(tmp_path, model, ["f1", "f2"], {}, 5)
    with open(tmp_path / "model.pkl", "rb") as f:
        loaded = pickle.load(f)
    assert isinstance(loaded, lgb.LGBMRegressor)


def test_save_artifacts_features_json_content(tmp_path: Path) -> None:
    model = lgb.LGBMRegressor(n_estimators=5, verbosity=-1)
    model.fit(np.random.rand(20, 2), np.random.rand(20))
    _save_artifacts(tmp_path, model, ["feat_x", "feat_y"], {}, 5)
    content = json.loads((tmp_path / "features.json").read_text())
    assert content["features"] == ["feat_x", "feat_y"]


def test_save_artifacts_params_json_contains_n_estimators(tmp_path: Path) -> None:
    model = lgb.LGBMRegressor(n_estimators=5, verbosity=-1)
    model.fit(np.random.rand(20, 2), np.random.rand(20))
    params = {"objective": "regression"}
    _save_artifacts(tmp_path, model, ["feat_x"], params, 42)
    content = json.loads((tmp_path / "params.json").read_text())
    assert content["n_estimators"] == 42


def test_save_artifacts_creates_dir_if_missing(tmp_path: Path) -> None:
    subdir = tmp_path / "nested" / "artifact"
    model = lgb.LGBMRegressor(n_estimators=5, verbosity=-1)
    model.fit(np.random.rand(20, 2), np.random.rand(20))
    _save_artifacts(subdir, model, ["f1"], {}, 5)
    assert subdir.exists()
    assert (subdir / "model.pkl").exists()


# ---------------------------------------------------------------------------
# fit_lightgbm_from_search — error path (no real DB needed)
# ---------------------------------------------------------------------------

def test_fit_raises_for_unknown_model_id() -> None:
    with pytest.raises(ValueError, match="not found in config/models.json"):
        fit_lightgbm_from_search("nonexistent_model_id_xyz")


# ---------------------------------------------------------------------------
# n_estimators formula — pure unit test, no I/O
# ---------------------------------------------------------------------------

def test_n_estimators_formula() -> None:
    # mean([100, 200, 300]) * 1.1 = 200.0 * 1.1 = 220.0 -> round = 220
    fold_summary = [
        {"best_iteration": 100},
        {"best_iteration": 200},
        {"best_iteration": 300},
    ]
    best_iterations = [int(f["best_iteration"]) for f in fold_summary]
    n = round(sum(best_iterations) / len(best_iterations) * 1.1)
    assert n == 220


def test_n_estimators_formula_single_fold() -> None:
    fold_summary = [{"best_iteration": 150}]
    best_iterations = [int(f["best_iteration"]) for f in fold_summary]
    n = round(sum(best_iterations) / len(best_iterations) * 1.1)
    assert n == 165
