"""Smoke tests for the feature_engineering package.

Uses an in-memory DuckDB with a synthetic quant_train table so no real database
is required.  Verifies public entry points are callable and return the expected
schema; does not validate business correctness (that belongs in sanity tests).
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import duckdb
import pytest

pytestmark = pytest.mark.smoke

_N_ROWS = 1000  # synthetic row count — enough to exercise all branches


# --------------------------------------------------------------------------- #
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def mem_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with a minimal quant_train table (2 features, 1000 rows)."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE quant_train (
            open_time       TIMESTAMP PRIMARY KEY,
            feat_rsi_14     DOUBLE,
            feat_roc_10     DOUBLE,
            long_mfe_fw60   DOUBLE,
            short_mfe_fw60  DOUBLE
        )
    """)
    conn.execute(f"""
        INSERT INTO quant_train
        SELECT
            TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i) MINUTE AS open_time,
            SIN(i * 0.01)          AS feat_rsi_14,
            COS(i * 0.01)          AS feat_roc_10,
            SIN(i * 0.005) * 0.01  AS long_mfe_fw60,
            -SIN(i * 0.005) * 0.01 AS short_mfe_fw60
        FROM generate_series(0, {_N_ROWS - 1}) t(i)
    """)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def cfg():
    from modeling.feature_engineering import FeatureEngineeringConfig
    return FeatureEngineeringConfig(asset_id="solusdt", run_id="smoke_test_001")


# --------------------------------------------------------------------------- #
# Config tests                                                                 #
# --------------------------------------------------------------------------- #

def test_public_imports() -> None:
    from modeling.feature_engineering import (  # noqa: F401
        FeatureEngineeringConfig,
        analyze_quality,
        analyze_redundancy,
        analyze_stability,
        analyze_target_relation,
        generate_outputs,
    )


def test_config_default_instantiation() -> None:
    from modeling.feature_engineering import FeatureEngineeringConfig

    cfg = FeatureEngineeringConfig(asset_id="solusdt", run_id="r1")
    assert cfg.asset_id == "solusdt"
    assert cfg.target_cols == ("long_mfe_fw60", "short_mfe_fw60")
    assert 0.0 < cfg.max_null_rate < 1.0
    assert 0.0 < cfg.pearson_cluster_thr <= 1.0
    assert cfg.stability_bucket_days > 0


def test_config_is_frozen() -> None:
    from modeling.feature_engineering import FeatureEngineeringConfig

    cfg = FeatureEngineeringConfig(asset_id="solusdt", run_id="r1")
    with pytest.raises(FrozenInstanceError):
        cfg.asset_id = "other"  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# analyze_quality                                                              #
# --------------------------------------------------------------------------- #

def test_quality_returns_dataframe(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_quality

    result = analyze_quality(mem_conn, cfg)

    assert hasattr(result, "schema"), "must return a Polars DataFrame"
    assert set(result.columns) >= {"feature", "null_rate", "inf_rate",
                                    "variance", "outlier_ratio", "decision"}


def test_quality_one_row_per_feature(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_quality

    result = analyze_quality(mem_conn, cfg)
    assert len(result) == 2  # feat_rsi_14, feat_roc_10
    assert set(result["feature"].to_list()) == {"feat_rsi_14", "feat_roc_10"}


def test_quality_decision_values(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_quality

    result = analyze_quality(mem_conn, cfg)
    valid = {"keep", "drop", "review"}
    assert all(d in valid for d in result["decision"].to_list())


# --------------------------------------------------------------------------- #
# analyze_target_relation                                                      #
# --------------------------------------------------------------------------- #

def test_target_relation_returns_dataframe(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_target_relation

    result = analyze_target_relation(mem_conn, cfg)
    assert set(result.columns) >= {"feature", "target", "pearson_r",
                                    "spearman_rho", "signal_proxy",
                                    "leakage_flag", "decision"}


def test_target_relation_rows_per_target(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_target_relation

    result = analyze_target_relation(mem_conn, cfg)
    # 2 features × 2 targets = 4 rows
    assert len(result) == 4


def test_target_relation_decision_values(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_target_relation

    result = analyze_target_relation(mem_conn, cfg)
    valid = {"keep", "weak", "leakage"}
    assert all(d in valid for d in result["decision"].to_list())


# --------------------------------------------------------------------------- #
# analyze_redundancy                                                           #
# --------------------------------------------------------------------------- #

def test_redundancy_returns_dataframe(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_redundancy

    result = analyze_redundancy(mem_conn, cfg)
    assert set(result.columns) >= {"feature", "cluster_id",
                                    "is_representative", "max_pearson", "decision"}


def test_redundancy_one_row_per_feature(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_redundancy

    result = analyze_redundancy(mem_conn, cfg)
    assert len(result) == 2


def test_redundancy_decision_values(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_redundancy

    result = analyze_redundancy(mem_conn, cfg)
    valid = {"keep", "drop"}
    assert all(d in valid for d in result["decision"].to_list())


# --------------------------------------------------------------------------- #
# analyze_stability                                                            #
# --------------------------------------------------------------------------- #

def test_stability_returns_dataframe(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_stability

    result = analyze_stability(mem_conn, cfg)
    assert set(result.columns) >= {"feature", "bucket_idx", "bucket_start",
                                    "bucket_end", "n", "null_rate", "mean",
                                    "std", "spearman_long", "spearman_short",
                                    "stability_flag"}


def test_stability_flag_values(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_stability

    result = analyze_stability(mem_conn, cfg)
    valid = {"stable", "unstable", "decayed", "review"}
    assert all(f in valid for f in result["stability_flag"].to_list())


def test_stability_has_buckets_for_each_feature(mem_conn, cfg) -> None:
    from modeling.feature_engineering import analyze_stability

    result = analyze_stability(mem_conn, cfg)
    features = result["feature"].unique().to_list()
    assert set(features) == {"feat_rsi_14", "feat_roc_10"}


# --------------------------------------------------------------------------- #
# generate_outputs                                                             #
# --------------------------------------------------------------------------- #

def test_generate_outputs_writes_files(tmp_path: Path, mem_conn, cfg) -> None:
    from modeling.feature_engineering import (
        analyze_quality,
        analyze_redundancy,
        analyze_stability,
        analyze_target_relation,
        generate_outputs,
    )

    q  = analyze_quality(mem_conn, cfg)
    r  = analyze_target_relation(mem_conn, cfg)
    rd = analyze_redundancy(mem_conn, cfg)
    s  = analyze_stability(mem_conn, cfg)

    generate_outputs(q, r, rd, s, cfg, tmp_path)

    assert (tmp_path / "feature_set.json").exists()
    assert (tmp_path / "analyst_report.md").exists()


def test_generate_outputs_feature_set_schema(tmp_path: Path, mem_conn, cfg) -> None:
    import json

    from modeling.feature_engineering import (
        analyze_quality,
        analyze_redundancy,
        analyze_stability,
        analyze_target_relation,
        generate_outputs,
    )

    q  = analyze_quality(mem_conn, cfg)
    r  = analyze_target_relation(mem_conn, cfg)
    rd = analyze_redundancy(mem_conn, cfg)
    s  = analyze_stability(mem_conn, cfg)
    generate_outputs(q, r, rd, s, cfg, tmp_path)

    fs = json.loads((tmp_path / "feature_set.json").read_text())
    assert "selected" in fs
    assert "dropped" in fs
    assert "review" in fs
    assert "target_cols" in fs
    assert "thresholds" in fs
    assert fs["asset_id"] == cfg.asset_id
    assert fs["run_id"]   == cfg.run_id
