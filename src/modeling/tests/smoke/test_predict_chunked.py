"""Smoke tests for the chunked predict path introduced in t5.

Covers:
  - _score_snapshot_chunked is importable from modeling.predict
  - The function is callable and processes a synthetic in-memory snapshot
    without raising, returning the correct row count
"""

from __future__ import annotations


import duckdb
import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest

from modeling.predict import _score_snapshot_chunked

pytestmark = pytest.mark.smoke

SNAPSHOT_ID = "solusdt_fw60_chunked__aabbccdd"
MODEL_ID    = "lgbm_solusdt_l_fw60_chunked_test"
FEATURES    = ["feat_a", "feat_b"]


# ---------------------------------------------------------------------------
# Import smoke
# ---------------------------------------------------------------------------


def test_score_snapshot_chunked_is_importable() -> None:
    """Verifies that _score_snapshot_chunked can be imported (t5 regression guard)."""
    from modeling.predict import _score_snapshot_chunked as fn  # noqa: F401
    assert callable(fn)


# ---------------------------------------------------------------------------
# Functional smoke — synthetic in-memory snapshot
# ---------------------------------------------------------------------------


@pytest.fixture
def chunked_lab():
    """In-memory DuckDB with a synthetic snapshot and a tiny fitted LightGBM model."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS snap")
    conn.execute("CREATE SCHEMA IF NOT EXISTS model")

    rng = np.random.default_rng(99)
    n   = 500
    df  = pd.DataFrame({
        "open_time": np.arange(n).astype("datetime64[m]"),
        "feat_a":    rng.random(n),
        "feat_b":    rng.random(n),
        "target":    rng.random(n),
    })
    conn.register("_snap", df)
    conn.execute(f'CREATE TABLE snap."{SNAPSHOT_ID}" AS SELECT * FROM _snap ORDER BY open_time')
    conn.unregister("_snap")

    model = lgb.LGBMRegressor(n_estimators=5, verbosity=-1)
    model.fit(df[FEATURES].to_numpy(), df["target"].to_numpy())

    return conn, model


def test_score_snapshot_chunked_returns_correct_row_count(chunked_lab) -> None:
    conn, model = chunked_lab
    n_written = _score_snapshot_chunked(
        conn              = conn,
        model             = model,
        model_id          = MODEL_ID,
        snapshot_id       = SNAPSHOT_ID,
        selected_features = FEATURES,
        chunk_size        = 200,  # 3 chunks for 500 rows
    )
    assert n_written == 500


def test_score_snapshot_chunked_creates_pred_table(chunked_lab) -> None:
    conn, model = chunked_lab
    _score_snapshot_chunked(
        conn              = conn,
        model             = model,
        model_id          = MODEL_ID,
        snapshot_id       = SNAPSHOT_ID,
        selected_features = FEATURES,
        chunk_size        = 200,
    )
    fqn  = f'model."{MODEL_ID}__pred"'
    cols = [d[0] for d in conn.execute(f"SELECT * FROM {fqn} LIMIT 0").description]
    assert cols == ["open_time", "pred"]


def test_score_snapshot_chunked_is_idempotent(chunked_lab) -> None:
    """Running twice replaces the table — same row count, no duplicates."""
    conn, model = chunked_lab
    _score_snapshot_chunked(
        conn=conn, model=model, model_id=MODEL_ID,
        snapshot_id=SNAPSHOT_ID, selected_features=FEATURES, chunk_size=200,
    )
    first = conn.execute(f'SELECT COUNT(*) FROM model."{MODEL_ID}__pred"').fetchone()[0]

    _score_snapshot_chunked(
        conn=conn, model=model, model_id=MODEL_ID,
        snapshot_id=SNAPSHOT_ID, selected_features=FEATURES, chunk_size=200,
    )
    second = conn.execute(f'SELECT COUNT(*) FROM model."{MODEL_ID}__pred"').fetchone()[0]

    assert first == second == 500
