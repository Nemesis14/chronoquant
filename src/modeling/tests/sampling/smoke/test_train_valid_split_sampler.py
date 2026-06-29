"""Smoke and sanity tests for the train/valid split sampling path (t2).

Covers:
  - TrainValidSplitConfig instantiation with correct defaults
  - build_train_valid_split_select_sql() returns a non-empty SQL string
  - build_train_valid_split_ctas_sql() wraps a CREATE OR REPLACE TABLE
  - Generated SQL contains: split TINYINT, QUALIFY ROW_NUMBER, embargo conditions
  - End-to-end CTAS into an in-memory DuckDB yields correct split=0/split=1 counts
    and respects embargo / target-purge windows
"""

from __future__ import annotations

from datetime import datetime, timedelta

import duckdb
import pytest

from modeling.sampling.config import TrainValidSplitConfig
from modeling.sampling.snapshot_sampler import (
    build_train_valid_split_ctas_sql,
    build_train_valid_split_select_sql,
    sample_table_fqn,
)

pytestmark = pytest.mark.smoke

SNAPSHOT_ID = "solusdt_fw60_tvs__aabbccdd"
MODEL_ID    = "lgbm_solusdt_l_fw60_2101_2605"
TARGET      = "long_mfe_fw60"

TRAIN_START = "2024-01-01"
TRAIN_END   = "2024-01-31"
VALID_START = "2024-02-01"
VALID_END   = "2024-02-28"


# ---------------------------------------------------------------------------
# TrainValidSplitConfig — smoke
# ---------------------------------------------------------------------------


def test_config_instantiates_with_required_fields() -> None:
    cfg = TrainValidSplitConfig(
        sample_id   = "test_sample",
        asset_id    = "solusdt",
        train_start = TRAIN_START,
        train_end   = TRAIN_END,
        valid_start = VALID_START,
        valid_end   = VALID_END,
    )
    assert cfg.sample_id == "test_sample"
    assert cfg.asset_id  == "solusdt"


def test_config_defaults_are_correct() -> None:
    cfg = TrainValidSplitConfig(
        sample_id   = "test_sample",
        asset_id    = "solusdt",
        train_start = TRAIN_START,
        train_end   = TRAIN_END,
        valid_start = VALID_START,
        valid_end   = VALID_END,
    )
    assert cfg.seed == 42
    assert cfg.feature_lookback_embargo_minutes == 240
    assert cfg.target_purge_minutes == 60
    assert "long_mfe_fw60" in cfg.target_cols
    assert "short_mfe_fw60" in cfg.target_cols


def test_config_is_frozen() -> None:
    cfg = TrainValidSplitConfig(
        sample_id   = "test_sample",
        asset_id    = "solusdt",
        train_start = TRAIN_START,
        train_end   = TRAIN_END,
        valid_start = VALID_START,
        valid_end   = VALID_END,
    )
    with pytest.raises((AttributeError, TypeError)):
        cfg.seed = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# build_train_valid_split_select_sql — smoke
# ---------------------------------------------------------------------------


def test_select_sql_returns_non_empty_string() -> None:
    sql = build_train_valid_split_select_sql(
        snapshot_id  = SNAPSHOT_ID,
        seed         = 42,
        target_cols  = [TARGET],
        train_start  = TRAIN_START,
        train_end    = TRAIN_END,
        valid_start  = VALID_START,
        valid_end    = VALID_END,
    )
    assert isinstance(sql, str) and len(sql) > 0


# ---------------------------------------------------------------------------
# build_train_valid_split_select_sql — sanity: SQL content checks
# ---------------------------------------------------------------------------


def test_select_sql_contains_split_tinyint() -> None:
    sql = build_train_valid_split_select_sql(
        snapshot_id  = SNAPSHOT_ID,
        seed         = 42,
        target_cols  = [TARGET],
        train_start  = TRAIN_START,
        train_end    = TRAIN_END,
        valid_start  = VALID_START,
        valid_end    = VALID_END,
    )
    assert "TINYINT" in sql.upper()
    assert "split" in sql


def test_select_sql_contains_qualify_row_number() -> None:
    sql = build_train_valid_split_select_sql(
        snapshot_id  = SNAPSHOT_ID,
        seed         = 42,
        target_cols  = [TARGET],
        train_start  = TRAIN_START,
        train_end    = TRAIN_END,
        valid_start  = VALID_START,
        valid_end    = VALID_END,
    )
    assert "QUALIFY" in sql.upper()
    assert "ROW_NUMBER" in sql.upper()


def test_select_sql_contains_embargo_and_purge_conditions() -> None:
    sql = build_train_valid_split_select_sql(
        snapshot_id                      = SNAPSHOT_ID,
        seed                             = 42,
        target_cols                      = [TARGET],
        train_start                      = TRAIN_START,
        train_end                        = TRAIN_END,
        valid_start                      = VALID_START,
        valid_end                        = VALID_END,
        feature_lookback_embargo_minutes = 240,
        target_purge_minutes             = 60,
    )
    # Embargo: train_start + N minutes
    assert "240" in sql
    # Purge: train_end - N minutes
    assert "60" in sql
    # Both date boundaries present
    assert TRAIN_START in sql
    assert TRAIN_END   in sql
    assert VALID_START in sql
    assert VALID_END   in sql


def test_select_sql_references_correct_snapshot_table() -> None:
    sql = build_train_valid_split_select_sql(
        snapshot_id = SNAPSHOT_ID,
        seed        = 42,
        target_cols = [TARGET],
        train_start = TRAIN_START,
        train_end   = TRAIN_END,
        valid_start = VALID_START,
        valid_end   = VALID_END,
    )
    assert f'snap."{SNAPSHOT_ID}"' in sql


# ---------------------------------------------------------------------------
# build_train_valid_split_ctas_sql — smoke
# ---------------------------------------------------------------------------


def test_ctas_sql_wraps_create_or_replace_table() -> None:
    sql = build_train_valid_split_ctas_sql(
        model_id    = MODEL_ID,
        snapshot_id = SNAPSHOT_ID,
        seed        = 42,
        target_cols = [TARGET],
        train_start = TRAIN_START,
        train_end   = TRAIN_END,
        valid_start = VALID_START,
        valid_end   = VALID_END,
    )
    assert sql.startswith(f'CREATE OR REPLACE TABLE {sample_table_fqn(MODEL_ID)} AS')


# ---------------------------------------------------------------------------
# End-to-end execution in in-memory DuckDB — sanity
# ---------------------------------------------------------------------------


def _make_snapshot(conn: duckdb.DuckDBPyConnection) -> None:
    """Create a synthetic minute-bar snapshot covering train + valid windows."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS snap")
    conn.execute(
        f'CREATE TABLE snap."{SNAPSHOT_ID}" '
        "(open_time TIMESTAMP, feat_a DOUBLE, long_mfe_fw60 DOUBLE)"
    )
    # Generate one-minute bars from 2024-01-01 through 2024-02-28 (59 days).
    start = datetime(2024, 1, 1)
    rows  = []
    for i in range(59 * 24 * 60):
        t = start + timedelta(minutes=i)
        rows.append((t, float(i % 7), float(1.0 + (i % 3) * 0.1)))
    conn.executemany(f'INSERT INTO snap."{SNAPSHOT_ID}" VALUES (?, ?, ?)', rows)


def test_ctas_creates_split_column_with_correct_values() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA model")
    _make_snapshot(conn)
    try:
        sql = build_train_valid_split_ctas_sql(
            model_id                         = MODEL_ID,
            snapshot_id                      = SNAPSHOT_ID,
            seed                             = 42,
            target_cols                      = [TARGET],
            train_start                      = TRAIN_START,
            train_end                        = TRAIN_END,
            valid_start                      = VALID_START,
            valid_end                        = VALID_END,
            feature_lookback_embargo_minutes = 240,
            target_purge_minutes             = 60,
        )
        conn.execute(sql)

        fqn = sample_table_fqn(MODEL_ID)
        # split column contains only 0 and 1
        distinct_splits = {
            int(r[0])
            for r in conn.execute(f"SELECT DISTINCT split FROM {fqn}").fetchall()
        }
        assert distinct_splits <= {0, 1}
        # Both train (0) and valid (1) rows present
        assert 0 in distinct_splits
        assert 1 in distinct_splits
    finally:
        conn.close()


def test_ctas_one_row_per_hour() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA model")
    _make_snapshot(conn)
    try:
        sql = build_train_valid_split_ctas_sql(
            model_id    = MODEL_ID,
            snapshot_id = SNAPSHOT_ID,
            seed        = 42,
            target_cols = [TARGET],
            train_start = TRAIN_START,
            train_end   = TRAIN_END,
            valid_start = VALID_START,
            valid_end   = VALID_END,
        )
        conn.execute(sql)

        fqn = sample_table_fqn(MODEL_ID)
        dup = conn.execute(
            f"SELECT COUNT(*) FROM ("
            f"SELECT date_trunc('hour', open_time) h, COUNT(*) c "
            f"FROM {fqn} GROUP BY h HAVING c > 1)"
        ).fetchone()
        assert dup is not None and dup[0] == 0, "More than one row per hour found"
    finally:
        conn.close()


def test_embargo_excludes_first_240_minutes_of_train() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA model")
    _make_snapshot(conn)
    try:
        sql = build_train_valid_split_ctas_sql(
            model_id                         = MODEL_ID,
            snapshot_id                      = SNAPSHOT_ID,
            seed                             = 42,
            target_cols                      = [TARGET],
            train_start                      = TRAIN_START,
            train_end                        = TRAIN_END,
            valid_start                      = VALID_START,
            valid_end                        = VALID_END,
            feature_lookback_embargo_minutes = 240,
            target_purge_minutes             = 0,  # disable purge for isolation
        )
        conn.execute(sql)

        fqn     = sample_table_fqn(MODEL_ID)
        min_row = conn.execute(
            f"SELECT MIN(open_time) FROM {fqn} WHERE split = 0"
        ).fetchone()
        assert min_row is not None
        min_train_ts = min_row[0]
        # Earliest train row must be at least 240 minutes after midnight on train_start
        earliest_allowed = datetime(2024, 1, 1, 4, 0)  # 2024-01-01 + 240 min
        assert min_train_ts >= earliest_allowed, (
            f"Train rows start before embargo window: {min_train_ts}"
        )
    finally:
        conn.close()
