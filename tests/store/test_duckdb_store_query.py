"""DuckDB native store and query smoke tests.

Verifies schema creation, append-only inserts, native range queries, helper
metadata calls, and ASOF timestamp alignment.
"""

from pathlib import Path

import pandas as pd

from store.duckdb_query import (
    asof_join_predictions_features,
    dataset_columns,
    dataset_exists,
    latest_open_time,
    query_range,
    row_count,
)
from store.duckdb_store import (
    ensure_tables,
    get_connection,
    insert_features,
    insert_ohlcv,
    insert_predictions,
)

# %% Test data builders


def _data_dir(tmp_path: Path) -> str:
    """Return an isolated data_dir path for DuckDB tests."""
    return str(tmp_path / "asset_data")


def _ohlcv_frame() -> pd.DataFrame:
    """Build deterministic OHLCV rows."""
    return pd.DataFrame(
        {
            "open_time"       : pd.to_datetime(
                [
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:01:00",
                    "2024-01-01 00:02:00",
                ]
            ),
            "open"            : [10.0, 11.0, 12.0],
            "high"            : [11.0, 12.0, 13.0],
            "low"             : [9.0, 10.0, 11.0],
            "close"           : [10.5, 11.5, 12.5],
            "volume"          : [100.0, 110.0, 120.0],
            "quote_volume"    : [1000.0, 1100.0, 1200.0],
            "trades"          : [10, 11, 12],
            "taker_buy_base"  : [40.0, 44.0, 48.0],
            "taker_buy_quote" : [400.0, 440.0, 480.0],
        }
    )


def _feature_frame() -> pd.DataFrame:
    """Build deterministic feature rows."""
    return pd.DataFrame(
        {
            "open_time"       : pd.to_datetime(
                [
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:01:00",
                    "2024-01-01 00:02:00",
                ]
            ),
            "close"           : [10.5, 11.5, 12.5],
            "available_ts"    : pd.to_datetime(
                [
                    "2023-12-31 23:59:00",
                    "2024-01-01 00:00:30",
                    "2024-01-01 00:02:00",
                ]
            ),
            "lookback_end_ts" : pd.to_datetime(
                [
                    "2023-12-31 23:59:00",
                    "2024-01-01 00:00:30",
                    "2024-01-01 00:02:00",
                ]
            ),
            "dataset_split"   : ["train", "train", "test"],
            "fold_id"         : ["fold_1", "fold_1", "fold_2"],
            "feat_rsi_14"     : [40.0, 45.0, 50.0],
            "feat_roc_14"     : [0.1, 0.2, 0.3],
        }
    )


def _prediction_frame() -> pd.DataFrame:
    """Build deterministic prediction rows."""
    return pd.DataFrame(
        {
            "open_time"      : pd.to_datetime(
                ["2024-01-01 00:01:00", "2024-01-01 00:02:00"]
            ),
            "close"          : [11.5, 12.5],
            "label_end_ts"   : pd.to_datetime(
                ["2024-01-01 01:01:00", "2024-01-01 01:02:00"]
            ),
            "dataset_split"  : ["train", "test"],
            "fold_id"        : ["fold_1", "fold_2"],
            "trg_l_fw60_q90" : [True, False],
            "trg_s_fw60_q10" : [False, True],
            "long_pred"      : [0.7, 0.2],
            "short_pred"     : [0.3, 0.8],
        }
    )


def _seed_store(data_dir: str) -> None:
    """Create tables and insert all deterministic datasets."""
    conn = get_connection(data_dir)
    try:
        ensure_tables(conn)
        insert_ohlcv(conn, _ohlcv_frame())
        insert_features(conn, _feature_frame())
        insert_predictions(conn, _prediction_frame())
    finally:
        conn.close()


# %% Schema and insert tests


def test_ensure_tables_creates_core_schema(tmp_path: Path) -> None:
    """Verify ensure_tables creates all core native tables."""
    data_dir = _data_dir(tmp_path)
    conn     = get_connection(data_dir)
    try:
        ensure_tables(conn)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        ohlcv_cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'ohlcv'"
            ).fetchall()
        }
        feature_cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'features'"
            ).fetchall()
        }
        prediction_cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'predictions'"
            ).fetchall()
        }
    finally:
        conn.close()

    assert {"ohlcv", "features", "predictions"}.issubset(tables)
    assert {"open_time", "open", "high", "low", "close", "volume"}.issubset(ohlcv_cols)
    assert {"open_time", "close", "available_ts", "lookback_end_ts"}.issubset(feature_cols)
    assert {"open_time", "label_end_ts", "long_pred", "short_pred"}.issubset(
        prediction_cols
    )


def test_insert_helpers_are_append_only_and_sorted(tmp_path: Path) -> None:
    """Verify inserts skip old rows and keep open_time order."""
    data_dir = _data_dir(tmp_path)
    conn     = get_connection(data_dir)
    try:
        ensure_tables(conn)
        assert insert_ohlcv(conn, _ohlcv_frame().iloc[[1, 0]]) == 2
        assert insert_ohlcv(conn, _ohlcv_frame().iloc[[0, 1, 2]]) == 1
        assert insert_features(conn, _feature_frame().iloc[[1, 0]]) == 2
        assert insert_features(conn, _feature_frame()) == 1
        assert insert_predictions(conn, _prediction_frame().iloc[[0]]) == 1
        assert insert_predictions(conn, _prediction_frame()) == 1
    finally:
        conn.close()

    queried = query_range(data_dir, "ohlcv")

    assert row_count(data_dir, "ohlcv") == 3
    assert row_count(data_dir, "features") == 3
    assert row_count(data_dir, "predictions") == 2
    assert list(queried["open_time"]) == sorted(queried["open_time"])


# %% Query tests


def test_query_helpers_handle_ranges_projection_and_missing_tables(tmp_path: Path) -> None:
    """Verify query helpers handle projection, ranges, and missing tables."""
    data_dir = _data_dir(tmp_path)
    _seed_store(data_dir)

    df = query_range(
        data_dir,
        "ohlcv",
        start   = "2024-01-01 00:01:00",
        end     = "2024-01-01 00:02:00",
        columns = ["open_time", "close"],
    )

    assert list(df.columns) == ["open_time", "close"]
    assert len(df) == 2
    assert query_range(data_dir, "missing").empty
    assert dataset_exists(data_dir, "ohlcv")
    assert not dataset_exists(data_dir, "missing")
    assert "feat_rsi_14" in dataset_columns(data_dir, "features")
    assert latest_open_time(data_dir, "ohlcv") == pd.Timestamp("2024-01-01 00:02:00")


def test_asof_join_predictions_features_uses_past_features_only(tmp_path: Path) -> None:
    """Verify ASOF join only attaches features available at prediction time."""
    data_dir = _data_dir(tmp_path)
    _seed_store(data_dir)

    joined = asof_join_predictions_features(
        data_dir,
        feature_cols = ["feat_rsi_14"],
    )

    expected_prediction_ts = pd.to_datetime(
        ["2024-01-01 00:01:00", "2024-01-01 00:02:00"]
    )

    assert len(joined) == 2
    assert list(joined["prediction_ts"]) == list(expected_prediction_ts)
    assert (joined["lookback_end_ts"] <= joined["prediction_ts"]).all()
    assert list(joined["feat_rsi_14"]) == [45.0, 50.0]
