"""Smoke tests for the target sync pipeline.

Verifies that sync_targets writes expected label columns, produces correct
NULL tails (last horizon rows lack sufficient future data), and is idempotent.
Uses isolated tmp_path stores — no real database required.
"""

from pathlib import Path

import pandas as pd
import pytest

import database.sync_tables.sync_targets as sync_targets_module
from database.store.duckdb_query import query_range
from database.store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

pytestmark = pytest.mark.smoke

_HORIZON = 60


# %% Test data builders


def _build_ohlcv(rows: int = 200) -> pd.DataFrame:
    open_time = pd.date_range("2024-01-01 00:00:00", periods=rows, freq="min")
    base      = pd.Series(range(rows), dtype="float64")
    close     = 100.0 + base * 0.01 + (base % 17) * 0.03
    open_     = close.shift(1).fillna(close.iloc[0])
    high      = pd.concat([open_, close], axis=1).max(axis=1) + 0.5
    low       = pd.concat([open_, close], axis=1).min(axis=1) - 0.5
    volume    = 1000.0 + (base % 29) * 10

    return pd.DataFrame(
        {
            "open_time"       : open_time,
            "open"            : open_,
            "high"            : high,
            "low"             : low,
            "close"           : close,
            "volume"          : volume,
            "quote_volume"    : volume * 100,
            "trades"          : [10] * rows,
            "taker_buy_base"  : volume * 0.4,
            "taker_buy_quote" : volume * 40,
        }
    )


def _seed_ohlcv(db_path: str, rows: int = 200) -> None:
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        insert_ohlcv(conn, _build_ohlcv(rows))
    finally:
        conn.close()


def _asset_cfg(db_path: Path) -> dict:
    return {
        "database": {
            "db_path"       : str(db_path),
            "metadata_path" : str(db_path.parent / "metadata.json"),
            "asset_id"      : "solusdt_fw60",
            "features_profile": "solusdt_fw60",
        }
    }


def _features_cfg() -> dict:
    return {
        "database": {
            "features": {
                "targets": [
                    {
                        "direction"      : "long",
                        "name"           : "trg_l_fw60_q90",
                        "rolling_window" : _HORIZON,
                        "percentile"     : 0.9,
                    },
                    {
                        "direction"      : "short",
                        "name"           : "trg_s_fw60_q10",
                        "rolling_window" : _HORIZON,
                        "percentile"     : 0.1,
                    },
                ]
            }
        }
    }


# %% Tests


def test_sync_targets_writes_expected_columns_and_is_idempotent(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify target sync writes label columns, correct row count, and is idempotent."""
    db_path = tmp_path / "asset.duckdb"
    _seed_ohlcv(str(db_path))

    monkeypatch.setattr(
        sync_targets_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )
    monkeypatch.setattr(
        sync_targets_module.utils, "load_features_config",
        lambda asset_id=None: _features_cfg(),
    )

    sync_targets_module.sync_targets()
    sync_targets_module.sync_targets()  # idempotency check

    df = query_range(str(db_path), "target")

    assert "trg_l_fw60_q90" in df.columns, "long label column missing"
    assert "trg_s_fw60_q10" in df.columns, "short label column missing"
    assert len(df) > 0, "target table is empty"
    assert df["open_time"].is_unique, "open_time must be unique in target"


def test_sync_targets_last_rows_are_null(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify the last horizon rows have NULL labels (partial forward window).

    Rows with fewer than `horizon` future bars get NULL — this includes both
    the completely empty last row and all rows with a partial forward window.
    """
    rows    = 200
    db_path = tmp_path / "asset.duckdb"
    _seed_ohlcv(str(db_path), rows=rows)

    monkeypatch.setattr(
        sync_targets_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )
    monkeypatch.setattr(
        sync_targets_module.utils, "load_features_config",
        lambda asset_id=None: _features_cfg(),
    )

    sync_targets_module.sync_targets()

    df         = query_range(str(db_path), "target")
    null_count = df["trg_l_fw60_q90"].isna().sum()

    assert null_count == _HORIZON, (
        f"Expected exactly {_HORIZON} NULL rows (last horizon rows with partial window), got {null_count}"
    )
    assert df["trg_l_fw60_q90"].iloc[-1] is None or df["trg_l_fw60_q90"].isna().iloc[-1], \
        "Last row must have NULL label"


def test_sync_targets_label_distribution_near_decile(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Positive rate for q90/q10 labels should be near the expected 10% decile."""
    db_path = tmp_path / "asset.duckdb"
    _seed_ohlcv(str(db_path), rows=500)

    monkeypatch.setattr(
        sync_targets_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )
    monkeypatch.setattr(
        sync_targets_module.utils, "load_features_config",
        lambda asset_id=None: _features_cfg(),
    )

    sync_targets_module.sync_targets()

    df    = query_range(str(db_path), "target")
    valid = df[df["trg_l_fw60_q90"].notna()]

    long_rate  = valid["trg_l_fw60_q90"].mean()
    short_rate = valid["trg_s_fw60_q10"].mean()

    assert 0.05 <= long_rate <= 0.20, (
        f"trg_l_fw60_q90 positive rate {long_rate:.3%} outside expected 5-20% range"
    )
    assert 0.05 <= short_rate <= 0.20, (
        f"trg_s_fw60_q10 positive rate {short_rate:.3%} outside expected 5-20% range"
    )
