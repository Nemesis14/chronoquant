"""Smoke tests for the target sync pipeline.

Verifies that sync_targets writes all fw60 outcome columns, produces correct
NULL tails (last horizon rows lack sufficient future data), and is idempotent.
Uses isolated tmp_path stores — no real database required.
"""

from pathlib import Path

import polars as pl
import pytest

import database.sync_tables.sync_targets as sync_targets_module
from database.store.duckdb_query import query_range
from database.store.duckdb_store import ensure_tables, get_connection, insert_ohlcv

pytestmark = pytest.mark.smoke

_HORIZON = 60

_FW60_OUTCOME_COLS = [
    "fw60_close",
    "fw60_max",
    "fw60_min",
    "fw60_close_ret",
    "fw60_close_logret",
    "fw60_max_ratio",
    "fw60_min_ratio",
    "long_mfe_fw60",
    "short_mfe_fw60",
]


# %% Test data builders


def _build_ohlcv(rows: int = 200) -> pl.DataFrame:
    from datetime import datetime, timedelta
    base_ts = datetime(2024, 1, 1, 0, 0, 0)
    open_times = [base_ts + timedelta(minutes=i) for i in range(rows)]
    close = [100.0 + i * 0.01 + (i % 17) * 0.03 for i in range(rows)]
    open_ = [close[max(0, i - 1)] for i in range(rows)]
    high  = [max(open_[i], close[i]) + 0.5 for i in range(rows)]
    low   = [min(open_[i], close[i]) - 0.5 for i in range(rows)]
    vol   = [1000.0 + (i % 29) * 10 for i in range(rows)]

    return pl.DataFrame(
        {
            "open_time"       : open_times,
            "open"            : open_,
            "high"            : high,
            "low"             : low,
            "close"           : close,
            "volume"          : vol,
            "quote_volume"    : [v * 100 for v in vol],
            "trades"          : [10] * rows,
            "taker_buy_base"  : [v * 0.4 for v in vol],
            "taker_buy_quote" : [v * 40 for v in vol],
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


# %% Tests


def test_sync_targets_writes_fw60_outcome_columns(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify target sync writes all fw60 outcome columns and is idempotent."""
    db_path = tmp_path / "asset.duckdb"
    _seed_ohlcv(str(db_path))

    monkeypatch.setattr(
        sync_targets_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )

    sync_targets_module.sync_targets()
    sync_targets_module.sync_targets()  # idempotency check

    df = query_range(str(db_path), "target")

    for col in _FW60_OUTCOME_COLS:
        assert col in df.columns, f"fw60 outcome column missing: {col}"
    assert "open_time" in df.columns
    assert "close" in df.columns
    assert len(df) > 0, "target table is empty"
    assert df["open_time"].is_unique, "open_time must be unique in target"


def test_sync_targets_last_rows_are_null(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify the last horizon rows have NULL fw60 values (partial forward window)."""
    rows    = 200
    db_path = tmp_path / "asset.duckdb"
    _seed_ohlcv(str(db_path), rows=rows)

    monkeypatch.setattr(
        sync_targets_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )

    sync_targets_module.sync_targets()

    df         = query_range(str(db_path), "target")
    null_count = df["long_mfe_fw60"].isna().sum()

    assert null_count == _HORIZON, (
        f"Expected exactly {_HORIZON} NULL rows (last horizon rows with partial window), "
        f"got {null_count}"
    )
    assert df["long_mfe_fw60"].iloc[-1] is None or df["long_mfe_fw60"].isna().iloc[-1], \
        "Last row must have NULL long_mfe_fw60"


def test_sync_targets_fw60_values_are_nonzero(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """fw60 outcome columns should have non-null, meaningful values in valid rows."""
    db_path = tmp_path / "asset.duckdb"
    _seed_ohlcv(str(db_path), rows=300)

    monkeypatch.setattr(
        sync_targets_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )

    sync_targets_module.sync_targets()

    df    = query_range(str(db_path), "target")
    valid = df[df["long_mfe_fw60"].notna()]

    assert len(valid) > 0, "No valid rows with non-null fw60 outcomes"
    assert bool(valid["long_mfe_fw60"].notna().all()), "long_mfe_fw60 has unexpected NULLs in valid range"  # type: ignore[union-attr]
    assert bool(valid["short_mfe_fw60"].notna().all()), "short_mfe_fw60 has unexpected NULLs in valid range"  # type: ignore[union-attr]
    assert bool((valid["fw60_max_ratio"] > 0).all()), "fw60_max_ratio must be positive (price ratio)"
