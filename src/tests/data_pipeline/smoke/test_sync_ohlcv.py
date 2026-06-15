"""Smoke tests for the OHLCV sync pipeline.

Verifies that sync_ohlcv correctly parses Binance klines, inserts rows into the
ohlcv table, skips stale rows, and is idempotent. The Binance client is fully
mocked — no real API calls are made.
"""

import json
from pathlib import Path

import pytest

import database.data_pipeline.sync_ohlcv as sync_ohlcv_module
from database.store.duckdb_query import latest_open_time, row_count
from database.store.duckdb_store import ensure_tables, get_connection

pytestmark = pytest.mark.smoke

_SYMBOL     = "SOLUSDT"
_SERVER_MS  = 9_999_999_999_000  # far future server time so no candles are filtered


# %% Helpers


def _kline_row(open_time_ms: int, close: float = 100.0) -> list:
    """Build a minimal Binance kline row (12-element list)."""
    return [
        str(open_time_ms),          # open_time
        str(close - 1),             # open
        str(close + 0.5),           # high
        str(close - 0.5),           # low
        str(close),                 # close
        "1000.0",                   # volume
        str(open_time_ms + 59999),  # close_time
        "100000.0",                 # quote_volume
        "50",                       # trades
        "400.0",                    # taker_buy_base
        "40000.0",                  # taker_buy_quote
        "0",                        # ignore
    ]


def _make_klines(n: int, start_ms: int = 1_704_067_200_000) -> list:
    """Build n consecutive 1-minute kline rows starting from start_ms."""
    return [_kline_row(start_ms + i * 60_000, close=100.0 + i * 0.01) for i in range(n)]


def _write_keys(tmp_path: Path) -> str:
    keys_path = str(tmp_path / "keys.json")
    with open(keys_path, "w", encoding="utf-8") as f:
        json.dump({"api_key": "test-key", "api_secret": "test-secret"}, f)
    return keys_path


def _asset_cfg(db_path: Path) -> dict:
    return {
        "database": {
            "db_path"  : str(db_path),
            "symbol"   : _SYMBOL,
            "market"   : "futures",
            "asset_id" : "solusdt_fw60",
        }
    }


def _env_cfg(keys_path: str) -> dict:
    return {"api": {"binance_keys_path": keys_path}}


def _mock_client_cls(klines: list):
    """Return a mock Client class that returns canned klines on first call."""

    class _MockClient:
        KLINE_INTERVAL_1MINUTE = "1m"

        def __init__(self, api_key=None, api_secret=None):
            self._called = False

        def get_server_time(self) -> dict:
            return {"serverTime": _SERVER_MS}

        def futures_klines(self, symbol, interval, startTime, limit):
            if self._called:
                return []
            self._called = True
            return klines

        def get_klines(self, symbol, interval, startTime, limit):
            return self.futures_klines(symbol, interval, startTime, limit)

    return _MockClient


# %% Tests


def test_sync_ohlcv_inserts_klines_into_db(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify that mocked klines are parsed and inserted into the ohlcv table."""
    db_path   = tmp_path / "asset.duckdb"
    keys_path = _write_keys(tmp_path)
    klines    = _make_klines(5)
    start_ms  = int(klines[0][0])

    conn = get_connection(str(db_path))
    ensure_tables(conn)
    conn.close()

    monkeypatch.setattr(
        sync_ohlcv_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )
    monkeypatch.setattr(
        sync_ohlcv_module.utils, "load_env_config",
        lambda: _env_cfg(keys_path),
    )
    monkeypatch.setattr(
        sync_ohlcv_module,
        "Client",
        _mock_client_cls(klines),
    )

    sync_ohlcv_module.sync_ohlcv(open_time_ms_from=start_ms)

    assert row_count(str(db_path), "ohlcv") == 5


def test_sync_ohlcv_idempotent_on_rerun(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify that re-running sync with same klines does not duplicate rows."""
    db_path   = tmp_path / "asset.duckdb"
    keys_path = _write_keys(tmp_path)
    klines    = _make_klines(3)
    start_ms  = int(klines[0][0])

    conn = get_connection(str(db_path))
    ensure_tables(conn)
    conn.close()

    monkeypatch.setattr(
        sync_ohlcv_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )
    monkeypatch.setattr(
        sync_ohlcv_module.utils, "load_env_config",
        lambda: _env_cfg(keys_path),
    )
    monkeypatch.setattr(sync_ohlcv_module, "Client", _mock_client_cls(klines))

    sync_ohlcv_module.sync_ohlcv(open_time_ms_from=start_ms)
    sync_ohlcv_module.sync_ohlcv(open_time_ms_from=start_ms)

    assert row_count(str(db_path), "ohlcv") == 3


def test_sync_ohlcv_skips_stale_rows(
    tmp_path    : Path,
    monkeypatch : pytest.MonkeyPatch,
) -> None:
    """Verify the stale-row guard drops klines with open_time_ms < requested start."""
    db_path   = tmp_path / "asset.duckdb"
    keys_path = _write_keys(tmp_path)

    base_ms = 1_704_067_200_000
    # Return 3 rows where first two are stale (before start_ms)
    all_klines = _make_klines(3, start_ms=base_ms)
    start_ms   = base_ms + 2 * 60_000  # only the 3rd row is valid

    conn = get_connection(str(db_path))
    ensure_tables(conn)
    conn.close()

    monkeypatch.setattr(
        sync_ohlcv_module.utils, "load_asset_config",
        lambda asset_id=None: _asset_cfg(db_path),
    )
    monkeypatch.setattr(
        sync_ohlcv_module.utils, "load_env_config",
        lambda: _env_cfg(keys_path),
    )
    monkeypatch.setattr(sync_ohlcv_module, "Client", _mock_client_cls(all_klines))

    sync_ohlcv_module.sync_ohlcv(open_time_ms_from=start_ms)

    assert row_count(str(db_path), "ohlcv") == 1

    ts = latest_open_time(str(db_path), "ohlcv")
    assert ts is not None
    assert int(ts.timestamp() * 1000) >= start_ms
