"""Smoke tests for sync_pipeline helper functions.

Tests _monthly_chunks and _resolve_tables — pure logic, no DB or Binance
calls required. The main() function itself is a CLI entry point and is not
tested here.
"""

import argparse
import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke

# Import the module under test — it lives at src/database/02_sync_pipeline.py.
# The file starts with a digit, so importlib is used instead of a regular import
# statement to avoid a SyntaxError from the numeric prefix.

_MODULE_PATH = Path(__file__).resolve().parents[3] / "02_sync_pipeline.py"
_spec = importlib.util.spec_from_file_location("sync_pipeline", _MODULE_PATH)
assert _spec and _spec.loader, f"Could not load module from {_MODULE_PATH}"
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

_monthly_chunks = _mod._monthly_chunks
_resolve_tables = _mod._resolve_tables
ALL_TABLES = _mod.ALL_TABLES


# %% _monthly_chunks


def test_monthly_chunks_single_month() -> None:
    """A range shorter than one chunk produces one tuple."""
    result = _monthly_chunks("2024-01-01 00:00:00", "2024-02-01 00:00:00", 3)
    assert len(result) == 1
    assert result[0] == ("2024-01-01 00:00:00", "2024-02-01 00:00:00")


def test_monthly_chunks_splits_correctly() -> None:
    """A six-month range with chunk_months=3 produces exactly two chunks."""
    result = _monthly_chunks("2024-01-01 00:00:00", "2024-07-01 00:00:00", 3)
    assert len(result) == 2
    assert result[0][0] == "2024-01-01 00:00:00"
    assert result[0][1] == "2024-04-01 00:00:00"
    assert result[1][0] == "2024-04-01 00:00:00"
    assert result[1][1] == "2024-07-01 00:00:00"


def test_monthly_chunks_contiguous() -> None:
    """Each chunk end equals the next chunk start (no gaps)."""
    result = _monthly_chunks("2024-01-01 00:00:00", "2024-10-01 00:00:00", 2)
    for i in range(len(result) - 1):
        assert result[i][1] == result[i + 1][0]


def test_monthly_chunks_equal_start_end_returns_empty() -> None:
    """Equal start and end produces an empty list."""
    result = _monthly_chunks("2024-01-01 00:00:00", "2024-01-01 00:00:00", 3)
    assert result == []


def test_monthly_chunks_returns_list_of_tuples() -> None:
    """Return type is list[tuple[str, str]]."""
    result = _monthly_chunks("2024-01-01 00:00:00", "2024-04-01 00:00:00", 1)
    assert isinstance(result, list)
    for item in result:
        assert isinstance(item, tuple)
        assert len(item) == 2
        assert isinstance(item[0], str)
        assert isinstance(item[1], str)


# %% _resolve_tables


def _make_args(tables: str | None = None, skip_ohlcv: bool = False) -> argparse.Namespace:
    return argparse.Namespace(tables=tables, skip_ohlcv=skip_ohlcv)


def test_resolve_tables_default_returns_all() -> None:
    """No --tables arg returns the full set of tables."""
    result = _resolve_tables(_make_args())
    assert result == set(ALL_TABLES)


def test_resolve_tables_subset() -> None:
    """Explicit --tables=ohlcv,features returns only those two."""
    result = _resolve_tables(_make_args(tables="ohlcv,features"))
    assert result == {"ohlcv", "features"}


def test_resolve_tables_skip_ohlcv_removes_ohlcv() -> None:
    """--skip-ohlcv discards 'ohlcv' from the requested set."""
    result = _resolve_tables(_make_args(skip_ohlcv=True))
    assert "ohlcv" not in result
    assert result == {"targets", "features", "predictions"}


def test_resolve_tables_unknown_table_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unknown table name in --tables triggers sys.exit(1)."""
    with pytest.raises(SystemExit) as exc_info:
        _resolve_tables(_make_args(tables="ohlcv,unknown_table"))
    assert exc_info.value.code == 1


def test_resolve_tables_whitespace_stripped() -> None:
    """Whitespace around table names is stripped correctly."""
    result = _resolve_tables(_make_args(tables=" ohlcv , features "))
    assert result == {"ohlcv", "features"}
