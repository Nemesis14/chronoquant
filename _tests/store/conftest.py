"""Shared fixtures for store sanity and perf tests that require a real database."""

from collections.abc import Generator
from pathlib import Path

import duckdb
import pytest

import utils


@pytest.fixture(scope="module")
def db_path() -> str:
    cfg = utils.load_asset_config()
    return cfg["database"]["db_path"]


@pytest.fixture(scope="module")
def conn(db_path: str) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    p = Path(db_path)
    if not p.exists():
        pytest.skip(f"Database not found: {db_path}")
    c = duckdb.connect(db_path, read_only=True)
    yield c
    c.close()
