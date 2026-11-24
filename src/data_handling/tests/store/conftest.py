"""Shared fixtures for store sanity and perf tests that require a real database.

The live database is written by the worker (sync jobs) while the tests run.  On
Windows DuckDB takes a cross-process file lock, so even a ``read_only=True``
connect fails with ``duckdb.IOException`` while a writer holds the file.  The
writer lock is short-lived, so the fixture retries for a bounded time and then
SKIPS (not errors) — a busy worker is an environment condition, not a test
failure (epic_076 / t949).
"""

import time
from collections.abc import Generator
from pathlib import Path

import duckdb
import pytest

import utils

# Lock-retry budget: LOCK_RETRY_TRIES x LOCK_RETRY_DELAY_S seconds (~45 s) covers
# a typical worker sync write window.
LOCK_RETRY_TRIES   = 30
LOCK_RETRY_DELAY_S = 1.5


def connect_read_only_with_retry(
    db_path : str,
    tries   : int = LOCK_RETRY_TRIES,
    delay   : float = LOCK_RETRY_DELAY_S,
) -> duckdb.DuckDBPyConnection:
    """Open ``db_path`` read-only, retrying while another process holds the lock.

    Args:
        db_path : DuckDB file path.
        tries   : Connect attempts before giving up.
        delay   : Sleep between attempts, seconds.

    Returns:
        Open read-only connection.

    Raises:
        duckdb.IOException: If the file is still locked after ``tries`` attempts.
    """
    for attempt in range(tries):
        try:
            return duckdb.connect(db_path, read_only=True)
        except duckdb.IOException:
            if attempt == tries - 1:
                raise
            time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


@pytest.fixture(scope="module")
def db_path() -> str:
    cfg = utils.load_asset_config()
    return cfg["database"]["db_path"]


@pytest.fixture(scope="module")
def conn(db_path: str) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    p = Path(db_path)
    if not p.exists():
        pytest.skip(f"Database not found: {db_path}")
    try:
        c = connect_read_only_with_retry(db_path)
    except duckdb.IOException as exc:
        pytest.skip(f"Live DB locked by a writer (worker sync) after retries: {exc}")
    yield c
    c.close()
