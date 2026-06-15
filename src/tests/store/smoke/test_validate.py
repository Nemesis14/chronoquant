"""Smoke and exception tests for store.validate assertion helpers."""

import duckdb
import pytest

from database.store.validate import assert_zero

pytestmark = pytest.mark.smoke


def test_assert_zero_passes() -> None:
    """Verify assert_zero accepts zero-count SQL."""
    con = duckdb.connect()
    try:
        assert assert_zero(con, "SELECT 0 AS n", "should not fire") == 0
    finally:
        con.close()


def test_assert_zero_raises() -> None:
    """Verify assert_zero raises AssertionError when SQL reports violations."""
    con = duckdb.connect()
    try:
        with pytest.raises(AssertionError, match="1 violation"):
            assert_zero(con, "SELECT 1 AS n", "expected violation")
    finally:
        con.close()
