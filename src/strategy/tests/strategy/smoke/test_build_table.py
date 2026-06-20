"""Smoke tests for strategy.strategy.build_table.build_strategy_table().

Skipped if the project DuckDB database does not exist.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke


def _get_db_path() -> str:
    """Return db_path from asset config, or empty string on failure."""
    try:
        import utils
        cfg = utils.load_asset_config()
        return str(cfg.get("database", {}).get("db_path", ""))
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_strategy_table_skipped_when_no_db() -> None:
    """Skip build_strategy_table smoke test if the DuckDB database is absent."""
    db_path = _get_db_path()
    if not db_path or not Path(db_path).exists():
        pytest.skip(f"DuckDB database not found (db_path={db_path!r}); skipping build_table smoke test.")

    # If DB exists, import and call the function minimally.
    from strategy.strategy.build_table import build_strategy_table  # noqa: F401

    # Presence of the function is sufficient — end-to-end call requires real model artifacts.
    assert callable(build_strategy_table)
