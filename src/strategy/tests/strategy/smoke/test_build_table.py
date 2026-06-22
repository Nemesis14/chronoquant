"""Smoke tests for strategy.strategy.build_table.build_scored_table().

Exercises the snap ⋈ model_long.__pred ⋈ model_short.__pred join over a single
in-memory DuckDB connection with synthetic snap + pred tables (no real lab DB).
"""

from __future__ import annotations

import duckdb
import pytest

from strategy.strategy import build_table

pytestmark = pytest.mark.smoke

LONG_MODEL  = "lgbm_solusdt_l_fw60_2023"
SHORT_MODEL = "lgbm_solusdt_s_fw60_2023"
SNAPSHOT_ID = "solusdt_fw60_2023__deadbeef"


class _NoCloseConn:
    """Delegating proxy whose ``close`` is a no-op (keeps the conn alive across calls)."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def close(self) -> None:
        pass

    def __getattr__(self, name: str):
        return getattr(self._conn, name)


def _seed_lab(n: int = 120) -> duckdb.DuckDBPyConnection:
    """Build an in-memory lab connection with snap + both pred tables populated."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA snap")
    conn.execute("CREATE SCHEMA model")
    conn.execute("CREATE SCHEMA reg")
    conn.execute("""
        CREATE TABLE reg.models (
            model_id VARCHAR, snapshot_id VARCHAR, status VARCHAR
        )
    """)
    conn.execute(
        "INSERT INTO reg.models VALUES (?, ?, 'predicted'), (?, ?, 'predicted')",
        [LONG_MODEL, SNAPSHOT_ID, SHORT_MODEL, SNAPSHOT_ID],
    )
    conn.execute(f"""
        CREATE TABLE snap."{SNAPSHOT_ID}" AS
        SELECT
            (TIMESTAMP '2023-01-01 00:00:00' + (i * INTERVAL 1 MINUTE)) AS open_time,
            0.01 + i * 0.0001 AS long_mfe_fw60,
            0.02 - i * 0.0001 AS short_mfe_fw60
        FROM range({n}) t(i)
    """)
    for mid in (LONG_MODEL, SHORT_MODEL):
        conn.execute(f"""
            CREATE TABLE model."{mid}__pred" AS
            SELECT open_time, hash(open_time) % 100 / 100.0 AS pred
            FROM snap."{SNAPSHOT_ID}"
        """)
    return conn


def test_build_scored_table_joins_snap_and_preds(monkeypatch: pytest.MonkeyPatch) -> None:
    """build_scored_table joins snap + both pred tables on open_time."""
    conn  = _seed_lab(120)
    proxy = _NoCloseConn(conn)
    monkeypatch.setattr(build_table.utils, "open_lab_connection", lambda asset_id=None: proxy)
    monkeypatch.setattr(build_table.utils, "load_models_config", lambda: {"models": {}})

    scored = build_table.build_scored_table(
        long_model_id  = LONG_MODEL,
        short_model_id = SHORT_MODEL,
        asset_id       = "solusdt",
        snapshot_id    = SNAPSHOT_ID,
    )

    assert len(scored) == 120
    for col in ("open_time", "pred_long_raw", "pred_short_raw", "long_mfe_fw60", "short_mfe_fw60"):
        assert col in scored.columns, f"missing column {col}"
    # price columns present (NULL — no live.ohlcv attached)
    for col in ("open", "high", "low", "close"):
        assert col in scored.columns, f"missing price column {col}"


def test_build_scored_table_missing_pred_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing pred table raises ValueError."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA snap")
    conn.execute("CREATE SCHEMA model")
    conn.execute(f'CREATE TABLE snap."{SNAPSHOT_ID}" AS SELECT 1 AS long_mfe_fw60')
    proxy = _NoCloseConn(conn)
    monkeypatch.setattr(build_table.utils, "open_lab_connection", lambda asset_id=None: proxy)
    monkeypatch.setattr(build_table.utils, "load_models_config", lambda: {"models": {}})

    with pytest.raises(ValueError, match="pred"):
        build_table.build_scored_table(
            long_model_id  = LONG_MODEL,
            short_model_id = SHORT_MODEL,
            asset_id       = "solusdt",
            snapshot_id    = SNAPSHOT_ID,
        )
