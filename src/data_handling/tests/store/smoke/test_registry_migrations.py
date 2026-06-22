"""Smoke tests for the registry schema and the migrations framework."""

from pathlib import Path

import duckdb
import pytest

from data_handling.store import registry
from data_handling.store.migrations import (
    Migration,
    applied_versions,
    run_migrations,
)

pytestmark = pytest.mark.smoke


# %% Migrations framework


def test_run_migrations_applies_and_records(tmp_path: Path) -> None:
    db_path = str(tmp_path / "m.duckdb")
    conn = duckdb.connect(db_path)
    try:
        calls: list[int] = []
        migs = [
            Migration(1, "a", lambda c: calls.append(1)),
            Migration(2, "b", lambda c: calls.append(2)),
        ]
        applied = run_migrations(conn, migs)
        assert applied == [1, 2]
        assert calls == [1, 2]
        assert applied_versions(conn) == {1, 2}
    finally:
        conn.close()


def test_run_migrations_is_idempotent(tmp_path: Path) -> None:
    db_path = str(tmp_path / "m.duckdb")
    conn = duckdb.connect(db_path)
    try:
        calls: list[int] = []
        migs = [Migration(1, "a", lambda c: calls.append(1))]
        first  = run_migrations(conn, migs)
        second = run_migrations(conn, migs)
        assert first == [1]
        assert second == []          # nothing re-applied
        assert calls == [1]          # apply callable ran exactly once
    finally:
        conn.close()


def test_run_migrations_applies_only_pending(tmp_path: Path) -> None:
    db_path = str(tmp_path / "m.duckdb")
    conn = duckdb.connect(db_path)
    try:
        run_migrations(conn, [Migration(1, "a", lambda c: None)])
        applied = run_migrations(
            conn,
            [Migration(1, "a", lambda c: None), Migration(2, "b", lambda c: None)],
        )
        assert applied == [2]
        assert applied_versions(conn) == {1, 2}
    finally:
        conn.close()


def test_run_migrations_rejects_duplicate_versions(tmp_path: Path) -> None:
    db_path = str(tmp_path / "m.duckdb")
    conn = duckdb.connect(db_path)
    try:
        with pytest.raises(ValueError):
            run_migrations(
                conn,
                [Migration(1, "a", lambda c: None), Migration(1, "b", lambda c: None)],
            )
    finally:
        conn.close()


def test_run_migrations_rolls_back_on_failure(tmp_path: Path) -> None:
    db_path = str(tmp_path / "m.duckdb")
    conn = duckdb.connect(db_path)

    def boom(_: duckdb.DuckDBPyConnection) -> None:
        raise RuntimeError("fail")

    try:
        with pytest.raises(RuntimeError):
            run_migrations(conn, [Migration(1, "boom", boom)])
        assert applied_versions(conn) == set()   # version not recorded
    finally:
        conn.close()


# %% Registry schema


def test_ensure_registry_creates_eight_tables(tmp_path: Path) -> None:
    reg_path = str(tmp_path / "registry.duckdb")
    applied = registry.ensure_registry(reg_path)
    # All REG_MIGRATIONS applied on a fresh database (currently v1 + v2)
    assert set(applied) == {m.version for m in registry.REG_MIGRATIONS}
    assert Path(reg_path).exists()

    conn = duckdb.connect(reg_path)
    try:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    finally:
        conn.close()
    table_names = {r[0] for r in rows}
    assert set(registry.REG_TABLES) <= table_names
    assert len(registry.REG_TABLES) == 8


def test_ensure_registry_is_idempotent(tmp_path: Path) -> None:
    reg_path = str(tmp_path / "registry.duckdb")
    first  = registry.ensure_registry(reg_path)
    second = registry.ensure_registry(reg_path)
    # First call applies all pending migrations; second call applies none
    assert set(first) == {m.version for m in registry.REG_MIGRATIONS}
    assert second == []


# %% CRUD


def test_registry_crud_roundtrip(tmp_path: Path) -> None:
    reg_path = str(tmp_path / "registry.duckdb")
    conn = registry.open_crud_connection(reg_path)
    try:
        registry.upsert(conn, "assets", {"asset_id": "solusdt", "symbol": "SOLUSDT"})
        row = registry.get(conn, "assets", "solusdt")
        assert row is not None
        assert row["symbol"] == "SOLUSDT"
        assert row["status"] == "draft"

        # update via upsert (idempotent, refreshes fields)
        registry.upsert(conn, "assets", {"asset_id": "solusdt", "market": "futures"})
        row = registry.get(conn, "assets", "solusdt")
        assert row is not None and row["market"] == "futures"

        # status transition
        registry.set_status(conn, "assets", "solusdt", "active")
        row = registry.get(conn, "assets", "solusdt")
        assert row is not None and row["status"] == "active"

        active = registry.list_rows(conn, "assets", status="active")
        assert len(active) == 1

        registry.delete(conn, "assets", "solusdt")
        assert registry.get(conn, "assets", "solusdt") is None
    finally:
        conn.close()


def test_registry_json_column_serialization(tmp_path: Path) -> None:
    reg_path = str(tmp_path / "registry.duckdb")
    conn = registry.open_crud_connection(reg_path)
    try:
        registry.upsert(
            conn,
            "feature_sets",
            {
                "feature_set_id": "fs_demo",
                "snapshot_id":    "snap_demo",
                "n_input":        10,
                "n_selected":     3,
                "selected_cols":  ["feat_a", "feat_b", "feat_c"],
            },
        )
        stored = conn.execute(
            "SELECT selected_cols FROM reg.feature_sets WHERE feature_set_id = 'fs_demo'"
        ).fetchone()
        assert stored is not None
        assert "feat_b" in str(stored[0])
    finally:
        conn.close()


def test_upsert_requires_primary_key(tmp_path: Path) -> None:
    reg_path = str(tmp_path / "registry.duckdb")
    conn = registry.open_crud_connection(reg_path)
    try:
        with pytest.raises(ValueError):
            registry.upsert(conn, "models", {"direction": "long"})
    finally:
        conn.close()


def test_attach_live_and_registry_joinable(tmp_path: Path) -> None:
    """live (READ_ONLY) + reg attached on one connection are joinable."""
    live_path = str(tmp_path / "live.duckdb")
    reg_path  = str(tmp_path / "registry.duckdb")
    lab_path  = str(tmp_path / "lab.duckdb")

    # seed a minimal live db
    live = duckdb.connect(live_path)
    try:
        live.execute("CREATE TABLE quant_train (open_time TIMESTAMP, asset_id VARCHAR)")
        live.execute("INSERT INTO quant_train VALUES (TIMESTAMP '2024-01-01', 'solusdt')")
    finally:
        live.close()

    registry.ensure_registry(reg_path)
    conn = registry.open_lab_connection(
        lab_path=lab_path, live_path=live_path, registry_path=reg_path
    )
    try:
        registry.upsert(conn, "assets", {"asset_id": "solusdt", "symbol": "SOLUSDT"})
        joined = conn.execute(
            "SELECT a.symbol, q.open_time"
            " FROM reg.assets a JOIN live.quant_train q ON a.asset_id = q.asset_id"
        ).fetchall()
    finally:
        conn.close()
    assert len(joined) == 1
    assert joined[0][0] == "SOLUSDT"
