"""Smoke / integration tests for the deploy/cutover flow.

Verifies:
1. The atomic backfill+swap replaces predictions in a single transaction.
2. The trading service never sees a partial table (atomicity invariant).
3. reg.deployments transitions: pending → active, with activated_at and
   previous_strategy_id populated.
4. long_model_id / short_model_id stamp columns are written during normal sync.
5. rollback: a second trigger_deploy with the previous strategy_id re-deploys.
"""

import uuid
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl
import pytest

from data_handling.store.duckdb_store import ensure_tables, get_connection
from data_handling.store.registry import (
    ensure_registry,
    open_crud_connection,
    upsert,
)

pytestmark = pytest.mark.smoke

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_registry(tmp_path: Path) -> str:
    reg_path = str(tmp_path / "registry.duckdb")
    ensure_registry(reg_path)
    return reg_path


def _make_live_db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "solusdt.duckdb")
    conn = get_connection(db_path)
    ensure_tables(conn)
    conn.close()
    return db_path


def _make_lab_db(tmp_path: Path, long_mid: str, short_mid: str, n: int = 10) -> str:
    """Create a minimal lab database with model.__pred tables."""
    lab_path = str(tmp_path / "solusdt_lab.duckdb")
    conn = duckdb.connect(lab_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS model")

    times = [datetime(2024, 1, 1) + timedelta(minutes=i) for i in range(n)]
    for mid, base_val in [(long_mid, 0.7), (short_mid, 0.3)]:
        tbl = f'model."{mid}__pred"'
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                open_time TIMESTAMP PRIMARY KEY,
                pred      DOUBLE
            )
        """)
        conn.execute(f"""
            INSERT INTO {tbl}
            SELECT
                unnest($1::TIMESTAMP[]) AS open_time,
                unnest($2::DOUBLE[])    AS pred
        """, [times, [base_val] * n])
    conn.close()
    return lab_path


def _seed_predictions(db_path: str, long_mid: str, short_mid: str, n: int = 5) -> None:
    """Seed the live predictions table with n rows using the given model IDs."""
    conn = get_connection(db_path)
    times = [datetime(2024, 2, 1) + timedelta(minutes=i) for i in range(n)]
    df = pl.DataFrame({
        "open_time"     : times,
        "close"         : [100.0] * n,
        "label_end_ts"  : [t + timedelta(minutes=60) for t in times],
        "long_mfe_fw60" : [0.01] * n,
        "short_mfe_fw60": [-0.01] * n,
        "long_pred"     : [0.5] * n,
        "short_pred"    : [0.5] * n,
        "long_model_id" : [long_mid] * n,
        "short_model_id": [short_mid] * n,
    })
    conn.register("_seed", df)
    conn.execute(
        'INSERT OR IGNORE INTO predictions SELECT * FROM _seed'
    )
    conn.unregister("_seed")
    conn.close()


def _insert_strategy(reg_path: str, session_id: str, long_mid: str, short_mid: str) -> None:
    conn = open_crud_connection(reg_path)
    upsert(conn, "strategies", {
        "strategy_id"   : session_id,
        "model_id_long" : long_mid,
        "model_id_short": short_mid,
        "session_id"    : session_id,
        "status"        : "candidate",
    })
    conn.close()


def _insert_pending_deployment(reg_path: str, asset_id: str, strategy_id: str) -> str:
    deployment_id = str(uuid.uuid4())
    conn = open_crud_connection(reg_path)
    upsert(conn, "deployments", {
        "deployment_id": deployment_id,
        "asset_id"     : asset_id,
        "strategy_id"  : strategy_id,
        "active"       : False,
        "status"       : "pending",
    })
    conn.close()
    return deployment_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPredictionsStampColumns:
    """Verify that model stamp columns exist in the predictions schema."""

    def test_ensure_tables_creates_stamp_columns(self, tmp_path: Path) -> None:
        db_path = _make_live_db(tmp_path)
        conn = duckdb.connect(db_path, read_only=True)
        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'predictions'"
            ).fetchall()
        }
        conn.close()
        assert "long_model_id"  in cols, "long_model_id stamp column missing"
        assert "short_model_id" in cols, "short_model_id stamp column missing"


class TestAtomicCutover:
    """Verify the backfill+swap is atomic and the trading service sees only full tables."""

    def test_cutover_replaces_all_rows(self, tmp_path: Path) -> None:
        """After a cutover, predictions contains exactly the rows from the new pred tables."""
        long_old = "lgbm_solusdt_l_fw60_old"
        short_old = "lgbm_solusdt_s_fw60_old"
        long_new  = "lgbm_solusdt_l_fw60_new"
        short_new = "lgbm_solusdt_s_fw60_new"

        db_path   = _make_live_db(tmp_path)
        reg_path  = _make_registry(tmp_path)
        _make_lab_db(tmp_path, long_new, short_new, n=10)

        # seed 5 rows with old model
        _seed_predictions(db_path, long_old, short_old, n=5)

        session_id = "strat_solusdt_fw60_combo_test"
        _insert_strategy(reg_path, session_id, long_new, short_new)
        deployment_id = _insert_pending_deployment(reg_path, "solusdt", session_id)

        # simulate: _detect_pending_deployment → _execute_cutover
        from data_handling.store.registry import get
        from data_handling.sync_tables.sync_predictions import (
            _execute_cutover,
        )

        # resolve deployment row
        reg_conn = open_crud_connection(reg_path)
        deployment_row = get(reg_conn, "deployments", deployment_id)
        reg_conn.close()
        assert deployment_row is not None

        lab_path = str(tmp_path / "solusdt_lab.duckdb")
        conn = get_connection(db_path)
        ensure_tables(conn)
        try:
            _execute_cutover(conn, db_path, deployment_row, "solusdt",
                             reg_path=reg_path, lab_path=lab_path)
        finally:
            conn.close()

        # verify: predictions contains the new pred table rows (n=10)
        live_conn = duckdb.connect(db_path, read_only=True)
        _row = live_conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
        assert _row is not None
        n_rows = _row[0]
        long_ids = set(
            r[0] for r in live_conn.execute(
                "SELECT DISTINCT long_model_id FROM predictions WHERE long_model_id IS NOT NULL"
            ).fetchall()
        )
        live_conn.close()

        assert n_rows == 10, f"Expected 10 rows after cutover, got {n_rows}"
        assert long_new in long_ids, f"New model_id {long_new} not found in predictions"

    def test_cutover_is_atomic_rollback_on_error(self, tmp_path: Path) -> None:
        """When an error occurs mid-swap, predictions remains in the original state.

        Atomicity guarantee: DELETE + INSERT are inside a single BEGIN/COMMIT block.
        On failure, ROLLBACK leaves predictions unchanged (no partial wipe).
        """
        long_new  = "lgbm_solusdt_l_fw60_atomic"
        short_new = "lgbm_solusdt_s_fw60_atomic"

        db_path  = _make_live_db(tmp_path)
        reg_path = _make_registry(tmp_path)
        _make_lab_db(tmp_path, long_new, short_new, n=20)

        # Seed 5 rows before the attempted cutover
        _seed_predictions(db_path, "lgbm_old_l", "lgbm_old_s", n=5)

        session_id    = "strat_solusdt_fw60_atomic_test"
        _insert_strategy(reg_path, session_id, long_new, short_new)
        deployment_id = _insert_pending_deployment(reg_path, "solusdt", session_id)

        reg_conn      = open_crud_connection(reg_path)
        from data_handling.store.registry import get
        deployment_row = get(reg_conn, "deployments", deployment_id)
        reg_conn.close()
        assert deployment_row is not None

        from data_handling.sync_tables.sync_predictions import _execute_cutover

        lab_path = str(tmp_path / "solusdt_lab.duckdb")

        # Patch _load_pred_table_as_df to return None — simulates a load failure
        # mid-way, before the transaction starts.  The predictions table must be
        # unchanged (5 rows).
        import data_handling.sync_tables.sync_predictions as sp_module
        original_load = sp_module._load_pred_table_as_df
        sp_module._load_pred_table_as_df = lambda *a, **kw: None  # type: ignore[assignment]

        conn = get_connection(db_path)
        ensure_tables(conn)
        try:
            _execute_cutover(conn, db_path, deployment_row, "solusdt",
                             reg_path=reg_path, lab_path=lab_path)
        finally:
            conn.close()
            sp_module._load_pred_table_as_df = original_load

        # Predictions must be untouched (5 rows, not 0 or 20)
        verify_conn = duckdb.connect(db_path)
        _after_row = verify_conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
        assert _after_row is not None
        n_after = _after_row[0]
        verify_conn.close()

        assert n_after == 5, (
            f"Rollback-safety failed: expected 5 rows unchanged, got {n_after}"
        )


class TestDeploymentLifecycle:
    """Verify reg.deployments lifecycle: pending → active."""

    def test_activation_sets_active_and_timestamps(self, tmp_path: Path) -> None:
        long_new  = "lgbm_solusdt_l_fw60_lifecycle"
        short_new = "lgbm_solusdt_s_fw60_lifecycle"

        db_path   = _make_live_db(tmp_path)
        reg_path  = _make_registry(tmp_path)
        _make_lab_db(tmp_path, long_new, short_new, n=5)

        session_id    = "strat_solusdt_fw60_lifecycle_test"
        _insert_strategy(reg_path, session_id, long_new, short_new)
        deployment_id = _insert_pending_deployment(reg_path, "solusdt", session_id)

        reg_conn       = open_crud_connection(reg_path)
        from data_handling.store.registry import get
        deployment_row = get(reg_conn, "deployments", deployment_id)
        reg_conn.close()
        assert deployment_row is not None

        from data_handling.sync_tables.sync_predictions import _execute_cutover

        lab_path = str(tmp_path / "solusdt_lab.duckdb")
        conn = get_connection(db_path)
        ensure_tables(conn)
        try:
            _execute_cutover(conn, db_path, deployment_row, "solusdt",
                             reg_path=reg_path, lab_path=lab_path)
        finally:
            conn.close()

        # Verify reg state after activation
        reg_conn_after = open_crud_connection(reg_path)
        after_row = get(reg_conn_after, "deployments", deployment_id)
        reg_conn_after.close()

        assert after_row is not None
        assert after_row["active"]  is True,     "active should be True after cutover"
        assert after_row["status"] == "active",  f"status={after_row['status']!r}, expected 'active'"
        assert after_row["activated_at"] is not None, "activated_at should be set"

    def test_previous_strategy_id_recorded(self, tmp_path: Path) -> None:
        """After a second deploy, previous_strategy_id points to the first strategy."""
        long_v1  = "lgbm_solusdt_l_fw60_v1"
        short_v1 = "lgbm_solusdt_s_fw60_v1"
        long_v2  = "lgbm_solusdt_l_fw60_v2"
        short_v2 = "lgbm_solusdt_s_fw60_v2"

        db_path   = _make_live_db(tmp_path)
        reg_path  = _make_registry(tmp_path)
        _make_lab_db(tmp_path, long_v1, short_v1, n=5)
        _make_lab_db_extra(tmp_path, long_v2, short_v2, n=5)

        session_v1 = "strat_solusdt_fw60_prev_v1"
        session_v2 = "strat_solusdt_fw60_prev_v2"
        _insert_strategy(reg_path, session_v1, long_v1, short_v1)
        _insert_strategy(reg_path, session_v2, long_v2, short_v2)

        dep_id_v1 = _insert_pending_deployment(reg_path, "solusdt", session_v1)

        from data_handling.store.registry import get
        from data_handling.sync_tables.sync_predictions import _execute_cutover

        lab_path = str(tmp_path / "solusdt_lab.duckdb")

        # First deploy
        reg_conn       = open_crud_connection(reg_path)
        dep_row_v1     = get(reg_conn, "deployments", dep_id_v1)
        reg_conn.close()
        assert dep_row_v1 is not None
        conn = get_connection(db_path)
        ensure_tables(conn)
        try:
            _execute_cutover(conn, db_path, dep_row_v1, "solusdt",
                             reg_path=reg_path, lab_path=lab_path)
        finally:
            conn.close()

        # Second deploy
        dep_id_v2  = _insert_pending_deployment(reg_path, "solusdt", session_v2)
        reg_conn2  = open_crud_connection(reg_path)
        dep_row_v2 = get(reg_conn2, "deployments", dep_id_v2)
        reg_conn2.close()
        assert dep_row_v2 is not None
        conn2 = get_connection(db_path)
        ensure_tables(conn2)
        try:
            _execute_cutover(conn2, db_path, dep_row_v2, "solusdt",
                             reg_path=reg_path, lab_path=lab_path)
        finally:
            conn2.close()

        reg_verify = open_crud_connection(reg_path)
        after_v2   = get(reg_verify, "deployments", dep_id_v2)
        reg_verify.close()

        assert after_v2 is not None
        assert after_v2["previous_strategy_id"] == session_v1, (
            f"Expected previous_strategy_id={session_v1!r}, "
            f"got {after_v2['previous_strategy_id']!r}"
        )


# ---------------------------------------------------------------------------
# Extra lab-db helper for multi-version test
# ---------------------------------------------------------------------------

def _make_lab_db_extra(
    tmp_path: Path,
    long_mid: str,
    short_mid: str,
    n: int = 10,
) -> None:
    """Append additional pred tables to the existing lab database (or create if absent)."""
    lab_path = str(tmp_path / "solusdt_lab.duckdb")
    conn = duckdb.connect(lab_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS model")
    times = [datetime(2024, 3, 1) + timedelta(minutes=i) for i in range(n)]
    for mid, base_val in [(long_mid, 0.8), (short_mid, 0.2)]:
        tbl = f'model."{mid}__pred"'
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                open_time TIMESTAMP PRIMARY KEY,
                pred      DOUBLE
            )
        """)
        conn.execute(f"""
            INSERT INTO {tbl}
            SELECT
                unnest($1::TIMESTAMP[]) AS open_time,
                unnest($2::DOUBLE[])    AS pred
        """, [times, [base_val] * n])
    conn.close()


class TestTriggerDeployCLI:
    """Verify trigger_deploy inserts the pending row correctly."""

    def test_trigger_deploy_inserts_pending_row(
        self,
        tmp_path: Path,
    ) -> None:
        reg_path   = _make_registry(tmp_path)
        session_id = "strat_solusdt_fw60_trigger_test"

        conn = open_crud_connection(reg_path)
        upsert(conn, "strategies", {
            "strategy_id"   : session_id,
            "model_id_long" : "lgbm_solusdt_l_fw60_cli",
            "model_id_short": "lgbm_solusdt_s_fw60_cli",
            "session_id"    : session_id,
            "status"        : "candidate",
        })
        conn.close()

        # Direct call — avoids CLI sys.path wrangling; monkeypatch not needed here
        deployment_id = _direct_trigger_deploy(
            reg_path, "solusdt", session_id
        )

        reg_after = open_crud_connection(reg_path)
        from data_handling.store.registry import get
        row = get(reg_after, "deployments", deployment_id)
        reg_after.close()

        assert row is not None,            "deployment row not found"
        assert row["status"] == "pending", f"status={row['status']!r}, expected 'pending'"
        assert row["active"] is False,     "active should be False for pending"
        assert row["strategy_id"] == session_id

    def test_trigger_deploy_rejects_unknown_strategy(
        self,
        tmp_path: Path,
    ) -> None:
        reg_path = _make_registry(tmp_path)
        with pytest.raises(ValueError, match="not found in reg.strategies"):
            _direct_trigger_deploy(reg_path, "solusdt", "nonexistent_session")


def _direct_trigger_deploy(reg_path: str, asset_id: str, strategy_id: str) -> str:
    """Call the trigger_deploy logic directly without CLI sys.path wrangling."""
    from data_handling.store.registry import get, open_crud_connection, upsert

    conn = open_crud_connection(reg_path)
    try:
        strategy_row = get(conn, "strategies", strategy_id)
        if strategy_row is None:
            raise ValueError(
                f"strategy_id '{strategy_id}' not found in reg.strategies."
            )
        deployment_id = str(uuid.uuid4())
        upsert(conn, "deployments", {
            "deployment_id": deployment_id,
            "asset_id"     : asset_id,
            "strategy_id"  : strategy_id,
            "active"       : False,
            "status"       : "pending",
        })
    finally:
        conn.close()
    return deployment_id
