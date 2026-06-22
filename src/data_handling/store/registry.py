"""Central registry (``reg`` schema) for the DuckDB-native architecture.

The registry is the catalog that links the asset -> snapshot -> feature_set ->
model -> search_run -> strategy -> deployment chain, plus produced artifacts.
It lives in its own DuckDB file (``database/_registry/registry.duckdb``) so the
live sync/trading database and the modeling lab database can ATTACH it READ-ONLY
or read-write as needed.

This module owns:
  * the versioned schema (8 tables) applied via the migrations framework,
  * ATTACH helpers that expose ``live`` (read-only) + ``reg`` in one connection,
  * generic CRUD over every registry table, with a status lifecycle
    (draft -> ... -> archived).

Reference: _doc_/_plans_/data_process_architecture.md sections 4.2, 4.3.
Callers must reach the registry through src/utils.py (config-gateway), never by
opening the registry file directly.
"""

import json
import logging
from typing import Any

import duckdb

from data_handling.store.migrations import Migration, run_migrations

logger = logging.getLogger(__name__)

# %% Schema constants

REG_SCHEMA = "reg"

# Ordered list of registry tables (ER per plan 4.3).
REG_TABLES: tuple[str, ...] = (
    "assets",
    "snapshots",
    "feature_sets",
    "models",
    "search_runs",
    "strategies",
    "deployments",
    "artifacts",
)

# Lifecycle stages for the status column. Order is informational only.
STATUS_LIFECYCLE: tuple[str, ...] = (
    "draft",
    "candidate",
    "champion",
    "active",
    "archived",
)

# Primary-key column for each table (used by upsert/get/set_status).
_PK: dict[str, str] = {
    "assets":       "asset_id",
    "snapshots":    "snapshot_id",
    "feature_sets": "feature_set_id",
    "models":       "model_id",
    "search_runs":  "search_run_id",
    "strategies":   "strategy_id",
    "deployments":  "deployment_id",
    "artifacts":    "artifact_id",
}

# JSON-typed columns per table — dict/list values are serialized on write.
_JSON_COLS: dict[str, frozenset[str]] = {
    "feature_sets": frozenset({"selected_cols"}),
    "search_runs":  frozenset({"best_params"}),
}


# %% Migrations (reg schema)


def _migration_001_reg_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the 8 catalog tables (ER per plan 4.3).

    Tables live in the default (main) schema of registry.duckdb. The ``reg``
    namespace in the plan's SQL (``reg.models``) is the ATTACH alias of this
    file, so cross-database access reads ``reg.<table>`` directly without a
    nested schema (which would otherwise force ``reg.reg.<table>``).
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            asset_id   VARCHAR PRIMARY KEY,
            symbol     VARCHAR,
            interval   VARCHAR,
            market     VARCHAR,
            status     VARCHAR DEFAULT 'draft',
            created_at TIMESTAMP DEFAULT now(),
            updated_at TIMESTAMP DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_id      VARCHAR PRIMARY KEY,
            asset_id         VARCHAR,
            range_start      TIMESTAMP,
            range_end        TIMESTAMP,
            row_count        BIGINT,
            content_sha256   VARCHAR,
            feature_set_hash VARCHAR,
            status           VARCHAR DEFAULT 'draft',
            created_at       TIMESTAMP DEFAULT now(),
            updated_at       TIMESTAMP DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS feature_sets (
            feature_set_id VARCHAR PRIMARY KEY,
            snapshot_id    VARCHAR,
            n_input        INTEGER,
            n_selected     INTEGER,
            selected_cols  JSON,
            status         VARCHAR DEFAULT 'draft',
            created_at     TIMESTAMP DEFAULT now(),
            updated_at     TIMESTAMP DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS models (
            model_id       VARCHAR PRIMARY KEY,
            snapshot_id    VARCHAR,
            feature_set_id VARCHAR,
            search_run_id  VARCHAR,
            direction      VARCHAR,
            status         VARCHAR DEFAULT 'draft',
            oos_metric     DOUBLE,
            created_at     TIMESTAMP DEFAULT now(),
            updated_at     TIMESTAMP DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS search_runs (
            search_run_id VARCHAR PRIMARY KEY,
            model_id      VARCHAR,
            stage         VARCHAR,
            objective     DOUBLE,
            best_params   JSON,
            status        VARCHAR DEFAULT 'draft',
            created_at    TIMESTAMP DEFAULT now(),
            updated_at    TIMESTAMP DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS strategies (
            strategy_id    VARCHAR PRIMARY KEY,
            model_id_long  VARCHAR,
            model_id_short VARCHAR,
            session_id     VARCHAR,
            status         VARCHAR DEFAULT 'draft',
            created_at     TIMESTAMP DEFAULT now(),
            updated_at     TIMESTAMP DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS deployments (
            deployment_id VARCHAR PRIMARY KEY,
            asset_id      VARCHAR,
            strategy_id   VARCHAR,
            active        BOOLEAN DEFAULT FALSE,
            status        VARCHAR DEFAULT 'draft',
            created_at    TIMESTAMP DEFAULT now(),
            updated_at    TIMESTAMP DEFAULT now()
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS artifacts (
            artifact_id VARCHAR PRIMARY KEY,
            owner_id    VARCHAR,
            kind        VARCHAR,
            path        VARCHAR,
            status      VARCHAR DEFAULT 'draft',
            created_at  TIMESTAMP DEFAULT now(),
            updated_at  TIMESTAMP DEFAULT now()
        )
    """)


def _migration_002_deployments_lifecycle(conn: duckdb.DuckDBPyConnection) -> None:
    """Add deploy/cutover lifecycle columns to the deployments table.

    - ``activated_at``          : timestamp when the deployment became active.
    - ``previous_strategy_id``  : strategy_id of the deployment that was replaced,
                                  enabling rollback (re-deploy the previous strategy).

    These columns are nullable — pending/inactive deployments have NULL values.
    """
    conn.execute(
        "ALTER TABLE deployments ADD COLUMN IF NOT EXISTS activated_at TIMESTAMP"
    )
    conn.execute(
        "ALTER TABLE deployments ADD COLUMN IF NOT EXISTS previous_strategy_id VARCHAR"
    )


REG_MIGRATIONS: list[Migration] = [
    Migration(version=1, name="reg_schema_initial",        apply=_migration_001_reg_schema),
    Migration(version=2, name="deployments_lifecycle_cols", apply=_migration_002_deployments_lifecycle),
]


# %% Connection / schema setup


def get_registry_connection(registry_path: str) -> duckdb.DuckDBPyConnection:
    """Open a read-write connection to the registry database, ensuring its dir.

    Args:
        registry_path : Absolute path to registry.duckdb.

    Returns:
        Open DuckDB connection. Caller must close it.
    """
    from pathlib import Path

    Path(registry_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(registry_path)


def ensure_registry(registry_path: str) -> list[int]:
    """Create/upgrade the registry schema by running pending reg migrations.

    Safe to call repeatedly — applied migrations are skipped. Creates the
    registry.duckdb file and the 8 reg tables on first call.

    Args:
        registry_path : Absolute path to registry.duckdb.

    Returns:
        List of migration versions newly applied during this call.
    """
    conn = get_registry_connection(registry_path)
    try:
        applied = run_migrations(conn, REG_MIGRATIONS)
    finally:
        conn.close()
    logger.info("ensure_registry: path=%s newly_applied=%s", registry_path, applied)
    return applied


def open_crud_connection(registry_path: str) -> duckdb.DuckDBPyConnection:
    """Open a CRUD connection where the registry tables resolve as ``reg.<table>``.

    Attaches registry.duckdb under the ``reg`` alias from an in-memory session,
    so the CRUD default alias works identically whether accessed standalone here
    or alongside the lab/live databases (open_lab_connection). Ensures the schema
    exists first.

    Args:
        registry_path : Absolute path to registry.duckdb.

    Returns:
        Open DuckDB connection with the registry attached as ``reg``. Caller closes it.
    """
    ensure_registry(registry_path)
    conn = duckdb.connect(":memory:")
    attach_registry(conn, registry_path, read_only=False)
    return conn


def attach_registry(
    conn          : duckdb.DuckDBPyConnection,
    registry_path : str,
    read_only     : bool = False,
    alias         : str = "reg",
) -> None:
    """ATTACH the registry database onto an existing connection under ``alias``.

    Args:
        conn          : Open DuckDB connection (e.g. the lab database).
        registry_path : Absolute path to registry.duckdb.
        read_only     : If True, attach READ_ONLY.
        alias         : Schema alias to expose the registry under (default 'reg').
    """
    mode = " (READ_ONLY)" if read_only else ""
    conn.execute(f"ATTACH '{registry_path}' AS {alias}{mode}")
    logger.debug("attach_registry: alias=%s read_only=%s", alias, read_only)


def attach_live(
    conn      : duckdb.DuckDBPyConnection,
    live_path  : str,
    read_only : bool = True,
    alias     : str = "live",
) -> None:
    """ATTACH the live asset database, READ_ONLY by default (modeling side).

    The modeling/lab connection joins live tables (quant_train, predictions)
    without risking writes to the live sync target — see plan 4.2.

    Args:
        conn      : Open DuckDB connection (e.g. the lab database, default DB).
        live_path : Absolute path to the asset live .duckdb file.
        read_only : If True (default), attach READ_ONLY.
        alias     : Schema alias to expose the live database under (default 'live').
    """
    mode = " (READ_ONLY)" if read_only else ""
    conn.execute(f"ATTACH '{live_path}' AS {alias}{mode}")
    logger.debug("attach_live: alias=%s read_only=%s", alias, read_only)


def open_lab_connection(
    lab_path      : str,
    live_path     : str,
    registry_path : str,
) -> duckdb.DuckDBPyConnection:
    """Open a modeling connection: lab default + live (READ_ONLY) + reg attached.

    Mirrors the topology in plan 4.2 — a single connection from which
    ``snap`` / ``model`` / ``strat`` (lab), ``live.*`` and ``reg.*`` are all
    joinable, while the live sync target stays write-isolated.

    Args:
        lab_path      : Absolute path to the lab .duckdb (default database).
        live_path     : Absolute path to the live asset .duckdb (attached RO).
        registry_path : Absolute path to registry.duckdb (attached read-write).

    Returns:
        Open DuckDB connection with live + reg attached. Caller must close it.
    """
    from pathlib import Path

    Path(lab_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(lab_path)
    attach_live(conn, live_path, read_only=True)
    attach_registry(conn, registry_path, read_only=False)
    return conn


# %% CRUD


def _serialize_value(table: str, column: str, value: Any) -> Any:
    """Serialize dict/list values for JSON columns; pass everything else through."""
    if column in _JSON_COLS.get(table, frozenset()) and isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def _validate_table(table: str) -> str:
    """Return the primary-key column for a registry table, validating the name."""
    if table not in _PK:
        raise ValueError(f"Unknown registry table: {table!r}")
    return _PK[table]


def upsert(
    conn  : duckdb.DuckDBPyConnection,
    table : str,
    row   : dict[str, Any],
    alias : str = "reg",
) -> str:
    """Insert or update a single row in a registry table, keyed by its PK.

    Uses INSERT ... ON CONFLICT DO UPDATE so the call is idempotent. The
    ``updated_at`` column is refreshed on every write. JSON-typed columns
    accept dict/list values and are serialized automatically.

    Args:
        conn  : Open connection with the registry attached under ``alias``.
        table : One of REG_TABLES.
        row   : Column -> value mapping; must include the table's PK column.
        alias : Schema alias the registry is attached under (default 'reg').

    Returns:
        The primary-key value of the upserted row.

    Raises:
        ValueError: If the table is unknown or the PK column is missing from row.
    """
    pk = _validate_table(table)
    if pk not in row:
        raise ValueError(f"upsert into {table}: missing primary key column {pk!r}")

    cols   = list(row.keys())
    values = [_serialize_value(table, c, row[c]) for c in cols]

    col_sql      = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    update_cols  = [c for c in cols if c != pk]
    set_sql      = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    set_sql      = f"{set_sql}, updated_at = now()" if set_sql else "updated_at = now()"

    conn.execute(
        f"""
        INSERT INTO {alias}.{table} ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT ("{pk}") DO UPDATE SET {set_sql}
        """,
        values,
    )
    pk_value = str(row[pk])
    logger.debug("upsert: %s.%s %s=%s", alias, table, pk, pk_value)
    return pk_value


def get(
    conn  : duckdb.DuckDBPyConnection,
    table : str,
    pk_value : str,
    alias : str = "reg",
) -> dict[str, Any] | None:
    """Fetch a single registry row by primary key as a column->value dict.

    Args:
        conn     : Open connection with the registry attached under ``alias``.
        table    : One of REG_TABLES.
        pk_value : Primary-key value to look up.
        alias    : Schema alias (default 'reg').

    Returns:
        Row as a dict, or None if no row matches.
    """
    pk     = _validate_table(table)
    cursor = conn.execute(
        f'SELECT * FROM {alias}.{table} WHERE "{pk}" = ?', [pk_value]
    )
    fetched = cursor.fetchone()
    if fetched is None:
        return None
    columns = [d[0] for d in cursor.description]
    return dict(zip(columns, fetched, strict=True))


def list_rows(
    conn   : duckdb.DuckDBPyConnection,
    table  : str,
    status : str | None = None,
    alias  : str = "reg",
) -> list[dict[str, Any]]:
    """List registry rows, optionally filtered by status.

    Args:
        conn   : Open connection with the registry attached under ``alias``.
        table  : One of REG_TABLES.
        status : If given, return only rows with this status value.
        alias  : Schema alias (default 'reg').

    Returns:
        List of row dicts (possibly empty).
    """
    _validate_table(table)
    if status is None:
        cursor = conn.execute(f"SELECT * FROM {alias}.{table}")
    else:
        cursor = conn.execute(
            f"SELECT * FROM {alias}.{table} WHERE status = ?", [status]
        )
    rows    = cursor.fetchall()
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, r, strict=True)) for r in rows]


def set_status(
    conn     : duckdb.DuckDBPyConnection,
    table    : str,
    pk_value : str,
    status   : str,
    alias    : str = "reg",
) -> None:
    """Update the status of a registry row, refreshing updated_at.

    Args:
        conn     : Open connection with the registry attached under ``alias``.
        table    : One of REG_TABLES.
        pk_value : Primary-key value of the row to update.
        status   : New status value (typically from STATUS_LIFECYCLE).
        alias    : Schema alias (default 'reg').
    """
    pk = _validate_table(table)
    conn.execute(
        f'UPDATE {alias}.{table} SET status = ?, updated_at = now() WHERE "{pk}" = ?',
        [status, pk_value],
    )
    logger.debug("set_status: %s.%s %s=%s -> %s", alias, table, pk, pk_value, status)


def delete(
    conn     : duckdb.DuckDBPyConnection,
    table    : str,
    pk_value : str,
    alias    : str = "reg",
) -> None:
    """Delete a registry row by primary key.

    Prefer set_status(..., 'archived') for lifecycle retirement; use delete only
    for genuine removal (e.g. test cleanup).

    Args:
        conn     : Open connection with the registry attached under ``alias``.
        table    : One of REG_TABLES.
        pk_value : Primary-key value of the row to delete.
        alias    : Schema alias (default 'reg').
    """
    pk = _validate_table(table)
    conn.execute(f'DELETE FROM {alias}.{table} WHERE "{pk}" = ?', [pk_value])
    logger.debug("delete: %s.%s %s=%s", alias, table, pk, pk_value)
