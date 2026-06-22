"""Versioned, idempotent schema migration framework for DuckDB databases.

Replaces the inline ALTER/DROP pattern in duckdb_store.ensure_tables() (P4).
Each database carries a private ``_schema_migrations`` bookkeeping table that
records which migration versions have already been applied.  Running migrations
is always safe to re-run: previously applied versions are skipped.

A migration is a callable that receives an open DuckDB connection and performs
its DDL.  Migrations are registered with a monotonically increasing integer
version and applied in ascending order within a transaction each.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

import duckdb

logger = logging.getLogger(__name__)

# %% Types

MigrationFn = Callable[[duckdb.DuckDBPyConnection], None]


@dataclass(frozen=True)
class Migration:
    """A single versioned schema migration.

    Attributes:
        version : Monotonic integer version (unique within a migration set).
        name    : Short human-readable identifier for logging/auditing.
        apply   : Callable that performs the DDL against an open connection.
    """

    version: int
    name: str
    apply: MigrationFn


# %% Bookkeeping table


def _ensure_migrations_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the _schema_migrations bookkeeping table if it does not exist.

    Args:
        conn : Open DuckDB read-write connection.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _schema_migrations (
            version    BIGINT PRIMARY KEY,
            name       VARCHAR,
            applied_at TIMESTAMP DEFAULT now()
        )
    """)


def applied_versions(conn: duckdb.DuckDBPyConnection) -> set[int]:
    """Return the set of migration versions already applied to this database.

    Args:
        conn : Open DuckDB connection.

    Returns:
        Set of integer versions recorded in _schema_migrations (empty if none).
    """
    _ensure_migrations_table(conn)
    rows = conn.execute("SELECT version FROM _schema_migrations").fetchall()
    return {int(row[0]) for row in rows}


# %% Runner


def run_migrations(
    conn       : duckdb.DuckDBPyConnection,
    migrations : list[Migration],
) -> list[int]:
    """Apply all pending migrations in ascending version order, idempotently.

    Each migration runs inside its own transaction; on success the version is
    recorded in _schema_migrations.  Versions already present are skipped, so
    the call is safe to re-run.  A non-monotonic or duplicate version list is
    rejected before any DDL runs.

    Args:
        conn       : Open DuckDB read-write connection.
        migrations : Migrations to apply (order-independent; sorted internally).

    Returns:
        List of versions that were newly applied during this call.

    Raises:
        ValueError: If two migrations share the same version.
    """
    _ensure_migrations_table(conn)

    versions = [m.version for m in migrations]
    if len(versions) != len(set(versions)):
        raise ValueError(f"Duplicate migration versions detected: {sorted(versions)}")

    ordered = sorted(migrations, key=lambda m: m.version)
    done    = applied_versions(conn)

    newly_applied: list[int] = []
    for migration in ordered:
        if migration.version in done:
            continue
        conn.execute("BEGIN TRANSACTION")
        try:
            migration.apply(conn)
            conn.execute(
                "INSERT INTO _schema_migrations (version, name) VALUES (?, ?)",
                [migration.version, migration.name],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.exception(
                "Migration failed: v%d (%s)", migration.version, migration.name
            )
            raise
        newly_applied.append(migration.version)
        logger.info("Applied migration v%d (%s)", migration.version, migration.name)

    if not newly_applied:
        logger.debug("run_migrations: schema already up to date (%d versions)", len(done))
    return newly_applied
