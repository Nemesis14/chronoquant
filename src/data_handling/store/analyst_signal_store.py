"""Persistent DuckDB store for normalised analyst-signal calls.

One .duckdb file shared by every source — tradingview, reddit, leaderboard:
database/analyst_signal/analyst_signal.duckdb.  Deliberately separate from the
per-asset market-data stores: a call is source-scoped, not asset-scoped, and the
asset databases stay pure price data for quantitative work.

The calls table is source-agnostic (see plan.md section 3), so the scoring layer
never needs to know which platform a call came from.  Upserts are keyed by
call_id, which makes re-normalising the same raw layer idempotent.
"""

import hashlib
import logging

import duckdb
import polars as pl

from data_handling.store.duckdb_store import (
    get_connection,  # noqa: F401  (re-export: shared RW opener)
)
from data_handling.store.migrations import Migration, run_migrations

logger = logging.getLogger(__name__)

DB_SUBPATH = "database/analyst_signal/analyst_signal.duckdb"


# %% Connection

# %% Schema management

# ---------------------------------------------------------------------------
# Signal-DB migrations (versioned, idempotent).
# Always append — never reorder or delete existing entries.
# ---------------------------------------------------------------------------

def _m001_create_calls_table(conn: duckdb.DuckDBPyConnection) -> None:
    """v1: Create the source-agnostic calls table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calls (
            call_id           VARCHAR PRIMARY KEY,
            source            VARCHAR,
            native_id         VARCHAR,
            native_uuid       VARCHAR,

            author_id         VARCHAR,
            author_name       VARCHAR,

            asset             VARCHAR,
            native_symbol     VARCHAR,

            direction         SMALLINT,
            ts_utc            TIMESTAMP,
            ts_updated_utc    TIMESTAMP,

            horizon_hint_min  INTEGER,
            horizon_hint_raw  VARCHAR,

            confidence        DOUBLE,
            comments_count    BIGINT,
            views_count       BIGINT,

            extraction        VARCHAR,
            raw_ref           VARCHAR,
            first_seen_utc    TIMESTAMP,
            ingested_at_utc   TIMESTAMP
        )
    """)


def _noop(conn: duckdb.DuckDBPyConnection) -> None:
    """No-op migration body.

    v2-v10 formerly created the reddit/universe/scoring/validation tables
    (call_returns, author_scores, score_params, oos_validation, social_hourly,
    reddit_seen_authors, social_features, gate_validation, gate_power).  Those
    tables were dropped in epic_068 / t898 together with their writers.  The
    versions are kept as no-ops so existing databases (which already recorded
    them in _schema_migrations) and fresh databases end up with the same
    migration state, and a fresh DB no longer creates the dropped tables.
    """

SIGNAL_DB_MIGRATIONS: list[Migration] = [
    Migration(version=1, name="create_calls_table", apply=_m001_create_calls_table),
    Migration(version=2, name="create_call_returns_table", apply=_noop),
    Migration(version=3, name="create_scoring_tables", apply=_noop),
    Migration(version=4, name="create_validation_table", apply=_noop),
    Migration(version=5, name="create_social_hourly_table", apply=_noop),
    Migration(version=6, name="create_reddit_seen_authors_table", apply=_noop),
    Migration(version=7, name="add_social_hourly_source", apply=_noop),
    Migration(version=8, name="create_social_features_table", apply=_noop),
    Migration(version=9, name="create_gate_validation_table", apply=_noop),
    Migration(version=10, name="create_gate_power_table", apply=_noop),
]


def ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create and migrate the calls table.

    Args:
        conn : Open DuckDB connection.
    """
    run_migrations(conn, SIGNAL_DB_MIGRATIONS)
    logger.debug("ensure_tables: signal-db migrations applied")


# %% Identity

def make_call_id(source: str, native_id: str) -> str:
    """Return the stable call_id for a source-local publication id.

    Deliberately not derived from author plus timestamp: the raw layer contains
    24 author/minute collisions, which such a key would silently merge.

    Args:
        source    : Source name, e.g. "tradingview".
        native_id : Source-local publication id, stable over time.

    Returns:
        40-character hex sha1 digest.
    """
    return hashlib.sha1(f"{source}|{native_id}".encode()).hexdigest()


# %% Writes

def upsert_calls(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Insert or replace call rows, keyed by call_id.

    Re-running the normalisation over an unchanged raw layer is a no-op in
    effect: every row is rewritten to the same value.

    Args:
        conn : Open DuckDB connection.
        df   : Polars DataFrame whose columns are a subset of the calls schema.

    Returns:
        Number of rows written.
    """
    if df.is_empty():
        return 0

    col_list = ", ".join(f'"{c}"' for c in df.columns)
    conn.register("_calls_tmp", df)
    try:
        conn.execute(
            f"INSERT OR REPLACE INTO calls ({col_list}) "
            f"SELECT {col_list} FROM _calls_tmp ORDER BY ts_utc"
        )
    finally:
        conn.unregister("_calls_tmp")

    logger.info("upsert_calls: written=%d", df.height)
    return df.height
