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
from pathlib import Path

import duckdb
import polars as pl

from data_handling.store.migrations import Migration, run_migrations

logger = logging.getLogger(__name__)

DB_SUBPATH = "database/analyst_signal/analyst_signal.duckdb"


# %% Connection

def get_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Return a persistent read-write DuckDB connection for the signal store.

    Args:
        db_path : Absolute path to analyst_signal.duckdb.

    Returns:
        Open DuckDB connection.  Caller must close it.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


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


def _m002_create_call_returns_table(conn: duckdb.DuckDBPyConnection) -> None:
    """v2: Create the per-call, per-horizon forward-return table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS call_returns (
            call_id          VARCHAR,
            horizon_min      INTEGER,

            entry_ts         TIMESTAMP,
            entry_price      DOUBLE,
            exit_ts          TIMESTAMP,
            exit_price       DOUBLE,

            r_mkt            DOUBLE,
            r_raw            DOUBLE,

            drift_bucket     DOUBLE,
            drift_trailing   DOUBLE,
            r_excess         DOUBLE,
            r_excess_trail   DOUBLE,

            vol              DOUBLE,
            z                DOUBLE,

            computed_at_utc  TIMESTAMP,
            PRIMARY KEY (call_id, horizon_min)
        )
    """)


def _m003_create_scoring_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """v3: Create the rolling author-score table and its per-fit diagnostics."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS author_scores (
            as_of_date      DATE,
            horizon_min     INTEGER,
            author_id       VARCHAR,
            author_name     VARCHAR,

            n_calls         INTEGER,
            raw_mean        DOUBLE,
            shrink_weight   DOUBLE,
            shrunk_score    DOUBLE,
            rank            INTEGER,

            fwd_n_calls     INTEGER,
            fwd_mean_z      DOUBLE,

            computed_at_utc TIMESTAMP,
            PRIMARY KEY (as_of_date, horizon_min, author_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS score_params (
            as_of_date      DATE,
            horizon_min     INTEGER,
            n_authors       INTEGER,
            n_train_calls   INTEGER,
            grand_mean      DOUBLE,
            sigma2_within   DOUBLE,
            tau2_between    DOUBLE,
            shrink_k        DOUBLE,
            computed_at_utc TIMESTAMP,
            PRIMARY KEY (as_of_date, horizon_min)
        )
    """)


def _m004_create_validation_table(conn: duckdb.DuckDBPyConnection) -> None:
    """v4: Create the placebo / multiple-testing validation results table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oos_validation (
            test_name          VARCHAR,
            horizon_min        INTEGER,
            min_calls          INTEGER,

            n_authors          INTEGER,
            statistic_real     DOUBLE,
            null_mean          DOUBLE,
            null_p05           DOUBLE,
            null_p95           DOUBLE,
            p_value            DOUBLE,
            n_permutations     INTEGER,

            n_raw_p05          INTEGER,
            n_fdr10            INTEGER,
            n_fdr05            INTEGER,

            computed_at_utc    TIMESTAMP,
            PRIMARY KEY (test_name, horizon_min, min_calls)
        )
    """)


def _m005_create_social_hourly_table(conn: duckdb.DuckDBPyConnection) -> None:
    """v5: Create the hourly Reddit attention aggregate table (epic_053 / t202).

    Deliberately count-only. score/num_comments/upvote_ratio are never stored
    here: in the source dumps those fields reflect the crawl-time value, not
    the value at creation, which is a lookahead leak against the bar.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS social_hourly (
            ts_utc          TIMESTAMP,
            subreddit       VARCHAR,
            kind            VARCHAR,

            n_items         BIGINT,
            n_authors       BIGINT,
            n_new_authors   BIGINT,
            n_chars         BIGINT,

            ingested_at_utc TIMESTAMP,
            PRIMARY KEY (ts_utc, subreddit, kind)
        )
    """)


def _m006_create_reddit_seen_authors_table(conn: duckdb.DuckDBPyConnection) -> None:
    """v6: Bookkeeping table for n_new_authors -- first time an author is seen
    per subreddit, across all ingest runs. Not part of the t202 schema spec,
    but required to compute n_new_authors correctly on incremental/out-of-order
    ingests; documented in the t202 ticket Notes.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reddit_seen_authors (
            subreddit         VARCHAR,
            author            VARCHAR,
            first_seen_ts_utc TIMESTAMP,
            PRIMARY KEY (subreddit, author)
        )
    """)


def _m007_add_social_hourly_source(conn: duckdb.DuckDBPyConnection) -> None:
    """v7: Add the `source` column to social_hourly and put it in the PK.

    The backfill comes from the Watchful1 per-subreddit dumps, the 2026 gap-fill
    from the Arctic Shift API (epic_053 / t203). Their coverage is not identical
    -- different deletion handling, possibly missing hours -- so a row must
    carry which pipeline produced it. Without this the two sources collide on
    (ts_utc, subreddit, kind) and an overlap comparison becomes impossible.

    DuckDB cannot alter a primary key in place, so the table is rebuilt.
    Rows written before this migration came from the API proof run and are
    stamped `arctic_shift`.
    """
    has_source = conn.execute("""
        SELECT count(*) FROM information_schema.columns
        WHERE table_name = 'social_hourly' AND column_name = 'source'
    """).fetchone()
    if has_source and has_source[0]:
        return

    conn.execute("""
        CREATE TABLE social_hourly__v7 (
            ts_utc          TIMESTAMP,
            subreddit       VARCHAR,
            kind            VARCHAR,
            source          VARCHAR,

            n_items         BIGINT,
            n_authors       BIGINT,
            n_new_authors   BIGINT,
            n_chars         BIGINT,

            ingested_at_utc TIMESTAMP,
            PRIMARY KEY (ts_utc, subreddit, kind, source)
        )
    """)
    conn.execute("""
        INSERT INTO social_hourly__v7
        SELECT ts_utc, subreddit, kind, 'arctic_shift' AS source,
               n_items, n_authors, n_new_authors, n_chars, ingested_at_utc
        FROM social_hourly
    """)
    conn.execute("DROP TABLE social_hourly")
    conn.execute("ALTER TABLE social_hourly__v7 RENAME TO social_hourly")


def _m008_create_social_features_table(conn: duckdb.DuckDBPyConnection) -> None:
    """v8: Normalised attention features derived from social_hourly (epic_053 / t204).

    Every column here already carries the project's t-1 lag and is normalised
    against a trailing window only -- see modeling/analyst_signal/social_features.py.
    Nothing in this table may be recomputed with full-sample statistics.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS social_features (
            ts_utc            TIMESTAMP,
            subreddit         VARCHAR,

            att_z             DOUBLE,
            att_accel         DOUBLE,
            authors_z         DOUBLE,
            new_author_share  DOUBLE,
            items_per_author  DOUBLE,
            chars_per_item    DOUBLE,

            computed_at_utc   TIMESTAMP,
            PRIMARY KEY (ts_utc, subreddit)
        )
    """)


def _m009_create_gate_validation_table(conn: duckdb.DuckDBPyConnection) -> None:
    """v9: t205 gate-measurement results -- IC/MI plus block-permutation placebo
    and BH-FDR, one row per (regime, series, feature, target).

    `series` is a subreddit name or the synthetic `ALL_AVG` cross-subreddit
    average of `att_z`. `target_kind` is `magnitude` (the epic's hypothesis) or
    `direction` (the placebo-should-catch-this control).
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gate_validation (
            regime            VARCHAR,
            series            VARCHAR,
            feature           VARCHAR,
            target            VARCHAR,
            target_kind       VARCHAR,

            n_obs             INTEGER,
            block_len_hours   INTEGER,
            n_permutations    INTEGER,

            spearman_ic       DOUBLE,
            ic_perm_p         DOUBLE,
            ic_fdr_p          DOUBLE,

            mi_bits           DOUBLE,
            mi_perm_p         DOUBLE,
            mi_fdr_p          DOUBLE,

            computed_at_utc   TIMESTAMP,
            PRIMARY KEY (regime, series, feature, target)
        )
    """)


def _m010_create_gate_power_table(conn: duckdb.DuckDBPyConnection) -> None:
    """v10: t205 power analysis -- minimum |Spearman rho| detectable at 80%
    power, one row per regime, at both the nominal and family-wise
    (Bonferroni over the regime's test family) significance level.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gate_power (
            regime                          VARCHAR,
            n_obs                           INTEGER,
            family_size                     INTEGER,

            alpha_nominal                   DOUBLE,
            alpha_bonferroni                DOUBLE,
            min_detectable_rho_nominal      DOUBLE,
            min_detectable_rho_bonferroni   DOUBLE,
            max_observed_abs_ic             DOUBLE,

            computed_at_utc                 TIMESTAMP,
            PRIMARY KEY (regime)
        )
    """)


SIGNAL_DB_MIGRATIONS: list[Migration] = [
    Migration(version=1, name="create_calls_table", apply=_m001_create_calls_table),
    Migration(version=2, name="create_call_returns_table", apply=_m002_create_call_returns_table),
    Migration(version=3, name="create_scoring_tables", apply=_m003_create_scoring_tables),
    Migration(version=4, name="create_validation_table", apply=_m004_create_validation_table),
    Migration(version=5, name="create_social_hourly_table", apply=_m005_create_social_hourly_table),
    Migration(version=6, name="create_reddit_seen_authors_table", apply=_m006_create_reddit_seen_authors_table),
    Migration(version=7, name="add_social_hourly_source", apply=_m007_add_social_hourly_source),
    Migration(version=8, name="create_social_features_table", apply=_m008_create_social_features_table),
    Migration(version=9, name="create_gate_validation_table", apply=_m009_create_gate_validation_table),
    Migration(version=10, name="create_gate_power_table", apply=_m010_create_gate_power_table),
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

def upsert_table(conn: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> int:
    """Insert or replace rows into any signal-store table, keyed by its own PK.

    Args:
        conn  : Open DuckDB connection.
        table : Target table name.
        df    : Polars DataFrame whose columns are a subset of the table schema.

    Returns:
        Number of rows written.
    """
    if df.is_empty():
        return 0
    col_list = ", ".join(f'"{c}"' for c in df.columns)
    tmp = f"_upsert_tmp_{table}"
    conn.register(tmp, df)
    try:
        conn.execute(
            f"INSERT OR REPLACE INTO {table} ({col_list}) SELECT {col_list} FROM {tmp}"
        )
    finally:
        conn.unregister(tmp)
    logger.info("upsert_table(%s): written=%d", table, df.height)
    return df.height


def upsert_call_returns(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Insert or replace forward-return rows, keyed by (call_id, horizon_min).

    Args:
        conn : Open DuckDB connection.
        df   : Polars DataFrame matching the call_returns schema.

    Returns:
        Number of rows written.
    """
    if df.is_empty():
        return 0
    col_list = ", ".join(f'"{c}"' for c in df.columns)
    conn.register("_ret_tmp", df)
    try:
        conn.execute(
            f"INSERT OR REPLACE INTO call_returns ({col_list}) "
            f"SELECT {col_list} FROM _ret_tmp"
        )
    finally:
        conn.unregister("_ret_tmp")
    logger.info("upsert_call_returns: written=%d", df.height)
    return df.height


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
