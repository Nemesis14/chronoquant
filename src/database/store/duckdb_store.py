"""Persistent DuckDB store layer for ChronoQuant market data.

Single .duckdb file per asset (database/solusdt/solusdt.duckdb) with native
tables: ohlcv, target, feat_ohlcv_quant, predictions.  All inserts are
append-only, keyed by open_time.  The .duckdb path is passed directly as db_path.
"""

import logging
from pathlib import Path

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


# %% Connection


def get_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Return a persistent read-write DuckDB connection for the asset database.

    Args:
        db_path : Absolute path to the .duckdb file for the asset.

    Returns:
        Open DuckDB connection.  Caller must close it.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


# %% Schema management


def ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create ohlcv, target, feat_ohlcv_quant, and predictions tables if absent.

    feat_ohlcv_quant starts with stable metadata columns and evolves on the
    first insert_feat_ohlcv_quant() call because feature columns are config-driven.

    Args:
        conn : Open DuckDB connection.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            open_time       TIMESTAMP PRIMARY KEY,
            open            DOUBLE,
            high            DOUBLE,
            low             DOUBLE,
            close           DOUBLE,
            volume          DOUBLE,
            quote_volume    DOUBLE,
            trades          BIGINT,
            taker_buy_base  DOUBLE,
            taker_buy_quote DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS target (
            open_time      TIMESTAMP PRIMARY KEY,
            close          DOUBLE,
            trg_l_fw60_q90 BOOLEAN,
            trg_s_fw60_q10 BOOLEAN
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            open_time       TIMESTAMP PRIMARY KEY,
            close           DOUBLE,
            label_end_ts    TIMESTAMP,
            trg_l_fw60_q90  BOOLEAN,
            trg_s_fw60_q10  BOOLEAN,
            long_pred       DOUBLE,
            short_pred      DOUBLE
        )
    """)
    # Migration: drop legacy split columns if present
    for table, col in [
        ("feat_ohlcv_quant", "dataset_split"),
        ("feat_ohlcv_quant", "fold_id"),
        ("predictions",      "dataset_split"),
        ("predictions",      "fold_id"),
    ]:
        try:
            exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.columns"
                " WHERE table_name = ? AND column_name = ?",
                [table, col],
            ).fetchone()
            if exists and exists[0] > 0:
                conn.execute(f'ALTER TABLE "{table}" DROP COLUMN "{col}"')
                logger.info("Migration: dropped %s.%s", table, col)
        except Exception:
            logger.debug("Migration skip: %s.%s (table may not exist yet)", table, col)

    logger.debug("ensure_tables: ohlcv + target + predictions OK")


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Return True if the named table exists in the connected database."""
    result = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(result and result[0] > 0)


def _sql_type_from_polars(dtype: pl.DataType) -> str:
    """Map a Polars dtype to a DuckDB SQL type string."""
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype.is_integer():
        return "BIGINT"
    if dtype.is_float():
        return "DOUBLE"
    if isinstance(dtype, (pl.Datetime, pl.Date)):
        return "TIMESTAMP"
    return "VARCHAR"


def _ensure_feat_ohlcv_quant_table(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> None:
    """Create or evolve the feat_ohlcv_quant table to match df's schema.

    First call  : creates the table from df's column types (no hardcoded schema).
    Later calls : adds any new columns that appear in df but not in the table.

    Args:
        conn : Open DuckDB connection.
        df   : Polars DataFrame whose columns define the target schema.
    """
    if not _table_exists(conn, "feat_ohlcv_quant"):
        conn.register("_feat_schema_tmp", df)
        conn.execute("CREATE TABLE feat_ohlcv_quant AS SELECT * FROM _feat_schema_tmp LIMIT 0")
        conn.unregister("_feat_schema_tmp")
        logger.debug("_ensure_feat_ohlcv_quant_table: created, cols=%d", len(df.columns))
        return

    existing = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'feat_ohlcv_quant'"
        ).fetchall()
    }

    schema = df.schema
    for col in df.columns:
        if col in existing:
            continue
        sql_type = _sql_type_from_polars(schema[col])
        conn.execute(f'ALTER TABLE feat_ohlcv_quant ADD COLUMN IF NOT EXISTS "{col}" {sql_type}')
        logger.debug("_ensure_feat_ohlcv_quant_table: added column %s %s", col, sql_type)


# %% Insert helpers


def _insert_append_only(
    conn  : duckdb.DuckDBPyConnection,
    table : str,
    df    : pl.DataFrame,
) -> int:
    """Insert df rows where open_time > stored MAX(open_time).

    Rows are inserted in ascending open_time order so that DuckDB zonemap
    statistics remain tight on subsequent range queries.  Columns are matched
    by name so that df column ordering does not need to match the table schema.

    Args:
        conn  : Open DuckDB connection.
        table : Target table name.
        df    : Polars DataFrame with open_time column.

    Returns:
        Number of rows inserted.
    """
    if df.is_empty():
        return 0
    if "open_time" in df.columns and df.schema["open_time"] == pl.Utf8:
        df = df.with_columns(
            pl.col("open_time").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S")
        )

    col_list = ", ".join(f'"{c}"' for c in df.columns)
    conn.register("_ins_batch", df)
    try:
        max_row = conn.execute(
            f"SELECT COALESCE(MAX(open_time), TIMESTAMP '1970-01-01') FROM {table}"
        ).fetchone()
        max_open_time = max_row[0]  # always set due to COALESCE
        count_row = conn.execute(
            "SELECT COUNT(*) FROM _ins_batch WHERE open_time > ?",
            [max_open_time],
        ).fetchone()
        n = int(count_row[0]) if count_row else 0
        if n == 0:
            return 0

        conn.execute(f"""
            INSERT INTO {table} ({col_list})
            SELECT {col_list}
            FROM _ins_batch
            WHERE open_time > ?
            ORDER BY open_time
        """, [max_open_time])
    finally:
        conn.unregister("_ins_batch")

    logger.debug("_insert_append_only: table=%s inserted=%d", table, n)
    return n


# %% Public insert API


def insert_ohlcv(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Append new OHLCV rows to the ohlcv table.

    Only rows with open_time strictly greater than the current maximum are
    inserted.  Call ensure_tables(conn) before the first insert.

    Args:
        conn : Open DuckDB connection.
        df   : Polars DataFrame with OHLCV columns.

    Returns:
        Number of rows inserted.
    """
    ohlcv_cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "quote_volume", "trades", "taker_buy_base", "taker_buy_quote",
    ]
    df_ohlcv = df.select([c for c in ohlcv_cols if c in df.columns])
    n = _insert_append_only(conn, "ohlcv", df_ohlcv)
    logger.info("insert_ohlcv: inserted=%d", n)
    return n


def insert_feat_ohlcv_quant(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Append new feature rows to the feat_ohlcv_quant table.

    Creates the table from df's schema on the first call.
    Only rows with open_time strictly greater than the current maximum are inserted.

    Args:
        conn : Open DuckDB connection.
        df   : Polars DataFrame with open_time and feat_* columns.

    Returns:
        Number of rows inserted.
    """
    _ensure_feat_ohlcv_quant_table(conn, df)
    n = _insert_append_only(conn, "feat_ohlcv_quant", df)
    logger.info("insert_feat_ohlcv_quant: inserted=%d", n)
    return n


def insert_target(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Upsert target rows into the target table (DELETE + INSERT for full rebuild).

    The target table is rebuilt in full on each sync_targets run.  This function
    deletes rows within the open_time range of df, then inserts all rows from df.

    Args:
        conn : Open DuckDB connection.
        df   : Polars DataFrame with open_time and trg_* columns.

    Returns:
        Number of rows inserted.
    """
    if df.is_empty():
        return 0
    if "open_time" in df.columns and df.schema["open_time"] == pl.Utf8:
        df = df.with_columns(
            pl.col("open_time").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S")
        )

    conn.register("_target_batch", df)
    try:
        conn.execute("""
            DELETE FROM target
            WHERE open_time IN (SELECT open_time FROM _target_batch)
        """)

        col_list = ", ".join(f'"{c}"' for c in df.columns)
        conn.execute(f"""
            INSERT INTO target ({col_list})
            SELECT {col_list} FROM _target_batch
            ORDER BY open_time
        """)

        count_row = conn.execute("SELECT COUNT(*) FROM _target_batch").fetchone()
        n = int(count_row[0]) if count_row else 0
    finally:
        conn.unregister("_target_batch")

    logger.info("insert_target: inserted=%d", n)
    return n


def insert_predictions(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Append new prediction rows to the predictions table.

    The predictions table must already exist (call ensure_tables first).
    Only rows with open_time strictly greater than the current maximum are inserted.

    Args:
        conn : Open DuckDB connection.
        df   : Polars DataFrame with prediction columns.

    Returns:
        Number of rows inserted.
    """
    pred_cols = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'predictions' ORDER BY ordinal_position"
        ).fetchall()
    ]
    available = [c for c in pred_cols if c in df.columns]
    n = _insert_append_only(conn, "predictions", df.select(available))
    logger.info("insert_predictions: inserted=%d", n)
    return n
