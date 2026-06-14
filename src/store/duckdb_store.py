"""Persistent DuckDB store layer for ChronoQuant market data.

Single .duckdb file per asset (data/solusdt_fw60.duckdb) with three native
tables: ohlcv, features, predictions.  All inserts are append-only, keyed by
open_time.  The .duckdb path is derived from data_dir:
  data/solusdt_fw60 → data/solusdt_fw60.duckdb
"""

import logging
from pathlib import Path
from typing import Union

import duckdb
import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)

_AnyDF = Union[pd.DataFrame, pl.DataFrame]


# %% Connection


def get_connection(data_dir: str) -> duckdb.DuckDBPyConnection:
    """Return a persistent read-write DuckDB connection for the asset database.

    Args:
        data_dir : Resolved data directory path for the asset.

    Returns:
        Open DuckDB connection.  Caller must close it.
    """
    db_path = Path(data_dir).with_suffix(".duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


# %% Schema management


def ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create ohlcv and predictions tables if they do not exist.

    The features table is created lazily on the first insert_features() call
    because its column set is determined at runtime by the features.json config.

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
        CREATE TABLE IF NOT EXISTS predictions (
            open_time       TIMESTAMP PRIMARY KEY,
            close           DOUBLE,
            label_end_ts    TIMESTAMP,
            dataset_split   VARCHAR,
            fold_id         VARCHAR,
            trg_l_fw60_q90  BOOLEAN,
            trg_s_fw60_q10  BOOLEAN,
            long_pred       DOUBLE,
            short_pred      DOUBLE
        )
    """)
    logger.debug("ensure_tables: ohlcv + predictions OK")


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Return True if the named table exists in the connected database."""
    result = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(result and result[0] > 0)


def _ensure_features_table(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Create or evolve the features table to match df's schema.

    First call  : creates the table from df's column types (no hardcoded schema).
    Later calls : adds any new columns that appear in df but not in the table.

    Args:
        conn : Open DuckDB connection.
        df   : Pandas DataFrame whose columns define the target schema.
    """
    if not _table_exists(conn, "features"):
        conn.register("_feat_schema_tmp", df)
        conn.execute("CREATE TABLE features AS SELECT * FROM _feat_schema_tmp LIMIT 0")
        conn.unregister("_feat_schema_tmp")
        logger.debug("_ensure_features_table: created, cols=%d", len(df.columns))
        return

    existing = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'features'"
        ).fetchall()
    }
    for col in df.columns:
        if col in existing:
            continue
        dtype_str = str(df[col].dtype)
        if "bool" in dtype_str:
            sql_type = "BOOLEAN"
        elif "int" in dtype_str:
            sql_type = "BIGINT"
        elif "float" in dtype_str or "double" in dtype_str:
            sql_type = "DOUBLE"
        else:
            sql_type = "VARCHAR"
        conn.execute(f'ALTER TABLE features ADD COLUMN IF NOT EXISTS "{col}" {sql_type}')
        logger.debug("_ensure_features_table: added column %s %s", col, sql_type)


# %% Insert helpers


def _to_pandas(df: _AnyDF) -> pd.DataFrame:
    """Coerce df to pandas; return unchanged if already pandas."""
    if isinstance(df, pl.DataFrame):
        return df.to_pandas()
    return df  # type: ignore[return-value]


def _insert_append_only(
    conn  : duckdb.DuckDBPyConnection,
    table : str,
    df    : pd.DataFrame,
) -> int:
    """Insert df rows where open_time > stored MAX(open_time).

    Rows are inserted in ascending open_time order so that DuckDB zonemap
    statistics remain tight on subsequent range queries.  Columns are matched
    by name so that df column ordering does not need to match the table schema.

    Args:
        conn  : Open DuckDB connection.
        table : Target table name (ohlcv, features, or predictions).
        df    : Pandas DataFrame with open_time column.

    Returns:
        Number of rows inserted.
    """
    if df.empty:
        return 0

    col_list = ", ".join(f'"{c}"' for c in df.columns)
    conn.register("_ins_batch", df)
    try:
        result = conn.execute(f"""
            INSERT INTO {table} ({col_list})
            SELECT {col_list}
            FROM _ins_batch
            WHERE open_time > (
                SELECT COALESCE(MAX(open_time), TIMESTAMP '1970-01-01') FROM {table}
            )
            ORDER BY open_time
        """)
        n = result.rowcount if result.rowcount is not None else 0
    finally:
        conn.unregister("_ins_batch")

    logger.debug("_insert_append_only: table=%s inserted=%d", table, n)
    return n


# %% Public insert API


def insert_ohlcv(conn: duckdb.DuckDBPyConnection, df: _AnyDF) -> int:
    """Append new OHLCV rows to the ohlcv table.

    Only rows with open_time strictly greater than the current maximum are
    inserted.  Call ensure_tables(conn) before the first insert.

    Args:
        conn : Open DuckDB connection.
        df   : Polars or pandas DataFrame with OHLCV columns.

    Returns:
        Number of rows inserted.
    """
    pdf = _to_pandas(df)
    ohlcv_cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "quote_volume", "trades", "taker_buy_base", "taker_buy_quote",
    ]
    pdf = pdf[[c for c in ohlcv_cols if c in pdf.columns]].copy()
    n = _insert_append_only(conn, "ohlcv", pdf)
    logger.info("insert_ohlcv: inserted=%d", n)
    return n


def insert_features(conn: duckdb.DuckDBPyConnection, df: _AnyDF) -> int:
    """Append new feature rows to the features table.

    Creates the features table from df's schema on the first call.
    Only rows with open_time strictly greater than the current maximum are inserted.

    Args:
        conn : Open DuckDB connection.
        df   : Polars or pandas DataFrame with open_time and feature columns.

    Returns:
        Number of rows inserted.
    """
    pdf = _to_pandas(df)
    _ensure_features_table(conn, pdf)
    n = _insert_append_only(conn, "features", pdf)
    logger.info("insert_features: inserted=%d", n)
    return n


def insert_predictions(conn: duckdb.DuckDBPyConnection, df: _AnyDF) -> int:
    """Append new prediction rows to the predictions table.

    The predictions table must already exist (call ensure_tables first).
    Only rows with open_time strictly greater than the current maximum are inserted.

    Args:
        conn : Open DuckDB connection.
        df   : Polars or pandas DataFrame with prediction columns.

    Returns:
        Number of rows inserted.
    """
    pdf = _to_pandas(df)
    pred_cols = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'predictions' ORDER BY ordinal_position"
        ).fetchall()
    ]
    available = [c for c in pred_cols if c in pdf.columns]
    pdf = pdf[available].copy()
    n = _insert_append_only(conn, "predictions", pdf)
    logger.info("insert_predictions: inserted=%d", n)
    return n
