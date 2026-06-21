"""Persistent DuckDB store layer for ChronoQuant market data.

Single .duckdb file per asset (database/solusdt/solusdt.duckdb) with native
tables: ohlcv, target, feat_ohlcv_quant, predictions, quant_train.  All inserts
are append-only, keyed by open_time.  The .duckdb path is passed directly as db_path.
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
    # Migration: drop target table if it has old binary schema (BOOLEAN columns)
    # New fw60 outcome schema uses only DOUBLE columns — no BOOLEAN expected.
    try:
        old_bool_cols = conn.execute(
            "SELECT COUNT(*) FROM information_schema.columns"
            " WHERE table_name = 'target' AND data_type = 'BOOLEAN'"
        ).fetchone()
        if old_bool_cols and old_bool_cols[0] > 0:
            conn.execute("DROP TABLE IF EXISTS target")
            logger.info("Migration: dropped target table (old binary schema → fw60 outcome schema)")
    except Exception:
        logger.debug("Migration: target schema check skipped")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS target (
            open_time         TIMESTAMP PRIMARY KEY,
            close             DOUBLE,
            fw60_close        DOUBLE,
            fw60_max          DOUBLE,
            fw60_min          DOUBLE,
            fw60_close_ret    DOUBLE,
            fw60_close_logret DOUBLE,
            fw60_max_ratio    DOUBLE,
            fw60_min_ratio    DOUBLE,
            long_mfe_fw60     DOUBLE,
            short_mfe_fw60    DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            open_time       TIMESTAMP PRIMARY KEY,
            close           DOUBLE,
            label_end_ts    TIMESTAMP,
            long_mfe_fw60   DOUBLE,
            short_mfe_fw60  DOUBLE,
            long_pred       DOUBLE,
            short_pred      DOUBLE
        )
    """)
    # Migration: drop legacy split columns from feat_ohlcv_quant and predictions
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

    # Migration: drop old boolean target cols from predictions, add new double cols
    try:
        old_pred_bool_cols = [
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'predictions' AND data_type = 'BOOLEAN'"
            ).fetchall()
        ]
        for col in old_pred_bool_cols:
            conn.execute(f'ALTER TABLE predictions DROP COLUMN "{col}"')
            logger.info("Migration: dropped predictions.%s (old boolean target)", col)
    except Exception:
        logger.debug("Migration: predictions boolean target col drop skipped")

    # Migration: add new fw60 target cols to predictions if missing (existing tables)
    for col in ["long_mfe_fw60", "short_mfe_fw60"]:
        try:
            conn.execute(
                f'ALTER TABLE predictions ADD COLUMN IF NOT EXISTS "{col}" DOUBLE'
            )
        except Exception:
            logger.debug("Migration skip: predictions.%s add", col)

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


# %% quant_train


def rebuild_quant_train(
    conn       : duckdb.DuckDBPyConnection,
    start_time : str | None = None,
    end_time   : str | None = None,
) -> int:
    """Rebuild quant_train as an INNER JOIN of feat_ohlcv_quant and target.

    Full rebuild (both bounds None): DROP + CREATE OR REPLACE TABLE.
    Range rebuild (at least one bound given): DELETE range + INSERT.

    Rows where long_mfe_fw60 or short_mfe_fw60 is NULL are excluded.
    The feat_* column list is discovered dynamically from feat_ohlcv_quant.

    Args:
        conn       : Open DuckDB read-write connection.
        start_time : Optional range lower bound, inclusive (YYYY-MM-DD HH:MM:SS).
        end_time   : Optional range upper bound, inclusive (YYYY-MM-DD HH:MM:SS).

    Returns:
        Total row count in quant_train after rebuild.
    """
    if not _table_exists(conn, "feat_ohlcv_quant"):
        logger.warning("rebuild_quant_train: feat_ohlcv_quant tábla hiányzik — skip")
        return 0
    if not _table_exists(conn, "target"):
        logger.warning("rebuild_quant_train: target tábla hiányzik — skip")
        return 0

    feat_cols = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'feat_ohlcv_quant' ORDER BY ordinal_position"
        ).fetchall()
        if row[0].startswith("feat_")
    ]

    if feat_cols:
        feat_select  = ", ".join(f'f."{c}"' for c in feat_cols)
        select_cols  = f"f.open_time, {feat_select}, t.long_mfe_fw60, t.short_mfe_fw60"
    else:
        select_cols  = "f.open_time, t.long_mfe_fw60, t.short_mfe_fw60"

    null_filter = "t.long_mfe_fw60 IS NOT NULL AND t.short_mfe_fw60 IS NOT NULL"

    is_full = start_time is None and end_time is None

    if is_full:
        conn.execute(f"""
            CREATE OR REPLACE TABLE quant_train AS
            SELECT {select_cols}
            FROM feat_ohlcv_quant f
            INNER JOIN target t ON f.open_time = t.open_time
            WHERE {null_filter}
        """)
    else:
        # Range rebuild: ensure table exists, then delete + insert
        if not _table_exists(conn, "quant_train"):
            conn.execute(f"""
                CREATE TABLE quant_train AS
                SELECT {select_cols}
                FROM feat_ohlcv_quant f
                INNER JOIN target t ON f.open_time = t.open_time
                WHERE 1 = 0
            """)

        del_conds: list[str] = []
        del_params: list[str] = []
        if start_time:
            del_conds.append("open_time >= ?")
            del_params.append(start_time)
        if end_time:
            del_conds.append("open_time <= ?")
            del_params.append(end_time)
        if del_conds:
            conn.execute(
                f"DELETE FROM quant_train WHERE {' AND '.join(del_conds)}",
                del_params,
            )

        ins_conds  = [null_filter]
        ins_params: list[str] = []
        if start_time:
            ins_conds.append("f.open_time >= ?")
            ins_params.append(start_time)
        if end_time:
            ins_conds.append("f.open_time <= ?")
            ins_params.append(end_time)

        conn.execute(f"""
            INSERT INTO quant_train
            SELECT {select_cols}
            FROM feat_ohlcv_quant f
            INNER JOIN target t ON f.open_time = t.open_time
            WHERE {' AND '.join(ins_conds)}
            ORDER BY f.open_time
        """, ins_params)

    result = conn.execute("SELECT COUNT(*) FROM quant_train").fetchone()
    n = int(result[0]) if result else 0
    logger.info("rebuild_quant_train: rows=%d start=%s end=%s", n, start_time, end_time)
    return n


# %% Sample tables


def materialize_sample_table(
    conn       : duckdb.DuckDBPyConnection,
    sample_id  : str,
    segment_df : pl.DataFrame,
) -> int:
    """Materialize a segment DataFrame as a DuckDB sample table.

    Table name: ``sample_<sample_id>``.  Uses CREATE OR REPLACE — safe to rerun.
    Column order: open_time, fold_id, segment, feat_* (sorted), target columns.

    Args:
        conn       : Open DuckDB read-write connection.
        sample_id  : Sample identifier (appended to 'sample_' for the table name).
        segment_df : Polars DataFrame from assign_segments — contains open_time,
                     fold_id, segment, feat_*, and target columns.

    Returns:
        Row count in the newly created table.
    """
    table_name  = f"sample_{sample_id}"
    feat_cols   = sorted(c for c in segment_df.columns if c.startswith("feat_"))
    target_cols = [c for c in ("long_mfe_fw60", "short_mfe_fw60") if c in segment_df.columns]
    df_out = segment_df.select(["open_time", "fold_id", "segment"] + feat_cols + target_cols)

    conn.register("_sample_tmp", df_out)
    try:
        conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _sample_tmp')
    finally:
        conn.unregister("_sample_tmp")

    result = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
    n = int(result[0]) if result else 0
    logger.info("materialize_sample_table: table=%s rows=%d", table_name, n)
    return n


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
        assert max_row is not None  # COALESCE guarantees a row is returned
        max_open_time = max_row[0]
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


def backfill_predictions(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Insert prediction rows for historical gaps, skipping existing open_times.

    Unlike insert_predictions, this does not restrict to rows newer than the
    current maximum.  Use for backfilling gaps in historical prediction coverage.
    Existing rows (matching open_time) are left unchanged.

    Args:
        conn : Open DuckDB connection.
        df   : Polars DataFrame with prediction columns.

    Returns:
        Number of rows inserted.
    """
    if df.is_empty():
        return 0

    pred_cols = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'predictions' ORDER BY ordinal_position"
        ).fetchall()
    ]
    available = [c for c in pred_cols if c in df.columns]
    df_out = df.select(available)

    if "open_time" in df_out.columns and df_out.schema["open_time"] == pl.Utf8:
        df_out = df_out.with_columns(
            pl.col("open_time").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S")
        )

    col_list = ", ".join(f'"{c}"' for c in df_out.columns)
    conn.register("_backfill_batch", df_out)
    try:
        before = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
        n_before = int(before[0]) if before else 0
        conn.execute(f"""
            INSERT OR IGNORE INTO predictions ({col_list})
            SELECT {col_list}
            FROM _backfill_batch
            ORDER BY open_time
        """)
        after = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
        n_after = int(after[0]) if after else 0
    finally:
        conn.unregister("_backfill_batch")

    n = n_after - n_before
    logger.info("backfill_predictions: inserted=%d", n)
    return n
