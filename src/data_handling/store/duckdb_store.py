"""Persistent DuckDB store layer for ChronoQuant market data.

Single .duckdb file per asset (database/solusdt/solusdt.duckdb) with native
tables: ohlcv, target, feat_ohlcv_quant, predictions, quant_train.  All inserts
are append-only, keyed by open_time.  The .duckdb path is passed directly as db_path.
"""

import logging
from pathlib import Path

import duckdb
import polars as pl

from data_handling.store.duckdb_query import _tbl_exists
from data_handling.store.migrations import Migration, run_migrations

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

# ---------------------------------------------------------------------------
# Live-DB migrations (versioned, idempotent).
# Each Migration.apply callable receives an open DuckDB connection.
# Versions must be unique and monotonically increasing.
# Always append — never reorder or delete existing entries.
# ---------------------------------------------------------------------------


def _m001_create_core_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """v1: Create ohlcv, target, and predictions base tables."""
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


def _m002_drop_target_boolean_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """v2: Drop target table if it carries the old BOOLEAN column schema.

    The fw60 outcome schema uses only DOUBLE columns.  Any DB that was created
    before this migration will have target recreated by _m001 (CREATE TABLE IF
    NOT EXISTS — safe because the table was just dropped).
    """
    old_bool_cols = conn.execute(
        "SELECT COUNT(*) FROM information_schema.columns"
        " WHERE table_name = 'target' AND data_type = 'BOOLEAN'"
    ).fetchone()
    if old_bool_cols and old_bool_cols[0] > 0:
        conn.execute("DROP TABLE IF EXISTS target")
        logger.info("Migration v2: dropped target table (old binary schema -> fw60 outcome schema)")
        # Recreate immediately so subsequent code can rely on its existence.
        _m001_create_core_tables(conn)


def _m003_drop_legacy_split_cols(conn: duckdb.DuckDBPyConnection) -> None:
    """v3: Drop legacy dataset_split / fold_id columns from feat_ohlcv_quant and predictions."""
    for table, col in [
        ("feat_ohlcv_quant", "dataset_split"),
        ("feat_ohlcv_quant", "fold_id"),
        ("predictions", "dataset_split"),
        ("predictions", "fold_id"),
    ]:
        exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.columns"
            " WHERE table_name = ? AND column_name = ?",
            [table, col],
        ).fetchone()
        if exists and exists[0] > 0:
            conn.execute(f'ALTER TABLE "{table}" DROP COLUMN "{col}"')
            logger.info("Migration v3: dropped %s.%s", table, col)


def _m004_drop_predictions_boolean_targets(conn: duckdb.DuckDBPyConnection) -> None:
    """v4: Drop old BOOLEAN target columns from predictions; add DOUBLE fw60 cols."""
    old_pred_bool_cols = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'predictions' AND data_type = 'BOOLEAN'"
        ).fetchall()
    ]
    for col in old_pred_bool_cols:
        conn.execute(f'ALTER TABLE predictions DROP COLUMN "{col}"')
        logger.info("Migration v4: dropped predictions.%s (old boolean target)", col)
    for col in ["long_mfe_fw60", "short_mfe_fw60"]:
        conn.execute(f'ALTER TABLE predictions ADD COLUMN IF NOT EXISTS "{col}" DOUBLE')


def _m005_add_model_stamp_cols(conn: duckdb.DuckDBPyConnection) -> None:
    """v5: Add long_model_id / short_model_id stamp columns to predictions."""
    for col in ["long_model_id", "short_model_id"]:
        conn.execute(f'ALTER TABLE predictions ADD COLUMN IF NOT EXISTS "{col}" VARCHAR')


def _feat_levels_v1_columns() -> list[str]:
    """Frozen feat_* column list of feat_levels v1 (epic_071 / t912).

    Spelled out here (not imported from the feature module) so the migration
    DDL never changes silently; a new feature column needs a new migration.
    """
    cols: list[str] = []
    for x in ("s", "m", "l"):
        cols += [f"feat_sr_dist_res_atr_{x}", f"feat_sr_dist_sup_atr_{x}",
                 f"feat_sr_strength_res_{x}", f"feat_sr_strength_sup_{x}",
                 f"feat_sr_touches_res_{x}", f"feat_sr_touches_sup_{x}",
                 f"feat_sr_flips_res_{x}", f"feat_sr_flips_sup_{x}",
                 f"feat_sr_n_active_{x}"]
        if x in ("s", "m"):
            cols += [f"feat_sr_is_round_res_{x}", f"feat_sr_is_round_sup_{x}"]
        cols += [f"feat_sr_break_age_{x}", f"feat_sr_break_dir_{x}",
                 f"feat_sr_retest_flag_{x}"]
    for x in ("s", "m"):
        cols += [f"feat_tl_dist_sup_atr_{x}", f"feat_tl_dist_res_atr_{x}",
                 f"feat_tl_break_age_{x}", f"feat_tl_break_dir_{x}",
                 f"feat_tl_retest_flag_{x}"]
    return cols


def _m006_create_feat_levels(conn: duckdb.DuckDBPyConnection) -> None:
    """v6: Create feat_levels (S/R level + trendline features, epic_071 / t912).

    One row per 1m bar (PK open_time); every feature is DOUBLE and NULL when
    its object does not exist at the bar. Written by sync_tables/sync_levels.py.
    """
    feat_ddl = ",\n            ".join(f'"{c}" DOUBLE' for c in _feat_levels_v1_columns())
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS feat_levels (
            open_time    TIMESTAMP PRIMARY KEY,
            available_ts TIMESTAMP,
            {feat_ddl}
        )
    """)


def _m007_create_funding_rate(conn: duckdb.DuckDBPyConnection) -> None:
    """v7: Create funding_rate (settled Binance futures funding, epic_074 / t939).

    One row per settlement: ``funding_ts`` = settlement moment, ``funding_rate``
    = rate charged then (read by strategy.costs.load_funding_schedule).
    IF NOT EXISTS: the older ad-hoc SOL/BCH tables are left as they are.
    Written by sync_tables/sync_funding.py.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS funding_rate (
            funding_ts   TIMESTAMP,
            funding_rate DOUBLE,
            mark_price   DOUBLE
        )
    """)


#: Writer-owned scout_signals columns (not produced by scout.run).
SCOUT_SIGNALS_EXTRA_COLUMNS: dict[str, str] = {"scored_at": "TIMESTAMP", "backfilled": "BOOLEAN"}


def scout_signals_columns() -> dict[str, str]:
    """Column -> DuckDB type of scout_signals (epic_074 / t941).

    Single source of truth: ``scout.OUTPUT_SCHEMA`` (t940 runtime contract) plus
    the writer columns ``scored_at`` / ``backfilled``.  Imported lazily so the
    store (and the read-only API) does not pay the scout import cost.
    """
    from scout import OUTPUT_SCHEMA  # noqa: PLC0415  (lazy: heavy research imports)

    cols = {c: _sql_type_from_polars(t) for c, t in OUTPUT_SCHEMA.items()}
    return {**cols, **SCOUT_SIGNALS_EXTRA_COLUMNS}


def _m008_create_scout_signals(conn: duckdb.DuckDBPyConnection) -> None:
    """v8: Create scout_signals (Impulse Scout forward-log, epic_074 / t941).

    One row per detected event (PK event_id = ``asset|cell|t0``); columns =
    ``scout.OUTPUT_SCHEMA`` + scored_at + backfilled.  Prediction columns are
    immutable after insert (enforced by sync_tables/sync_scout.py).  A changed
    OUTPUT_SCHEMA (new artifact version) needs a new migration — sync_scout
    refuses to write when the table and the runtime schema differ.
    """
    ddl = ",\n            ".join(
        f'"{c}" {t}{" PRIMARY KEY" if c == "event_id" else ""}'
        for c, t in scout_signals_columns().items()
    )
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS scout_signals (
            {ddl}
        )
    """)


#: Ordered list of live-DB migrations.  Always append — never reorder or delete.
LIVE_DB_MIGRATIONS: list[Migration] = [
    Migration(version=1, name="create_core_tables", apply=_m001_create_core_tables),
    Migration(version=2, name="drop_target_boolean_schema", apply=_m002_drop_target_boolean_schema),
    Migration(version=3, name="drop_legacy_split_cols", apply=_m003_drop_legacy_split_cols),
    Migration(version=4, name="drop_predictions_boolean_targets", apply=_m004_drop_predictions_boolean_targets),
    Migration(version=5, name="add_model_stamp_cols", apply=_m005_add_model_stamp_cols),
    Migration(version=6, name="create_feat_levels", apply=_m006_create_feat_levels),
    Migration(version=7, name="create_funding_rate", apply=_m007_create_funding_rate),
    Migration(version=8, name="create_scout_signals", apply=_m008_create_scout_signals),
]


def ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create and migrate ohlcv, target, feat_ohlcv_quant, and predictions tables.

    Delegates all DDL to the versioned LIVE_DB_MIGRATIONS list via the
    migrations framework (idempotent, bookkeeping in _schema_migrations).

    feat_ohlcv_quant is schema-driven: it is created on the first
    insert_feat_ohlcv_quant() call, not here.

    Args:
        conn : Open DuckDB connection.
    """
    run_migrations(conn, LIVE_DB_MIGRATIONS)
    logger.debug("ensure_tables: migrations applied")


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
    if not _tbl_exists(conn, "feat_ohlcv_quant"):
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
    conn                 : duckdb.DuckDBPyConnection,
    start_time           : str | None = None,
    end_time             : str | None = None,
    extra_feature_tables : list[str] | None = None,
    table_name           : str = "quant_train",
) -> int:
    """Rebuild quant_train as an INNER JOIN of feat_ohlcv_quant and target.

    Full rebuild (both bounds None): DROP + CREATE OR REPLACE TABLE.
    Range rebuild (at least one bound given): DELETE range + INSERT.

    Rows where long_mfe_fw60 or short_mfe_fw60 is NULL are excluded.
    The feat_* column list is discovered dynamically from feat_ohlcv_quant.

    Challenger profiles (epic_071 / t912) add whole feature tables (e.g.
    ``feat_levels``) on top: each one is LEFT JOINed on open_time, so the row
    set is exactly that of the champion join, and its feat_* columns are
    appended. Such a build writes to its own table (``table_name``, by
    convention ``quant_train__<profile_id>``) -- the champion ``quant_train``
    is never touched by a challenger build.

    Args:
        conn                 : Open DuckDB read-write connection.
        start_time           : Optional range lower bound, inclusive (YYYY-MM-DD HH:MM:SS).
        end_time             : Optional range upper bound, inclusive (YYYY-MM-DD HH:MM:SS).
        extra_feature_tables : Additional feat tables to LEFT JOIN (default none).
        table_name           : Output table (default ``quant_train``).

    Returns:
        Total row count in the output table after rebuild.

    Raises:
        ValueError: If an extra table is missing or repeats a feature column.
    """
    if not _tbl_exists(conn, "feat_ohlcv_quant"):
        logger.warning("rebuild_quant_train: feat_ohlcv_quant tabla hianyzik -- skip")
        return 0
    if not _tbl_exists(conn, "target"):
        logger.warning("rebuild_quant_train: target tabla hianyzik -- skip")
        return 0
    extras = list(extra_feature_tables or [])
    if extras and table_name == "quant_train":
        raise ValueError("extra_feature_tables require a table_name other than quant_train")

    def _feat_cols(table: str) -> list[str]:
        return [
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = ? ORDER BY ordinal_position",
                [table],
            ).fetchall()
            if row[0].startswith("feat_")
        ]

    feat_cols = _feat_cols("feat_ohlcv_quant")

    # Target columns are discovered rather than hardcoded so a new horizon
    # (epic_057: fw240) or a new label family (epic_058: tb_label_*) flows
    # through without touching this join again.
    target_cols = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'target' ORDER BY ordinal_position"
        ).fetchall()
        if "_mfe_fw" in row[0] or row[0].startswith("tb_label_")
    ] or ["long_mfe_fw60", "short_mfe_fw60"]

    select_parts = ["f.open_time"]
    select_parts += [f'f."{c}"' for c in feat_cols]
    join_sql = ""
    seen = set(feat_cols)
    for i, extra in enumerate(extras):
        if not _tbl_exists(conn, extra):
            raise ValueError(f"rebuild_quant_train: extra feature table missing: {extra}")
        cols = _feat_cols(extra)
        dup = seen.intersection(cols)
        if dup:
            raise ValueError(f"rebuild_quant_train: {extra} repeats columns {sorted(dup)}")
        seen.update(cols)
        alias = f"x{i}"
        select_parts += [f'{alias}."{c}"' for c in cols]
        join_sql += f' LEFT JOIN "{extra}" {alias} ON f.open_time = {alias}.open_time'
    select_parts += [f't."{c}"' for c in target_cols]
    select_cols = ", ".join(select_parts)
    out = f'"{table_name}"' if table_name != "quant_train" else "quant_train"

    null_filter = "t.long_mfe_fw60 IS NOT NULL AND t.short_mfe_fw60 IS NOT NULL"

    is_full = start_time is None and end_time is None

    if is_full:
        conn.execute(f"""
            CREATE OR REPLACE TABLE {out} AS
            SELECT {select_cols}
            FROM feat_ohlcv_quant f
            INNER JOIN target t ON f.open_time = t.open_time{join_sql}
            WHERE {null_filter}
        """)
    else:
        # Range rebuild: ensure table exists, then delete + insert
        if not _tbl_exists(conn, table_name):
            conn.execute(f"""
                CREATE TABLE {out} AS
                SELECT {select_cols}
                FROM feat_ohlcv_quant f
                INNER JOIN target t ON f.open_time = t.open_time{join_sql}
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
                f"DELETE FROM {out} WHERE {' AND '.join(del_conds)}",
                del_params,
            )

        ins_conds = [null_filter]
        ins_params: list[str] = []
        if start_time:
            ins_conds.append("f.open_time >= ?")
            ins_params.append(start_time)
        if end_time:
            ins_conds.append("f.open_time <= ?")
            ins_params.append(end_time)

        conn.execute(
            f"""
            INSERT INTO {out}
            SELECT {select_cols}
            FROM feat_ohlcv_quant f
            INNER JOIN target t ON f.open_time = t.open_time{join_sql}
            WHERE {' AND '.join(ins_conds)}
            ORDER BY f.open_time
        """,
            ins_params,
        )

    result = conn.execute(f"SELECT COUNT(*) FROM {out}").fetchone()
    n = int(result[0]) if result else 0
    logger.info("rebuild_quant_train: table=%s rows=%d start=%s end=%s",
                table_name, n, start_time, end_time)
    return n


# %% Insert helpers


def _insert_append_only(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    df: pl.DataFrame,
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

        conn.execute(
            f"""
            INSERT INTO {table} ({col_list})
            SELECT {col_list}
            FROM _ins_batch
            WHERE open_time > ?
            ORDER BY open_time
        """,
            [max_open_time],
        )
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
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
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


def backfill_feat_ohlcv_quant(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Insert feature rows for interior gaps, skipping open_times already stored.

    Unlike insert_feat_ohlcv_quant (append-only after MAX(open_time)), this fills
    holes inside the stored range (epic_076 / t947 gap repair).  Existing rows are
    left unchanged.  feat_ohlcv_quant has no primary key, so the skip is an
    anti-join (NOT EXISTS) on open_time against the stored table.

    Args:
        conn : Open DuckDB connection.
        df   : Polars DataFrame with open_time and feat_* columns.

    Returns:
        Number of rows inserted.
    """
    if df.is_empty():
        return 0
    _ensure_feat_ohlcv_quant_table(conn, df)

    col_list = ", ".join(f'"{c}"' for c in df.columns)
    conn.register("_feat_backfill_batch", df)
    try:
        conn.execute("BEGIN")
        try:
            row = conn.execute(
                """
                SELECT COUNT(*) FROM _feat_backfill_batch b
                WHERE NOT EXISTS (
                    SELECT 1 FROM feat_ohlcv_quant f WHERE f.open_time = b.open_time
                )
                """
            ).fetchone()
            n = int(row[0]) if row else 0
            if n > 0:
                conn.execute(
                    f"""
                    INSERT INTO feat_ohlcv_quant ({col_list})
                    SELECT {col_list} FROM _feat_backfill_batch b
                    WHERE NOT EXISTS (
                        SELECT 1 FROM feat_ohlcv_quant f WHERE f.open_time = b.open_time
                    )
                    ORDER BY open_time
                    """
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.unregister("_feat_backfill_batch")

    logger.info("backfill_feat_ohlcv_quant: inserted=%d", n)
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


def insert_funding_rate(conn: duckdb.DuckDBPyConnection, df: pl.DataFrame) -> int:
    """Insert settled funding rows whose funding_ts is not stored yet (idempotent).

    Only the columns present in both ``df`` and the existing ``funding_rate``
    table are written, so the older ad-hoc SOL/BCH tables (possibly without
    ``mark_price``) are served too.  ``funding_ts`` and ``funding_rate`` are required.

    Args:
        conn : Open DuckDB connection (funding_rate must exist — call ensure_tables).
        df   : Polars DataFrame with funding_ts (Datetime), funding_rate[, mark_price].

    Returns:
        Number of rows inserted.
    """
    if df.is_empty():
        return 0

    table_cols = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'funding_rate'"
        ).fetchall()
    }
    cols = [c for c in df.columns if c in table_cols]
    if "funding_ts" not in cols or "funding_rate" not in cols:
        raise ValueError(f"funding_rate table/batch lacks funding_ts/funding_rate: {sorted(table_cols)}")

    col_list = ", ".join(f'"{c}"' for c in cols)
    conn.register("_funding_batch", df.select(cols).unique(subset=["funding_ts"], keep="last"))
    try:
        before = conn.execute("SELECT COUNT(*) FROM funding_rate").fetchone()
        conn.execute(f"""
            INSERT INTO funding_rate ({col_list})
            SELECT {col_list} FROM _funding_batch b
            WHERE NOT EXISTS (SELECT 1 FROM funding_rate f WHERE f.funding_ts = b.funding_ts)
            ORDER BY funding_ts
        """)
        after = conn.execute("SELECT COUNT(*) FROM funding_rate").fetchone()
    finally:
        conn.unregister("_funding_batch")

    n = (int(after[0]) if after else 0) - (int(before[0]) if before else 0)
    logger.info("insert_funding_rate: inserted=%d", n)
    return n
