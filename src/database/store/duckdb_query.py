"""Read layer for DuckDB native tables.

Queries ohlcv, target, feat_ohlcv_quant, and predictions from the persistent
.duckdb file.  The db_path is passed directly as an absolute file path.
All connections are short-lived.  query_range() returns pandas; query_range_pl() returns Polars.
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)


# %% Connection helpers


def _connect(db_path: str) -> duckdb.DuckDBPyConnection | None:
    """Open a read-only DuckDB connection to the asset database.

    Returns None when the .duckdb file does not exist yet (before first sync).

    Args:
        db_path : Absolute path to the asset .duckdb file.

    Returns:
        Open DuckDB connection, or None if the database file is absent.
    """
    p = Path(db_path)
    if not p.exists():
        logger.debug("DuckDB file not found: %s", p)
        return None
    return duckdb.connect(str(p), read_only=True)


def _tbl_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Return True if the named table exists in the connected database."""
    result = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(result and result[0] > 0)


# %% Query


def query_range(
    db_path  : str,
    dataset  : str,
    start    : str | None = None,
    end      : str | None = None,
    columns  : list[str] | None = None,
) -> pd.DataFrame:
    """Query rows from a native DuckDB table within an optional open_time range.

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Table name: 'ohlcv', 'target', 'feat_ohlcv_quant', or 'predictions'.
        start    : Optional lower bound, inclusive. YYYY-MM-DD HH:MM:SS.
        end      : Optional upper bound, inclusive. YYYY-MM-DD HH:MM:SS.
        columns  : Optional list of columns to SELECT. None = all columns.

    Returns:
        DataFrame sorted by open_time, or empty DataFrame if table not found.
    """
    conn = _connect(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        if not _tbl_exists(conn, dataset):
            logger.debug("query_range: table not found: %s", dataset)
            return pd.DataFrame()

        col_expr   = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        conditions : list[str] = []
        params     : list[str] = []
        if start:
            conditions.append("open_time >= ?")
            params.append(start)
        if end:
            conditions.append("open_time <= ?")
            params.append(end)
        where = " AND ".join(conditions) if conditions else "1=1"
        sql   = f"SELECT {col_expr} FROM {dataset} WHERE {where} ORDER BY open_time ASC"

        logger.debug("query_range: dataset=%s start=%s end=%s", dataset, start, end)
        df = conn.execute(sql, params).df()
        logger.debug("query_range: rows=%d", len(df))
        return df
    except Exception:
        logger.exception("query_range sikertelen: dataset=%s", dataset)
        raise
    finally:
        conn.close()


def query_range_pl(
    db_path  : str,
    dataset  : str,
    start    : str | None = None,
    end      : str | None = None,
    columns  : list[str] | None = None,
) -> pl.DataFrame:
    """Same as query_range but returns a Polars DataFrame (zero-copy from DuckDB).

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Table name: 'ohlcv', 'target', 'feat_ohlcv_quant', or 'predictions'.
        start    : Optional lower bound, inclusive. YYYY-MM-DD HH:MM:SS.
        end      : Optional upper bound, inclusive. YYYY-MM-DD HH:MM:SS.
        columns  : Optional list of columns to SELECT. None = all columns.

    Returns:
        Polars DataFrame sorted by open_time, or empty DataFrame if table not found.
    """
    conn = _connect(db_path)
    if conn is None:
        return pl.DataFrame()
    try:
        if not _tbl_exists(conn, dataset):
            logger.debug("query_range_pl: table not found: %s", dataset)
            return pl.DataFrame()

        col_expr   = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        conditions : list[str] = []
        params     : list[str] = []
        if start:
            conditions.append("open_time >= ?")
            params.append(start)
        if end:
            conditions.append("open_time <= ?")
            params.append(end)
        where = " AND ".join(conditions) if conditions else "1=1"
        sql   = f"SELECT {col_expr} FROM {dataset} WHERE {where} ORDER BY open_time ASC"

        logger.debug("query_range_pl: dataset=%s start=%s end=%s", dataset, start, end)
        df = conn.execute(sql, params).pl()
        logger.debug("query_range_pl: rows=%d", len(df))
        return df
    except Exception:
        logger.exception("query_range_pl sikertelen: dataset=%s", dataset)
        raise
    finally:
        conn.close()



# %% Schema


def dataset_columns(db_path: str, dataset: str) -> list[str]:
    """Return column names for a native DuckDB table.

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Table name.

    Returns:
        List of column name strings, or empty list if table not found.
    """
    conn = _connect(db_path)
    if conn is None:
        return []
    try:
        if not _tbl_exists(conn, dataset):
            return []
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = ? ORDER BY ordinal_position",
            [dataset],
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        logger.exception("dataset_columns sikertelen: dataset=%s", dataset)
        raise
    finally:
        conn.close()


def dataset_exists(db_path: str, dataset: str) -> bool:
    """Return True if the dataset table exists and has at least one row.

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Table name.

    Returns:
        True if the table exists with at least one row.
    """
    conn = _connect(db_path)
    if conn is None:
        return False
    try:
        if not _tbl_exists(conn, dataset):
            return False
        result = conn.execute(f"SELECT COUNT(*) FROM {dataset}").fetchone()
        return bool(result and result[0] > 0)
    except Exception:
        logger.exception("dataset_exists sikertelen: dataset=%s", dataset)
        return False
    finally:
        conn.close()


def row_count(db_path: str, dataset: str) -> int:
    """Return total row count for a native DuckDB table.

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Table name.

    Returns:
        Row count, or 0 if table not found.
    """
    conn = _connect(db_path)
    if conn is None:
        return 0
    try:
        if not _tbl_exists(conn, dataset):
            return 0
        result = conn.execute(f"SELECT COUNT(*) FROM {dataset}").fetchone()
        return int(result[0]) if result else 0
    except Exception:
        logger.exception("row_count sikertelen: dataset=%s", dataset)
        raise
    finally:
        conn.close()


# %% ASOF join


def asof_join_predictions_features(
    db_path       : str,
    feature_cols  : list[str] | None = None,
    start         : str | None = None,
    end           : str | None = None,
) -> pd.DataFrame:
    """Join predictions to feat_ohlcv_quant using ASOF JOIN for temporal alignment.

    For each prediction row, finds the most recent feature row where
    available_ts <= prediction open_time.  Prevents look-ahead bias.

    Args:
        db_path      : Absolute path to the asset .duckdb file.
        feature_cols : Feature columns to SELECT from feat_ohlcv_quant. None = all feat_* columns.
        start        : Optional lower bound on prediction open_time, inclusive.
        end          : Optional upper bound on prediction open_time, inclusive.

    Returns:
        DataFrame with prediction_ts, lookback_end_ts, and feature columns.
        Empty DataFrame if either table has no data.
    """
    conn = _connect(db_path)
    if conn is None:
        return pd.DataFrame()
    try:
        for tbl in ("feat_ohlcv_quant", "predictions"):
            if not _tbl_exists(conn, tbl):
                logger.debug("asof_join: table missing: %s", tbl)
                return pd.DataFrame()

        if feature_cols:
            feat_select = ", ".join(f'f."{c}"' for c in feature_cols)
        else:
            all_cols = conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'feat_ohlcv_quant' ORDER BY ordinal_position"
            ).fetchall()
            feat_cols_all = [r[0] for r in all_cols if r[0].startswith("feat_")]
            feat_select = (
                ", ".join(f'f."{c}"' for c in feat_cols_all)
                if feat_cols_all else "f.*"
            )

        conditions : list[str] = []
        params     : list[str] = []
        if start:
            conditions.append("p.open_time >= ?")
            params.append(start)
        if end:
            conditions.append("p.open_time <= ?")
            params.append(end)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        sql = f"""
            SELECT
                p.open_time    AS prediction_ts,
                f.available_ts AS lookback_end_ts,
                {feat_select}
            FROM predictions p
            ASOF LEFT JOIN feat_ohlcv_quant f
              ON p.open_time >= f.available_ts
            {where}
            ORDER BY prediction_ts ASC
        """

        logger.debug("asof_join_predictions_features: start=%s end=%s", start, end)
        df = conn.execute(sql, params).df()
        logger.debug("asof_join_predictions_features: rows=%d", len(df))
        return df
    except Exception:
        logger.exception("asof_join_predictions_features sikertelen")
        raise
    finally:
        conn.close()


# %% Generic helpers


def latest_open_time(db_path: str, dataset: str) -> pd.Timestamp | None:
    """Return the maximum open_time in a DuckDB table as a pandas Timestamp.

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Table name.

    Returns:
        Maximum open_time as pd.Timestamp, or None if table is absent/empty.
    """
    conn = _connect(db_path)
    if conn is None:
        return None
    try:
        if not _tbl_exists(conn, dataset):
            return None
        result = conn.execute(f"SELECT MAX(open_time) FROM {dataset}").fetchone()
        if not result or result[0] is None:
            return None
        ts = pd.Timestamp(result[0])
        if ts is pd.NaT:
            return None
        return ts  # type: ignore[return-value]
    except Exception:
        logger.exception("latest_open_time sikertelen: dataset=%s", dataset)
        raise
    finally:
        conn.close()


# %% OHLCV shortcuts


def ohlcv_dataset_exists(db_path: str) -> bool:
    """Return True if the ohlcv table exists with at least one row."""
    return dataset_exists(db_path, "ohlcv")



def ohlcv_row_count(db_path: str) -> int:
    """Return total OHLCV row count from the native ohlcv table."""
    return row_count(db_path, "ohlcv")


def ohlcv_latest_open_time(db_path: str) -> str | None:
    """Return the latest OHLCV open_time as a YYYY-MM-DD HH:MM:SS string.

    Args:
        db_path : Absolute path to the asset .duckdb file.

    Returns:
        Latest open_time string, or None if the table is empty.
    """
    conn = _connect(db_path)
    if conn is None:
        return None
    try:
        if not _tbl_exists(conn, "ohlcv"):
            return None
        result = conn.execute("SELECT MAX(open_time) FROM ohlcv").fetchone()
        if not result or result[0] is None:
            return None
        return str(pd.Timestamp(result[0]).strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        logger.exception("ohlcv_latest_open_time sikertelen")
        raise
    finally:
        conn.close()


def ohlcv_time_stats(db_path: str) -> tuple[int, str | None, str | None]:
    """Return (row_count, min_open_time, max_open_time) for the ohlcv table.

    Single aggregation query — no DataFrame materialisation.

    Args:
        db_path : Absolute path to the asset .duckdb file.

    Returns:
        Tuple of (count, min_ts_str, max_ts_str).  count=0 and None timestamps
        when the ohlcv table is absent or empty.
    """
    conn = _connect(db_path)
    if conn is None:
        return 0, None, None
    try:
        if not _tbl_exists(conn, "ohlcv"):
            return 0, None, None
        result = conn.execute(
            "SELECT COUNT(*), MIN(open_time), MAX(open_time) FROM ohlcv"
        ).fetchone()
        if not result:
            return 0, None, None
        n = int(result[0])

        def _fmt(ts: object) -> str | None:
            if ts is None:
                return None
            return str(pd.Timestamp(str(ts)).strftime("%Y-%m-%d %H:%M:%S"))

        return n, _fmt(result[1]), _fmt(result[2])
    except Exception:
        logger.exception("ohlcv_time_stats sikertelen")
        raise
    finally:
        conn.close()
