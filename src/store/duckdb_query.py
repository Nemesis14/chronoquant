"""Read layer for daily Parquet partitions via DuckDB.

Provides range queries and schema inspection over glob-based Parquet datasets.
All queries return pandas DataFrames. DuckDB connections are in-memory and
short-lived — no persistent .duckdb file is created.
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# %% Query


def query_range(
    data_dir  : str,
    dataset   : str,
    start     : str | None = None,
    end       : str | None = None,
    columns   : list[str] | None = None,
) -> pd.DataFrame:
    """Query rows from a dataset within an optional open_time range.

    Reads all daily .parquet partitions via DuckDB glob, filters by
    open_time, and returns a sorted DataFrame.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory: 'ohlcv', 'features', or 'predictions'.
        start    : Optional lower bound, inclusive. YYYY-MM-DD HH:MM:SS.
        end      : Optional upper bound, inclusive. YYYY-MM-DD HH:MM:SS.
        columns  : Optional list of columns to SELECT. None = all columns.

    Returns:
        DataFrame sorted by open_time, or empty DataFrame if no files found.
    """
    glob_path = str(Path(data_dir) / dataset / "*.parquet")
    if not list(Path(data_dir, dataset).glob("*.parquet")):
        return pd.DataFrame()

    col_expr = ", ".join(f'"{c}"' for c in columns) if columns else "*"

    conditions: list[str] = []
    params: list[str] = []
    if start:
        conditions.append("open_time >= ?")
        params.append(start)
    if end:
        conditions.append("open_time <= ?")
        params.append(end)

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT {col_expr}
        FROM read_parquet('{glob_path}', union_by_name = true)
        WHERE {where}
        ORDER BY open_time ASC
    """

    logger.debug("query_range: dataset=%s start=%s end=%s", dataset, start, end)
    conn = duckdb.connect()
    try:
        df = conn.execute(sql, params).df()
    except Exception:
        logger.exception("DuckDB query_range sikertelen: dataset=%s", dataset)
        raise
    finally:
        conn.close()

    logger.debug("query_range: visszaadott sorok=%d", len(df))
    return df


def query_all(
    data_dir : str,
    dataset  : str,
    columns  : list[str] | None = None,
) -> pd.DataFrame:
    """Return all rows from a dataset, sorted by open_time.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory: 'ohlcv', 'features', or 'predictions'.
        columns  : Optional list of columns to SELECT. None = all columns.

    Returns:
        Full DataFrame sorted by open_time, or empty DataFrame if no files.
    """
    return query_range(data_dir, dataset, columns=columns)


# %% Schema


def dataset_columns(data_dir: str, dataset: str) -> list[str]:
    """Return column names from the dataset schema (reads one partition).

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory: 'ohlcv', 'features', or 'predictions'.

    Returns:
        List of column name strings, or empty list if no partitions exist.
    """
    parts = sorted(Path(data_dir, dataset).glob("*.parquet"))
    if not parts:
        return []

    conn = duckdb.connect()
    try:
        row = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parts[-1]}')"
        ).fetchall()
    except Exception:
        logger.exception("DuckDB dataset_columns sikertelen: dataset=%s", dataset)
        raise
    finally:
        conn.close()

    return [r[0] for r in row]


def dataset_exists(data_dir: str, dataset: str) -> bool:
    """Return True if at least one partition exists for the dataset.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory to check.

    Returns:
        True if any .parquet file exists.
    """
    return any(Path(data_dir, dataset).glob("*.parquet"))


def row_count(data_dir: str, dataset: str) -> int:
    """Return total row count across all partitions in a dataset.

    Args:
        data_dir : Root data directory for the asset.
        dataset  : Sub-directory to count.

    Returns:
        Total row count, or 0 if no files exist.
    """
    glob_path = str(Path(data_dir) / dataset / "*.parquet")
    if not dataset_exists(data_dir, dataset):
        return 0

    conn = duckdb.connect()
    try:
        result = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{glob_path}', union_by_name = true)"
        ).fetchone()
    except Exception:
        logger.exception("DuckDB row_count sikertelen: dataset=%s", dataset)
        raise
    finally:
        conn.close()

    return int(result[0]) if result else 0
