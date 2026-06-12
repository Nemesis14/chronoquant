"""Low-level SQLite table helpers for ChronoQuant.

Provides idempotent insert support and schema management for time-indexed tables.
"""

import sqlite3
from typing import cast

import pandas as pd

SQLITE_TIMEOUT_SECONDS = 60
SQLITE_BUSY_TIMEOUT_MS = SQLITE_TIMEOUT_SECONDS * 1000


# %% Connection


def sqlite_connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with busy-wait tolerance for dashboard/sync overlap.

    Args:
        db_path: Absolute path to the SQLite database file.

    Returns:
        An open sqlite3.Connection with busy_timeout configured.
    """
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    return conn


# %% Schema helpers


def table_exists(db_path: str, table_name: str) -> bool:
    """Check whether a SQLite table exists.

    Args:
        db_path: Path to the SQLite database.
        table_name: Name of the table to check.

    Returns:
        True if the table exists, False otherwise.
    """
    with sqlite_connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
    return row is not None


def table_columns(db_path: str, table_name: str) -> list[str]:
    """Return column names of a SQLite table in storage order.

    Args:
        db_path: Path to the SQLite database.
        table_name: Name of the table.

    Returns:
        List of column name strings.
    """
    with sqlite_connect(db_path) as conn:
        return [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")]


def ensure_table_columns(db_path: str, table_name: str, df: pd.DataFrame) -> None:
    """Add any columns present in df but missing from the target table.

    Args:
        db_path: Path to the SQLite database.
        table_name: Target table name.
        df: DataFrame whose columns define the desired schema.
    """
    if df.empty or not table_exists(db_path, table_name):
        return

    existing_columns = set(table_columns(db_path, table_name))
    missing_columns  = [col for col in df.columns if col not in existing_columns]

    with sqlite_connect(db_path) as conn:
        for col in missing_columns:
            conn.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {_sqlite_type(cast(pd.Series, df[col]))}')
        conn.commit()

    if "open_time" in df.columns:
        ensure_open_time_index(db_path, table_name)


def ensure_open_time_index(db_path: str, table_name: str) -> None:
    """Create a unique index on open_time if it does not already exist.

    Args:
        db_path: Path to the SQLite database.
        table_name: Target table name.
    """
    index_name = f"idx_{table_name}_open_time"
    with sqlite_connect(db_path) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
            (index_name,),
        ).fetchone()
        if exists:
            return
        conn.execute(
            f'CREATE UNIQUE INDEX "{index_name}" ON "{table_name}" (open_time)'
        )
        conn.commit()


def _sqlite_type(series: pd.Series) -> str:
    """Map a pandas Series dtype to a SQLite storage class string.

    Args:
        series: The column to inspect.

    Returns:
        'INTEGER', 'REAL', or 'TEXT'.
    """
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series):
        return "REAL"
    return "TEXT"


# %% Data helpers


def drop_existing_open_times(
    df: pd.DataFrame,
    db_path: str,
    table_name: str,
) -> pd.DataFrame:
    """Remove rows whose open_time already exists in the target table.

    Keeps incremental and rebuild sync jobs idempotent across overlapping windows.

    Args:
        df: Input DataFrame with an open_time column.
        db_path: Path to the SQLite database.
        table_name: Target table to check against.

    Returns:
        Filtered DataFrame with already-stored rows removed.
    """
    if df.empty or not table_exists(db_path, table_name):
        return df

    min_time = df["open_time"].min()
    max_time = df["open_time"].max()

    with sqlite_connect(db_path) as conn:
        existing = cast(pd.DataFrame, pd.read_sql_query(
            f"""
            SELECT open_time FROM {table_name}
            WHERE open_time BETWEEN ? AND ?
            """,
            conn,
            params=[min_time, max_time],
        ))

    if existing.empty:
        return df

    existing_times = list(existing["open_time"].astype(str))
    return cast(pd.DataFrame, df[~df["open_time"].astype(str).isin(existing_times)].copy())
