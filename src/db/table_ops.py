# =============================================================================
# Shared SQLite table operations
# =============================================================================
# Purpose:
#  - Keep low-level SQLite table helpers in one place
#  - Provide idempotent insert support for time-indexed tables
# =============================================================================

import sqlite3

import pandas as pd


SQLITE_TIMEOUT_SECONDS = 60
SQLITE_BUSY_TIMEOUT_MS = SQLITE_TIMEOUT_SECONDS * 1000


# =============================================================================
# sqlite_connect(db_path: str) -> sqlite3.Connection
# =============================================================================
# Purpose:
#  - Open SQLite connections with enough wait time for dashboard/sync overlap
# =============================================================================
def sqlite_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    return conn


# =============================================================================
# table_exists(db_path: str, table_name: str) -> bool
# =============================================================================
# Purpose:
#  - Check whether a SQLite table exists
# =============================================================================
def table_exists(db_path: str, table_name: str) -> bool:
    with sqlite_connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
    return row is not None


# =============================================================================
# table_columns(db_path: str, table_name: str) -> list[str]
# =============================================================================
# Purpose:
#  - Return SQLite table column names in storage order
# =============================================================================
def table_columns(db_path: str, table_name: str) -> list[str]:
    with sqlite_connect(db_path) as conn:
        return [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")]


# =============================================================================
# ensure_table_columns(db_path: str, table_name: str, df: pd.DataFrame) -> None
# =============================================================================
# Purpose:
#  - Add missing SQLite columns before appending a widened DataFrame
# =============================================================================
def ensure_table_columns(db_path: str, table_name: str, df: pd.DataFrame) -> None:
    if df.empty or not table_exists(db_path, table_name):
        return

    existing_columns = set(table_columns(db_path, table_name))
    missing_columns = [col for col in df.columns if col not in existing_columns]

    with sqlite_connect(db_path) as conn:
        for col in missing_columns:
            conn.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {_sqlite_type(df[col])}')
        conn.commit()

    if "open_time" in df.columns:
        ensure_open_time_index(db_path, table_name)


def ensure_open_time_index(db_path: str, table_name: str) -> None:
    index_name = f"idx_{table_name}_open_time"
    with sqlite_connect(db_path) as conn:
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" (open_time)'
        )
        conn.commit()


# =============================================================================
# _sqlite_type(series: pd.Series) -> str
# =============================================================================
# Purpose:
#  - Map pandas dtypes to simple SQLite storage classes
# =============================================================================
def _sqlite_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series):
        return "REAL"
    return "TEXT"


# =============================================================================
# drop_existing_open_times(...) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Remove rows whose open_time already exists in the target table
#  - Keep incremental/rebuild sync jobs idempotent across overlapping windows
# =============================================================================
def drop_existing_open_times(
    df: pd.DataFrame,
    db_path: str,
    table_name: str,
) -> pd.DataFrame:
    if df.empty or not table_exists(db_path, table_name):
        return df

    min_time = df["open_time"].min()
    max_time = df["open_time"].max()

    with sqlite_connect(db_path) as conn:
        existing = pd.read_sql_query(
            f"""
            SELECT open_time FROM {table_name}
            WHERE open_time BETWEEN ? AND ?
            """,
            conn,
            params=(min_time, max_time),
        )

    if existing.empty:
        return df

    existing_times = set(existing["open_time"].astype(str))
    return df[~df["open_time"].astype(str).isin(existing_times)].copy()
