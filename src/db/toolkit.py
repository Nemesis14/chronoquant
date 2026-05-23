# =============================================================================
# Database inspection and control helpers (notebook-friendly)
# =============================================================================

import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd

import utils

# =============================================================================
# CONFIG HELPERS
# =============================================================================

def load_full_config() -> dict:
    config                         = utils.load_db_config()
    config["database"]["features"] = utils.load_features_config()["database"]["features"]
    model_cfg                      = utils.load_models_config()
    if "model" in model_cfg:
        config["model"] = model_cfg["model"]
    else:
        config["models"] = model_cfg.get("models", {})
    config["api"]                  = utils.load_env_config().get("api", {})
    return config


def resolve_db_path(env: str | None = None) -> str:
    db_cfg    = utils.load_db_config()["database"]
    db_paths  = db_cfg.get("db_paths", {})
    active    = env or db_cfg.get("active_env", "dev")
    db_path   = db_paths.get(active, db_cfg.get("db_path"))
    return db_path


# =============================================================================
# BASIC QUERIES
# =============================================================================

def list_tables(env: str | None = None) -> list[str]:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            conn
        )
    return df["name"].tolist()


def get_table_columns(table_name: str, env: str | None = None) -> list[str]:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def table_exists(table_name: str, env: str | None = None) -> bool:
    return table_name in list_tables(env)


def column_exists(table_name: str, column_name: str, env: str | None = None) -> bool:
    cols = get_table_columns(table_name, env)
    return column_name in cols


def get_row_count(table_name: str, env: str | None = None) -> int:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            f"SELECT COUNT(*) as cnt FROM {table_name}",
            conn
        )
    return int(df["cnt"].iloc[0])


def get_min_max_open_time(table_name: str, env: str | None = None) -> tuple[str | None, str | None]:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            f"SELECT MIN(open_time) as min_time, MAX(open_time) as max_time FROM {table_name}",
            conn
        )
    min_time = df["min_time"].iloc[0] or None
    max_time = df["max_time"].iloc[0] or None
    return min_time, max_time


# =============================================================================
# NOTEBOOK-FRIENDLY PRINTS
# =============================================================================

def print_config_summary() -> None:
    config                         = load_full_config()
    db_cfg                          = config["database"]
    active_env                      = db_cfg.get("active_env", "dev")
    db_paths                        = db_cfg.get("db_paths", {})
    db_path                         = db_paths.get(active_env, db_cfg.get("db_path"))
    tables_config                   = db_cfg["tables"]
    table_ohlcv                     = tables_config["ohlcv"]
    table_features                  = tables_config["features"]
    table_predictions               = tables_config["predictions"]

    print("=" * 70)
    print("DATABASE CONFIGURATION")
    print("=" * 70)
    print(f"\nActive env: {active_env}")
    print(f"  Dev path:  {db_paths.get('dev')}")
    print(f"  Prod path: {db_paths.get('prod')}")
    print("\nActive DB Path:")
    print(f"  {db_path}")
    print("\nTables:")
    print(f"  [ohlcv]        {table_ohlcv}")
    print(f"  [features]     {table_features}")
    print(f"  [predictions]  {table_predictions}")
    print("\n" + "=" * 70 + "\n")


def print_tables_summary(env: str | None = None) -> None:
    env_name = env or utils.load_db_config()["database"].get("active_env", "dev")
    tables   = list_tables(env)

    print("\n" + "=" * 80)
    print(f"DATABASE TABLES SUMMARY (env={env_name})")
    print("=" * 80)
    print(f"{'Table Name':<25} {'Rows':<12} {'Min Date':<20} {'Max Date':<20}")
    print("-" * 80)

    for table_name in tables:
        row_count             = get_row_count(table_name, env)
        min_time, max_time    = get_min_max_open_time(table_name, env)
        min_time_str          = str(min_time) if min_time else "-"
        max_time_str          = str(max_time) if max_time else "-"
        print(f"{table_name:<25} {row_count:<12} {min_time_str:<20} {max_time_str:<20}")

    print("=" * 80 + "\n")


def tail(table_name: str, n: int = 5, env: str | None = None) -> None:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        count_df = pd.read_sql_query(
            f"SELECT COUNT(*) as cnt FROM {table_name}",
            conn
        )
        total_rows = int(count_df["cnt"].iloc[0])
        df = pd.read_sql_query(
            f"SELECT * FROM {table_name} ORDER BY rowid DESC LIMIT {n}",
            conn
        )

    print("\n" + "=" * 80)
    print(f"TABLE: {table_name} (env={env or 'active'})")
    print("=" * 80)
    print(f"Total rows: {total_rows}")
    print(f"\nLast {n} rows:")
    print("-" * 80)
    print(df.to_string(index=False))
    print("=" * 80 + "\n")


# =============================================================================
# VALIDATION / MAINTENANCE
# =============================================================================

def validate_open_time(table_name: str, env: str | None = None, max_issues: int = 10) -> None:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            f"SELECT open_time FROM {table_name} ORDER BY open_time ASC",
            conn
        )

    if df.empty:
        print(f"Table '{table_name}' is empty")
        return

    open_times = df["open_time"].tolist()

    print("\n" + "=" * 70)
    print(f"VALIDATING TABLE: {table_name} (env={env or 'active'})")
    print("=" * 70)

    unique_count = len(set(open_times))
    total_count  = len(open_times)

    print(f"\nTotal rows: {total_count}")
    print(f"Unique open_time values: {unique_count}")

    if unique_count != total_count:
        print("VIOLATION: open_time is NOT unique!")
        print(f"Duplicates found: {total_count - unique_count}")
        return

    print("open_time is unique")

    violations = []
    for i in range(len(open_times) - 1):
        current_str = open_times[i]
        next_str    = open_times[i + 1]
        current     = datetime.strptime(current_str, "%Y-%m-%d %H:%M:%S")
        next_ts     = datetime.strptime(next_str, "%Y-%m-%d %H:%M:%S")
        diff        = next_ts - current
        if diff != timedelta(minutes=1):
            violations.append({
                "index": i,
                "current": current_str,
                "next": next_str,
                "diff_seconds": int(diff.total_seconds())
            })

    if violations:
        print(f"VIOLATION: {len(violations)} gaps are NOT 1 minute!")
        print("\nIssues found:")
        for v in violations[:max_issues]:
            print(f"Row {v['index']}: {v['current']} -> {v['next']} (diff: {v['diff_seconds']}s)")
        if len(violations) > max_issues:
            print(f"... and {len(violations) - max_issues} more")
    else:
        print("All consecutive open_time values are exactly 1 minute apart")

    print("\n" + "=" * 70 + "\n")


def drop_table(table_name: str, env: str | None = None) -> None:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
    print(f"Dropped table '{table_name}' (env={env or 'active'})")


def rename_column(env: str, table_name: str, old_col: str, new_col: str) -> None:
    db_cfg   = utils.load_db_config()["database"]
    db_paths = db_cfg.get("db_paths", {})
    db_path  = db_paths.get(env)
    if not db_path:
        raise ValueError(f"Unknown env: {env}")

    with sqlite3.connect(db_path) as conn:
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        if table_name not in tables:
            raise ValueError(f"Table not found: {table_name}")
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")]
        if old_col not in cols:
            raise ValueError(f"Column not found: {old_col}")
        if new_col in cols:
            raise ValueError(f"New column already exists: {new_col}")
        conn.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_col} TO {new_col}")
        conn.commit()

    print(f"[{env}] Renamed {table_name}.{old_col} -> {new_col}")


def add_column(env: str, table_name: str, column_name: str, column_type: str) -> None:
    db_cfg   = utils.load_db_config()["database"]
    db_paths = db_cfg.get("db_paths", {})
    db_path  = db_paths.get(env)
    if not db_path:
        raise ValueError(f"Unknown env: {env}")

    with sqlite3.connect(db_path) as conn:
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        if table_name not in tables:
            raise ValueError(f"Table not found: {table_name}")
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")]
        if column_name in cols:
            raise ValueError(f"Column already exists: {column_name}")
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        conn.commit()

    print(f"[{env}] Added {table_name}.{column_name} ({column_type})")


def pragma_table_info(table_name: str, env: str | None = None) -> pd.DataFrame:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)


def sqlite_integrity_check(env: str | None = None) -> str:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        result = conn.execute("PRAGMA integrity_check").fetchone()
    return result[0] if result else "unknown"


def vacuum(env: str | None = None) -> None:
    db_path = resolve_db_path(env)
    with sqlite3.connect(db_path) as conn:
        conn.execute("VACUUM")
        conn.commit()
    print(f"VACUUM complete (env={env or 'active'})")



