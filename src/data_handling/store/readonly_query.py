"""Read-only DuckDB query layer (shared by the CLI, the API and the MCP server).

Opens the asset's live / lab / registry databases strictly READ_ONLY on a fresh
in-memory connection and runs a single SELECT-like statement.  Short-lived
connection: it never blocks a running sync / trading writer (read-only attach,
closed right after the query).

Aliases available in the SQL (see _doc_/database_and_code_doc/0002_data_architecture.md):

    live.<table>            live.duckdb      (ohlcv, feat_ohlcv_quant, target, quant_train, predictions)
    snap."<id>" / model."<id>__pred" / strat."<id>__trades"   lab.duckdb (default catalog)
    reg.<table>             registry.duckdb  (assets, snapshots, models, deployments, ...)

Safety layers
-------------
1. Statement check: exactly ONE statement, parsed by DuckDB itself
   (``extract_statements``), parser type must be SELECT and the leading keyword one
   of SELECT / WITH / FROM / VALUES / DESCRIBE / SHOW / SUMMARIZE.  Semicolon-chained
   statements, COPY, ATTACH, INSTALL/LOAD, PRAGMA, SET, EXPORT, CALL, DML/DDL are rejected.
2. ``enable_external_access=false`` + ``lock_configuration=true`` after attach, so
   read_csv / read_parquet / httpfs file access from inside a SELECT is blocked.
3. All catalogs attached with READ_ONLY (the final safety net).

The CLI wrapper lives in ``src/data_handling/07_query_readonly.py``.
"""

import json
import re
import threading
import time
from pathlib import Path
from typing import Any

import duckdb

import utils

DEFAULT_LIMIT   = 200
DEFAULT_TIMEOUT = 60.0

ALLOWED_KEYWORDS = frozenset({"select", "with", "from", "values", "describe", "show", "summarize"})

_LEADING_NOISE = re.compile(r"\A(?:\s+|--[^\n]*(?:\n|\Z)|/\*.*?\*/|\()+", re.DOTALL)
_FIRST_WORD    = re.compile(r"[A-Za-z_]+")


class QueryRejectedError(ValueError):
    """Raised when a statement fails the read-only safety check."""


def validate_statement(sql: str) -> str:
    """Return the cleaned SQL if it is a single read-only statement, else raise.

    Args:
        sql : Raw SQL text from the caller.

    Raises:
        QueryRejectedError: Empty input, multiple statements, non-SELECT parser type,
            or a leading keyword outside the allowlist.
    """
    text = sql.strip().rstrip(";").strip()
    if not text:
        raise QueryRejectedError("empty statement")
    try:
        statements = duckdb.extract_statements(text)
    except duckdb.Error as exc:
        raise QueryRejectedError(f"SQL parse error: {exc}") from exc
    if len(statements) != 1:
        raise QueryRejectedError(f"exactly one statement allowed, got {len(statements)}")
    if statements[0].type != duckdb.StatementType.SELECT:
        raise QueryRejectedError(f"statement type {statements[0].type.name} not allowed (read-only: SELECT/WITH/DESCRIBE/SHOW)")
    body  = _LEADING_NOISE.sub("", text, count=1)
    match = _FIRST_WORD.match(body)
    word  = match.group(0).lower() if match else ""
    if word not in ALLOWED_KEYWORDS:
        raise QueryRejectedError(f"leading keyword '{word or body[:12]}' not allowed (allowed: {', '.join(sorted(ALLOWED_KEYWORDS))})")
    return text


def _attach_with_retry(conn: duckdb.DuckDBPyConnection, alias: str, path: str, tries: int = 6, delay: float = 0.5) -> None:
    """ATTACH READ_ONLY, retrying briefly when another process holds a file lock.

    A cross-process writer lock (Windows) is normally short-lived; after the last
    retry the original duckdb error propagates (database error, exit code 3).
    """
    stmt = f"ATTACH '{path.replace(chr(39), chr(39) * 2)}' AS {alias} (READ_ONLY)"
    for attempt in range(tries):
        try:
            conn.execute(stmt)
            return
        except duckdb.IOException:
            if attempt == tries - 1:
                raise
            time.sleep(delay)


def open_readonly_connection(
    asset_id      : str | None = None,
    *,
    tries         : int = 6,
    delay         : float = 0.5,
    extra_targets : dict[str, str] | None = None,
) -> duckdb.DuckDBPyConnection:
    """Open an in-memory connection with live / lab / reg attached READ_ONLY.

    Missing database files are skipped.  The lab catalog becomes the default
    (``USE lab``) so ``snap.*`` / ``model.*`` / ``strat.*`` resolve unqualified.
    External file access and configuration changes are disabled afterwards.

    Args:
        asset_id      : Asset key; uses default_asset_id if None.
        tries         : ATTACH attempts per file when another process holds a lock.
        delay         : Seconds between ATTACH attempts.
        extra_targets : Additional ``alias -> path`` databases to attach READ_ONLY
            (e.g. the trading journal for the API); missing files are skipped.

    Returns:
        Open connection. Caller closes it.
    """
    targets = {
        "live": utils.load_asset_config(asset_id)["database"]["db_path"],
        "lab" : utils.lab_db_path(asset_id),
        "reg" : utils.registry_path(),
    }
    conn = duckdb.connect(":memory:")
    try:
        for alias, path in {**targets, **(extra_targets or {})}.items():
            if Path(path).exists():
                _attach_with_retry(conn, alias, path, tries=tries, delay=delay)
        if Path(targets["lab"]).exists():
            conn.execute("USE lab")
        conn.execute("SET enable_external_access=false")
        conn.execute("SET lock_configuration=true")
    except Exception:
        conn.close()
        raise
    return conn


def run_query(
    conn    : duckdb.DuckDBPyConnection,
    sql     : str,
    limit   : int = DEFAULT_LIMIT,
    timeout : float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Run a validated statement and return columns, rows (<= limit) and truncation flag.

    Args:
        conn    : Connection from open_readonly_connection.
        sql     : SQL text; validated here again (defence in depth).
        limit   : Max rows returned; one extra row is fetched to detect truncation.
        timeout : Seconds before the query is interrupted (0 disables).
    """
    text  = validate_statement(sql)
    timer = threading.Timer(timeout, conn.interrupt) if timeout > 0 else None
    if timer:
        timer.start()
    try:
        cur     = conn.execute(text)
        columns = [d[0] for d in (cur.description or [])]
        rows    = cur.fetchmany(limit + 1)
    finally:
        if timer:
            timer.cancel()
    truncated = len(rows) > limit
    return {"columns": columns, "rows": [list(r) for r in rows[:limit]], "row_count": min(len(rows), limit), "truncated": truncated, "limit": limit}


def schema_listing(conn: duckdb.DuckDBPyConnection, name_filter: str | None = None, catalog: str | None = None) -> dict[str, Any]:
    """List tables and their columns across the attached catalogs.

    Args:
        conn        : Connection from open_readonly_connection.
        name_filter : Case-insensitive substring on schema.table (optional).
        catalog     : Restrict to one catalog alias (live / lab / reg).
    """
    rows = conn.execute(
        """
        SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.data_type, t.table_type
        FROM information_schema.columns c
        JOIN information_schema.tables t USING (table_catalog, table_schema, table_name)
        WHERE c.table_catalog IN ('live', 'lab', 'reg')
        ORDER BY c.table_catalog, c.table_schema, c.table_name, c.ordinal_position
        """
    ).fetchall()
    tables: dict[tuple[str, str, str], dict[str, Any]] = {}
    for cat, sch, tbl, col, dtype, ttype in rows:
        if catalog and cat != catalog:
            continue
        if name_filter and name_filter.lower() not in f"{sch}.{tbl}".lower():
            continue
        entry = tables.setdefault((cat, sch, tbl), {"catalog": cat, "schema": sch, "table": tbl, "type": ttype, "columns": []})
        entry["columns"].append({"name": col, "type": dtype})
    return {"tables": list(tables.values()), "table_count": len(tables)}


def run_readonly_query(
    sql      : str,
    limit    : int = DEFAULT_LIMIT,
    asset_id : str | None = None,
    timeout  : float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Validate, open a short-lived read-only connection, run the query, close it.

    Args:
        sql      : Single SELECT-like statement.
        limit    : Max rows returned.
        asset_id : Asset key; uses default_asset_id if None.
        timeout  : Query timeout in seconds (0 disables).

    Raises:
        QueryRejectedError: Statement failed the safety check.
        duckdb.Error: Database error.
    """
    validate_statement(sql)  # reject before touching any database
    conn = open_readonly_connection(asset_id)
    try:
        return run_query(conn, sql, limit, timeout)
    finally:
        conn.close()


def describe_schema(
    name_filter : str | None = None,
    catalog     : str | None = None,
    asset_id    : str | None = None,
) -> dict[str, Any]:
    """Open a short-lived read-only connection and return the schema listing."""
    conn = open_readonly_connection(asset_id)
    try:
        return schema_listing(conn, name_filter, catalog)
    finally:
        conn.close()


def _md_cell(value: Any) -> str:
    return "" if value is None else str(value).replace("|", "\\|").replace("\n", " ")


def format_result(result: dict[str, Any], fmt: str) -> str:
    """Render a run_query result as JSON or a markdown table."""
    if fmt == "json":
        return json.dumps(result, default=str, ensure_ascii=False)
    cols  = result["columns"]
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    lines += ["| " + " | ".join(_md_cell(v) for v in row) + " |" for row in result["rows"]]
    note = f"\n\n{result['row_count']} row(s)" + (f" (truncated at limit {result['limit']})" if result["truncated"] else "")
    return "\n".join(lines) + note


def format_schema(listing: dict[str, Any], fmt: str) -> str:
    """Render a schema_listing as JSON or markdown."""
    if fmt == "json":
        return json.dumps(listing, default=str, ensure_ascii=False)
    out = []
    for t in listing["tables"]:
        cols = ", ".join(f"{c['name']} {c['type']}" for c in t["columns"])
        out.append(f"- **{t['catalog']}.{t['schema']}.{t['table']}** ({t['type']}): {cols}")
    return "\n".join(out) + f"\n\n{listing['table_count']} table(s)"
