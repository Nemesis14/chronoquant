#!/usr/bin/env python3
"""Read-only DuckDB query helper (CLI) for the UI chat.

Opens the asset's live / lab / registry databases strictly READ_ONLY on a fresh
in-memory connection and runs a single SELECT-like statement, printing the result
as JSON (default) or a markdown table.  Short-lived connection: it never blocks a
running sync / trading writer (read-only attach, closed right after the query).

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

Usage
-----
    uv run python src/data_handling/07_query_readonly.py "SELECT count(*) AS n FROM live.ohlcv"
    uv run python src/data_handling/07_query_readonly.py "SELECT * FROM reg.models" --limit 20 --format markdown
    uv run python src/data_handling/07_query_readonly.py --schema
    uv run python src/data_handling/07_query_readonly.py --schema --filter predictions
    uv run python src/data_handling/07_query_readonly.py --schema --catalog reg

Exit codes: 0 ok, 2 rejected statement / bad usage, 3 database error.
"""

import argparse
import json
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

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


def open_readonly_connection(asset_id: str | None = None) -> duckdb.DuckDBPyConnection:
    """Open an in-memory connection with live / lab / reg attached READ_ONLY.

    Missing database files are skipped.  The lab catalog becomes the default
    (``USE lab``) so ``snap.*`` / ``model.*`` / ``strat.*`` resolve unqualified.
    External file access and configuration changes are disabled afterwards.

    Args:
        asset_id : Asset key; uses default_asset_id if None.

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
        for alias, path in targets.items():
            if Path(path).exists():
                _attach_with_retry(conn, alias, path)
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


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description     = "Read-only DuckDB query helper (live/lab/reg attached READ_ONLY).",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("sql", nargs="?", default=None, help="Single SELECT/WITH/DESCRIBE/SHOW statement.")
    parser.add_argument("--schema", action="store_true", help="List tables and columns instead of running SQL.")
    parser.add_argument("--filter", default=None, help="With --schema: substring filter on schema.table.")
    parser.add_argument("--catalog", default=None, choices=["live", "lab", "reg"], help="With --schema: one catalog only.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max rows returned.")
    parser.add_argument("--format", dest="fmt", choices=["json", "markdown"], default="json")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="Query timeout in seconds (0 = none).")
    parser.add_argument("--asset-id", default=None, help="Asset key; default asset if omitted.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    args = _parse_args(argv)
    if not args.schema and not args.sql:
        sys.stderr.write("error: provide a SQL statement or --schema\n")
        return 2
    if args.limit < 1:
        sys.stderr.write("error: --limit must be >= 1\n")
        return 2
    try:
        if not args.schema:
            validate_statement(args.sql)  # reject before touching any database
        conn = open_readonly_connection(args.asset_id)
    except QueryRejectedError as exc:
        sys.stderr.write(f"rejected: {exc}\n")
        return 2
    except duckdb.Error as exc:
        sys.stderr.write(f"database error: {exc}\n")
        return 3
    try:
        if args.schema:
            output = format_schema(schema_listing(conn, args.filter, args.catalog), args.fmt)
        else:
            output = format_result(run_query(conn, args.sql, args.limit, args.timeout), args.fmt)
    except QueryRejectedError as exc:
        sys.stderr.write(f"rejected: {exc}\n")
        return 2
    except duckdb.Error as exc:
        sys.stderr.write(f"database error: {exc}\n")
        return 3
    finally:
        conn.close()
    sys.stdout.write(output + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
