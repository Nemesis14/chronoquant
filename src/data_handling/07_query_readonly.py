#!/usr/bin/env python3
"""Read-only DuckDB query helper (CLI).

Thin wrapper over ``data_handling.store.readonly_query`` (all safety layers and
the query logic live there).  Prints the result as JSON (default) or a markdown table.

Aliases available in the SQL (see _doc_/database_and_code_doc/0002_data_architecture.md):

    live.<table>            live.duckdb      (ohlcv, feat_ohlcv_quant, target, quant_train, predictions)
    snap."<id>" / model."<id>__pred" / strat."<id>__trades"   lab.duckdb (default catalog)
    reg.<table>             registry.duckdb  (assets, snapshots, models, deployments, ...)

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
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import utils  # noqa: E402, F401  (re-exported: tests monkeypatch ``utils`` through this module)
from data_handling.store.readonly_query import (  # noqa: E402, F401
    ALLOWED_KEYWORDS,
    DEFAULT_LIMIT,
    DEFAULT_TIMEOUT,
    QueryRejectedError,
    describe_schema,
    format_result,
    format_schema,
    open_readonly_connection,
    run_query,
    run_readonly_query,
    schema_listing,
    validate_statement,
)


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
