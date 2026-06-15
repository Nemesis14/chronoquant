"""Informational DuckDB statistics and timing smoke reports.

Collects non-blocking count, range-query, and aggregation metrics for native
DuckDB tables.  Intended for validation output after data reloads; only explicit
SQL/data integrity errors should fail callers.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from database.store.duckdb_query import (
    dataset_exists,
    ohlcv_dataset_exists,
    ohlcv_time_stats,
    query_range_pl,
)

logger = logging.getLogger(__name__)

# %% Schemas


@dataclass(frozen=True)
class TableStats:
    """Summary statistics for one DuckDB table."""

    table        : str
    status       : str
    row_count    : int
    min_open_time: str | None
    max_open_time: str | None
    column_count : int
    null_ratios  : dict[str, float]


@dataclass(frozen=True)
class TimedMetric:
    """Timing result for one informational DuckDB smoke query."""

    label     : str
    status    : str
    elapsed_ms: float
    row_count : int | None
    detail    : str


@dataclass(frozen=True)
class DuckDBStatsReport:
    """Complete informational DuckDB smoke report."""

    db_path: str
    tables : list[TableStats]
    timings: list[TimedMetric]


# %% Helpers


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    """Return True when table exists in the connected database."""
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(row and row[0] > 0)


def _fmt_ts(value: object) -> str | None:
    """Format a DuckDB timestamp value for report output."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)[:19]


def _time_query(
    conn  : duckdb.DuckDBPyConnection,
    label : str,
    sql   : str,
    params: list[object] | None = None,
) -> TimedMetric:
    """Execute a query and return elapsed time plus result row count."""
    t0 = time.perf_counter()
    rows = conn.execute(sql, params or []).fetchall()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return TimedMetric(
        label      = label,
        status     = "OK",
        elapsed_ms = elapsed_ms,
        row_count  = len(rows),
        detail     = f"rows={len(rows)}",
    )


def _empty_table_stats(table: str, status: str) -> TableStats:
    """Build a zero-row stats object for missing or empty tables."""
    return TableStats(
        table         = table,
        status        = status,
        row_count     = 0,
        min_open_time = None,
        max_open_time = None,
        column_count  = 0,
        null_ratios   = {},
    )


# %% Public API


def collect_duckdb_stats_report(
    db_path : str,
    tables  : list[str] | None = None,
) -> DuckDBStatsReport:
    """Collect informational DuckDB statistics and timing smoke metrics.

    Args:
        db_path : Absolute path to the asset .duckdb file.
        tables  : Table names to inspect. Defaults to core native tables.

    Returns:
        DuckDBStatsReport with SKIP/EMPTY statuses when data is unavailable.
    """
    names   = tables or ["ohlcv", "target", "feat_ohlcv_quant", "predictions"]
    p = Path(db_path)
    if not p.exists():
        return DuckDBStatsReport(
            db_path = db_path,
            tables  = [_empty_table_stats(table, "SKIP_DB_MISSING") for table in names],
            timings = [],
        )

    conn = duckdb.connect(db_path, read_only=True)
    try:
        table_stats: list[TableStats] = []
        for table in names:
            if not _table_exists(conn, table):
                table_stats.append(_empty_table_stats(table, "SKIP_TABLE_MISSING"))
                continue

            row = conn.execute(
                f"SELECT COUNT(*), MIN(open_time), MAX(open_time) FROM {table}"
            ).fetchone()
            if row is None:
                table_stats.append(_empty_table_stats(table, "EMPTY"))
                continue
            count = int(row[0])
            cols = [
                r[0]
                for r in conn.execute(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_name = ? ORDER BY ordinal_position",
                    [table],
                ).fetchall()
            ]
            if count == 0:
                table_stats.append(_empty_table_stats(table, "EMPTY"))
                continue

            sampled_cols = [c for c in cols if c != "open_time"][:5]
            null_ratios: dict[str, float] = {}
            for col in sampled_cols:
                null_row = conn.execute(
                    f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NULL'
                ).fetchone()
                null_count = int(null_row[0]) if null_row else 0
                null_ratios[col] = float(null_count) / float(count)

            table_stats.append(
                TableStats(
                    table         = table,
                    status        = "OK",
                    row_count     = count,
                    min_open_time = _fmt_ts(row[1]),
                    max_open_time = _fmt_ts(row[2]),
                    column_count  = len(cols),
                    null_ratios   = null_ratios,
                )
            )

        timings: list[TimedMetric] = []
        ohlcv_stats = next((s for s in table_stats if s.table == "ohlcv"), None)
        if ohlcv_stats is not None and ohlcv_stats.status == "OK" and ohlcv_stats.max_open_time:
            end_dt = datetime.strptime(ohlcv_stats.max_open_time, "%Y-%m-%d %H:%M:%S")
            windows = {
                "1d"  : end_dt - timedelta(days=1),
                "1w"  : end_dt - timedelta(days=7),
                "1mo" : end_dt - timedelta(days=30),
                "full": None,
            }
            for label, start_dt in windows.items():
                if start_dt is None:
                    sql = "SELECT COUNT(*) FROM ohlcv"
                    params: list[object] = []
                else:
                    sql = "SELECT COUNT(*) FROM ohlcv WHERE open_time BETWEEN ? AND ?"
                    params = [
                        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    ]
                timings.append(_time_query(conn, f"range_ohlcv_{label}", sql, params))

            timings.append(
                _time_query(
                    conn,
                    "groupby_ohlcv_year",
                    """
                    SELECT DATE_TRUNC('year', open_time) AS year, COUNT(*)
                    FROM ohlcv
                    GROUP BY 1
                    ORDER BY 1
                    """,
                )
            )
        else:
            timings.append(
                TimedMetric(
                    label      = "ohlcv_timings",
                    status     = "SKIP_EMPTY",
                    elapsed_ms = 0.0,
                    row_count  = None,
                    detail     = "ohlcv table missing or empty",
                )
            )

        return DuckDBStatsReport(db_path=str(db_path), tables=table_stats, timings=timings)
    finally:
        conn.close()


def raw_manifest_audit(db_path: str, dataset: str) -> None:
    """Run a DuckDB native integrity audit and log statistics.

    Checks row count, open_time range, null timestamps, and duplicate
    open_time values for the given native DuckDB table.

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Table name: 'ohlcv', 'target', 'feat_ohlcv_quant', or 'predictions'.
    """
    db_file = Path(db_path)
    if not db_file.exists():
        logger.warning("%s: raw_manifest_audit - DuckDB file not found: %s", dataset, db_file)
        return

    conn = duckdb.connect(str(db_file), read_only=True)
    try:
        tbl_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [dataset],
        ).fetchone()
        if not (tbl_exists and tbl_exists[0] > 0):
            logger.warning("%s: raw_manifest_audit - tabla nem talalhato", dataset)
            return

        result = conn.execute(f"""
            SELECT
                COUNT(*)                                                    AS row_count,
                MIN(open_time)                                              AS min_ts,
                MAX(open_time)                                              AS max_ts,
                SUM(CASE WHEN open_time IS NULL THEN 1 ELSE 0 END)         AS null_ts,
                COUNT(*) - COUNT(DISTINCT CAST(open_time AS VARCHAR))       AS dup_ts
            FROM {dataset}
        """).fetchone()
        if not result:
            logger.warning("%s: raw_manifest_audit - nincs adat", dataset)
            return

        row_count, min_ts, max_ts, null_ts, dup_ts = result
        logger.info(
            "%s | rows=%d min=%s max=%s null_ts=%d dup_ts=%d",
            dataset, row_count, min_ts, max_ts, null_ts, dup_ts,
        )
        if null_ts > 0 or dup_ts > 0:
            logger.warning("%s: GYANÚS - null_ts=%d dup_ts=%d", dataset, null_ts, dup_ts)
        else:
            logger.info("%s: raw_manifest_audit OK", dataset)
    except Exception:
        logger.exception("raw_manifest_audit DuckDB hiba: dataset=%s", dataset)
    finally:
        conn.close()


def log_dataset_check(db_path: str, dataset: str) -> None:
    """Log row count, open_time range, and integrity audit for a DuckDB dataset.

    Args:
        db_path  : Absolute path to the asset .duckdb file.
        dataset  : Dataset name: 'ohlcv', 'target', 'feat_ohlcv_quant', or 'predictions'.
    """
    if dataset == "ohlcv":
        if not ohlcv_dataset_exists(db_path):
            logger.warning("%s: nincsenek sorok", dataset)
            return
        n, mn, mx = ohlcv_time_stats(db_path)
    else:
        if not dataset_exists(db_path, dataset):
            logger.warning("%s: nincsenek sorok", dataset)
            return
        df = query_range_pl(db_path, dataset, columns=["open_time"])
        n  = len(df)
        mn = df["open_time"].min() if n else None
        mx = df["open_time"].max() if n else None
    logger.info("%s: sorok=%d, tartomany=%s -> %s", dataset, n, mn, mx)

    raw_manifest_audit(db_path, dataset)


def format_duckdb_stats_report(report: DuckDBStatsReport) -> str:
    """Render a DuckDBStatsReport as concise validation text."""
    lines = [
        "DuckDB statistics smoke report",
        f"db_path: {report.db_path}",
        "informational: timing metrics do not fail validation by themselves",
        "",
        "Tables:",
    ]
    for stat in report.tables:
        lines.append(
            f"- {stat.table}: status={stat.status} rows={stat.row_count} "
            f"min={stat.min_open_time} max={stat.max_open_time} cols={stat.column_count}"
        )
        if stat.null_ratios:
            nulls = ", ".join(f"{col}={ratio:.3f}" for col, ratio in stat.null_ratios.items())
            lines.append(f"  null_ratios: {nulls}")

    lines.append("")
    lines.append("Timings:")
    for metric in report.timings:
        lines.append(
            f"- {metric.label}: status={metric.status} elapsed_ms={metric.elapsed_ms:.3f} "
            f"row_count={metric.row_count} {metric.detail}"
        )
    return "\n".join(lines)
