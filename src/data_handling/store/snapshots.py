"""Immutable snapshot layer (``snap`` schema) for the DuckDB-native architecture.

A snapshot freezes a date range of the live ``quant_train`` table into an
immutable DuckDB table ``snap."<snapshot_id>"`` inside the lab database, and
registers it in ``reg.snapshots``.  The frozen table — not the mutable live
table — is what the modeling pipeline reads, which is what makes a model
reproducible (plan sections 4.1, 5 step 1, 6).

This module owns:
  * the ``snap`` schema bootstrap in the lab database,
  * the content hash (sha256 over ordered row content) and the feature-set hash
    (sha256 over the ordered feature column list),
  * the snapshot_id naming ``{asset}_fw{h}_{range}__{hash8}`` (plan 6),
  * CTAS of the immutable snapshot table + a ``reg.snapshots`` row,
  * reuse detection: an identical-content re-run does NOT overwrite — it returns
    the existing snapshot.

Reference: _doc_/_plans_/data_process_architecture.md sections 4.1, 4.2, 5, 6.
Callers reach the lab/live/registry topology through src/utils.py
(open_lab_connection), never by opening DuckDB files directly.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import duckdb

from data_handling.store import registry

logger = logging.getLogger(__name__)

# %% Constants

SNAP_SCHEMA = "snap"

# Source table in the live (attached READ_ONLY) database.
LIVE_SOURCE = "live.quant_train"

# Number of hex chars of the content hash embedded in the snapshot_id (plan 6).
HASH8_LEN = 8


@dataclass(frozen=True)
class SnapshotResult:
    """Outcome of a create_snapshot call.

    Attributes:
        snapshot_id      : The ``{asset}_fw{h}_{range}__{hash8}`` identifier.
        asset_id         : Asset the snapshot belongs to.
        range_start      : Inclusive lower bound (YYYY-MM-DD HH:MM:SS) actually used.
        range_end        : Inclusive upper bound (YYYY-MM-DD HH:MM:SS) actually used.
        row_count        : Number of rows frozen into the snapshot table.
        content_sha256   : Full sha256 over the ordered row content.
        feature_set_hash : sha256 over the ordered feature column list (superset).
        reused           : True if an identical snapshot already existed (no write).
    """

    snapshot_id      : str
    asset_id         : str
    range_start      : str
    range_end        : str
    row_count        : int
    content_sha256   : str
    feature_set_hash : str
    reused           : bool


# %% Schema bootstrap


def ensure_snap_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the ``snap`` schema in the (default) lab database if absent.

    Args:
        conn : Open lab connection (the lab database is the default DB).
    """
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SNAP_SCHEMA}")


# %% Naming


def format_range(range_start: str, range_end: str) -> str:
    """Build the range token for a snapshot_id (plan 6).

    Uses ``{year}`` when both bounds fall in the same calendar year, otherwise
    ``{YYMM_start}_{YYMM_end}``.

    Args:
        range_start : Inclusive lower bound, ``YYYY-MM-DD HH:MM:SS``.
        range_end   : Inclusive upper bound, ``YYYY-MM-DD HH:MM:SS``.

    Returns:
        The range token, e.g. ``2023`` or ``2101_2605``.
    """
    y_start, m_start = range_start[:4], range_start[5:7]
    y_end,   m_end   = range_end[:4],   range_end[5:7]
    if y_start == y_end:
        return y_start
    return f"{y_start[2:]}{m_start}_{y_end[2:]}{m_end}"


def build_snapshot_id(asset_id: str, horizon: int, range_token: str, content_sha256: str) -> str:
    """Assemble the snapshot_id ``{asset}_fw{h}_{range}__{hash8}`` (plan 6).

    Args:
        asset_id       : Asset key (lower-case symbol), e.g. ``solusdt``.
        horizon        : Forward-window bar count, e.g. ``60`` -> ``fw60``.
        range_token    : Output of format_range, e.g. ``2101_2605``.
        content_sha256 : Full content hash; its first HASH8_LEN chars are used.

    Returns:
        The snapshot identifier.
    """
    return f"{asset_id}_fw{horizon}_{range_token}__{content_sha256[:HASH8_LEN]}"


# %% Hashing


def _ordered_feature_columns(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Return the source table's feature columns, sorted (excludes open_time/targets).

    The feature set the snapshot carries is the ``feat_*`` superset of
    ``live.quant_train``.  Ordering makes the feature_set_hash deterministic.
    """
    rows = conn.execute(f"SELECT * FROM {LIVE_SOURCE} LIMIT 0").description
    cols = [d[0] for d in rows]
    return sorted(c for c in cols if c.startswith("feat_"))


def _resolve_range(
    conn       : duckdb.DuckDBPyConnection,
    start_time : str | None,
    end_time   : str | None,
) -> tuple[str, str]:
    """Resolve the effective inclusive range as ``YYYY-MM-DD HH:MM:SS`` strings.

    Unbounded sides fall back to the source table's min/max open_time.
    """
    conds: list[str] = []
    params: list[str] = []
    if start_time:
        conds.append("open_time >= ?")
        params.append(start_time)
    if end_time:
        conds.append("open_time <= ?")
        params.append(end_time)
    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    row = conn.execute(
        f"""
        SELECT strftime(MIN(open_time), '%Y-%m-%d %H:%M:%S'),
               strftime(MAX(open_time), '%Y-%m-%d %H:%M:%S')
        FROM {LIVE_SOURCE} {where}
        """,
        params,
    ).fetchone()
    if row is None or row[0] is None:
        raise ValueError("No rows in the requested range of live.quant_train")
    return str(row[0]), str(row[1])


def compute_content_sha256(
    conn        : duckdb.DuckDBPyConnection,
    range_start : str,
    range_end   : str,
) -> tuple[str, int]:
    """Compute the content hash and row count for a range of the source table.

    The hash is a deterministic fingerprint of the row content and count,
    so two ranges with identical data hash identically — this is what drives
    reuse detection (plan 6).

    Uses ``SUM(hash(open_time))`` + ``COUNT(*)`` + range bounds to build a
    memory-efficient fingerprint without materialising full row JSON strings.
    This avoids OOM on large ranges (the original ``string_agg(to_json())``
    approach exceeded 12 GB for a 5-year minute dataset).

    Args:
        conn        : Open lab connection (live attached as ``live``).
        range_start : Inclusive lower bound, ``YYYY-MM-DD HH:MM:SS``.
        range_end   : Inclusive upper bound, ``YYYY-MM-DD HH:MM:SS``.

    Returns:
        (content_sha256, row_count).
    """
    import hashlib

    row = conn.execute(
        f"""
        SELECT
            COUNT(*)                              AS n,
            SUM(hash(open_time))                  AS time_hash_sum,
            strftime(MIN(open_time), '%Y-%m-%d %H:%M:%S') AS min_t,
            strftime(MAX(open_time), '%Y-%m-%d %H:%M:%S') AS max_t
        FROM {LIVE_SOURCE}
        WHERE open_time BETWEEN ? AND ?
        """,
        [range_start, range_end],
    ).fetchone()
    if row is None or row[0] == 0:
        raise ValueError("No rows in the requested range of live.quant_train")

    n, time_hash_sum, min_t, max_t = row
    # Build a deterministic fingerprint: row count + cumulative time hash + range bounds
    fingerprint = f"n={n}|sum_hash={time_hash_sum}|min={min_t}|max={max_t}"
    content_sha256 = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return content_sha256, int(n)


def compute_feature_set_hash(conn: duckdb.DuckDBPyConnection) -> str:
    """Compute sha256 over the ordered feature column list of the source table."""
    cols = _ordered_feature_columns(conn)
    joined = ",".join(cols)
    row = conn.execute("SELECT sha256(?)", [joined]).fetchone()
    assert row is not None
    return str(row[0])


# %% Snapshot creation


def _snapshot_table_fqn(snapshot_id: str) -> str:
    """Return the fully-qualified, quoted snap table name."""
    return f'{SNAP_SCHEMA}."{snapshot_id}"'


def _find_reusable_snapshot(
    conn           : duckdb.DuckDBPyConnection,
    asset_id       : str,
    content_sha256 : str,
) -> str | None:
    """Return an existing snapshot_id with matching content hash, if any.

    Reuse is keyed on (asset_id, content_sha256): identical content for the same
    asset means the immutable table already exists and must not be rewritten.
    """
    row = conn.execute(
        "SELECT snapshot_id FROM reg.snapshots"
        " WHERE asset_id = ? AND content_sha256 = ? LIMIT 1",
        [asset_id, content_sha256],
    ).fetchone()
    return str(row[0]) if row is not None else None


def create_snapshot(
    conn       : duckdb.DuckDBPyConnection,
    asset_id   : str,
    horizon    : int = 60,
    start_time : str | None = None,
    end_time   : str | None = None,
) -> SnapshotResult:
    """Freeze a range of ``live.quant_train`` into an immutable snap table + reg row.

    Steps (plan 5, step 1):
      1. resolve the effective inclusive range (defaults to full history),
      2. compute the content hash + row count and the feature-set hash,
      3. if an identical-content snapshot already exists for the asset, reuse it
         (no table write, no overwrite) — the snapshot is immutable,
      4. otherwise CTAS ``snap."<snapshot_id>"`` from the range and INSERT a
         ``reg.snapshots`` row.

    Args:
        conn       : Open lab connection (lab default + ``live`` RO + ``reg``),
                     i.e. utils.open_lab_connection(asset_id).
        asset_id   : Asset key (lower-case symbol), e.g. ``solusdt``.
        horizon    : Forward-window bar count for the id token (default 60 -> fw60).
        start_time : Optional inclusive lower bound, ``YYYY-MM-DD HH:MM:SS``.
        end_time   : Optional inclusive upper bound, ``YYYY-MM-DD HH:MM:SS``.

    Returns:
        SnapshotResult describing the (new or reused) snapshot.

    Raises:
        ValueError: If the requested range contains no rows.
    """
    ensure_snap_schema(conn)

    range_start, range_end = _resolve_range(conn, start_time, end_time)
    content_sha256, row_count = compute_content_sha256(conn, range_start, range_end)
    feature_set_hash = compute_feature_set_hash(conn)
    range_token = format_range(range_start, range_end)
    snapshot_id = build_snapshot_id(asset_id, horizon, range_token, content_sha256)

    existing = _find_reusable_snapshot(conn, asset_id, content_sha256)
    table_exists = conn.execute(
        "SELECT 1 FROM information_schema.tables"
        " WHERE table_schema = ? AND table_name = ?",
        [SNAP_SCHEMA, snapshot_id],
    ).fetchone() is not None

    if existing is not None and table_exists:
        logger.info(
            "create_snapshot: reuse %s (content hash matches, immutable)", existing
        )
        return SnapshotResult(
            snapshot_id      = existing,
            asset_id         = asset_id,
            range_start      = range_start,
            range_end        = range_end,
            row_count        = row_count,
            content_sha256   = content_sha256,
            feature_set_hash = feature_set_hash,
            reused           = True,
        )

    fqn = _snapshot_table_fqn(snapshot_id)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} AS
        SELECT *
        FROM {LIVE_SOURCE}
        WHERE open_time BETWEEN ? AND ?
        ORDER BY open_time
        """,
        [range_start, range_end],
    )

    registry.upsert(
        conn,
        "snapshots",
        {
            "snapshot_id":      snapshot_id,
            "asset_id":         asset_id,
            "range_start":      range_start,
            "range_end":        range_end,
            "row_count":        row_count,
            "content_sha256":   content_sha256,
            "feature_set_hash": feature_set_hash,
            "status":           "candidate",
        },
    )

    logger.info(
        "create_snapshot: created %s rows=%d range=[%s..%s]",
        snapshot_id, row_count, range_start, range_end,
    )
    return SnapshotResult(
        snapshot_id      = snapshot_id,
        asset_id         = asset_id,
        range_start      = range_start,
        range_end        = range_end,
        row_count        = row_count,
        content_sha256   = content_sha256,
        feature_set_hash = feature_set_hash,
        reused           = False,
    )
