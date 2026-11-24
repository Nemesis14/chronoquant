"""Impulse Scout forward-log: write ``scout.run`` output into the live ``scout_signals`` table.

Pipeline position (epic_074 / t941)
-----------------------------------
sync_ohlcv  →  sync_scout   (worker job: t942; manual: ``02_sync_pipeline --tables scout``)

The runtime (``src/scout/``, t940) is deterministic and DB-free: every call re-detects all events since
``art.detect_anchor`` and returns one row per event (``t0 >= since``).  This module is the only writer of
``scout_signals`` and enforces the forward-log contract:

- **new event** → inserted with ``scored_at = now`` and ``backfilled = t_c < first_live_sync_at``;
- **immutable columns** (``scout.IMMUTABLE_COLUMNS``: identity, geometry, features, prediction) are never
  updated; a recomputed value that differs is logged and counted (``immutable_mismatch``), the stored value wins;
- **set-once columns** (``entry``, ``{rule}_tp``, ``{rule}_sl``) are written only while NULL;
- **outcome columns** (``{rule}_outcome / _outcome_at / _exit_price / _net_bps``, ``status``, ``label_class``)
  are written once, when the stored ``status`` is ``open`` and the run reports ``resolved`` / ``invalid``;
- ``as_of`` is refreshed while the stored row is still ``open`` (frozen at resolution);
- rows are never deleted (an event missing from a run is only logged).

``first_live_sync_at`` = ``MIN(scored_at)`` of the table, i.e. the time of the first successful write for the
asset (the first run itself uses its own ``now``).  Every event already closed (``t_c``) before that moment is a
backfill; later events are forward predictions.  ``scored_at - t_c`` gives the write lag of each row.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb
import polars as pl

import utils
from data_handling.store.duckdb_store import ensure_tables, get_connection, scout_signals_columns
from scout import (
    IMMUTABLE_COLUMNS,
    OUTPUT_COLUMNS,
    OUTPUT_SCHEMA,
    RULE_KEYS,
    ScoutArtifact,
    load_artifact,
)
from scout import run as scout_run

logger = logging.getLogger(__name__)

TABLE = "scout_signals"

#: plan columns fixed once known (entry needs the bar opening at t_c)
SET_ONCE_COLUMNS: tuple[str, ...] = ("entry", *(f"{k}_{c}" for k in RULE_KEYS for c in ("tp", "sl")))
#: written once at the open → resolved / invalid transition
OUTCOME_COLUMNS: tuple[str, ...] = (
    *(f"{k}_{c}" for k in RULE_KEYS for c in ("outcome", "outcome_at", "exit_price", "net_bps")),
    "status", "label_class",
)
_FINAL_STATUSES = ("resolved", "invalid")

#: row schema of the table (OUTPUT_SCHEMA + writer columns)
_ROW_SCHEMA: dict[str, Any] = {**OUTPUT_SCHEMA, "scored_at": pl.Datetime("us"), "backfilled": pl.Boolean}

_REL_TOL = 1e-9
_ABS_TOL = 1e-12


# %% Result


@dataclass
class ScoutSyncResult:
    """Counters of one sync_scout / upsert_scout_signals call (JSON-friendly via :meth:`to_dict`)."""

    asset_id: str
    model_id: str = ""
    events: int = 0                 # rows returned by scout.run
    inserted: int = 0               # new events written
    inserted_backfilled: int = 0    # ... of which backfilled = true
    resolved: int = 0               # open → resolved / invalid transitions written
    filled: int = 0                 # rows whose set-once plan fields were filled
    refreshed: int = 0              # open rows whose as_of was refreshed
    immutable_mismatch: int = 0     # rows where a recomputed immutable / set-once value differed (kept stored)
    missing_in_run: int = 0         # stored events absent from this run (kept, logged)
    open_events: int = 0            # rows with status = 'open' after the write
    total_rows: int = 0             # rows in scout_signals after the write
    as_of: str | None = None        # close time of the last input bar
    first_live_sync_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# %% Helpers


def _same(a: Any, b: Any) -> bool:
    """Null-safe equality with a float tolerance (the runtime is deterministic; this only absorbs fp noise)."""
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return False
        if math.isnan(fa) or math.isnan(fb):
            return math.isnan(fa) and math.isnan(fb)
        return math.isclose(fa, fb, rel_tol=_REL_TOL, abs_tol=_ABS_TOL)
    return a == b


def _connect_with_retry(db_path: str, attempts: int, wait_s: float) -> duckdb.DuckDBPyConnection:
    """Open the live DB read-write, waiting out a transient lock (worker / other writer)."""
    last: Exception | None = None
    for i in range(max(attempts, 1)):
        try:
            return get_connection(db_path)
        except duckdb.IOException as exc:
            last = exc
            if i + 1 < attempts:
                logger.info("sync_scout: DB zarolt, ujraprobalas %d/%d (%s)", i + 1, attempts, db_path)
                time.sleep(wait_s)
    raise RuntimeError(f"A live DuckDB tartosan zarolt: {db_path}") from last


def check_table_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Raise if scout_signals' columns differ from ``scout_signals_columns()`` (runtime schema drift).

    A new artifact version with a different OUTPUT_SCHEMA needs a new migration, never a silent write.
    """
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
        [TABLE],
    ).fetchall()
    have = [r[0] for r in rows]
    want = list(scout_signals_columns())
    if have != want:
        missing = [c for c in want if c not in have]
        extra = [c for c in have if c not in want]
        raise RuntimeError(f"{TABLE} sema elter a scout.OUTPUT_SCHEMA-tol (uj migracio kell): "
                           f"hianyzik={missing} tobblet={extra}")


def load_ohlcv(conn: duckdb.DuckDBPyConnection, start: datetime) -> pl.DataFrame:
    """1m OHLCV rows with ``open_time >= start`` (closed bars only, as stored by sync_ohlcv)."""
    return conn.execute(
        "SELECT open_time, open, high, low, close, volume FROM ohlcv WHERE open_time >= ? ORDER BY open_time",
        [start],
    ).pl()


def _first_live_sync_at(conn: duckdb.DuckDBPyConnection, asset_id: str) -> datetime | None:
    row = conn.execute(f"SELECT MIN(scored_at) FROM {TABLE} WHERE asset_id = ?", [asset_id]).fetchone()
    return row[0] if row and row[0] is not None else None


# %% Upsert core


def upsert_scout_signals(
    conn     : duckdb.DuckDBPyConnection,
    df       : pl.DataFrame,
    asset_id : str,
    now      : datetime,
) -> ScoutSyncResult:
    """Apply the forward-log rules (module docstring) for one ``scout.run`` output, in one transaction.

    Args:
        conn     : Open read-write connection (``ensure_tables`` already run).
        df       : ``scout.run`` output (``OUTPUT_SCHEMA`` columns) for ``asset_id``.
        asset_id : Asset whose rows are compared (``missing_in_run`` / ``first_live_sync_at`` scope).
        now      : Write time (naive UTC) → ``scored_at`` of new rows.

    Returns:
        Counters of the call.
    """
    res = ScoutSyncResult(asset_id=asset_id)
    if not df.is_empty():
        other = df.filter(pl.col("asset_id") != asset_id)
        if not other.is_empty():
            raise ValueError(f"upsert_scout_signals: foreign asset rows in batch ({other['asset_id'].unique().to_list()})")
        res.model_id = str(df["model_id"][0])
        as_of = df["as_of"].max()
        res.as_of = str(as_of) if as_of is not None else None
    res.events = df.height
    new_rows = {r["event_id"]: r for r in df.select(list(OUTPUT_COLUMNS)).iter_rows(named=True)}

    conn.execute("BEGIN TRANSACTION")
    try:
        first_live = _first_live_sync_at(conn, asset_id) or now
        res.first_live_sync_at = str(first_live)
        stored_df = conn.execute(f"SELECT * FROM {TABLE} WHERE asset_id = ?", [asset_id]).pl()
        stored = {r["event_id"]: r for r in stored_df.iter_rows(named=True)}

        inserts: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        for eid, new in new_rows.items():
            old = stored.get(eid)
            if old is None:
                inserts.append({**new, "scored_at": now, "backfilled": bool(new["t_c"] < first_live)})
                continue

            diff = [c for c in IMMUTABLE_COLUMNS if not _same(old[c], new[c])]
            if diff:
                res.immutable_mismatch += 1
                logger.warning("sync_scout: %s immutable elteres (tarolt marad, sor kihagyva): %s", eid,
                               {c: (old[c], new[c]) for c in diff})
                continue

            merged = {c: old[c] for c in OUTPUT_COLUMNS}
            changed = False
            fill = [c for c in SET_ONCE_COLUMNS if old[c] is None and new[c] is not None]
            for c in fill:
                merged[c] = new[c]
            if fill:
                res.filled += 1
                changed = True
            conflict = [c for c in SET_ONCE_COLUMNS
                        if old[c] is not None and not _same(old[c], new[c])]
            if conflict:
                res.immutable_mismatch += 1
                logger.warning("sync_scout: %s set-once elteres (tarolt marad): %s", eid,
                               {c: (old[c], new[c]) for c in conflict})

            if old["status"] == "open":
                if new["status"] in _FINAL_STATUSES:
                    for c in OUTCOME_COLUMNS:
                        merged[c] = new[c]
                    res.resolved += 1
                merged["as_of"] = new["as_of"]
                if not _same(old["as_of"], new["as_of"]) and new["status"] == "open":
                    res.refreshed += 1
                changed = changed or not all(_same(old[c], merged[c]) for c in ("as_of", *OUTCOME_COLUMNS))
            else:
                diff_out = [c for c in OUTCOME_COLUMNS if not _same(old[c], new[c])]
                if diff_out:
                    res.immutable_mismatch += 1
                    logger.warning("sync_scout: %s lezart kimenet elter (tarolt marad): %s", eid,
                                   {c: (old[c], new[c]) for c in diff_out})
            if changed:
                updates.append(merged)

        missing = sorted(set(stored) - set(new_rows))
        res.missing_in_run = len(missing)
        if missing:
            logger.warning("sync_scout: %d tarolt esemeny hianyzik a futasbol (megtartva): %s",
                           len(missing), missing[:10])

        cols = list(scout_signals_columns())
        if inserts:
            ins = pl.DataFrame(inserts, schema=_ROW_SCHEMA, orient="row").select(cols)
            col_list = ", ".join(f'"{c}"' for c in cols)
            conn.register("_scout_ins", ins)
            try:
                conn.execute(f"INSERT INTO {TABLE} ({col_list}) SELECT {col_list} FROM _scout_ins ORDER BY t0")
            finally:
                conn.unregister("_scout_ins")
            res.inserted = len(inserts)
            res.inserted_backfilled = sum(1 for r in inserts if r["backfilled"])

        if updates:
            mutable = [c for c in OUTPUT_COLUMNS if c not in IMMUTABLE_COLUMNS]
            upd = pl.DataFrame(updates, schema=OUTPUT_SCHEMA, orient="row").select(["event_id", *mutable])
            set_sql = ", ".join(f'"{c}" = u."{c}"' for c in mutable)
            conn.register("_scout_upd", upd)
            try:
                conn.execute(f"UPDATE {TABLE} AS t SET {set_sql} FROM _scout_upd AS u WHERE t.event_id = u.event_id")
            finally:
                conn.unregister("_scout_upd")

        row = conn.execute(
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'open') FROM {TABLE} WHERE asset_id = ?", [asset_id]
        ).fetchone()
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    res.total_rows, res.open_events = (int(row[0]), int(row[1])) if row else (0, 0)
    return res


# %% Public API


def sync_scout(
    asset_id  : str | None = None,
    *,
    since     : datetime | None = None,
    art       : ScoutArtifact | None = None,
    now       : datetime | None = None,
    attempts  : int = 24,
    wait_s    : float = 5.0,
) -> ScoutSyncResult:
    """Detect / score / resolve Scout events for one asset and upsert them into ``scout_signals``.

    Reads the asset's 1m OHLCV from ``art.detect_anchor``, calls ``scout.run(asset_id, ohlcv, art, since=since)``
    and applies :func:`upsert_scout_signals`.  Idempotent: a re-run with the same data writes nothing new
    (only ``as_of`` of open events moves forward).  One connection, one write transaction (short).

    Args:
        asset_id : Asset key from config/assets.json; uses default if None.
        since    : Earliest ``t0`` (default and minimum: ``art.live_start``, 2026-11-01).
        art      : Pre-loaded artifact (worker: load once per process); ``load_artifact()`` if None.
        now      : Write time override (naive or aware UTC; tests).
        attempts : Connect attempts while the DB is locked by another process.
        wait_s   : Seconds between connect attempts.

    Returns:
        :class:`ScoutSyncResult` counters (``to_dict()`` for job payloads).
    """
    db_cfg   = utils.load_asset_config(asset_id)["database"]
    db_path  = db_cfg["db_path"]
    resolved = db_cfg.get("asset_id") or utils.resolve_asset_id(asset_id)
    art      = art or load_artifact()
    now_utc  = now or datetime.now(UTC)
    if now_utc.tzinfo is not None:
        now_utc = now_utc.astimezone(UTC).replace(tzinfo=None)

    t_start = time.monotonic()
    conn = _connect_with_retry(db_path, attempts, wait_s)
    try:
        ensure_tables(conn)
        check_table_schema(conn)
        ohlcv = load_ohlcv(conn, art.detect_anchor)
        df = scout_run(resolved, ohlcv, art, since=since)
        res = upsert_scout_signals(conn, df, resolved, now_utc)
    finally:
        conn.close()

    res.model_id = res.model_id or art.model_id
    logger.info(
        "sync_scout kesz: asset=%s events=%d inserted=%d (backfilled=%d) resolved=%d filled=%d open=%d "
        "mismatch=%d missing=%d as_of=%s elapsed=%.1fs",
        resolved, res.events, res.inserted, res.inserted_backfilled, res.resolved, res.filled, res.open_events,
        res.immutable_mismatch, res.missing_in_run, res.as_of, time.monotonic() - t_start,
    )
    return res
