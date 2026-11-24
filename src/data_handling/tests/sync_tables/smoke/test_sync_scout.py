"""Smoke tests for the scout_signals table + sync_scout (epic_074 / t941).

Synthetic ``scout.run`` rows (OUTPUT_SCHEMA) on temporary DuckDB files — no live DB.  Covers the v8 migration
(fresh / v6 / v7 databases), insert + backfilled flag, idempotency, immutable and set-once protection, the
open → resolved / invalid outcome transition, rows missing from a run, the sync_scout wiring (OHLCV window from
the detection anchor) and the schema-drift guard.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest

import data_handling.sync_tables.sync_scout as ss
from data_handling.store.duckdb_store import (
    LIVE_DB_MIGRATIONS,
    _m007_create_funding_rate,
    ensure_tables,
    get_connection,
    scout_signals_columns,
)
from data_handling.store.migrations import run_migrations
from scout import IMMUTABLE_COLUMNS, OUTPUT_COLUMNS, OUTPUT_SCHEMA

pytestmark = pytest.mark.smoke

ASSET = "btcusdt"
T_FIRST = datetime(2026, 11, 23, 12, 0)          # first live sync ("deploy")


def _row(t0: datetime, status: str = "open", **over: Any) -> dict[str, Any]:
    """One synthetic OUTPUT_SCHEMA row (event W4h_H8h: t_c = t0 + 2h, label_end = t_c + 8h)."""
    t_c = t0 + timedelta(hours=2)
    r: dict[str, Any] = {c: None for c in OUTPUT_COLUMNS}
    r.update({
        "event_id": f"{ASSET}|W4h_H8h|{t0:%Y%m%dT%H%M}", "asset_id": ASSET, "model_id": "scout_s0c_r1",
        "cell_id": "W4h_H8h", "t0": t0, "t_c": t_c, "label_end": t_c + timedelta(hours=8),
        "low_t0": 100.0, "close_tc": 101.0, "a_log": 0.01, "sigma_w": 0.004, "a_sigma": 2.5, "p_star": 102.0,
        "stop": 100.0, "velocity_1h": 0.1, "pretrend_3d": -0.02, "log_sigma_1m": -6.0, "drop_depth": 0.03,
        "volume_ratio": 1.2, "scorable": True, "p_impulse": 0.15, "p_false_start": 0.3, "p_sideways": 0.55,
        "oof_percentile": 60.0, "top20": False, "top10": False,
        "entry": 101.1, "default_tp": 102.0, "default_sl": 100.0, "r2_03_tp": 102.3, "r2_03_sl": 100.5,
        "status": status, "as_of": t_c + timedelta(hours=1),
    })
    if status in ("resolved", "invalid"):
        out = "tp" if status == "resolved" else "invalid"
        for k in ("default", "r2_03"):
            r[f"{k}_outcome"] = out
            if status == "resolved":
                r[f"{k}_outcome_at"] = t_c + timedelta(hours=3)
                r[f"{k}_exit_price"] = 102.0
                r[f"{k}_net_bps"] = 80.0
        r["label_class"] = "impulse" if status == "resolved" else None
        r["as_of"] = t_c + timedelta(hours=9)
    r.update(over)
    return r


def _df(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=OUTPUT_SCHEMA, orient="row") if rows else pl.DataFrame(schema=OUTPUT_SCHEMA)


@pytest.fixture
def conn(tmp_path: Path):
    c = get_connection(str(tmp_path / "live.duckdb"))
    ensure_tables(c)
    yield c
    c.close()


def _scalar(conn, sql: str) -> Any:
    row = conn.execute(sql).fetchone()
    assert row is not None
    return row[0]


def _stored(conn) -> dict[str, dict[str, Any]]:
    df = conn.execute("SELECT * FROM scout_signals").pl()
    return {r["event_id"]: r for r in df.iter_rows(named=True)}


# %% migration


def _table_cols(conn) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'scout_signals' "
        "ORDER BY ordinal_position").fetchall()]


def test_migration_fresh_db_schema_matches_runtime(conn):
    assert max(m.version for m in LIVE_DB_MIGRATIONS) == 8
    assert conn.execute("SELECT MAX(version) FROM _schema_migrations").fetchone()[0] == 8
    assert _table_cols(conn) == [*OUTPUT_COLUMNS, "scored_at", "backfilled"] == list(scout_signals_columns())
    ss.check_table_schema(conn)                              # no drift
    ensure_tables(conn)                                      # idempotent re-run
    assert conn.execute("SELECT COUNT(*) FROM _schema_migrations").fetchone()[0] == 8
    pk = conn.execute("SELECT constraint_column_names FROM duckdb_constraints() "
                      "WHERE table_name = 'scout_signals' AND constraint_type = 'PRIMARY KEY'").fetchone()
    assert pk is not None and list(pk[0]) == ["event_id"]


@pytest.mark.parametrize("upto", [6, 7])
def test_migration_upgrades_v6_and_v7_databases_without_touching_data(tmp_path: Path, upto: int):
    c = get_connection(str(tmp_path / f"v{upto}.duckdb"))
    try:
        if upto == 6:   # SOL/BCH shape: old ad-hoc 2-column funding_rate created outside the migrations
            c.execute("CREATE TABLE funding_rate (funding_ts TIMESTAMP, funding_rate DOUBLE)")
            c.execute("INSERT INTO funding_rate VALUES ('2026-11-01 00:00:00', 0.0001)")
        run_migrations(c, [m for m in LIVE_DB_MIGRATIONS if m.version <= upto])
        c.execute("INSERT INTO ohlcv (open_time, open, high, low, close, volume) "
                  "VALUES ('2026-11-01 00:00:00', 1, 2, 0.5, 1.5, 10)")
        assert _scalar(c, "SELECT MAX(version) FROM _schema_migrations") == upto

        ensure_tables(c)

        assert _scalar(c, "SELECT MAX(version) FROM _schema_migrations") == 8
        assert _scalar(c, "SELECT COUNT(*) FROM ohlcv") == 1
        fcols = [r[0] for r in c.execute("SELECT column_name FROM information_schema.columns "
                                         "WHERE table_name = 'funding_rate'").fetchall()]
        assert fcols == (["funding_ts", "funding_rate"] if upto == 6 else ["funding_ts", "funding_rate", "mark_price"])
        assert _scalar(c, "SELECT COUNT(*) FROM funding_rate") == (1 if upto == 6 else 0)
        assert _table_cols(c)[-2:] == ["scored_at", "backfilled"]
    finally:
        c.close()


def test_migration_v8_is_create_if_not_exists(conn):
    conn.execute("INSERT INTO scout_signals (event_id, asset_id) VALUES ('x', 'btcusdt')")
    LIVE_DB_MIGRATIONS[-1].apply(conn)                      # re-applying the DDL keeps the rows
    _m007_create_funding_rate(conn)
    assert conn.execute("SELECT COUNT(*) FROM scout_signals").fetchone()[0] == 1


# %% upsert rules


def test_insert_sets_scored_at_and_backfilled(conn):
    early = _row(datetime(2026, 11, 5, 3, 0), "resolved")
    res = ss.upsert_scout_signals(conn, _df([early]), ASSET, T_FIRST)
    assert (res.inserted, res.inserted_backfilled, res.total_rows, res.open_events) == (1, 1, 1, 0)
    assert res.first_live_sync_at == str(T_FIRST)

    later = _row(T_FIRST + timedelta(hours=1))                # t_c after the first live sync → forward
    now2 = T_FIRST + timedelta(hours=3, minutes=1)
    res2 = ss.upsert_scout_signals(conn, _df([early, later]), ASSET, now2)
    assert (res2.inserted, res2.inserted_backfilled, res2.open_events) == (1, 0, 1)
    assert res2.first_live_sync_at == str(T_FIRST)            # MIN(scored_at), not the new now

    st = _stored(conn)
    assert st[early["event_id"]]["backfilled"] is True and st[early["event_id"]]["scored_at"] == T_FIRST
    assert st[later["event_id"]]["backfilled"] is False and st[later["event_id"]]["scored_at"] == now2


def test_idempotent_rerun_writes_nothing(conn):
    rows = [_row(datetime(2026, 11, 5, 3, 0), "resolved"), _row(datetime(2026, 11, 22, 1, 0), "invalid"),
            _row(datetime(2026, 11, 23, 9, 0))]
    ss.upsert_scout_signals(conn, _df(rows), ASSET, T_FIRST)
    before = _stored(conn)
    res = ss.upsert_scout_signals(conn, _df(rows), ASSET, T_FIRST + timedelta(minutes=5))
    assert (res.inserted, res.resolved, res.filled, res.refreshed, res.immutable_mismatch) == (0, 0, 0, 0, 0)
    assert _stored(conn) == before
    assert res.total_rows == 3


def test_immutable_columns_never_change(conn):
    r = _row(datetime(2026, 11, 23, 9, 0))
    ss.upsert_scout_signals(conn, _df([r]), ASSET, T_FIRST)
    before = _stored(conn)[r["event_id"]]

    tampered = {**r, "p_impulse": 0.99, "top10": True, "low_t0": 90.0,
                "status": "resolved", "default_outcome": "tp", "as_of": r["as_of"] + timedelta(hours=9)}
    res = ss.upsert_scout_signals(conn, _df([tampered]), ASSET, T_FIRST + timedelta(hours=9))
    assert res.immutable_mismatch == 1 and res.resolved == 0  # whole row skipped
    assert _stored(conn)[r["event_id"]] == before
    assert all(_stored(conn)[r["event_id"]][c] == before[c] for c in IMMUTABLE_COLUMNS)


def test_set_once_fields_fill_but_never_overwrite(conn):
    r = _row(datetime(2026, 11, 23, 9, 0), entry=None, r2_03_tp=None, r2_03_sl=None)
    ss.upsert_scout_signals(conn, _df([r]), ASSET, T_FIRST)
    assert _stored(conn)[r["event_id"]]["entry"] is None

    filled = {**r, "entry": 101.1, "r2_03_tp": 102.3, "r2_03_sl": 100.5}
    res = ss.upsert_scout_signals(conn, _df([filled]), ASSET, T_FIRST + timedelta(minutes=1))
    assert res.filled == 1
    st = _stored(conn)[r["event_id"]]
    assert (st["entry"], st["r2_03_tp"], st["r2_03_sl"]) == (101.1, 102.3, 100.5)

    changed = {**filled, "entry": 150.0}
    res = ss.upsert_scout_signals(conn, _df([changed]), ASSET, T_FIRST + timedelta(minutes=2))
    assert res.immutable_mismatch == 1 and res.filled == 0
    assert _stored(conn)[r["event_id"]]["entry"] == 101.1


def test_outcome_written_once_on_open_to_resolved(conn):
    t0 = datetime(2026, 11, 23, 9, 0)
    ss.upsert_scout_signals(conn, _df([_row(t0)]), ASSET, T_FIRST)
    st = _stored(conn)[_row(t0)["event_id"]]
    assert st["status"] == "open" and st["default_outcome"] is None

    later_open = _row(t0, as_of=_row(t0)["as_of"] + timedelta(hours=1))
    res = ss.upsert_scout_signals(conn, _df([later_open]), ASSET, T_FIRST + timedelta(hours=1))
    assert res.refreshed == 1 and _stored(conn)[later_open["event_id"]]["as_of"] == later_open["as_of"]

    done = _row(t0, "resolved")
    res = ss.upsert_scout_signals(conn, _df([done]), ASSET, T_FIRST + timedelta(hours=9))
    assert res.resolved == 1 and res.open_events == 0
    st = _stored(conn)[done["event_id"]]
    assert {c: st[c] for c in ss.OUTCOME_COLUMNS} == {c: done[c] for c in ss.OUTCOME_COLUMNS}
    assert st["as_of"] == done["as_of"] and st["scored_at"] == T_FIRST   # scored_at stays the insert time

    other = _row(t0, "invalid")                                           # a later, different outcome is ignored
    res = ss.upsert_scout_signals(conn, _df([other]), ASSET, T_FIRST + timedelta(hours=20))
    assert res.resolved == 0 and res.immutable_mismatch == 1
    assert _stored(conn)[done["event_id"]]["status"] == "resolved"


def test_open_to_invalid_transition(conn):
    t0 = datetime(2026, 11, 23, 9, 0)
    ss.upsert_scout_signals(conn, _df([_row(t0)]), ASSET, T_FIRST)
    res = ss.upsert_scout_signals(conn, _df([_row(t0, "invalid")]), ASSET, T_FIRST + timedelta(hours=9))
    st = _stored(conn)[_row(t0)["event_id"]]
    assert res.resolved == 1 and st["status"] == "invalid" and st["default_outcome"] == "invalid"
    assert st["label_class"] is None


def test_missing_event_is_kept(conn):
    a, b = _row(datetime(2026, 11, 5, 3, 0), "resolved"), _row(datetime(2026, 11, 6, 3, 0), "resolved")
    ss.upsert_scout_signals(conn, _df([a, b]), ASSET, T_FIRST)
    res = ss.upsert_scout_signals(conn, _df([a]), ASSET, T_FIRST + timedelta(minutes=1))
    assert res.missing_in_run == 1 and res.total_rows == 2


def test_foreign_asset_rows_rejected(conn):
    with pytest.raises(ValueError, match="foreign asset"):
        ss.upsert_scout_signals(conn, _df([_row(datetime(2026, 11, 5), asset_id="ethusdt")]), ASSET, T_FIRST)
    assert conn.execute("SELECT COUNT(*) FROM scout_signals").fetchone()[0] == 0


# %% sync_scout wiring


@dataclass
class _Art:
    model_id: str = "scout_s0c_r1"
    detect_anchor: datetime = datetime(2026, 7, 24)
    live_start: datetime = datetime(2026, 11, 1)


def _install(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    db_path = str(tmp_path / "live.duckdb")
    c = get_connection(db_path)
    try:
        ensure_tables(c)
        c.execute("INSERT INTO ohlcv (open_time, open, high, low, close, volume) VALUES "
                  "('2026-07-23 23:59:00', 1, 1, 1, 1, 1), ('2026-07-24 00:00:00', 1, 1, 1, 1, 1), "
                  "('2026-11-23 10:00:00', 1, 1, 1, 1, 1)")
    finally:
        c.close()
    calls: dict[str, Any] = {"db_path": db_path}

    def _run(asset_id, ohlcv, art, *, since=None):
        calls.update(asset_id=asset_id, n_ohlcv=ohlcv.height, first=ohlcv["open_time"].min(), since=since)
        return _df(rows)

    monkeypatch.setattr(ss, "scout_run", _run)
    monkeypatch.setattr(ss.utils, "load_asset_config",
                        lambda _a=None: {"database": {"db_path": db_path, "asset_id": ASSET}})
    return calls


def test_sync_scout_end_to_end(monkeypatch, tmp_path):
    rows = [_row(datetime(2026, 11, 5, 3, 0), "resolved"), _row(datetime(2026, 11, 23, 9, 0))]
    calls = _install(monkeypatch, tmp_path, rows)

    res = ss.sync_scout(ASSET, art=_Art(), now=T_FIRST)      # type: ignore[arg-type]
    assert calls["asset_id"] == ASSET and calls["n_ohlcv"] == 2 and calls["first"] == datetime(2026, 7, 24)
    assert (res.inserted, res.inserted_backfilled, res.open_events, res.model_id) == (2, 2, 1, "scout_s0c_r1")
    assert set(res.to_dict()) >= {"asset_id", "inserted", "resolved", "open_events", "as_of"}

    res2 = ss.sync_scout(ASSET, art=_Art(), now=T_FIRST + timedelta(minutes=1))  # type: ignore[arg-type]
    assert (res2.inserted, res2.resolved, res2.total_rows) == (0, 0, 2)


def test_sync_scout_refuses_schema_drift(monkeypatch, tmp_path):
    calls = _install(monkeypatch, tmp_path, [])
    c = get_connection(calls["db_path"])
    try:
        c.execute("ALTER TABLE scout_signals DROP COLUMN top10")
    finally:
        c.close()
    with pytest.raises(RuntimeError, match="sema elter"):
        ss.sync_scout(ASSET, art=_Art(), now=T_FIRST)        # type: ignore[arg-type]
