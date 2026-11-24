"""Smoke tests for the feat_levels sync and the challenger quant_train (epic_071 / t912).

Pins: the v6 migration creates feat_levels, the sync is an idempotent upsert
(full == incremental == re-run), a challenger profile builds its own
``quant_train__<profile>`` with the champion rows + feat_levels columns while
the champion ``quant_train`` stays byte-identical, and a snapshot can be taken
from the challenger table.
"""

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import utils
from data_handling.store import registry, snapshots
from data_handling.store.duckdb_store import (
    _feat_levels_v1_columns,
    ensure_tables,
    get_connection,
    insert_ohlcv,
    rebuild_quant_train,
)
from data_handling.sync_tables import sync_levels as sl

pytestmark = pytest.mark.smoke

T0 = datetime(2024, 1, 1)


def _ohlcv(days: int, seed: int = 5) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    n = days * 1440
    x = np.cumsum(rng.normal(0, 0.0012, n)) * 0.3
    close = 100.0 * np.exp(x + 0.1 * np.sin(np.arange(n) / 3000.0))
    open_ = np.concatenate(([close[0]], close[:-1]))
    wig = np.abs(rng.normal(0, 0.0006, n)) * close
    return pl.DataFrame({
        "open_time": pl.datetime_range(T0, T0 + timedelta(minutes=n - 1), "1m", eager=True),
        "open": open_, "high": np.maximum(open_, close) + wig,
        "low": np.minimum(open_, close) - wig, "close": close, "volume": np.ones(n),
        "quote_volume": np.ones(n), "trades": np.ones(n, dtype=np.int64),
        "taker_buy_base": np.ones(n), "taker_buy_quote": np.ones(n),
    })


@pytest.fixture()
def live_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    db_path = str(tmp_path / "live.duckdb")
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        insert_ohlcv(conn, _ohlcv(30))
    finally:
        conn.close()
    monkeypatch.setattr(sl.utils, "load_asset_config",
                        lambda asset_id=None: {"database": {"db_path": db_path}})
    return db_path


def _table(db_path: str, table: str) -> pl.DataFrame:
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute(f'SELECT * FROM "{table}" ORDER BY open_time').pl()
    finally:
        conn.close()


def test_migration_creates_feat_levels(live_db: str) -> None:
    conn = duckdb.connect(live_db, read_only=True)
    try:
        cols = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns"
            " WHERE table_name = 'feat_levels' ORDER BY ordinal_position").fetchall()
        pk = conn.execute(
            "SELECT constraint_type FROM information_schema.table_constraints"
            " WHERE table_name = 'feat_levels'").fetchall()
    finally:
        conn.close()
    assert [c[0] for c in cols] == ["open_time", "available_ts", *_feat_levels_v1_columns()]
    assert all(c[1] == "DOUBLE" for c in cols[2:])
    assert ("PRIMARY KEY",) in pk


def test_sync_is_idempotent_upsert(live_db: str) -> None:
    n_ohlcv = 30 * 1440
    # incremental in two steps == one full run == re-run
    assert sl.sync_levels(end_time="2024-01-20 00:00:00") == 19 * 1440 + 1
    assert sl.sync_levels() == n_ohlcv - (19 * 1440 + 1)
    inc = _table(live_db, "feat_levels")
    assert sl.sync_levels() == 0                          # nothing new
    assert sl.sync_levels(full=True) == n_ohlcv           # rewrite in place
    full = _table(live_db, "feat_levels")
    assert full.height == n_ohlcv
    assert_frame_equal(inc, full, check_exact=False, rel_tol=1e-12, abs_tol=1e-12)
    assert sl.sync_levels(start_time="2024-01-25 00:00:00") == 6 * 1440
    assert_frame_equal(_table(live_db, "feat_levels"), full,
                       check_exact=False, rel_tol=1e-12, abs_tol=1e-12)
    assert full["feat_sr_dist_sup_atr_s"].drop_nulls().len() > 0


def _seed_feat_and_target(db_path: str) -> None:
    conn = get_connection(db_path)
    try:
        conn.execute("""
            CREATE TABLE feat_ohlcv_quant AS
            SELECT open_time, open_time AS available_ts, close AS feat_close_copy
            FROM ohlcv
        """)
        conn.execute("""
            INSERT INTO target (open_time, close, long_mfe_fw60, short_mfe_fw60)
            SELECT open_time, close,
                   CASE WHEN minute(open_time) = 7 THEN NULL ELSE 0.01 END, -0.01
            FROM ohlcv
        """)
    finally:
        conn.close()


def test_challenger_quant_train_keeps_champion(live_db: str) -> None:
    _seed_feat_and_target(live_db)
    sl.sync_levels(end_time="2024-01-29 23:59:00")       # last day has no feat_levels rows
    conn = get_connection(live_db)
    try:
        n_champ = rebuild_quant_train(conn)
        champ_before = conn.execute("SELECT * FROM quant_train ORDER BY open_time").pl()
        n_chal = rebuild_quant_train(conn, extra_feature_tables=["feat_levels"],
                                     table_name="quant_train__solusdt_fw60_sr")
        champ_after = conn.execute("SELECT * FROM quant_train ORDER BY open_time").pl()
        chal = conn.execute(
            'SELECT * FROM "quant_train__solusdt_fw60_sr" ORDER BY open_time').pl()
        with pytest.raises(ValueError):
            rebuild_quant_train(conn, extra_feature_tables=["feat_levels"])
    finally:
        conn.close()
    assert n_champ == n_chal and n_champ == 30 * 1440 - 30 * 24
    assert champ_before.equals(champ_after)
    assert chal["open_time"].equals(champ_after["open_time"])
    assert set(chal.columns) == set(champ_after.columns) | set(_feat_levels_v1_columns())
    assert "available_ts" not in chal.columns
    assert_frame_equal(chal.select(champ_after.columns), champ_after)
    last_day = chal.filter(pl.col("open_time") >= datetime(2024, 1, 30))
    assert last_day["feat_sr_n_active_s"].null_count() == last_day.height   # LEFT JOIN


def test_snapshot_from_challenger_table(live_db: str, tmp_path: Path) -> None:
    _seed_feat_and_target(live_db)
    sl.sync_levels()
    conn = get_connection(live_db)
    try:
        rebuild_quant_train(conn)
        rebuild_quant_train(conn, extra_feature_tables=["feat_levels"],
                            table_name=utils.quant_train_table("solusdt_fw60_sr"))
    finally:
        conn.close()
    reg_path = str(tmp_path / "registry.duckdb")
    registry.ensure_registry(reg_path)
    lab = registry.open_lab_connection(lab_path=str(tmp_path / "lab.duckdb"),
                                       live_path=live_db, registry_path=reg_path)
    try:
        champ = snapshots.create_snapshot(lab, asset_id="solusdt", horizon=60)
        chal = snapshots.create_snapshot(lab, asset_id="solusdt", horizon=60,
                                         source_table="quant_train__solusdt_fw60_sr")
        cols = [d[0] for d in lab.execute(
            f'SELECT * FROM snap."{chal.snapshot_id}" LIMIT 0').description]
    finally:
        lab.close()
    assert champ.snapshot_id != chal.snapshot_id
    assert champ.row_count == chal.row_count
    assert set(_feat_levels_v1_columns()) <= set(cols)


def test_profile_helpers() -> None:
    assert utils.quant_train_table("solusdt_fw60") == "quant_train"
    assert utils.quant_train_table(None) == "quant_train"
    assert utils.quant_train_table("solusdt_fw60_sr") == "quant_train__solusdt_fw60_sr"
    assert utils.feature_profile_extra_tables("solusdt_fw60_sr") == ["feat_levels"]
    cfg = utils._load_json("features.json")
    chal = utils.apply_feature_profile(cfg, "solusdt_fw60_sr")["database"]["features"]
    base = utils.apply_feature_profile(cfg, "solusdt_fw60")["database"]["features"]
    assert chal["indicators"] == base["indicators"]       # same feat_ohlcv_quant
    assert chal["extra_feature_tables"] == ["feat_levels"]
    assert chal["profile_id"] == "solusdt_fw60_sr"
