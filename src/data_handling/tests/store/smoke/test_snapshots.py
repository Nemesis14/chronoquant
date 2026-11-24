"""Smoke tests for the immutable snapshot layer (snap schema + reg.snapshots)."""

from pathlib import Path

import duckdb
import pytest

from data_handling.store import registry, snapshots

pytestmark = pytest.mark.smoke


def _seed_live(live_path: str, n: int = 120) -> None:
    """Create a minimal live.quant_train with open_time + feat_* + target columns."""
    conn = duckdb.connect(live_path)
    try:
        conn.execute("""
            CREATE TABLE quant_train (
                open_time      TIMESTAMP,
                feat_a         DOUBLE,
                feat_b         DOUBLE,
                long_mfe_fw60  DOUBLE,
                short_mfe_fw60 DOUBLE
            )
        """)
        conn.execute(f"""
            INSERT INTO quant_train
            SELECT TIMESTAMP '2023-01-01 00:00:00' + INTERVAL (i) MINUTE,
                   i * 1.0, i * 2.0, 0.1, -0.1
            FROM range({n}) t(i)
        """)
    finally:
        conn.close()


def _open_lab(tmp_path: Path) -> tuple[duckdb.DuckDBPyConnection, str, str]:
    live_path = str(tmp_path / "live.duckdb")
    reg_path  = str(tmp_path / "registry.duckdb")
    lab_path  = str(tmp_path / "lab.duckdb")
    _seed_live(live_path)
    registry.ensure_registry(reg_path)
    conn = registry.open_lab_connection(
        lab_path=lab_path, live_path=live_path, registry_path=reg_path
    )
    return conn, live_path, reg_path


def test_create_snapshot_freezes_table_and_registers(tmp_path: Path) -> None:
    conn, _, _ = _open_lab(tmp_path)
    try:
        result = snapshots.create_snapshot(conn, asset_id="solusdt", horizon=60)

        # naming: {asset}_fw{h}_{range}__{hash8}
        assert result.snapshot_id.startswith("solusdt_fw60_2023__")
        assert result.snapshot_id.endswith(result.content_sha256[:8])
        assert result.row_count == 120
        assert not result.reused

        # snap table exists with the frozen row count
        n = conn.execute(
            f'SELECT COUNT(*) FROM snap."{result.snapshot_id}"'
        ).fetchone()
        assert n is not None and n[0] == 120

        # reg.snapshots row created with hashes + range
        row = registry.get(conn, "snapshots", result.snapshot_id)
        assert row is not None
        assert row["asset_id"] == "solusdt"
        assert row["row_count"] == 120
        assert row["content_sha256"] == result.content_sha256
        assert row["feature_set_hash"] == result.feature_set_hash
    finally:
        conn.close()


def test_create_snapshot_is_immutable_on_rerun(tmp_path: Path) -> None:
    conn, _, _ = _open_lab(tmp_path)
    try:
        first = snapshots.create_snapshot(conn, asset_id="solusdt", horizon=60)

        # tamper with the frozen table to detect any overwrite
        conn.execute(f'DELETE FROM snap."{first.snapshot_id}"')

        second = snapshots.create_snapshot(conn, asset_id="solusdt", horizon=60)
        assert second.reused
        assert second.snapshot_id == first.snapshot_id

        # re-run did NOT rewrite the (now-emptied) immutable table
        n = conn.execute(
            f'SELECT COUNT(*) FROM snap."{first.snapshot_id}"'
        ).fetchone()
        assert n is not None and n[0] == 0

        # exactly one reg.snapshots row for this asset
        n_rows = conn.execute("SELECT COUNT(*) FROM reg.snapshots").fetchone()
        assert n_rows is not None and n_rows[0] == 1
    finally:
        conn.close()


def test_feature_set_hash_uses_feature_columns_only(tmp_path: Path) -> None:
    conn, _, _ = _open_lab(tmp_path)
    try:
        cols = snapshots._ordered_feature_columns(conn)
        assert cols == ["feat_a", "feat_b"]   # open_time + targets excluded, sorted
        h = snapshots.compute_feature_set_hash(conn)
        assert isinstance(h, str) and len(h) == 64
    finally:
        conn.close()


def test_values_sha256_registered_and_value_sensitive(tmp_path: Path) -> None:
    """values_sha256 is stored, deterministic, and changes with values (t949)."""
    conn, _, _ = _open_lab(tmp_path)
    try:
        result = snapshots.create_snapshot(conn, asset_id="solusdt", horizon=60)
        assert result.values_sha256 is not None and len(result.values_sha256) == 64
        row = registry.get(conn, "snapshots", result.snapshot_id)
        assert row is not None and row["values_sha256"] == result.values_sha256

        fqn = f'snap."{result.snapshot_id}"'
        assert snapshots.compute_values_sha256(conn, fqn) == result.values_sha256

        # Same grid / row count / columns, different values -> content_sha256
        # would be identical, values_sha256 must differ.
        conn.execute(f"CREATE TABLE other AS SELECT * FROM {fqn}")
        conn.execute("UPDATE other SET feat_a = feat_a + 1")
        assert snapshots.compute_values_sha256(conn, "other") != result.values_sha256

        # Swapping values between rows is detected (value bound to open_time).
        conn.execute(f"CREATE TABLE swapped AS SELECT * FROM {fqn}")
        conn.execute("UPDATE swapped SET feat_a = 119 - feat_a")
        assert snapshots.compute_values_sha256(conn, "swapped") != result.values_sha256
    finally:
        conn.close()


def test_values_sha256_backfill_and_lazy_fill_on_reuse(tmp_path: Path) -> None:
    """Pre-existing rows without values_sha256 get it via backfill / reuse; ids unchanged."""
    conn, _, _ = _open_lab(tmp_path)
    try:
        first = snapshots.create_snapshot(conn, asset_id="solusdt", horizon=60)
        conn.execute("UPDATE reg.snapshots SET values_sha256 = NULL")

        filled = snapshots.backfill_values_sha256(conn, "solusdt")
        assert filled == {first.snapshot_id: first.values_sha256}

        conn.execute("UPDATE reg.snapshots SET values_sha256 = NULL")
        again = snapshots.create_snapshot(conn, asset_id="solusdt", horizon=60)
        assert again.reused
        assert again.snapshot_id == first.snapshot_id
        assert again.content_sha256 == first.content_sha256
        assert again.values_sha256 == first.values_sha256
        row = registry.get(conn, "snapshots", first.snapshot_id)
        assert row is not None and row["values_sha256"] == first.values_sha256
    finally:
        conn.close()


def test_format_range_single_year_vs_span() -> None:
    assert snapshots.format_range("2023-01-01 00:00:00", "2023-12-31 23:59:00") == "2023"
    assert (
        snapshots.format_range("2021-01-01 00:00:00", "2026-05-31 23:59:00")
        == "2101_2605"
    )
