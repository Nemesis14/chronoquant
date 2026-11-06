"""Tests for data_handling.store.readonly_query (module API used by the CLI, API and MCP)."""

from pathlib import Path

import duckdb
import pytest

import utils
from data_handling.store import readonly_query as rq

pytestmark = pytest.mark.smoke


@pytest.fixture
def dbs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    live, lab, reg = tmp_path / "live.duckdb", tmp_path / "lab.duckdb", tmp_path / "reg.duckdb"
    extra = tmp_path / "journal.duckdb"
    with duckdb.connect(str(live)) as c:
        c.execute("CREATE TABLE ohlcv AS SELECT i AS open_time FROM range(10) t(i)")
    with duckdb.connect(str(lab)) as c:
        c.execute("CREATE SCHEMA snap")
    with duckdb.connect(str(reg)) as c:
        c.execute("CREATE TABLE models AS SELECT 'm1' AS model_id")
    with duckdb.connect(str(extra)) as c:
        c.execute("CREATE TABLE trading_positions AS SELECT 1 AS position_id")
    monkeypatch.setattr(utils, "load_asset_config", lambda asset_id=None: {"database": {"db_path": str(live)}})
    monkeypatch.setattr(utils, "lab_db_path", lambda asset_id=None: str(lab))
    monkeypatch.setattr(utils, "registry_path", lambda: str(reg))
    return {"live": live, "lab": lab, "reg": reg, "jrn": extra}


def test_run_readonly_query(dbs: dict[str, Path]) -> None:
    result = rq.run_readonly_query("SELECT count(*) AS n FROM live.ohlcv")
    assert result["rows"] == [[10]]
    with pytest.raises(rq.QueryRejectedError):
        rq.run_readonly_query("DROP TABLE live.ohlcv")


def test_extra_targets_attached_read_only(dbs: dict[str, Path]) -> None:
    conn = rq.open_readonly_connection(extra_targets={"jrn": str(dbs["jrn"])}, tries=1, delay=0)
    try:
        assert conn.execute("SELECT position_id FROM jrn.trading_positions").fetchone() == (1,)
        with pytest.raises(duckdb.Error):
            conn.execute("INSERT INTO jrn.trading_positions VALUES (2)")
    finally:
        conn.close()


def test_describe_schema(dbs: dict[str, Path]) -> None:
    listing = rq.describe_schema(name_filter="models", catalog="reg")
    assert [t["table"] for t in listing["tables"]] == ["models"]
