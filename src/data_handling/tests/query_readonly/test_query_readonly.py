"""Tests for src/data_handling/07_query_readonly.py (read-only SQL helper)."""

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import duckdb
import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "07_query_readonly.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("query_readonly", _SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


qr = _load()


@pytest.fixture
def dbs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    """Temp live/lab/reg databases wired into the helper via utils patches."""
    live, lab, reg = tmp_path / "live.duckdb", tmp_path / "lab.duckdb", tmp_path / "reg.duckdb"
    with duckdb.connect(str(live)) as c:
        c.execute("CREATE TABLE ohlcv AS SELECT i AS open_time, i * 1.5 AS close FROM range(500) t(i)")
    with duckdb.connect(str(lab)) as c:
        c.execute("CREATE SCHEMA snap")
        c.execute("CREATE TABLE snap.s1 AS SELECT 1 AS a, 'x' AS b")
    with duckdb.connect(str(reg)) as c:
        c.execute("CREATE TABLE models AS SELECT 'm1' AS model_id")
    monkeypatch.setattr(qr.utils, "load_asset_config", lambda asset_id=None: {"database": {"db_path": str(live)}})
    monkeypatch.setattr(qr.utils, "lab_db_path", lambda asset_id=None: str(lab))
    monkeypatch.setattr(qr.utils, "registry_path", lambda: str(reg))
    return {"live": live, "lab": lab, "reg": reg}


@pytest.mark.parametrize("sql", [
    "SELECT 1", "with a as (select 1 x) select * from a", "DESCRIBE live.ohlcv", "SHOW TABLES",
    "from range(3)", "VALUES (1)", "  -- c\n SELECT 1;", "(select 1)", "SUMMARIZE live.ohlcv",
])
def test_validate_accepts(sql: str) -> None:
    assert qr.validate_statement(sql)


@pytest.mark.parametrize("sql", [
    "", "   ;", "SELECT 1; SELECT 2", "SELECT 1; DROP TABLE x",
    "INSERT INTO t VALUES (1)", "UPDATE t SET a=1", "DELETE FROM t", "DROP TABLE t",
    "CREATE TABLE t AS SELECT 1", "COPY (SELECT 1) TO 'x.csv'", "ATTACH 'x.duckdb' AS y",
    "INSTALL httpfs", "LOAD httpfs", "PRAGMA version", "SET threads=1", "EXPORT DATABASE 'd'",
    "CALL pragma_version()", "EXPLAIN SELECT 1", "ALTER TABLE t ADD COLUMN z INT", "SELEC 1",
])
def test_validate_rejects(sql: str) -> None:
    with pytest.raises(qr.QueryRejectedError):
        qr.validate_statement(sql)


def test_limit_and_truncation(dbs: dict[str, Path]) -> None:
    conn = qr.open_readonly_connection()
    try:
        res = qr.run_query(conn, "SELECT * FROM live.ohlcv ORDER BY open_time", limit=10)
        assert res["row_count"] == 10 and res["truncated"] is True and res["columns"] == ["open_time", "close"]
        full = qr.run_query(conn, "SELECT * FROM live.ohlcv", limit=1000)
        assert full["row_count"] == 500 and full["truncated"] is False
    finally:
        conn.close()


def test_aliases_resolve(dbs: dict[str, Path]) -> None:
    conn = qr.open_readonly_connection()
    try:
        assert qr.run_query(conn, "SELECT a FROM snap.s1")["rows"] == [[1]]
        assert qr.run_query(conn, "SELECT model_id FROM reg.models")["rows"] == [["m1"]]
        assert qr.run_query(conn, "DESCRIBE live.ohlcv")["row_count"] == 2
    finally:
        conn.close()


def test_connection_is_read_only(dbs: dict[str, Path]) -> None:
    conn = qr.open_readonly_connection()
    try:
        for stmt in ("CREATE TABLE live.x(a INT)", "INSERT INTO reg.models VALUES ('z')", "DROP TABLE live.ohlcv"):
            with pytest.raises(duckdb.Error):
                conn.execute(stmt)
        with pytest.raises(duckdb.Error):
            conn.execute("SET enable_external_access=true")
        with pytest.raises(duckdb.Error):
            conn.execute(f"SELECT * FROM read_csv('{_SCRIPT.as_posix()}')")
    finally:
        conn.close()


def test_schema_listing(dbs: dict[str, Path]) -> None:
    conn = qr.open_readonly_connection()
    try:
        listing = qr.schema_listing(conn)
        names = {(t["catalog"], t["schema"], t["table"]) for t in listing["tables"]}
        assert {("live", "main", "ohlcv"), ("lab", "snap", "s1"), ("reg", "main", "models")} <= names
        only = qr.schema_listing(conn, name_filter="OHLCV")
        assert [t["table"] for t in only["tables"]] == ["ohlcv"]
        assert qr.schema_listing(conn, catalog="reg")["table_count"] == 1
        assert "live.main.ohlcv" in qr.format_schema(only, "markdown")
    finally:
        conn.close()


def test_cli_json_markdown_and_exit_codes(dbs: dict[str, Path], capsys: pytest.CaptureFixture[str]) -> None:
    assert qr.main(["SELECT count(*) AS n FROM live.ohlcv"]) == 0
    assert json.loads(capsys.readouterr().out)["rows"] == [[500]]
    assert qr.main(["SELECT * FROM live.ohlcv", "--limit", "2", "--format", "markdown"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("| open_time | close |") and "truncated" in out
    assert qr.main(["DROP TABLE live.ohlcv"]) == 2
    assert "rejected" in capsys.readouterr().err
    assert qr.main(["SELECT * FROM nope"]) == 3
    assert qr.main([]) == 2
    assert qr.main(["--schema", "--filter", "models"]) == 0
    assert json.loads(capsys.readouterr().out)["table_count"] == 1
