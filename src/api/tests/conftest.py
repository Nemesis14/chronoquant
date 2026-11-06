"""Fixtures: a complete temporary ChronoQuant environment (3 DuckDBs + journal + artifacts)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

import utils
from api.main import create_app

LONG_SESSION = "sess_long"
SHORT_SESSION = "sess_short"
MODEL_L = "lgbm_test_l"
MODEL_S = "lgbm_test_s"
N_BARS = 600


def _build_live(path: Path) -> None:
    with duckdb.connect(str(path)) as c:
        c.execute(
            "CREATE TABLE ohlcv AS SELECT TIMESTAMP '2026-01-01 00:00:00' + to_minutes(i) AS open_time,"
            " 100 + i * 0.01 AS open, 101 + i * 0.01 AS high, 99 + i * 0.01 AS low,"
            " 100.5 + i * 0.01 AS close, 10.0 AS volume FROM range(600) t(i)"
        )
        c.execute("CREATE TABLE feat_ohlcv_quant AS SELECT open_time FROM ohlcv")
        # raw score ramps -0.01 .. 0.01 -> percentile 0..1 through the rank lookup
        c.execute(
            "CREATE TABLE predictions AS SELECT open_time, close,"
            " -0.01 + 0.02 * (i / 599.0) AS long_pred, 0.01 - 0.02 * (i / 599.0) AS short_pred,"
            f" '{MODEL_L}' AS long_model_id, '{MODEL_S}' AS short_model_id,"
            " NULL::TIMESTAMP AS label_end_ts, NULL::DOUBLE AS long_mfe_fw60,"
            " NULL::DOUBLE AS short_mfe_fw60"
            " FROM (SELECT *, row_number() OVER (ORDER BY open_time) - 1 AS i FROM ohlcv)"
        )


def _build_lab(path: Path) -> None:
    with duckdb.connect(str(path)) as c:
        c.execute("CREATE SCHEMA strat")
        for sid, direction in ((LONG_SESSION, "long"), (SHORT_SESSION, "short")):
            c.execute(
                f'CREATE TABLE strat."{sid}__trades" AS SELECT'
                " TIMESTAMP '2026-01-01 01:00:00' + to_minutes(i * 30) AS entry_time,"
                " TIMESTAMP '2026-01-01 01:20:00' + to_minutes(i * 30) AS exit_time,"
                f" '{direction}' AS direction, 100.0 AS entry_price, 101.0 AS exit_price,"
                " 20 AS hold_minutes, 'tp' AS exit_reason, 0.95 AS score_pct_at_entry,"
                " 0.01 AS bucket_mean_mfe, 0.01 AS fact_log_return, 0.01 AS tp_lr, 0.005 AS sl_lr,"
                " 0.9 AS entry_cutoff, 'bucket_mean_mfe' AS tp_spec, '0.5x_tp' AS sl_spec"
                " FROM range(5) t(i)"
            )
            c.execute(
                f'CREATE TABLE strat."{sid}__equity" AS SELECT i AS trade_index,'
                " TIMESTAMP '2026-01-01 01:00:00' + to_minutes(i * 30) AS entry_time,"
                " 0.01 AS bucket_mean_mfe, 0.01 * (i + 1) AS cumulative_mfe,"
                " 0.01 AS fact_log_return, 0.01 * (i + 1) AS cumulative_fact_log_return"
                " FROM range(5) t(i)"
            )
            c.execute(
                f'CREATE TABLE strat."{sid}__grid_results" AS SELECT'
                f" '{direction}' AS direction, 0.9 + i * 0.01 AS entry_cutoff,"
                " 'a' AS tp_spec, 'b' AS sl_spec, 10 AS n_trades,"
                " i * 0.1 AS total_fact_log_return FROM range(5) t(i)"
            )


def _build_reg(path: Path) -> None:
    with duckdb.connect(str(path)) as c:
        c.execute(
            "CREATE TABLE models AS SELECT * FROM (VALUES"
            f" ('{MODEL_L}', 'l', 'predicted', TIMESTAMP '2026-01-01 00:00:00'),"
            f" ('{MODEL_S}', 's', 'predicted', TIMESTAMP '2026-01-02 00:00:00'))"
            " t(model_id, direction, status, created_at)"
        )
        c.execute(
            "CREATE TABLE strategies AS SELECT * FROM (VALUES"
            f" ('{LONG_SESSION}', '{MODEL_L}', '{MODEL_S}', '{LONG_SESSION}', 'active',"
            " TIMESTAMP '2026-01-03 00:00:00'))"
            " t(strategy_id, model_id_long, model_id_short, session_id, status, created_at)"
        )
        c.execute(
            "CREATE TABLE deployments AS SELECT 'd1' AS deployment_id, 'pending' AS status,"
            " TIMESTAMP '2026-01-04 00:00:00' AS created_at"
        )


def _build_journal(path: Path) -> None:
    with duckdb.connect(str(path)) as c:
        c.execute(
            "CREATE TABLE trading_positions (position_id VARCHAR, run_id VARCHAR, side VARCHAR,"
            " status VARCHAR, entry_time TIMESTAMP, exit_time TIMESTAMP, entry_price DOUBLE,"
            " exit_price DOUBLE, quantity DOUBLE, pnl_usdt DOUBLE, exit_reason VARCHAR)"
        )
        c.execute(
            "INSERT INTO trading_positions VALUES"
            " ('p1', 'r1', 'LONG', 'OPEN', TIMESTAMP '2026-01-01 05:00:00', NULL, 100.0, NULL, 2.0,"
            " NULL, NULL),"
            " ('p0', 'r1', 'SHORT', 'CLOSED', TIMESTAMP '2026-01-01 02:00:00',"
            " TIMESTAMP '2026-01-01 03:00:00', 100.0, 99.0, 2.0, 2.0, 'tp')"
        )
        c.execute(
            "CREATE TABLE trading_orders (order_id VARCHAR, created_at TIMESTAMP,"
            " request_json VARCHAR, response_json VARCHAR)"
        )
        c.execute("INSERT INTO trading_orders VALUES ('o1', TIMESTAMP '2026-01-01 05:00:00', '{}', '{}')")


def _build_artifacts(root: Path) -> None:
    ranks = pd.DataFrame({"score_raw": [-0.01, 0.01], "score_pct": [0.0, 1.0]})
    inv = pd.DataFrame({"score_raw": [-0.01, 0.01], "score_pct": [1.0, 0.0]})
    for sid, direction, cutoff in ((LONG_SESSION, "long", 0.9), (SHORT_SESSION, "short", 0.8)):
        d = root / "artifacts" / sid
        d.mkdir(parents=True)
        (ranks if direction == "long" else inv).to_parquet(d / f"rank_lookup_{direction}.parquet")
        (d / "strategy_artifact.json").write_text(
            json.dumps({
                "session_id": sid,
                "fit_period": {"start": "2026-01-01", "end": "2026-02-01"},
                f"rank_lookup_{direction}_path": f"rank_lookup_{direction}.parquet",
                "decision_params": {
                    "entry_cutoff": cutoff, "tp_spec": "bucket_mean_mfe", "sl_spec": "0.5x_tp",
                    "max_hold_minutes": 60,
                },
                "metrics": {
                    "n_trades": 5, "win_rate": 0.5, "total_fact_log_return": 0.05,
                    "compounded_return_pct": 5.1, "avg_hold_minutes": 20.0,
                },
            }),
            encoding="utf-8",
        )
    m = root / "artifacts" / MODEL_L
    m.mkdir(parents=True)
    (m / "manifest.json").write_text(json.dumps({"model_id": MODEL_L}), encoding="utf-8")
    (m / "features.json").write_text(json.dumps({"features": ["feat_a", "feat_b"]}), encoding="utf-8")


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp repo root with live/lab/reg/journal DBs wired in through ``utils`` patches."""
    live, lab = tmp_path / "live.duckdb", tmp_path / "live_lab.duckdb"
    reg, journal = tmp_path / "reg.duckdb", tmp_path / "journal.duckdb"
    _build_live(live)
    _build_lab(lab)
    _build_reg(reg)
    _build_journal(journal)
    _build_artifacts(tmp_path)
    (tmp_path / "logs").mkdir()

    asset = {"database": {"asset_id": "solusdt", "db_path": str(live), "symbol": "SOLUSDT",
                          "interval": "1m"}}
    trading = {
        "db_path": str(journal),
        "strategy_session_long_id": LONG_SESSION,
        "strategy_session_short_id": SHORT_SESSION,
    }
    monkeypatch.setattr(utils, "load_asset_config", lambda asset_id=None: asset)
    monkeypatch.setattr(utils, "lab_db_path", lambda asset_id=None: str(lab))
    monkeypatch.setattr(utils, "registry_path", lambda: str(reg))
    monkeypatch.setattr(utils, "load_trading_config", lambda: trading)
    monkeypatch.setattr(utils, "repo_root", lambda: str(tmp_path))
    return tmp_path


@pytest.fixture
def client(env: Path) -> Iterator[TestClient]:
    with TestClient(create_app(frontend_dist=env / "no_dist")) as c:
        yield c


@pytest.fixture(autouse=True)
def no_binance(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never touch the network / real keys from tests."""
    from api.services import binance  # noqa: PLC0415

    def _boom() -> None:
        raise RuntimeError("binance disabled in tests")

    monkeypatch.setattr(binance, "_make_client", _boom)
