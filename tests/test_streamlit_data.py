# =============================================================================
# Streamlit dashboard data helper tests
# =============================================================================

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from streamlit_app import data


def test_prediction_history_uses_latest_stored_timestamp(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE predictions (
                open_time TEXT,
                close REAL,
                target INTEGER,
                prediction REAL,
                signal TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO predictions VALUES (?, ?, ?, ?, ?)",
            [
                ("2026-01-01 00:00:00", 100.0, 0, 0.10, "NEUTRAL"),
                ("2026-01-01 01:00:00", 101.0, 1, 0.36, "LONG"),
                ("2026-01-01 02:00:00", 102.0, 0, 0.20, "NEUTRAL"),
            ],
        )

    monkeypatch.setattr(
        data.utils,
        "load_db_config",
        lambda: {
            "database": {
                "active_env": "test",
                "db_path": str(db_path),
                "symbol": "BCHUSDT",
                "interval": "1m",
                "tables": {"predictions": "predictions"},
            }
        },
    )

    result = data.prediction_history(lookback_hours=1)

    assert len(result) == 2
    assert result["prediction"].tolist() == [0.36, 0.20]


def test_prediction_history_joins_ohlcv_for_candles(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE predictions (
                open_time TEXT,
                close REAL,
                target INTEGER,
                prediction REAL,
                signal TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE ohlcv (
                open_time TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
            """
        )
        conn.executemany(
            "INSERT INTO predictions VALUES (?, ?, ?, ?, ?)",
            [
                ("2026-01-01 00:00:00", 100.0, 0, 0.10, "NEUTRAL"),
                ("2026-01-01 00:01:00", 101.0, 1, 0.40, "LONG"),
            ],
        )
        conn.executemany(
            "INSERT INTO ohlcv VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("2026-01-01 00:00:00", 99.0, 102.0, 98.0, 100.0, 12.0),
                ("2026-01-01 00:01:00", 100.0, 103.0, 99.0, 102.0, 13.0),
            ],
        )

    monkeypatch.setattr(
        data.utils,
        "load_db_config",
        lambda: {
            "database": {
                "active_env": "test",
                "db_path": str(db_path),
                "symbol": "BCHUSDT",
                "interval": "1m",
                "tables": {"predictions": "predictions", "ohlcv": "ohlcv"},
            }
        },
    )

    result = data.prediction_history(lookback_hours=1)

    assert result[["open", "high", "low", "close"]].iloc[-1].tolist() == [100.0, 103.0, 99.0, 102.0]


def test_optional_trading_tables_return_empty_frames(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.db"
    sqlite3.connect(db_path).close()
    monkeypatch.setattr(
        data.utils,
        "load_db_config",
        lambda: {
            "database": {
                "active_env": "test",
                "db_path": str(db_path),
                "symbol": "BCHUSDT",
                "interval": "1m",
                "tables": {},
            }
        },
    )

    assert data.active_position() is None
    assert data.recent_orders().empty
    assert data.recent_errors().empty
