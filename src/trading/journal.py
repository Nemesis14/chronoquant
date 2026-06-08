from __future__ import annotations

import csv
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import utils


# =============================================================================
# Connection helper
# =============================================================================

@contextmanager
def _connect(db_path: str):
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def trading_db_path() -> str:
    cfg = utils.load_trading_config()
    return utils._resolve_path(cfg["db_path"])


# =============================================================================
# Schema creation
# =============================================================================

def ensure_tables(db_path: str) -> None:
    with _connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS trading_runs (
                run_id          TEXT PRIMARY KEY,
                started_at      TEXT NOT NULL,
                stopped_at      TEXT,
                mode            TEXT NOT NULL,
                asset_id        TEXT NOT NULL,
                long_strategy_id TEXT NOT NULL,
                short_strategy_id TEXT NOT NULL,
                config_json     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trading_signals (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id          TEXT NOT NULL,
                bar_open_time   TEXT NOT NULL,
                pred_long       REAL,
                pred_short      REAL,
                state_before    TEXT NOT NULL,
                decision        TEXT NOT NULL,
                reason          TEXT NOT NULL,
                processed_at    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trading_positions (
                position_id     TEXT PRIMARY KEY,
                run_id          TEXT NOT NULL,
                side            TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'OPEN',
                entry_time      TEXT NOT NULL,
                exit_time       TEXT,
                entry_price     REAL NOT NULL,
                exit_price      REAL,
                quantity        REAL NOT NULL,
                pnl_usdt        REAL,
                exit_reason     TEXT,
                entry_order_id  TEXT,
                exit_order_id   TEXT
            );

            CREATE TABLE IF NOT EXISTS trading_orders (
                order_id        TEXT PRIMARY KEY,
                run_id          TEXT NOT NULL,
                position_id     TEXT,
                side            TEXT NOT NULL,
                order_type      TEXT NOT NULL,
                status          TEXT NOT NULL,
                client_order_id TEXT,
                binance_order_id TEXT,
                requested_qty   REAL,
                filled_qty      REAL,
                avg_price       REAL,
                request_json    TEXT,
                response_json   TEXT,
                created_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trading_errors (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id          TEXT,
                error_time      TEXT NOT NULL,
                component       TEXT NOT NULL,
                error_type      TEXT NOT NULL,
                message         TEXT NOT NULL,
                traceback       TEXT
            );
        """)


# =============================================================================
# trading_runs
# =============================================================================

def insert_run(db_path: str, run_id: str, mode: str, asset_id: str,
               long_strategy_id: str, short_strategy_id: str, config: dict) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO trading_runs
               (run_id, started_at, mode, asset_id, long_strategy_id, short_strategy_id, config_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (run_id, utils.now_utc_str(), mode, asset_id,
             long_strategy_id, short_strategy_id, json.dumps(config)),
        )


def mark_run_stopped(db_path: str, run_id: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE trading_runs SET stopped_at = ? WHERE run_id = ?",
            (utils.now_utc_str(), run_id),
        )


# =============================================================================
# trading_signals
# =============================================================================

def insert_signal(db_path: str, run_id: str, bar_open_time: str,
                  pred_long: Optional[float], pred_short: Optional[float],
                  state_before: str, decision: str, reason: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO trading_signals
               (run_id, bar_open_time, pred_long, pred_short, state_before, decision, reason, processed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, bar_open_time, pred_long, pred_short,
             state_before, decision, reason, utils.now_utc_str()),
        )


# =============================================================================
# trading_positions
# =============================================================================

def insert_position(db_path: str, position_id: str, run_id: str, side: str,
                    entry_time: str, entry_price: float, quantity: float,
                    entry_order_id: Optional[str] = None) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO trading_positions
               (position_id, run_id, side, status, entry_time, entry_price, quantity, entry_order_id)
               VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?)""",
            (position_id, run_id, side, entry_time, entry_price, quantity, entry_order_id),
        )


def close_position(db_path: str, position_id: str, exit_time: str,
                   exit_price: float, pnl_usdt: float, exit_reason: str,
                   exit_order_id: Optional[str] = None) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """UPDATE trading_positions
               SET status = 'CLOSED', exit_time = ?, exit_price = ?,
                   pnl_usdt = ?, exit_reason = ?, exit_order_id = ?
               WHERE position_id = ?""",
            (exit_time, exit_price, pnl_usdt, exit_reason, exit_order_id, position_id),
        )


def get_open_position(db_path: str) -> Optional[dict]:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM trading_positions WHERE status = 'OPEN' ORDER BY entry_time DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def get_latest_run(db_path: str) -> Optional[dict]:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM trading_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


# =============================================================================
# trading_orders
# =============================================================================

def insert_order(db_path: str, order_id: str, run_id: str, position_id: Optional[str],
                 side: str, order_type: str, status: str,
                 client_order_id: Optional[str], binance_order_id: Optional[str],
                 requested_qty: Optional[float], filled_qty: Optional[float],
                 avg_price: Optional[float], request_json: Optional[dict],
                 response_json: Optional[dict]) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO trading_orders
               (order_id, run_id, position_id, side, order_type, status,
                client_order_id, binance_order_id, requested_qty, filled_qty,
                avg_price, request_json, response_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (order_id, run_id, position_id, side, order_type, status,
             client_order_id, binance_order_id, requested_qty, filled_qty, avg_price,
             json.dumps(request_json) if request_json else None,
             json.dumps(response_json) if response_json else None,
             utils.now_utc_str()),
        )


# =============================================================================
# trading_errors
# =============================================================================

def insert_error(db_path: str, run_id: Optional[str], component: str,
                 error_type: str, message: str, traceback: Optional[str] = None) -> None:
    try:
        with _connect(db_path) as conn:
            conn.execute(
                """INSERT INTO trading_errors
                   (run_id, error_time, component, error_type, message, traceback)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (run_id, utils.now_utc_str(), component, error_type, message, traceback),
            )
    except Exception:
        pass  # never crash the service on journal error


# =============================================================================
# Dashboard reads
# =============================================================================

def get_recent_signals(db_path: str, limit: int = 20) -> list[dict]:
    try:
        with _connect(db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM trading_signals ORDER BY processed_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_recent_positions(db_path: str, limit: int = 50) -> list[dict]:
    try:
        with _connect(db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM trading_positions ORDER BY entry_time DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_current_run_status(db_path: str) -> Optional[dict]:
    try:
        run = get_latest_run(db_path)
        if not run:
            return None
        position = get_open_position(db_path)
        signal = get_recent_signals(db_path, limit=1)
        return {
            "run_id": run["run_id"],
            "mode": run["mode"],
            "started_at": run["started_at"],
            "stopped_at": run.get("stopped_at"),
            "open_position": position,
            "last_signal": signal[0] if signal else None,
        }
    except Exception:
        return None


# =============================================================================
# Export on stop
# =============================================================================

def export_run(db_path: str, run_id: str, report_dir: str) -> None:
    out = Path(utils._resolve_path(report_dir)) / run_id
    out.mkdir(parents=True, exist_ok=True)

    with _connect(db_path) as conn:
        _export_table(conn, "trading_positions", out / "positions.csv",
                      "WHERE run_id = ?", (run_id,))
        _export_table(conn, "trading_signals", out / "signals.csv",
                      "WHERE run_id = ?", (run_id,))
        _export_table(conn, "trading_orders", out / "orders.csv",
                      "WHERE run_id = ?", (run_id,))

    _write_run_summary(db_path, run_id, out / "summary.json")


def _export_table(conn, table: str, path: Path, where: str = "", params=()) -> None:
    rows = conn.execute(f"SELECT * FROM {table} {where}", params).fetchall()
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows([dict(r) for r in rows])


def _write_run_summary(db_path: str, run_id: str, path: Path) -> None:
    with _connect(db_path) as conn:
        positions = conn.execute(
            "SELECT * FROM trading_positions WHERE run_id = ?", (run_id,)
        ).fetchall()

    closed = [dict(r) for r in positions if r["status"] == "CLOSED"]
    total = len(closed)
    wins = sum(1 for p in closed if (p.get("pnl_usdt") or 0) > 0)
    total_pnl = sum((p.get("pnl_usdt") or 0) for p in closed)

    summary = {
        "run_id": run_id,
        "total_trades": total,
        "winning_trades": wins,
        "losing_trades": total - wins,
        "win_rate": round(wins / total, 4) if total else None,
        "total_pnl_usdt": round(total_pnl, 4),
    }
    path.write_text(json.dumps(summary, indent=4), encoding="utf-8")
