"""DuckDB journal for the live trading service.

Stores trading runs, signals, positions, orders, and errors.
All writes are transactional; reads are safe from Streamlit fragments.
"""

from __future__ import annotations

import csv
import json
from contextlib import contextmanager
from pathlib import Path

import duckdb

import utils

# %% Connection helper


@contextmanager
def _connect(db_path: str):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.begin()
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


# %% Schema creation


def ensure_tables(db_path: str) -> None:
    with _connect(db_path) as conn:
        conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_trading_signals START 1")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_trading_errors START 1")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trading_runs (
                run_id               TEXT PRIMARY KEY,
                started_at           TEXT NOT NULL,
                stopped_at           TEXT,
                mode                 TEXT NOT NULL,
                asset_id             TEXT NOT NULL,
                strategy_session_id  TEXT NOT NULL,
                config_json          TEXT NOT NULL
            )
        """)
        # Migrate old schema: long_strategy_id / short_strategy_id → strategy_session_id
        existing_cols = {
            r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_name = 'trading_runs'"
            ).fetchall()
        }
        if "long_strategy_id" in existing_cols and "strategy_session_id" not in existing_cols:
            conn.execute("ALTER TABLE trading_runs ADD COLUMN strategy_session_id TEXT")
            conn.execute("UPDATE trading_runs SET strategy_session_id = long_strategy_id")
            conn.execute("ALTER TABLE trading_runs DROP COLUMN long_strategy_id")
            conn.execute("ALTER TABLE trading_runs DROP COLUMN short_strategy_id")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trading_signals (
                id            BIGINT PRIMARY KEY DEFAULT nextval('seq_trading_signals'),
                run_id        TEXT NOT NULL,
                bar_open_time TEXT NOT NULL,
                pred_long     REAL,
                pred_short    REAL,
                state_before  TEXT NOT NULL,
                decision      TEXT NOT NULL,
                reason        TEXT NOT NULL,
                processed_at  TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trading_positions (
                position_id    TEXT PRIMARY KEY,
                run_id         TEXT NOT NULL,
                side           TEXT NOT NULL,
                status         TEXT NOT NULL DEFAULT 'OPEN',
                entry_time     TEXT NOT NULL,
                exit_time      TEXT,
                entry_price    REAL NOT NULL,
                exit_price     REAL,
                quantity       REAL NOT NULL,
                pnl_usdt       REAL,
                exit_reason    TEXT,
                entry_order_id TEXT,
                exit_order_id  TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trading_orders (
                order_id          TEXT PRIMARY KEY,
                run_id            TEXT NOT NULL,
                position_id       TEXT,
                side              TEXT NOT NULL,
                order_type        TEXT NOT NULL,
                status            TEXT NOT NULL,
                client_order_id   TEXT,
                binance_order_id  TEXT,
                requested_qty     REAL,
                filled_qty        REAL,
                avg_price         REAL,
                request_json      TEXT,
                response_json     TEXT,
                created_at        TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trading_errors (
                id         BIGINT PRIMARY KEY DEFAULT nextval('seq_trading_errors'),
                run_id     TEXT,
                error_time TEXT NOT NULL,
                component  TEXT NOT NULL,
                error_type TEXT NOT NULL,
                message    TEXT NOT NULL,
                traceback  TEXT
            )
        """)


# %% trading_runs


def insert_run(
    db_path             : str,
    run_id              : str,
    mode                : str,
    asset_id            : str,
    strategy_session_id : str,
    config              : dict,
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO trading_runs
               (run_id, started_at, mode, asset_id, strategy_session_id, config_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [run_id, utils.now_utc_str(), mode, asset_id,
             strategy_session_id, json.dumps(config)],
        )


def mark_run_stopped(db_path: str, run_id: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE trading_runs SET stopped_at = ? WHERE run_id = ?",
            [utils.now_utc_str(), run_id],
        )


# %% trading_signals


def insert_signal(
    db_path       : str,
    run_id        : str,
    bar_open_time : str,
    pred_long     : float | None,
    pred_short    : float | None,
    state_before  : str,
    decision      : str,
    reason        : str,
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO trading_signals
               (run_id, bar_open_time, pred_long, pred_short, state_before, decision, reason, processed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [run_id, bar_open_time, pred_long, pred_short,
             state_before, decision, reason, utils.now_utc_str()],
        )


# %% trading_positions


def insert_position(
    db_path        : str,
    position_id    : str,
    run_id         : str,
    side           : str,
    entry_time     : str,
    entry_price    : float,
    quantity       : float,
    entry_order_id : str | None = None,
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO trading_positions
               (position_id, run_id, side, status, entry_time, entry_price, quantity, entry_order_id)
               VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?)""",
            [position_id, run_id, side, entry_time, entry_price, quantity, entry_order_id],
        )


def close_position(
    db_path        : str,
    position_id    : str,
    exit_time      : str,
    exit_price     : float,
    pnl_usdt       : float,
    exit_reason    : str,
    exit_order_id  : str | None = None,
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """UPDATE trading_positions
               SET status = 'CLOSED', exit_time = ?, exit_price = ?,
                   pnl_usdt = ?, exit_reason = ?, exit_order_id = ?
               WHERE position_id = ?""",
            [exit_time, exit_price, pnl_usdt, exit_reason, exit_order_id, position_id],
        )


def get_open_position(db_path: str) -> dict | None:
    with _connect(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM trading_positions WHERE status = 'OPEN' ORDER BY entry_time DESC LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row, strict=True))


def get_latest_run(db_path: str) -> dict | None:
    with _connect(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM trading_runs ORDER BY started_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row, strict=True))


# %% trading_orders


def insert_order(
    db_path          : str,
    order_id         : str,
    run_id           : str,
    position_id      : str | None,
    side             : str,
    order_type       : str,
    status           : str,
    client_order_id  : str | None,
    binance_order_id : str | None,
    requested_qty    : float | None,
    filled_qty       : float | None,
    avg_price        : float | None,
    request_json     : dict | None,
    response_json    : dict | None,
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO trading_orders
               (order_id, run_id, position_id, side, order_type, status,
                client_order_id, binance_order_id, requested_qty, filled_qty,
                avg_price, request_json, response_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [order_id, run_id, position_id, side, order_type, status,
             client_order_id, binance_order_id, requested_qty, filled_qty, avg_price,
             json.dumps(request_json) if request_json else None,
             json.dumps(response_json) if response_json else None,
             utils.now_utc_str()],
        )


# %% trading_errors


def insert_error(
    db_path    : str,
    run_id     : str | None,
    component  : str,
    error_type : str,
    message    : str,
    traceback  : str | None = None,
) -> None:
    try:
        with _connect(db_path) as conn:
            conn.execute(
                """INSERT INTO trading_errors
                   (run_id, error_time, component, error_type, message, traceback)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [run_id, utils.now_utc_str(), component, error_type, message, traceback],
            )
    except Exception:
        pass  # never crash the service on journal error


# %% Dashboard reads


def get_recent_signals(db_path: str, limit: int = 20) -> list[dict]:
    try:
        with _connect(db_path) as conn:
            cur = conn.execute(
                "SELECT * FROM trading_signals ORDER BY processed_at DESC LIMIT ?", [limit]
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    except Exception:
        return []


def get_recent_positions(db_path: str, limit: int = 50) -> list[dict]:
    try:
        with _connect(db_path) as conn:
            cur = conn.execute(
                "SELECT * FROM trading_positions ORDER BY entry_time DESC LIMIT ?", [limit]
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    except Exception:
        return []


def get_current_run_status(db_path: str) -> dict | None:
    try:
        run = get_latest_run(db_path)
        if not run:
            return None
        position = get_open_position(db_path)
        signal   = get_recent_signals(db_path, limit=1)

        # Infer runtime state from open position and last signal decision
        if position:
            state = position["side"]  # "LONG" or "SHORT"
        elif signal:
            last_decision = signal[0].get("decision", "")
            state_before  = signal[0].get("state_before", "FLAT")
            if "EXIT" in last_decision:
                state = "COOLDOWN"
            elif state_before in ("FLAT", "COOLDOWN"):
                state = state_before
            else:
                state = "FLAT"
        else:
            state = "FLAT"

        return {
            "run_id":        run["run_id"],
            "mode":          run["mode"],
            "started_at":    run["started_at"],
            "stopped_at":    run.get("stopped_at"),
            "state":         state,
            "open_position": position,
            "last_signal":   signal[0] if signal else None,
        }
    except Exception:
        return None


# %% Export on stop


def export_run(db_path: str, run_id: str, report_dir: str) -> None:
    out = Path(utils._resolve_path(report_dir)) / run_id
    out.mkdir(parents=True, exist_ok=True)

    with _connect(db_path) as conn:
        _export_table(conn, "trading_positions", out / "positions.csv",
                      "WHERE run_id = ?", params=[run_id])
        _export_table(conn, "trading_signals",   out / "signals.csv",
                      "WHERE run_id = ?", params=[run_id])
        _export_table(conn, "trading_orders",    out / "orders.csv",
                      "WHERE run_id = ?", params=[run_id])

    _write_run_summary(db_path, run_id, out / "summary.json")


def _export_table(
    conn   : duckdb.DuckDBPyConnection,
    table  : str,
    path   : Path,
    where  : str = "",
    params : list | None = None,
) -> None:
    cur  = conn.execute(f"SELECT * FROM {table} {where}", params or [])
    rows = cur.fetchall()
    if not rows:
        return
    fieldnames = [d[0] for d in cur.description]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows([dict(zip(fieldnames, r, strict=True)) for r in rows])


def _write_run_summary(db_path: str, run_id: str, path: Path) -> None:
    with _connect(db_path) as conn:
        cur       = conn.execute(
            "SELECT * FROM trading_positions WHERE run_id = ?", [run_id]
        )
        cols      = [d[0] for d in cur.description]
        positions = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    closed    = [r for r in positions if r.get("status") == "CLOSED"]
    total     = len(closed)
    wins      = sum(1 for p in closed if (p.get("pnl_usdt") or 0) > 0)
    total_pnl = sum((p.get("pnl_usdt") or 0) for p in closed)

    summary = {
        "run_id":          run_id,
        "total_trades":    total,
        "winning_trades":  wins,
        "losing_trades":   total - wins,
        "win_rate":        round(wins / total, 4) if total else None,
        "total_pnl_usdt":  round(total_pnl, 4),
    }
    path.write_text(json.dumps(summary, indent=4), encoding="utf-8")
