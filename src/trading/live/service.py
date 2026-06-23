"""Live trading service main loop.

Runs in a background thread, executing one cycle per 1-minute bar:
  1. Sync OHLCV + features + predictions via the pipeline.
  2. Read the latest closed-bar predictions from DuckDB.
  3. Convert raw scores to rank percentiles via lookup.
  4. Evaluate strategy -> decision.
  5. Execute: open/close position via the exchange client.
  6. Persist signal, position, and order to trading.db.
  7. Sleep until the next 1-minute bar closes.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
import traceback
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

import utils
from data_handling.store.duckdb_query import (
    latest_open_time,
    ohlcv_latest_open_time,
    ohlcv_row_count,
    query_range,
)
from data_handling.sync_tables.sync_features import sync_features
from data_handling.sync_tables.sync_ohlcv import sync_ohlcv
from data_handling.sync_tables.sync_predictions import sync_predictions
from strategy.strategy.artifacts import read_strategy_artifact
from trading.live import journal, strategy
from trading.live.exchange import BinanceFuturesClient
from trading.live.state import FLAT, TradingState

logger = logging.getLogger("chronoquant.trading")


def _dash_log(msg: str, level: str = "info") -> None:
    _ = (msg, level)


# %% TradingService


class TradingService:
    """Main trading service loop."""

    def __init__(self, config: dict) -> None:
        self.config   = config
        self.mode     = config["mode"]
        self.asset_id = config["asset_id"]
        self.db_path       = utils._resolve_path(config["db_path"])
        self.asset_db_path = utils._resolve_path(
            utils.load_asset_config(self.asset_id)["database"]["db_path"]
        )
        self.risk_cfg    = config.get("risk", {})
        self.journal_cfg = config.get("journal", {})
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.state  : TradingState | None = None
        self.run_id : str | None          = None

        # Load strategy artifact
        session_id   = config["strategy_session_id"]
        artifact     = read_strategy_artifact(session_id)
        self.session_id      : str  = session_id
        self.decision_params : dict = artifact["decision_params"]

        # Load rank lookup arrays for np.interp
        artifact_dir = Path(utils._resolve_path("artifacts")) / session_id
        lookup_long  = pd.read_parquet(artifact_dir / artifact["rank_lookup_long_path"])
        lookup_short = pd.read_parquet(artifact_dir / artifact["rank_lookup_short_path"])
        self._rank_scores_long  = lookup_long["score_raw"].to_numpy(dtype=float)
        self._rank_pct_long     = lookup_long["score_pct"].to_numpy(dtype=float)
        self._rank_scores_short = lookup_short["score_raw"].to_numpy(dtype=float)
        self._rank_pct_short    = lookup_short["score_pct"].to_numpy(dtype=float)

        # Stable prediction column names (not model-ID-derived)
        self.long_pred_col  = "long_pred"
        self.short_pred_col = "short_pred"

        # Exchange client
        self.exchange = BinanceFuturesClient(
            symbol          = utils.load_asset_config(self.asset_id)["database"]["symbol"],
            leverage        = config["leverage"],
            quote_order_qty = config["quote_order_qty"],
            mode            = self.mode,
        )

    # --- public API ---

    def start(self) -> None:
        if self.is_running():
            logger.warning("Trading service already running")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target = self._run,
            name   = "chronoquant-trading",
            daemon = True,
        )
        self._thread.start()
        logger.info("Trading service started (mode=%s)", self.mode)

    def stop(self) -> None:
        logger.info("Trading service stop requested")
        self._stop_event.set()

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # --- main loop ---

    def _run(self) -> None:
        try:
            self._startup()
            while not self._stop_event.is_set():
                try:
                    self._cycle()
                except Exception as exc:
                    self._handle_error("cycle", exc)
                    if self.state:
                        self.state.consecutive_errors += 1
                    if self._consecutive_errors_exceeded():
                        logger.error("Max consecutive errors reached — stopping service")
                        break
                self._sleep_until_next_bar()
        except Exception as exc:
            logger.exception("Trading service fatal error: %s", exc)
        finally:
            self._shutdown()

    def _startup(self) -> None:
        journal.ensure_tables(self.db_path)

        self.run_id = (
            f"run_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        )
        journal.insert_run(
            self.db_path, self.run_id, self.mode, self.asset_id,
            self.session_id,
            self.config,
        )

        # Reconcile: restore state from DB if there was an open position
        open_pos   = journal.get_open_position(self.db_path)
        self.state = TradingState.from_db(self.run_id, open_pos)

        if open_pos:
            logger.info(
                "Reconcile: found open %s position from %s, price=%.4f qty=%s",
                open_pos["side"], open_pos["entry_time"],
                open_pos.get("entry_price", 0), open_pos.get("quantity", 0),
            )
        else:
            logger.info("Reconcile: no open position — starting FLAT")

        self.exchange.set_leverage()
        logger.info("Service ready (run_id=%s)", self.run_id)

    def _cycle(self) -> None:
        assert self.state   is not None
        assert self.run_id  is not None
        # 1. Sync data
        self._sync_data()

        # 2. Read latest closed bar
        bar = self._read_latest_bar()
        if bar is None:
            logger.warning("No prediction data available — skipping cycle")
            return

        bar_open_time, pred_long, pred_short, bar_close = bar

        # 3. Convert raw scores to rank percentiles
        score_pct_long, score_pct_short = self._to_percentiles(pred_long, pred_short)

        # 4. Evaluate strategy
        now = datetime.now(UTC)
        decision, reason = strategy.evaluate(
            self.state, score_pct_long, score_pct_short,
            self.decision_params, now,
        )
        state_before = self.state.status
        logger.info(
            "Bar %s | raw=%.4f/%.4f pct=%.3f/%.3f | state=%s -> %s (%s)",
            bar_open_time, pred_long, pred_short, score_pct_long, score_pct_short,
            state_before, decision, reason,
        )
        _dash_log(
            f"[trading] {bar_open_time} raw={pred_long:.4f}/{pred_short:.4f}"
            f" pct={score_pct_long:.3f}/{score_pct_short:.3f}"
            f" {state_before} -> {decision} ({reason})"
        )

        # 6. Execute
        self._execute(decision, reason, bar_open_time, bar_close, now)

        # 7. Persist signal
        journal.insert_signal(
            self.db_path, self.run_id, bar_open_time,
            pred_long, pred_short, state_before, decision, reason,
        )

        # 8. Reset error counter on successful cycle
        self.state.consecutive_errors = 0

    def _to_percentiles(self, pred_long: float, pred_short: float) -> tuple[float, float]:
        pct_long  = float(
            np.interp(pred_long,  self._rank_scores_long,  self._rank_pct_long).clip(0.0, 1.0)
        )
        pct_short = float(
            np.interp(pred_short, self._rank_scores_short, self._rank_pct_short).clip(0.0, 1.0)
        )
        return pct_long, pct_short

    def _execute(
        self,
        decision      : str,
        reason        : str,
        bar_open_time : str,
        bar_close     : float,
        now           : datetime,
    ) -> None:
        assert self.state is not None
        from trading.live.strategy import ENTER_LONG, ENTER_SHORT, EXIT_LONG, EXIT_SHORT, HOLD

        if decision == HOLD:
            return

        try:
            mark_price = self.exchange.get_mark_price() or bar_close

            if decision == ENTER_LONG:
                self._open_position("LONG", mark_price, bar_open_time, reason)

            elif decision == ENTER_SHORT:
                self._open_position("SHORT", mark_price, bar_open_time, reason)

            elif decision in (EXIT_LONG, EXIT_SHORT):
                self._close_position(mark_price, reason, now)

        except Exception as exc:
            self._handle_error("execute", exc)
            self.state.consecutive_errors += 1

    def _open_position(
        self,
        side          : str,
        mark_price    : float,
        bar_open_time : str,
        reason        : str,
    ) -> None:
        assert self.state  is not None
        assert self.run_id is not None
        if not self._daily_limits_ok():
            logger.warning("Daily limits reached — skipping entry")
            return

        if side == "LONG":
            response = self.exchange.open_long(mark_price)
        else:
            response = self.exchange.open_short(mark_price)

        filled_qty = float(response.get("executedQty", 0))
        avg_price  = float(response.get("avgPrice", mark_price))
        if avg_price == 0:
            avg_price = mark_price

        position_id = f"pos_{uuid.uuid4().hex[:12]}"
        order_id    = f"ord_{uuid.uuid4().hex[:12]}"
        entry_time  = utils.now_utc_str()

        journal.insert_position(
            self.db_path, position_id, self.run_id, side,
            entry_time, avg_price, filled_qty, order_id,
        )
        journal.insert_order(
            self.db_path, order_id, self.run_id, position_id,
            side, "MARKET", response.get("status", "FILLED"),
            response.get("clientOrderId"), str(response.get("orderId", "")),
            None, filled_qty, avg_price, None, response,
        )

        self.state.status      = side
        self.state.position_id = position_id
        self.state.side        = side
        self.state.entry_time  = datetime.now(UTC)
        self.state.entry_price = avg_price
        self.state.quantity    = filled_qty

        logger.info(
            "Opened %s | price=%.4f qty=%s reason=%s",
            side, avg_price, filled_qty, reason,
        )
        _dash_log(
            f"[trading] OPENED {side} price={avg_price:.4f} qty={filled_qty} reason={reason}",
            level="warning",
        )

    def _close_position(
        self,
        mark_price : float,
        reason     : str,
        now        : datetime,
    ) -> None:
        assert self.state  is not None
        assert self.run_id is not None
        if self.state.quantity is None or self.state.quantity <= 0:
            logger.warning("close_position called but quantity is None/0 — skipping")
            return

        side = self.state.side
        if side == "LONG":
            response = self.exchange.close_long(self.state.quantity, mark_price)
        else:
            response = self.exchange.close_short(self.state.quantity, mark_price)

        avg_price = float(response.get("avgPrice", mark_price))
        if avg_price == 0:
            avg_price = mark_price

        # PnL calculation (notional, before leverage cost)
        entry    = self.state.entry_price or mark_price
        qty      = self.state.quantity
        pnl_usdt = (avg_price - entry) * qty if side == "LONG" else (entry - avg_price) * qty

        exit_time  = utils.now_utc_str()
        order_id   = f"ord_{uuid.uuid4().hex[:12]}"
        assert self.state.position_id is not None

        journal.close_position(
            self.db_path, self.state.position_id, exit_time,
            avg_price, pnl_usdt, reason, order_id,
        )
        journal.insert_order(
            self.db_path, order_id, self.run_id, self.state.position_id,
            "SELL" if side == "LONG" else "BUY", "MARKET",
            response.get("status", "FILLED"),
            response.get("clientOrderId"), str(response.get("orderId", "")),
            None, qty, avg_price, None, response,
        )

        self.state.record_trade_result(pnl_usdt)
        self.state.status = FLAT
        self.state.clear_position()

        logger.info(
            "Closed %s | exit=%.4f pnl=%.4f USDT reason=%s",
            side, avg_price, pnl_usdt, reason,
        )
        _dash_log(
            f"[trading] CLOSED {side} exit={avg_price:.4f} pnl={pnl_usdt:+.4f} USDT reason={reason}",
            level="warning",
        )

    # --- data sync + prediction read ---

    def _sync_data(self) -> None:
        try:
            rows_before    = ohlcv_row_count(self.asset_db_path)
            last_open_time = ohlcv_latest_open_time(self.asset_db_path)
            start_time     = _next_open_time(last_open_time) if last_open_time else _INITIAL_SYNC_START
            start_ms       = _utc_str_to_ms(start_time)

            sync_ohlcv(open_time_ms_from=start_ms, asset_id=self.asset_id)

            rows_after       = ohlcv_row_count(self.asset_db_path)
            latest_open      = ohlcv_latest_open_time(self.asset_db_path)
            inserted_rows    = rows_after - rows_before
            logger.info(
                "OHLCV sync rows before=%s after=%s inserted=%s",
                rows_before, rows_after, inserted_rows,
            )

            if not latest_open or pd.to_datetime(latest_open) < pd.to_datetime(start_time):
                logger.info("No new OHLCV interval found for feature and prediction sync")
                return

            sync_features(start_time, end_time=latest_open, asset_id=self.asset_id)
            sync_predictions(start_time, end_time=latest_open, asset_id=self.asset_id)
        except Exception as exc:
            logger.warning("Data sync failed: %s", exc)
            raise

    def _read_latest_bar(self) -> tuple[str, float, float, float] | None:
        if latest_open_time(self.asset_db_path, "predictions") is None:
            return None

        cutoff    = datetime.now(UTC).replace(tzinfo=None)
        start_str = (cutoff - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        end_str   = cutoff.strftime("%Y-%m-%d %H:%M:%S")

        try:
            df = query_range(
                self.asset_db_path, "predictions",
                start   = start_str,
                end     = end_str,
                columns = ["open_time", "close", self.long_pred_col, self.short_pred_col],
            )
        except Exception as exc:
            logger.error("Failed to read predictions from DuckDB: %s", exc)
            return None

        if df.empty:
            return None

        df["open_time"] = pd.to_datetime(df["open_time"])
        df = df.loc[df["open_time"] < pd.Timestamp(cutoff)]
        pred_mask = df[self.long_pred_col].notna() & df[self.short_pred_col].notna()
        df = df.loc[pred_mask]
        if df.empty:
            return None

        row = df.sort_values("open_time").iloc[-1]
        return (
            row["open_time"].strftime("%Y-%m-%d %H:%M:%S"),
            float(row[self.long_pred_col]),
            float(row[self.short_pred_col]),
            float(row["close"]),
        )

    # --- helpers ---

    def _daily_limits_ok(self) -> bool:
        assert self.state is not None
        max_trades = self.risk_cfg.get("max_daily_trades", 20)
        max_loss   = self.risk_cfg.get("max_daily_loss_usdt", 30.0)
        if self.state.daily_trade_count >= max_trades:
            logger.warning("Daily trade limit reached (%d)", max_trades)
            return False
        if self.state.daily_loss_usdt >= max_loss:
            logger.warning("Daily loss limit reached (%.2f USDT)", max_loss)
            return False
        return True

    def _consecutive_errors_exceeded(self) -> bool:
        max_err = self.risk_cfg.get("max_consecutive_errors", 3)
        return (self.state.consecutive_errors if self.state else 0) >= max_err

    def _handle_error(self, component: str, exc: Exception) -> None:
        msg = str(exc)
        tb  = traceback.format_exc()
        logger.error("[%s] %s: %s", component, type(exc).__name__, msg)
        with contextlib.suppress(Exception):
            journal.insert_error(
                self.db_path, self.run_id, component, type(exc).__name__, msg, tb
            )

    def _sleep_until_next_bar(self) -> None:
        now            = time.time()
        next_boundary  = (now // 60 + 1) * 60 + 5
        sleep_sec      = max(1.0, next_boundary - now)
        logger.debug("Sleeping %.1fs until next bar", sleep_sec)
        self._stop_event.wait(timeout=sleep_sec)

    def _shutdown(self) -> None:
        self._stop_event.set()
        logger.info("Trading service shutting down (run_id=%s)", self.run_id)
        if self.run_id:
            with contextlib.suppress(Exception):
                journal.mark_run_stopped(self.db_path, self.run_id)

        if self.journal_cfg.get("export_on_stop") and self.run_id:
            try:
                journal.export_run(
                    self.db_path, self.run_id,
                    self.journal_cfg.get("report_dir", "trading_reports"),
                )
                logger.info("Run exported to trading_reports/%s/", self.run_id)
            except Exception as exc:
                logger.warning("Export failed: %s", exc)

        logger.info("Trading service stopped")
        self._thread = None


_INITIAL_SYNC_START = "2017-01-01 00:00:00"


def _next_open_time(open_time: str) -> str:
    value = pd.to_datetime(open_time, errors="raise") + timedelta(minutes=1)
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _utc_str_to_ms(value: str) -> int:
    timestamp = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    return int(timestamp.timestamp() * 1000)
