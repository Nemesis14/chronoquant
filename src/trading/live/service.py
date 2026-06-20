"""Live trading service main loop.

Runs in a background thread, executing one cycle per 1-minute bar:
  1. Sync OHLCV + features + predictions via the pipeline.
  2. Read the latest closed-bar predictions from DuckDB.
  3. Evaluate strategy -> decision.
  4. Execute: open/close position via the exchange client.
  5. Persist signal, position, and order to trading.db.
  6. Sleep until the next 1-minute bar closes.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
import traceback
import uuid
from datetime import UTC, datetime, timedelta

import pandas as pd

import utils
from trading.live import journal, strategy
from trading.live.exchange import BinanceFuturesClient
from trading.live.state import COOLDOWN, FLAT, TradingState

logger = logging.getLogger("chronoquant.trading")


def _dash_log(msg: str, level: str = "info") -> None:
    """Forward a message to the dashboard logger (no-op when headless).

    Args:
        msg   : Message string to log.
        level : Log level name (``"info"`` or ``"warning"``).
    """
    try:
        from ui.dashboard_logging import get_dashboard_logger
        getattr(get_dashboard_logger(), level)(msg)
    except Exception:
        pass


# %% TradingService


class TradingService:
    """Main trading service loop.

    Runs in a background thread. Each cycle (every ~60s):
      1. Sync OHLCV + features + predictions via existing pipeline.
      2. Read latest closed bar predictions for both models.
      3. Evaluate strategy -> decision.
      4. Execute: open/close position via exchange client.
      5. Persist signal + position + order to trading.db.
      6. Sleep until next 1-minute bar closes.

    Mirrors the simulation logic in trading.calibration.backtest exactly.
    """

    def __init__(self, config: dict) -> None:
        """Initialise the service from a trading config dict.

        Args:
            config : Full trading config dict (from config/trading.json).
        """
        self.config   = config
        self.mode     = config["mode"]
        self.asset_id = config["asset_id"]
        self.db_path  = utils._resolve_path(config["db_path"])
        self.risk_cfg    = config.get("risk", {})
        self.journal_cfg = config.get("journal", {})
        self._stop_event = threading.Event()
        self.state  : TradingState | None = None
        self.run_id : str | None          = None

        # Load strategy configs once
        strategies      = utils.load_strategies_config()["strategies"]
        self.long_cfg   = strategies[config["long_strategy_id"]]
        self.short_cfg  = strategies[config["short_strategy_id"]]

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
        """Start the trading loop in a daemon background thread."""
        self._stop_event.clear()
        t = threading.Thread(target=self._run, name="chronoquant-trading", daemon=True)
        t.start()
        logger.info("Trading service started (mode=%s)", self.mode)

    def stop(self) -> None:
        """Request a graceful stop of the trading loop."""
        logger.info("Trading service stop requested")
        self._stop_event.set()

    def is_running(self) -> bool:
        """Return True if the service has not been stopped.

        Returns:
            Boolean running status.
        """
        return not self._stop_event.is_set()

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
            self.config["long_strategy_id"], self.config["short_strategy_id"],
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
            logger.info("Reconcile: no open position — starting FLAT armed=True")

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

        # 3. Evaluate cooldown / rearm transitions before strategy decision
        now = datetime.now(UTC)
        self._apply_cooldown_rearm(pred_long, pred_short, now)

        # 4. Evaluate strategy
        decision, reason = strategy.evaluate(
            self.state, pred_long, pred_short,
            self.long_cfg, self.short_cfg, now,
        )
        state_before = self.state.status
        logger.info(
            "Bar %s | long=%.3f short=%.3f | state=%s -> %s (%s)",
            bar_open_time, pred_long, pred_short, state_before, decision, reason,
        )
        _dash_log(
            f"[trading] {bar_open_time} L={pred_long:.3f} S={pred_short:.3f}"
            f" {state_before} -> {decision} ({reason})"
        )

        # 5. Execute
        self._execute(decision, reason, bar_open_time, bar_close, now)

        # 6. Persist signal
        journal.insert_signal(
            self.db_path, self.run_id, bar_open_time,
            pred_long, pred_short, state_before, decision, reason,
        )

        # 7. Reset error counter on successful cycle
        self.state.consecutive_errors = 0

    def _apply_cooldown_rearm(
        self,
        pred_long  : float,
        pred_short : float,
        now        : datetime,
    ) -> None:
        """Handle COOLDOWN -> FLAT transition and armed flag.

        Args:
            pred_long  : Latest long prediction probability.
            pred_short : Latest short prediction probability.
            now        : Current UTC datetime.
        """
        assert self.state is not None
        if self.state.status != COOLDOWN:
            return

        if self.state.cooldown_until and now < self.state.cooldown_until:
            return

        # Cooldown elapsed — rearm when both predictions are below their thresholds
        if (pred_long <= self.long_cfg["rearm_threshold"]
                and pred_short <= self.short_cfg["rearm_threshold"]):
            self.state.armed  = True
            self.state.status = FLAT
            logger.info("Rearmed — entering FLAT long=%.3f short=%.3f", pred_long, pred_short)

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
        self.state.armed       = False

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
        cooldown_min = max(
            self.long_cfg.get("cooldown_minutes", 60),
            self.short_cfg.get("cooldown_minutes", 60),
        )
        self.state.cooldown_until = datetime.now(UTC).replace(microsecond=0) + timedelta(
            minutes=cooldown_min
        )
        self.state.status = COOLDOWN
        self.state.clear_position()

        logger.info(
            "Closed %s | exit=%.4f pnl=%.4f USDT reason=%s cooldown=%dmin",
            side, avg_price, pnl_usdt, reason, cooldown_min,
        )
        _dash_log(
            f"[trading] CLOSED {side} exit={avg_price:.4f} pnl={pnl_usdt:+.4f} USDT reason={reason}",
            level="warning",
        )

    # --- data sync + prediction read ---

    def _sync_data(self) -> None:
        try:
            from ui.sync import run_database_sync
            run_database_sync(asset_id=self.asset_id)
        except Exception as exc:
            logger.warning("Data sync failed: %s", exc)
            raise

    def _read_latest_bar(self) -> tuple[str, float, float, float] | None:
        """Read the latest safely closed 1-minute bar's predictions from DuckDB.

        Returns:
            Tuple of (bar_open_time, pred_long, pred_short, close_price) or None.
        """
        from data_handling.store.duckdb_query import latest_open_time, query_range

        db_path = utils.load_asset_config(self.asset_id)["database"]["db_path"]
        if latest_open_time(db_path, "predictions") is None:
            return None

        cutoff    = datetime.now(UTC).replace(tzinfo=None)
        start_str = (cutoff - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        end_str   = cutoff.strftime("%Y-%m-%d %H:%M:%S")

        try:
            df = query_range(
                db_path, "predictions",
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
        """Sleep until 5 seconds after the next 1-minute boundary."""
        now            = time.time()
        next_boundary  = (now // 60 + 1) * 60 + 5
        sleep_sec      = max(1.0, next_boundary - now)
        logger.debug("Sleeping %.1fs until next bar", sleep_sec)
        self._stop_event.wait(timeout=sleep_sec)

    def _shutdown(self) -> None:
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
