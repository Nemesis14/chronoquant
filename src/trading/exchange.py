from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Optional

import utils

_logger = logging.getLogger("chronoquant.trading")

# Binance Futures lot size for SOLUSDT perpetual: step 0.1 SOL
_SOL_QTY_STEP = 0.1


def _round_qty(qty: float, step: float = _SOL_QTY_STEP) -> float:
    return round(int(qty / step) * step, 8)


# =============================================================================
# BinanceFuturesClient
# =============================================================================

class BinanceFuturesClient:
    """
    Wrapper around Binance Futures (USDT-M) order placement.

    mode = "dry_run": no real orders; simulates fills at mark price.
    mode = "live":    real signed orders via python-binance.
    """

    def __init__(self, symbol: str, leverage: int, quote_order_qty: float, mode: str):
        self.symbol = symbol
        self.leverage = leverage
        self.quote_order_qty = quote_order_qty
        self.mode = mode
        self._client = None

        if mode == "live":
            self._client = self._make_client()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def set_leverage(self) -> None:
        if self.mode == "dry_run":
            _logger.info("[dry_run] set_leverage %s × %d", self.symbol, self.leverage)
            return
        try:
            self._client.futures_change_leverage(
                symbol=self.symbol, leverage=self.leverage
            )
            _logger.info("Leverage set: %s × %d", self.symbol, self.leverage)
        except Exception as exc:
            _logger.warning("set_leverage failed (may already be set): %s", exc)

    # ------------------------------------------------------------------
    # Price
    # ------------------------------------------------------------------

    def get_mark_price(self) -> float:
        if self.mode == "dry_run":
            return self._dry_run_price()
        try:
            result = self._client.futures_mark_price(symbol=self.symbol)
            return float(result["markPrice"])
        except Exception as exc:
            raise RuntimeError(f"get_mark_price failed: {exc}") from exc

    def _dry_run_price(self) -> float:
        try:
            result = self._make_client().futures_mark_price(symbol=self.symbol)
            return float(result["markPrice"])
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    def open_long(self, mark_price: float) -> dict:
        qty = _round_qty((self.quote_order_qty * self.leverage) / mark_price)
        _logger.info(
            "[%s] OPEN LONG %s qty=%s notional=%.2f USDT price=~%.4f",
            self.mode, self.symbol, qty, qty * mark_price, mark_price,
        )
        if self.mode == "dry_run":
            return self._dry_fill("BUY", qty, mark_price)
        return self._place_order("BUY", qty, reduce_only=False)

    def close_long(self, quantity: float, mark_price: float) -> dict:
        qty = _round_qty(quantity)
        _logger.info("[%s] CLOSE LONG %s qty=%s price=~%.4f", self.mode, self.symbol, qty, mark_price)
        if self.mode == "dry_run":
            return self._dry_fill("SELL", qty, mark_price)
        return self._place_order("SELL", qty, reduce_only=True)

    def open_short(self, mark_price: float) -> dict:
        qty = _round_qty((self.quote_order_qty * self.leverage) / mark_price)
        _logger.info(
            "[%s] OPEN SHORT %s qty=%s notional=%.2f USDT price=~%.4f",
            self.mode, self.symbol, qty, qty * mark_price, mark_price,
        )
        if self.mode == "dry_run":
            return self._dry_fill("SELL", qty, mark_price)
        return self._place_order("SELL", qty, reduce_only=False)

    def close_short(self, quantity: float, mark_price: float) -> dict:
        qty = _round_qty(quantity)
        _logger.info("[%s] CLOSE SHORT %s qty=%s price=~%.4f", self.mode, self.symbol, qty, mark_price)
        if self.mode == "dry_run":
            return self._dry_fill("BUY", qty, mark_price)
        return self._place_order("BUY", qty, reduce_only=True)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _place_order(self, side: str, quantity: float, reduce_only: bool) -> dict:
        params: dict = {
            "symbol": self.symbol,
            "side": side,
            "type": "MARKET",
            "quantity": str(quantity),
            "newClientOrderId": f"CQ_{uuid.uuid4().hex[:16]}",
            "newOrderRespType": "RESULT",
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        try:
            response = self._client.futures_create_order(**params)
            _logger.info("Order placed: %s", response.get("orderId"))
            return response
        except Exception as exc:
            raise RuntimeError(f"Order failed {side} {quantity}: {exc}") from exc

    def _dry_fill(self, side: str, quantity: float, price: float) -> dict:
        return {
            "orderId": f"DRY_{int(time.time() * 1000)}",
            "clientOrderId": f"CQ_DRY_{uuid.uuid4().hex[:8]}",
            "symbol": self.symbol,
            "side": side,
            "type": "MARKET",
            "status": "FILLED",
            "executedQty": str(quantity),
            "avgPrice": str(price),
            "dry_run": True,
        }

    @staticmethod
    def _make_client():
        from binance.client import Client
        env_cfg = utils.load_env_config()
        import json as _json
        with open(env_cfg["api"]["binance_keys_path"], encoding="utf-8") as f:
            keys = _json.load(f)
        api_key = (keys.get("api_key") or keys.get("key", "")).strip()
        api_secret = (keys.get("api_secret") or keys.get("secret", "")).strip()
        client = Client(api_key, api_secret)
        try:
            server_ms = client.get_server_time()["serverTime"]
            client.timestamp_offset = server_ms - int(time.time() * 1000)
        except Exception:
            pass
        return client
