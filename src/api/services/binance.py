"""Read-only Binance access (port of ``ui.binance_data``): recent fills and open position.

Never places or modifies orders.  Failures raise ``BinanceUnavailableError`` so the API
can answer ``503 binance_unavailable`` and the frontend degrades gracefully.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import UTC, datetime
from typing import Any

import utils

logger = logging.getLogger(__name__)

# Binance trade endpoints cap a single request at 1000 rows; the default window is returned
# in ASCENDING id order and truncated at the OLDEST end, so we always fetch the full window
# and keep the newest rows locally.
_FETCH_LIMIT = 1000


class BinanceUnavailableError(Exception):
    """Keys missing, client init failed, or both futures and spot calls failed."""


def _make_client():  # noqa: ANN202 — python-binance Client, imported lazily
    from binance.client import Client  # noqa: PLC0415

    keys_path = utils.load_env_config()["api"]["binance_keys_path"]
    with open(keys_path, encoding="utf-8") as f:
        keys = json.load(f)
    api_key = (keys.get("api_key") or keys.get("key", "")).strip()
    api_secret = (keys.get("api_secret") or keys.get("secret", "")).strip()
    client = Client(api_key, api_secret)
    # Sync the local clock with Binance to avoid -1022 signature errors (unauthenticated call).
    try:
        server_ms = client.get_server_time()["serverTime"]
        client.timestamp_offset = server_ms - int(time.time() * 1000)
    except Exception:
        logger.debug("binance server time sync failed", exc_info=True)
    return client


def _symbol() -> str:
    return str(utils.load_asset_config(None)["database"]["symbol"])


def _f(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_futures(rows: Any) -> list[dict[str, Any]]:
    """Closing fills only (realizedPnl != 0); BUY closes a SHORT, SELL closes a LONG."""
    out = []
    for r in rows:
        pnl = _f(r.get("realizedPnl"))
        if abs(pnl) < 0.001:
            continue
        side = str(r.get("side", "")).upper()
        out.append({
            "time_ms": int(_f(r.get("time"))),
            "side": {"BUY": "SHORT", "SELL": "LONG"}.get(side, side),
            "price": _f(r.get("price")), "qty": _f(r.get("qty")),
            "quote_qty": _f(r.get("quoteQty")), "pnl": pnl,
        })
    return out


def _normalize_spot(rows: Any) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        side = ("BUY" if r.get("isBuyer") else "SELL") if "isBuyer" in r else str(r.get("side", "")).upper()
        out.append({
            "time_ms": int(_f(r.get("time"))), "side": side,
            "price": _f(r.get("price")), "qty": _f(r.get("qty")),
            "quote_qty": _f(r.get("quoteQty")), "pnl": None,
        })
    return out


def group_by_minute(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge fills of the same minute+side (volume-weighted price, summed qty / pnl), newest first."""
    groups: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for fill in fills:
        key = (fill["time_ms"] // 60_000, fill["side"])
        groups.setdefault(key, []).append(fill)
    rows: list[dict[str, Any]] = []
    for (minute, side), grp in groups.items():
        qty = sum(g["qty"] for g in grp)
        quote = sum(g["quote_qty"] for g in grp)
        pnls = [g["pnl"] for g in grp if g["pnl"] is not None]
        rows.append({
            "t": datetime.fromtimestamp(minute * 60, UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "side": side,
            "price": quote / qty if qty > 0 else sum(g["price"] for g in grp) / len(grp),
            "qty": qty,
            "realized_pnl": sum(pnls) if pnls else None,
        })
    rows.sort(key=lambda r: r["t"], reverse=True)
    return rows


def recent_trades(limit: int) -> dict[str, Any]:
    """Last fills (futures -> spot fallback), grouped per minute.

    Raises:
        BinanceUnavailableError: client init failed or both endpoints failed.
    """
    try:
        client = _make_client()
        symbol = _symbol()
    except Exception as exc:
        logger.warning("Binance client init failed: %s", exc)
        raise BinanceUnavailableError(str(exc)) from exc

    try:
        rows = client.futures_account_trades(symbol=symbol, limit=_FETCH_LIMIT)
        if rows:
            return {"items": group_by_minute(_normalize_futures(rows))[:limit], "source": "futures"}
    except Exception as exc:
        logger.debug("futures trade fetch failed (%s), trying spot", exc)

    try:
        rows = client.get_my_trades(symbol=symbol, limit=_FETCH_LIMIT)
    except Exception as exc:
        logger.warning("spot trade fetch also failed: %s", exc)
        raise BinanceUnavailableError(str(exc)) from exc
    if not rows:
        return {"items": [], "source": "spot"}
    return {"items": group_by_minute(_normalize_spot(rows))[:limit], "source": "spot"}


# Open-position lookups are polled (dashboard chart + active trade) and each uncached call costs
# 4-6 s when the journal has no open position, so the result is memoised for a short TTL.
# Failures and "no position" results are cached too, so a down Binance is not hit on every poll.
POSITION_CACHE_TTL_S = 45.0
_monotonic = time.monotonic
_position_lock = threading.Lock()
_position_cache: tuple[float, dict[str, Any] | None] | None = None  # (expires_at, value)


def clear_position_cache() -> None:
    """Drop the memoised open-position result (tests / forced refresh)."""
    global _position_cache
    with _position_lock:
        _position_cache = None


def current_position() -> dict[str, Any] | None:
    """The open Binance futures position for the asset (cached ``POSITION_CACHE_TTL_S``), or None."""
    global _position_cache
    with _position_lock:
        now = _monotonic()
        if _position_cache is not None and now < _position_cache[0]:
            cached = _position_cache[1]
            return dict(cached) if cached is not None else None
        value = _fetch_current_position()
        _position_cache = (_monotonic() + POSITION_CACHE_TTL_S, value)
        return dict(value) if value is not None else None


def _fetch_current_position() -> dict[str, Any] | None:
    """Uncached Binance lookup; errors are logged and swallowed (-> None)."""
    try:
        client = _make_client()
        for p in client.futures_position_information(symbol=_symbol()):
            amt = float(p.get("positionAmt", 0))
            if amt == 0.0:
                continue
            return {
                "side": "LONG" if amt > 0 else "SHORT",
                "entry_time": None,
                "entry_price": float(p.get("entryPrice", 0)),
                "quantity": abs(amt),
                "stop_loss": None,
                "take_profit": None,
                "source": "binance",
            }
    except Exception as exc:
        logger.warning("Binance current_position failed: %s", exc)
    return None
