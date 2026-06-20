# =============================================================================
# Binance API data access for the Streamlit dashboard
# =============================================================================
# Purpose:
#  - Fetch live trade history from Binance (spot or futures)
#  - Read-only; never places or modifies orders
# =============================================================================

from __future__ import annotations

import json
import logging
import time

import pandas as pd

import utils

_logger = logging.getLogger("chronoquant.streamlit")


# =============================================================================
# _make_client() -> Client
# =============================================================================
def _make_client():
    from binance.client import Client  # noqa: PLC0415

    env_cfg    = utils.load_env_config()
    keys_path  = env_cfg["api"]["binance_keys_path"]
    with open(keys_path, encoding="utf-8") as f:
        keys = json.load(f)
    api_key    = (keys.get("api_key") or keys.get("key", "")).strip()
    api_secret = (keys.get("api_secret") or keys.get("secret", "")).strip()
    client = Client(api_key, api_secret)

    # Sync local clock with Binance server time to avoid -1022 signature errors.
    # get_server_time() is unauthenticated so it works even when keys are restricted.
    try:
        server_ms = client.get_server_time()["serverTime"]
        client.timestamp_offset = server_ms - int(time.time() * 1000)
    except Exception:
        pass

    return client


# =============================================================================
# recent_trades(asset_id, limit) -> pd.DataFrame
# =============================================================================
# Purpose:
#  - Fetch the most recent trade fills from Binance.
#  - Tries futures account trades first (includes realizedPnl).
#  - Falls back to spot trade history when futures returns nothing or fails.
#  - Returns empty DataFrame on any error so the UI degrades gracefully.
#
# Returned columns (always present):
#   time        : pd.Timestamp (UTC)
#   side        : str  — "BUY" or "SELL"
#   price       : float
#   qty         : float
#   quote_qty   : float
#   pnl         : float | None  — realized PnL (futures only)
#   commission  : float
#   source      : str  — "futures" or "spot"
# =============================================================================
def recent_trades(asset_id: str | None = None, limit: int = 50) -> pd.DataFrame:
    try:
        cfg    = utils.load_asset_config(asset_id)
        symbol = cfg["database"]["symbol"]
        client = _make_client()
    except Exception as exc:
        _logger.warning("Binance client init failed: %s", exc)
        return pd.DataFrame()

    # Try futures first
    try:
        rows = client.futures_account_trades(symbol=symbol, limit=limit)
        if rows:
            return _normalize_futures(rows)  # type: ignore[arg-type]
    except Exception as exc:
        _logger.debug("Futures trade fetch failed (%s), trying spot", exc)

    # Fall back to spot
    try:
        rows = client.get_my_trades(symbol=symbol, limit=limit)
        if rows:
            return _normalize_spot(rows)  # type: ignore[arg-type]
    except Exception as exc:
        _logger.warning("Spot trade fetch also failed: %s", exc)

    return pd.DataFrame()


# =============================================================================
# _normalize_futures / _normalize_spot
# =============================================================================

def _normalize_futures(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    out = _empty_frame()
    out["time"]       = pd.to_datetime(pd.to_numeric(df.get("time", pd.Series()), errors="coerce"), unit="ms", utc=True)  # type: ignore[arg-type]
    out["side"]       = df.get("side", pd.Series(dtype=str)).str.upper()  # type: ignore[union-attr]
    out["price"]      = pd.to_numeric(df.get("price"),      errors="coerce")
    out["qty"]        = pd.to_numeric(df.get("qty"),        errors="coerce")
    out["quote_qty"]  = pd.to_numeric(df.get("quoteQty"),   errors="coerce")
    out["pnl"]        = pd.to_numeric(df.get("realizedPnl"),errors="coerce")
    out["commission"] = pd.to_numeric(df.get("commission"), errors="coerce")
    out["source"]     = "futures"
    return out.sort_values("time", ascending=False).reset_index(drop=True)


def _normalize_spot(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    out = _empty_frame()
    out["time"]       = pd.to_datetime(pd.to_numeric(df.get("time", pd.Series()), errors="coerce"), unit="ms", utc=True)  # type: ignore[arg-type]
    if "isBuyer" in df.columns:
        out["side"]   = df["isBuyer"].map({True: "BUY", False: "SELL"})  # type: ignore[arg-type]
    elif "side" in df.columns:
        out["side"]   = df["side"].str.upper()
    out["price"]      = pd.to_numeric(df.get("price"),     errors="coerce")
    out["qty"]        = pd.to_numeric(df.get("qty"),       errors="coerce")
    out["quote_qty"]  = pd.to_numeric(df.get("quoteQty"),  errors="coerce")
    out["pnl"]        = None
    out["commission"] = pd.to_numeric(df.get("commission"),errors="coerce")
    out["source"]     = "spot"
    return out.sort_values("time", ascending=False).reset_index(drop=True)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["time", "side", "price", "qty", "quote_qty", "pnl", "commission", "source"])
