#!/usr/bin/env python3
"""Binance forced-liquidation stream collector (epic_054 / t302).

The only derivatives data that cannot be backfilled. The t301 survey confirmed
that Binance Vision carries no `liquidationSnapshot` archive at either daily or
monthly granularity, and the REST statistics endpoints do not expose
liquidations at all. Whatever is not captured live is gone permanently -- which
is why this runs independently of the modelling chain.

⚠️ KNOWN DATA-QUALITY LIMIT: Binance has throttled the `@forceOrder` stream to
roughly one event per second per symbol since 2021. The captured liquidation
volume is therefore a systematic UNDERCOUNT, and it undercounts worst during
violent moves -- exactly when the signal would matter most. Any analysis built
on this data must state that limit; it is not a detail.

Storage follows the epic_050 decision: append-only Parquet, never a DuckDB
write. DuckDB is single-writer, so a collector holding the database would make
every notebook read fail (and vice versa).

Usage:
    python -m data_handling.collectors.derivatives.liquidations
    python -m data_handling.collectors.derivatives.liquidations --duration-hours 24
    python -m data_handling.collectors.derivatives.liquidations --stats
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from datetime import UTC, datetime

import polars as pl
import websockets

import utils

logger = logging.getLogger(__name__)

WS_BASE = "wss://fstream.binance.com/ws"
RAW_SUBDIR = os.path.join("data", "analyst_signal", "raw", "source=liquidations")
LOG_SUBDIR = os.path.join("data", "analyst_signal", "collection_log")

FLUSH_EVERY_N = 50
FLUSH_EVERY_S = 300.0
RECONNECT_BASE_S = 2.0
RECONNECT_MAX_S = 60.0

EVENT_SCHEMA = {
    "value_ts": pl.Datetime("us"),
    "observed_at": pl.Datetime("us"),
    "symbol": pl.Utf8,
    "side": pl.Utf8,
    "order_type": pl.Utf8,
    "time_in_force": pl.Utf8,
    "qty": pl.Float64,
    "price": pl.Float64,
    "avg_price": pl.Float64,
    "status": pl.Utf8,
    "last_filled_qty": pl.Float64,
    "filled_accum_qty": pl.Float64,
}


def _raw_dir() -> str:
    return os.path.join(utils._repo_root(), RAW_SUBDIR)


def _log_dir() -> str:
    return os.path.join(utils._repo_root(), LOG_SUBDIR)


def _parse_event(payload: dict) -> dict | None:
    """Map one `@forceOrder` message to a flat row.

    Returns None for anything that is not a liquidation order, so an unexpected
    control frame cannot poison the buffer.
    """
    order = payload.get("o")
    if not isinstance(order, dict):
        return None

    def _f(key: str) -> float | None:
        raw = order.get(key)
        try:
            return float(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    trade_ms = order.get("T") or payload.get("E")
    if trade_ms is None:
        return None

    return {
        # value_ts: when the liquidation happened on the exchange.
        "value_ts": datetime.fromtimestamp(int(trade_ms) / 1000, tz=UTC).replace(tzinfo=None),
        # observed_at: when it reached us. The pair is what makes a
        # point-in-time reconstruction possible later.
        "observed_at": datetime.now(UTC).replace(tzinfo=None),
        "symbol": order.get("s"),
        "side": order.get("S"),
        "order_type": order.get("o"),
        "time_in_force": order.get("f"),
        "qty": _f("q"),
        "price": _f("p"),
        "avg_price": _f("ap"),
        "status": order.get("X"),
        "last_filled_qty": _f("l"),
        "filled_accum_qty": _f("z"),
    }


def _flush(buffer: list[dict]) -> int:
    """Append the buffer to the current day's Parquet partition.

    Parquet has no append mode, so the day file is read back and rewritten.
    Daily files stay small (the throttled stream yields hundreds of rows a day,
    not millions), so this is cheap and keeps one file per day.
    """
    if not buffer:
        return 0

    os.makedirs(_raw_dir(), exist_ok=True)
    day = buffer[0]["value_ts"].strftime("%Y-%m-%d")
    path = os.path.join(_raw_dir(), f"liquidations-{day}.parquet")

    fresh = pl.DataFrame(buffer, schema=EVENT_SCHEMA)
    if os.path.exists(path):
        fresh = pl.concat([pl.read_parquet(path), fresh], how="vertical_relaxed")

    # A reconnect can replay the last message; dedup on the exchange-side
    # identity of the event rather than on arrival time.
    fresh = fresh.unique(
        subset=["value_ts", "symbol", "side", "qty", "price"], keep="first"
    ).sort("value_ts")
    fresh.write_parquet(path)

    buffer.clear()
    return fresh.height


def _log_run(event: str, **fields: object) -> None:
    """Append one line to the collection log (JSONL)."""
    os.makedirs(_log_dir(), exist_ok=True)
    path = os.path.join(_log_dir(), "liquidations.jsonl")
    row = {"ts": datetime.now(UTC).isoformat(), "event": event, **fields}
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row) + "\n")


async def collect(symbol: str, duration_hours: float | None = None) -> None:
    """Stream liquidations until interrupted or the duration elapses.

    Reconnects with exponential backoff. Every gap is logged, because for this
    data source a gap cannot be filled in afterwards -- the log is the only
    record of what was missed.
    """
    stream = f"{WS_BASE}/{symbol.lower()}@forceOrder"
    deadline = (
        asyncio.get_event_loop().time() + duration_hours * 3600
        if duration_hours
        else None
    )

    buffer: list[dict] = []
    total = 0
    backoff = RECONNECT_BASE_S
    stopping = asyncio.Event()

    def _request_stop(*_: object) -> None:
        stopping.set()

    try:
        signal.signal(signal.SIGINT, _request_stop)
        signal.signal(signal.SIGTERM, _request_stop)
    except ValueError:
        pass  # not on the main thread; caller handles shutdown

    _log_run("start", symbol=symbol, stream=stream)
    logger.info("likvidacios gyujtes indul: %s", stream)

    while not stopping.is_set():
        if deadline and asyncio.get_event_loop().time() >= deadline:
            break
        connected_at = datetime.now(UTC)
        try:
            async with websockets.connect(stream, ping_interval=180, ping_timeout=600) as ws:
                backoff = RECONNECT_BASE_S
                _log_run("connected", symbol=symbol)
                logger.info("kapcsolodva")
                last_flush = asyncio.get_event_loop().time()

                while not stopping.is_set():
                    if deadline and asyncio.get_event_loop().time() >= deadline:
                        break
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except TimeoutError:
                        # Silence is normal: liquidations are sparse. Flush on
                        # the time trigger so a quiet hour is not lost if the
                        # process dies before the count trigger fires.
                        now = asyncio.get_event_loop().time()
                        if buffer and now - last_flush >= FLUSH_EVERY_S:
                            _flush(buffer)
                            last_flush = now
                        continue

                    row = _parse_event(json.loads(msg))
                    if row is None:
                        continue
                    buffer.append(row)
                    total += 1

                    now = asyncio.get_event_loop().time()
                    if len(buffer) >= FLUSH_EVERY_N or now - last_flush >= FLUSH_EVERY_S:
                        _flush(buffer)
                        last_flush = now

        except (websockets.WebSocketException, OSError) as exc:
            uptime = (datetime.now(UTC) - connected_at).total_seconds()
            _flush(buffer)
            _log_run("disconnected", symbol=symbol, uptime_s=round(uptime, 1), error=str(exc))
            logger.warning("kapcsolat megszakadt %.0f mp utan: %s", uptime, exc)
            if stopping.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_S)

    _flush(buffer)
    _log_run("stop", symbol=symbol, events_total=total)
    logger.info("gyujtes vege, osszesen %d esemeny", total)


def stats() -> pl.DataFrame:
    """Per-day event count and volume from the raw layer."""
    pattern = os.path.join(_raw_dir(), "liquidations-*.parquet")
    try:
        frame = pl.read_parquet(pattern)
    except FileNotFoundError:
        return pl.DataFrame()
    if frame.is_empty():
        return frame

    return (
        frame.with_columns(pl.col("value_ts").dt.date().alias("nap"))
        .group_by("nap")
        .agg(
            pl.len().alias("esemeny"),
            pl.col("side").eq("SELL").sum().alias("long_likvidalas"),
            pl.col("side").eq("BUY").sum().alias("short_likvidalas"),
            (pl.col("qty") * pl.col("avg_price")).sum().round(0).alias("notional_usd"),
        )
        .sort("nap")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Binance liquidation stream collector")
    parser.add_argument("--symbol", default="SOLUSDT")
    parser.add_argument(
        "--duration-hours", type=float, default=None,
        help="Stop after this many hours (default: run until interrupted)",
    )
    parser.add_argument("--stats", action="store_true", help="Napi osszesito a raw retegbol")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.stats:
        report = stats()
        print(report if not report.is_empty() else "meg nincs gyujtott adat")
        return

    asyncio.run(collect(args.symbol, args.duration_hours))


if __name__ == "__main__":
    main()
