#!/usr/bin/env python3
"""Inventory and sanity report for the multi-coin panel (epic_064 / t852).

Written to be read by a human before anything is modelled on this data. It
answers four questions in order:

    what is here          tables, row counts, columns, disk size
    how far back          per-coin coverage and where each source starts
    what does it look like  per-year price and volume, so an obviously wrong
                          number stands out without opening a chart
    what is broken        gaps, duplicates, nulls, and the places a naive
                          pipeline would silently swallow bad data

Usage:
    python -m data_handling.collectors.universe.panel_report
    python -m data_handling.collectors.universe.panel_report --symbol SOLUSDT
"""

import argparse
import logging
import os
import sys

import duckdb

import utils
from data_handling.collectors.universe.build_universe import DB_REL

logger = logging.getLogger(__name__)

TABLES = ("ohlcv_1m", "ohlcv_5m", "metrics_5m", "premium_5m", "funding")


def _conn() -> duckdb.DuckDBPyConnection:
    path = os.path.join(utils._repo_root(), DB_REL)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{DB_REL} hianyzik")
    return duckdb.connect(path, read_only=True)


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def inventory(conn: duckdb.DuckDBPyConnection) -> None:
    section("1. TABLAK")
    path = os.path.join(utils._repo_root(), DB_REL)
    print(f"  fajl: {DB_REL}   {os.path.getsize(path) / 1e9:.2f} GB\n")
    print(f"  {'tabla':<14}{'sor':>14}{'oszlop':>8}{'coin':>7}  elso .. utolso")
    for t in TABLES:
        try:
            ts_col = "funding_ts" if t == "funding" else "open_time"
            n, k, lo, hi = conn.execute(f"""
                SELECT count(*), count(DISTINCT symbol),
                       min({ts_col}), max({ts_col}) FROM {t}
            """).fetchone()  # type: ignore[misc]
            ncol = len(conn.execute(f"DESCRIBE {t}").fetchall())
            print(f"  {t:<14}{n:>14,}{ncol:>8}{k:>7}  {lo} .. {hi}")
        except duckdb.CatalogException:
            print(f"  {t:<14}{'-- nincs meg --':>14}")

    print("\n  oszlopok:")
    for t in TABLES:
        try:
            cols = [r[0] for r in conn.execute(f"DESCRIBE {t}").fetchall()]
            print(f"    {t:<14}{', '.join(cols)}")
        except duckdb.CatalogException:
            pass


def coverage(conn: duckdb.DuckDBPyConnection) -> None:
    section("2. COINONKENTI LEFEDETTSEG")
    print("  A metrics (OI + long/short) kesobb indul, mint az ar -- a")
    print("  derivativa-feature-ok csak onnantol hasznalhatok.\n")
    rows = conn.execute("""
        SELECT p.symbol,
               min(o.open_time)::DATE  AS ar_tol,
               max(o.open_time)::DATE  AS ar_ig,
               count(o.open_time)      AS n_1m,
               p.n_metrics, p.n_premium, p.n_funding
        FROM panel_log p
        JOIN ohlcv_1m o ON o.symbol = p.symbol
        GROUP BY p.symbol, p.n_metrics, p.n_premium, p.n_funding
        ORDER BY ar_tol, p.symbol
    """).fetchall()
    met = dict(conn.execute("""
        SELECT symbol, min(open_time)::DATE FROM metrics_5m GROUP BY 1
    """).fetchall())

    print(f"  {'symbol':<14}{'ar-tol':>12}{'ar-ig':>12}{'1m sor':>11}"
          f"{'metrics-tol':>13}{'metrics sor':>12}")
    for r in rows:
        m0 = met.get(r[0], "--")
        print(f"  {r[0]:<14}{r[1]!s:>12}{r[2]!s:>12}{r[3]:>11,}"
              f"{m0!s:>13}{r[4]:>12,}")


def yearly(conn: duckdb.DuckDBPyConnection) -> None:
    section("3. EVENKENTI OSSZESITES (az egesz panel)")
    print(f"  {'ev':>6}{'coin':>6}{'1m sor':>13}{'nap':>6}"
          f"{'napi forgalom (M USD)':>23}{'atlag n_trades/perc':>21}")
    for r in conn.execute("""
        SELECT strftime(open_time, '%Y') AS ev,
               count(DISTINCT symbol) AS coin,
               count(*) AS n,
               count(DISTINCT open_time::DATE) AS napok,
               sum(quote_volume) / count(DISTINCT open_time::DATE) / 1e6 AS napi_M,
               avg(n_trades) AS trades
        FROM ohlcv_1m GROUP BY 1 ORDER BY 1
    """).fetchall():
        print(f"  {r[0]:>6}{r[1]:>6}{r[2]:>13,}{r[3]:>6}{r[4]:>23,.0f}{r[5]:>21,.0f}")


def per_coin_year(conn: duckdb.DuckDBPyConnection, symbols: list[str]) -> None:
    section("4. COINONKENT / EVENKENT — atlagar es napi forgalom")
    print("  Az atlagar a nyers ar (nem osztaszorzott) -- a 1000-es prefixu")
    print("  parok (1000PEPE, 1000BONK, 1000SHIB) ezer coinra vonatkoznak.\n")
    for s in symbols:
        rows = conn.execute("""
            SELECT strftime(open_time, '%Y') AS ev,
                   avg(close) AS atlagar,
                   min(low) AS min_ar, max(high) AS max_ar,
                   sum(quote_volume) / count(DISTINCT open_time::DATE) / 1e6 AS napi_M
            FROM ohlcv_1m WHERE symbol = ? GROUP BY 1 ORDER BY 1
        """, [s]).fetchall()
        if not rows:
            continue
        print(f"  {s}")
        print(f"    {'ev':>6}{'atlagar':>14}{'min':>14}{'max':>14}"
              f"{'napi forgalom (M USD)':>23}")
        for r in rows:
            print(f"    {r[0]:>6}{r[1]:>14,.4f}{r[2]:>14,.4f}{r[3]:>14,.4f}"
                  f"{r[4]:>23,.1f}")
        print()


def quality(conn: duckdb.DuckDBPyConnection) -> None:
    section("5. ADATMINOSEG")

    print("\n  5a. Idobeli lyukak az 1m racson (coinonkent, > 1 perc ugras)")
    print("      Tozsdei kieses miatt normalis, hogy van nehany. A kerdes a nagysagrend.")
    rows = conn.execute("""
        WITH d AS (
            SELECT symbol, open_time,
                   datediff('minute',
                            lag(open_time) OVER (PARTITION BY symbol ORDER BY open_time),
                            open_time) AS perc
            FROM ohlcv_1m
        )
        SELECT symbol, count(*) AS lyukak, sum(perc - 1) AS hianyzo_perc,
               max(perc) AS legnagyobb
        FROM d WHERE perc > 1 GROUP BY 1 ORDER BY 3 DESC LIMIT 12
    """).fetchall()
    if not rows:
        print("      nincs lyuk")
    else:
        print(f"      {'symbol':<14}{'lyukak':>9}{'hianyzo perc':>15}{'legnagyobb':>12}")
        for r in rows:
            print(f"      {r[0]:<14}{r[1]:>9,}{r[2]:>15,}{r[3]:>12,}")

    print("\n  5b. Duplikatumok (a PK miatt 0-nak kell lennie)")
    for t in ("ohlcv_1m", "ohlcv_5m", "metrics_5m"):
        ts = "open_time"
        n = conn.execute(f"""
            SELECT count(*) FROM (
                SELECT symbol, {ts} FROM {t} GROUP BY 1,2 HAVING count(*) > 1)
        """).fetchone()
        print(f"      {t:<14}{n[0] if n else 0:>10,}")

    print("\n  5c. Gyanus arak (nem pozitiv, vagy high < low)")
    r = conn.execute("""
        SELECT count(*) FILTER (WHERE close <= 0 OR open <= 0),
               count(*) FILTER (WHERE high < low),
               count(*) FILTER (WHERE volume < 0)
        FROM ohlcv_1m
    """).fetchone()
    bad = r or (0, 0, 0)
    print(f"      nem pozitiv ar: {bad[0]:,}   high<low: {bad[1]:,}   "
          f"negativ volumen: {bad[2]:,}")

    print("\n  5d. Nulla forgalmu percek (arjegyzes van, kotes nincs)")
    for r in conn.execute("""
        SELECT symbol,
               round(100.0 * avg(CASE WHEN volume = 0 THEN 1.0 ELSE 0 END), 2) AS nulla_pct
        FROM ohlcv_1m GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """).fetchall():
        print(f"      {r[0]:<14}{r[1]:>7.2f}%")

    print("\n  5e. metrics NULL-arany (a derivativa-feature-ok hasznalhatosaga)")
    for r in conn.execute("""
        SELECT symbol,
               round(100.0 * avg(CASE WHEN oi_coin IS NULL THEN 1.0 ELSE 0 END), 2),
               round(100.0 * avg(CASE WHEN ls_count IS NULL THEN 1.0 ELSE 0 END), 2)
        FROM metrics_5m GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """).fetchall():
        print(f"      {r[0]:<14} oi_coin NULL {r[1]:>6.2f}%   ls_count NULL {r[2]:>6.2f}%")


def samples(conn: duckdb.DuckDBPyConnection, symbol: str) -> None:
    section(f"6. MINTASOROK — {symbol}")
    for t, ts, cols in (
        ("ohlcv_1m", "open_time",
         "open_time, open, high, low, close, volume, quote_volume, n_trades"),
        ("metrics_5m", "open_time",
         "open_time, oi_coin, oi_value, ls_count, ls_top_count, taker_vol_ratio"),
        ("premium_5m", "open_time", "open_time, prem_open, prem_close"),
        ("funding", "funding_ts", "funding_ts, funding_rate"),
    ):
        try:
            df = conn.execute(
                f"SELECT {cols} FROM {t} WHERE symbol = ? ORDER BY {ts} DESC LIMIT 4",
                [symbol],
            ).df()
        except duckdb.CatalogException:
            continue
        print(f"\n  {t}  (a 4 legutolso sor)")
        print("    " + df.to_string(index=False).replace("\n", "\n    "))


def main() -> None:
    parser = argparse.ArgumentParser(description="Panel riport")
    parser.add_argument("--symbol", default="SOLUSDT",
                        help="Melyik coinbol mutasson mintasorokat")
    parser.add_argument("--no-per-coin", action="store_true",
                        help="A coinonkenti/evenkenti blokk elhagyasa")
    args = parser.parse_args()

    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    conn = _conn()
    try:
        inventory(conn)
        coverage(conn)
        yearly(conn)
        if not args.no_per_coin:
            symbols = [
                r[0] for r in conn.execute(
                    "SELECT DISTINCT symbol FROM ohlcv_1m ORDER BY 1"
                ).fetchall()
            ]
            per_coin_year(conn, symbols)
        quality(conn)
        samples(conn, args.symbol)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
