#!/usr/bin/env python3
"""Normalise the TradingView raw layer into source-agnostic calls (epic_051 / t3).

Reads data/analyst_signal/raw/source=tradingview/ and writes the calls table in
database/analyst_signal/analyst_signal.duckdb. The raw layer is never modified.

Four things happen here:
  filter   - which raw ideas count as a call at all
  map      - TradingView field -> plan.md call field
  decode   - direction 2 -> -1, interval "1D" -> 1440 minutes
  identify - idempotent call_id from the source-local publication id

Usage:
    python -m data_handling.collectors.tradingview.normalize
    python -m data_handling.collectors.tradingview.normalize --dry-run
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import UTC, datetime

import polars as pl

import utils
from data_handling.collectors.tradingview.backfill import raw_dir
from data_handling.store.analyst_signal_store import (
    DB_SUBPATH,
    ensure_tables,
    get_connection,
    make_call_id,
    upsert_calls,
)

logger = logging.getLogger(__name__)

SOURCE = "tradingview"

#: Normalised asset every whitelisted symbol maps onto.
TARGET_ASSET = "BTCUSDT"

#: TradingView short_names that genuinely track the BTC/USD spot price.
#: A substring match on BTC would also catch BTC.D (dominance index), ETHBTC
#: and other altcoin/BTC pairs, BTC1! and dated futures, and BTCUSDSHORTS --
#: none of which are BTC price calls.
SYMBOL_WHITELIST = frozenset({
    "BTCUSD",       # spot
    "BTCUSDT",      # spot
    "BTCUSDT.P",    # perpetual, tracks spot
    "BTCUSD.P",     # perpetual
    "BTCUSDC.P",    # perpetual, USDC-quoted
    "BTCPERP",      # perpetual
})
# Deliberately excluded although the name contains BTC: BTC.D (dominance index),
# BTC1! (CME continuous future -- basis and roll artefacts), BTCUSDSHORTS
# (positioning indicator), and every altcoin/BTC pair such as ETHBTC, where a
# long call means the altcoin beats BTC, not that BTC rises.

#: symbol.direction encoding, verified against TradingView bundle source.
DIRECTION_MAP = {1: 1, 2: -1}

_INTERVAL_RE = re.compile(r"^(\d*)([A-Z]?)$")

#: Multipliers to minutes for TradingView interval suffixes. A bare number is
#: already minutes. R is a range-bar count, not a time horizon, so it maps to
#: NULL rather than to a misleading number.
_INTERVAL_UNITS = {"": 1, "H": 60, "D": 1440, "W": 10080, "M": 43200}


# %% Decoding

def interval_to_minutes(raw: str | None) -> int | None:
    """Convert a TradingView interval string to minutes.

    Handles bare minute counts (240), suffixed units (1D, W, 12H), and returns
    None for range bars (8192R) and anything unrecognised.

    Args:
        raw : The symbol.interval value, e.g. "240", "1D", "8192R".

    Returns:
        Horizon in minutes, or None if the value is not a time horizon.
    """
    if not raw:
        return None
    match = _INTERVAL_RE.match(str(raw).strip().upper())
    if not match:
        return None
    digits, unit = match.groups()
    if unit not in _INTERVAL_UNITS:
        return None
    count = int(digits) if digits else 1
    return count * _INTERVAL_UNITS[unit]


def _parse_ts(value: str | None) -> datetime | None:
    """Parse a TradingView ISO timestamp into a naive UTC datetime."""
    if not value:
        return None
    return datetime.fromisoformat(value).astimezone(UTC).replace(tzinfo=None)


# %% Loading

def load_raw_with_provenance(root: str) -> tuple[list[tuple[dict, str]], dict[str, str]]:
    """Load every raw idea together with the file it came from.

    Args:
        root : Raw-layer root directory.

    Returns:
        Pairs of (idea, rel_path), and the manifest map rel_path -> fetched_at_utc.
    """
    manifest: dict[str, str] = {}
    manifest_path = os.path.join(root, "_manifest.jsonl")
    if os.path.exists(manifest_path):
        with open(manifest_path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                rel_path = entry.get("rel_path")
                if rel_path:
                    manifest[rel_path.replace("\\", "/")] = entry.get("fetched_at_utc")

    seen: dict[int, tuple[dict, str]] = {}
    for sub in ("authors", "discover"):
        folder = os.path.join(root, sub)
        if not os.path.isdir(folder):
            continue
        for name in sorted(os.listdir(folder)):
            if not name.endswith(".json"):
                continue
            rel = f"{sub}/{name}"
            with open(os.path.join(folder, name), encoding="utf-8") as fh:
                try:
                    payload = json.load(fh)
                except json.JSONDecodeError:
                    logger.warning("Hibas JSON, kihagyva: %s", rel)
                    continue
            for row in payload.get("results", []):
                if isinstance(row.get("id"), int) and row["id"] not in seen:
                    seen[row["id"]] = (row, rel)
    return list(seen.values()), manifest


# %% Normalisation

_SCHEMA = {
    "call_id":          pl.Utf8,
    "source":           pl.Utf8,
    "native_id":        pl.Utf8,
    "native_uuid":      pl.Utf8,
    "author_id":        pl.Utf8,
    "author_name":      pl.Utf8,
    "asset":            pl.Utf8,
    "native_symbol":    pl.Utf8,
    "direction":        pl.Int16,
    "ts_utc":           pl.Datetime("us"),
    "ts_updated_utc":   pl.Datetime("us"),
    "horizon_hint_min": pl.Int32,
    "horizon_hint_raw": pl.Utf8,
    "confidence":       pl.Float64,
    "comments_count":   pl.Int64,
    "views_count":      pl.Int64,
    "extraction":       pl.Utf8,
    "raw_ref":          pl.Utf8,
    "first_seen_utc":   pl.Datetime("us"),
    "ingested_at_utc":  pl.Datetime("us"),
}


def normalise(
    pairs: list[tuple[dict, str]],
    manifest: dict[str, str],
) -> tuple[pl.DataFrame, dict]:
    """Turn raw TradingView ideas into call rows.

    Args:
        pairs    : (idea, rel_path) pairs from load_raw_with_provenance.
        manifest : rel_path -> fetched_at_utc, used for first_seen_utc.

    Returns:
        The calls DataFrame and a dict of drop counts by reason.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    rows: list[dict] = []
    dropped = {
        "symbol_not_whitelisted": 0,
        "is_education": 0,
        "is_script": 0,
        "no_direction": 0,
        "no_timestamp": 0,
    }
    excluded_symbols: dict[str, int] = {}

    for idea, rel in pairs:
        symbol = idea.get("symbol") or {}
        short_name = symbol.get("short_name")

        if short_name not in SYMBOL_WHITELIST:
            dropped["symbol_not_whitelisted"] += 1
            if short_name:
                excluded_symbols[short_name] = excluded_symbols.get(short_name, 0) + 1
            continue
        if idea.get("is_education"):
            dropped["is_education"] += 1
            continue
        if idea.get("is_script"):
            dropped["is_script"] += 1
            continue

        dir_code = symbol.get("direction")
        direction = DIRECTION_MAP.get(dir_code) if isinstance(dir_code, int) else None
        if direction is None:
            dropped["no_direction"] += 1
            continue

        ts = _parse_ts(idea.get("created_at"))
        if ts is None:
            dropped["no_timestamp"] += 1
            continue

        user = idea.get("user") or {}
        native_id = str(idea["id"])
        raw_interval = symbol.get("interval")
        user_id = user.get("id")

        rows.append({
            "call_id":          make_call_id(SOURCE, native_id),
            "source":           SOURCE,
            "native_id":        native_id,
            "native_uuid":      idea.get("image_url"),
            "author_id":        str(user_id) if user_id else None,
            "author_name":      user.get("username"),
            "asset":            TARGET_ASSET,
            "native_symbol":    symbol.get("full_name") or short_name,
            "direction":        direction,
            # Always created_at, never updated_at: the listing page shows the
            # edit time, which ran up to 90 hours later (see t1 findings).
            "ts_utc":           ts,
            "ts_updated_utc":   _parse_ts(idea.get("updated_at")),
            "horizon_hint_min": interval_to_minutes(raw_interval),
            "horizon_hint_raw": str(raw_interval) if raw_interval else None,
            "confidence":       float(idea.get("likes_count") or 0),
            "comments_count":   int(idea.get("comments_count") or 0),
            "views_count":      int(idea.get("views_count") or 0),
            "extraction":       "native",
            "raw_ref":          rel,
            "first_seen_utc":   _parse_ts(manifest.get(rel)),
            "ingested_at_utc":  now,
        })

    df = pl.DataFrame(rows, schema=_SCHEMA)
    if df.height:
        df = df.sort("ts_utc")
    stats = {
        "raw_ideas": len(pairs),
        "calls": df.height,
        "dropped": dropped,
        "excluded_symbols_top": sorted(excluded_symbols.items(), key=lambda x: -x[1])[:15],
    }
    return df, stats


def print_stats(df: pl.DataFrame, stats: dict) -> None:
    """Render the normalisation summary to stdout."""
    print("\n=== NORMALIZALAS ===")
    print(f"  nyers idea        : {stats['raw_ideas']:>8,d}")
    print(f"  call              : {stats['calls']:>8,d}")
    print("\n  kiszurve:")
    for reason, count in stats["dropped"].items():
        print(f"    {reason:26s} {count:>8,d}")
    print("\n  kiszurt szimbolumok (top 15):")
    for name, count in stats["excluded_symbols_top"]:
        print(f"    {name:26s} {count:>8,d}")
    if not df.height:
        return
    counts = dict(df["direction"].value_counts().iter_rows())
    print(f"\n  irany   : long={counts.get(1, 0):,d}  short={counts.get(-1, 0):,d}")
    print(f"  idoszak : {df['ts_utc'].min()}  ..  {df['ts_utc'].max()}")
    print(f"  szerzo  : {df['author_id'].n_unique():,d}")
    print(f"  horizon_hint_min NULL : {df['horizon_hint_min'].null_count():,d} / {df.height:,d}")
    print(f"  first_seen_utc  NULL  : {df['first_seen_utc'].null_count():,d} / {df.height:,d}")


# %% CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Normalise and report, but do not write the database.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    pairs, manifest = load_raw_with_provenance(raw_dir())
    logger.info("Nyers idea: %d  (manifest bejegyzes: %d)", len(pairs), len(manifest))

    df, stats = normalise(pairs, manifest)
    print_stats(df, stats)

    if args.dry_run:
        logger.info("dry-run: nincs iras")
        return 0

    db_path = os.path.join(utils._repo_root(), DB_SUBPATH)
    conn = get_connection(db_path)
    try:
        ensure_tables(conn)
        before = conn.execute("SELECT COUNT(*) FROM calls").fetchone()
        upsert_calls(conn, df)
        after = conn.execute("SELECT COUNT(*) FROM calls").fetchone()
        logger.info("calls: %d -> %d sor  (%s)",
                    before[0] if before else 0, after[0] if after else 0, db_path)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
