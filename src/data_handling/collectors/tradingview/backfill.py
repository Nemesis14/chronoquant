#!/usr/bin/env python3
"""Two-phase TradingView idea backfill (epic_051 / t2).

Phase 1 — discover : symbol=BTCUSD|BTCUSDT paged to the query cap, yielding the
                     set of authors active in the covered window (~2 months).
Phase 2 — authors  : by={username} for every discovered author, paged to
                     page_count. Returns that author's full history across all
                     symbols; filtering to BTC is deliberately left to t3.

Nothing is normalised here. Raw responses land in the raw layer untouched, per
the raw/ vs calls/ split in plan.md.

Usage:
    python -m data_handling.collectors.tradingview.backfill discover
    python -m data_handling.collectors.tradingview.backfill authors
    python -m data_handling.collectors.tradingview.backfill status
"""

import argparse
import json
import logging
import os
import sys

import utils
from data_handling.collectors.tradingview.api import (
    PER_PAGE,
    QUERY_RESULT_CAP,
    IdeasClient,
    RateLimitStop,
)

logger = logging.getLogger(__name__)

#: BTC symbol namespaces to seed discovery with. BTCUSD and BTCUSDT are the
#: same asset on different venues; TradingView keeps them on separate pages.
DISCOVERY_SYMBOLS = ("BTCUSD", "BTCUSDT")

RAW_SUBDIR = os.path.join("data", "analyst_signal", "raw", "source=tradingview")

#: Author list produced by phase 1 and consumed by phase 2.
AUTHORS_FILE = "_authors.json"


# %% Paths

def raw_dir() -> str:
    """Absolute path of the TradingView raw layer."""
    return os.path.join(utils._repo_root(), RAW_SUBDIR)


# %% Phase 1 — discovery

def discover(client: IdeasClient, symbols: tuple[str, ...] = DISCOVERY_SYMBOLS) -> dict:
    """Page the symbol queries and collect the active-author set.

    The API caps any single query at QUERY_RESULT_CAP results, so this reaches
    roughly the last two months. That is intentional: only authors still posting
    can produce future signal (see epic.md, BTC-first).

    Args:
        client  : Configured IdeasClient.
        symbols : Symbol filters to seed discovery with.

    Returns:
        Summary dict with per-symbol counts and the merged author map.
    """
    authors: dict[str, dict] = {}
    per_symbol: dict[str, dict] = {}

    for symbol in symbols:
        logger.info("Felderites: symbol=%s", symbol)
        pages = client.fetch_all_pages(f"discover/{symbol}", symbol=symbol)
        ideas = [row for page in pages for row in page.get("results", [])]
        for row in ideas:
            user = row.get("user") or {}
            username = user.get("username")
            if not username:
                continue
            entry = authors.setdefault(username, {
                "user_id": user.get("id"),
                "username": username,
                "seen_in": [],
                "ideas_seen": 0,
            })
            entry["ideas_seen"] += 1
            if symbol not in entry["seen_in"]:
                entry["seen_in"].append(symbol)

        total = int(pages[0].get("count") or 0) if pages else 0
        per_symbol[symbol] = {
            "count_reported": total,
            "capped": total >= QUERY_RESULT_CAP,
            "pages": len(pages),
            "ideas_collected": len(ideas),
        }
        logger.info("  %s: %d idea, %d lap, count=%d%s",
                    symbol, len(ideas), len(pages), total,
                    " (PLAFONON)" if total >= QUERY_RESULT_CAP else "")

    summary = {"per_symbol": per_symbol, "authors": authors, "author_count": len(authors)}
    dest = os.path.join(client.raw_dir, AUTHORS_FILE)
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=1)
    logger.info("Felderites kesz: %d szerzo -> %s", len(authors), dest)
    return summary


# %% Phase 2 — author histories

def fetch_authors(client: IdeasClient, limit: int | None = None) -> dict:
    """Fetch the full history of every discovered author.

    Resumable: pages already present in the raw layer cost no request, so an
    interrupted run can simply be restarted.

    Args:
        client : Configured IdeasClient.
        limit  : Optional cap on the number of authors processed this run.

    Returns:
        Per-author stats: reported count, pages fetched, whether capped.
    """
    authors_path = os.path.join(client.raw_dir, AUTHORS_FILE)
    if not os.path.exists(authors_path):
        raise FileNotFoundError(
            f"Nincs szerzolista ({authors_path}). Futtasd eloszor: discover"
        )
    with open(authors_path, encoding="utf-8") as fh:
        usernames = sorted(json.load(fh)["authors"])

    if limit is not None:
        usernames = usernames[:limit]

    stats: dict[str, dict] = {}
    for index, username in enumerate(usernames, start=1):
        safe = _safe_name(username)
        try:
            pages = client.fetch_all_pages(f"authors/{safe}", by=username)
        except RateLimitStop:
            logger.error("Leallitva a(z) %d./%d szerzonel (%s). Ujrainditas folytatja.",
                         index, len(usernames), username)
            raise
        total = int(pages[0].get("count") or 0) if pages else 0
        stats[username] = {
            "count_reported": total,
            "capped": total >= QUERY_RESULT_CAP,
            "pages": len(pages),
            "ideas_collected": sum(len(p.get("results", [])) for p in pages),
        }
        if index % 25 == 0 or index == len(usernames):
            logger.info("  %d/%d szerzo kesz (%d keres eddig)",
                        index, len(usernames), client.request_count)

    dest = os.path.join(client.raw_dir, "_author_stats.json")
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(stats, fh, ensure_ascii=False, indent=1)
    logger.info("Szerzo-history kesz: %d szerzo -> %s", len(stats), dest)
    return stats


# %% Status

def status(client: IdeasClient) -> dict:
    """Report what the raw layer currently holds, without any request."""
    def count_files(sub: str) -> int:
        path = os.path.join(client.raw_dir, sub)
        return len(os.listdir(path)) if os.path.isdir(path) else 0

    manifest_lines = 0
    if os.path.exists(client.manifest_path):
        with open(client.manifest_path, encoding="utf-8") as fh:
            manifest_lines = sum(1 for _ in fh)

    authors_known = 0
    authors_path = os.path.join(client.raw_dir, AUTHORS_FILE)
    if os.path.exists(authors_path):
        with open(authors_path, encoding="utf-8") as fh:
            authors_known = len(json.load(fh)["authors"])

    report = {
        "raw_dir": client.raw_dir,
        "discover_pages": count_files("discover"),
        "author_pages": count_files("authors"),
        "authors_known": authors_known,
        "manifest_entries": manifest_lines,
    }
    for key, value in report.items():
        logger.info("  %-18s %s", key, value)
    return report


# %% Helpers

def _safe_name(username: str) -> str:
    """Filesystem-safe filename stem for an arbitrary username."""
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in username)


# %% CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["discover", "authors", "status"])
    parser.add_argument("--limit", type=int, default=None,
                        help="Authors phase: process at most N authors this run.")
    parser.add_argument("--dry-run", action="store_true",
                        help="No HTTP; cached pages are still served.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    client = IdeasClient(raw_dir(), dry_run=args.dry_run)
    logger.info("Raw layer: %s  (per_page=%d)", client.raw_dir, PER_PAGE)

    try:
        if args.phase == "discover":
            discover(client)
        elif args.phase == "authors":
            fetch_authors(client, limit=args.limit)
        else:
            status(client)
    except RateLimitStop as exc:
        logger.error("%s", exc)
        return 2

    logger.info("Kesz. Keresek ebben a futasban: %d", client.request_count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
