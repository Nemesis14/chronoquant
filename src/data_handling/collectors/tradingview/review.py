#!/usr/bin/env python3
"""Data-quality review of the TradingView raw layer (epic_051 / t2).

Read-only. Answers what was actually collected and whether it can carry the
author-skill measurement: volume, per-author depth, field fill rates, direction
coverage, date span — and, explicitly, which fields are absent.

Usage:
    python -m data_handling.collectors.tradingview.review
    python -m data_handling.collectors.tradingview.review --json report.json
"""

import argparse
import json
import logging
import os
import sys
from collections import Counter

from data_handling.collectors.tradingview.backfill import raw_dir

logger = logging.getLogger(__name__)

#: symbol.direction encoding, verified against TradingView's own bundle source.
DIRECTION_LABELS = {1: "long", 2: "short", 0: "none"}

#: Minimum BTC calls per author for a shrinkage-based skill estimate to mean
#: anything. Below this the author is noise, not signal (see s102 Notes).
MIN_CALLS_PER_AUTHOR = 10


# %% Loading

def load_ideas(root: str) -> list[dict]:
    """Load every idea record from the raw layer, de-duplicated by idea id.

    Args:
        root : Raw-layer root directory.

    Returns:
        One dict per distinct idea, in arbitrary order.
    """
    ideas: dict[int, dict] = {}
    for sub in ("authors", "discover"):
        folder = os.path.join(root, sub)
        if not os.path.isdir(folder):
            continue
        for name in sorted(os.listdir(folder)):
            if not name.endswith(".json"):
                continue
            with open(os.path.join(folder, name), encoding="utf-8") as fh:
                try:
                    payload = json.load(fh)
                except json.JSONDecodeError:
                    logger.warning("Hibas JSON, kihagyva: %s/%s", sub, name)
                    continue
            for row in payload.get("results", []):
                if isinstance(row.get("id"), int):
                    ideas[row["id"]] = row
    return list(ideas.values())


def is_btc(idea: dict) -> bool:
    """True if the idea's symbol is a BTC instrument."""
    symbol = idea.get("symbol") or {}
    return "BTC" in str(symbol.get("short_name") or "").upper()


def direction_of(idea: dict) -> str:
    """Label an idea: education, long, short, or none."""
    if idea.get("is_education"):
        return "education"
    symbol = idea.get("symbol") or {}
    code = symbol.get("direction")
    if not isinstance(code, int):
        return "none"
    return DIRECTION_LABELS.get(code, "none")


# %% Report sections

def _pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:5.1f}%" if whole else "    -"


def _quantiles(values: list[int]) -> dict[str, int]:
    if not values:
        return {}
    ordered = sorted(values)

    def at(q: float) -> int:
        return ordered[min(len(ordered) - 1, int(q * len(ordered)))]

    return {"min": ordered[0], "p25": at(0.25), "median": at(0.50),
            "p75": at(0.75), "p90": at(0.90), "max": ordered[-1]}


def build_report(ideas: list[dict]) -> dict:
    """Compute the full review over the loaded ideas."""
    btc = [i for i in ideas if is_btc(i)]
    labels = Counter(direction_of(i) for i in btc)
    usable = [i for i in btc if direction_of(i) in ("long", "short")]

    per_author: Counter = Counter()
    for idea in usable:
        user = idea.get("user") or {}
        if user.get("username"):
            per_author[user["username"]] += 1
    deep = {a: n for a, n in per_author.items() if n >= MIN_CALLS_PER_AUTHOR}

    dates = sorted(i["created_at"] for i in usable if i.get("created_at"))
    by_year = Counter(d[:4] for d in dates)

    fields = {
        "created_at":       sum(1 for i in ideas if i.get("created_at")),
        "date_timestamp":   sum(1 for i in ideas if i.get("date_timestamp")),
        "updated_at":       sum(1 for i in ideas if i.get("updated_at")),
        "symbol.direction": sum(1 for i in ideas if (i.get("symbol") or {}).get("direction") is not None),
        "symbol.interval":  sum(1 for i in ideas if (i.get("symbol") or {}).get("interval")),
        "user.id":          sum(1 for i in ideas if (i.get("user") or {}).get("id")),
        "description":      sum(1 for i in ideas if i.get("description")),
        "likes_count":      sum(1 for i in ideas if i.get("likes_count") is not None),
    }

    return {
        "totals": {
            "ideas_all_symbols": len(ideas),
            "ideas_btc": len(btc),
            "usable_btc_calls": len(usable),
            "authors_with_btc_calls": len(per_author),
            "authors_above_min": len(deep),
            "min_calls_threshold": MIN_CALLS_PER_AUTHOR,
        },
        "direction_btc": dict(labels),
        "calls_per_author": _quantiles(list(per_author.values())),
        "top_authors": per_author.most_common(15),
        "date_span": {"first": dates[0][:19] if dates else None,
                      "last": dates[-1][:19] if dates else None},
        "calls_by_year": dict(sorted(by_year.items())),
        "interval_btc": dict(Counter((i.get("symbol") or {}).get("interval")
                                     for i in usable).most_common(10)),
        "exchanges_btc": dict(Counter((i.get("symbol") or {}).get("exchange")
                                      for i in btc).most_common(10)),
        "field_fill": fields,
        "field_total": len(ideas),
    }


def print_report(report: dict) -> None:
    """Render the report to stdout."""
    t = report["totals"]
    n = report["field_total"]

    print("\n" + "=" * 72)
    print("  TRADINGVIEW RAW LAYER — ADATMINŐSÉGI KIMUTATÁS")
    print("=" * 72)

    print("\n## Mennyiség")
    print(f"  Idea összesen (minden szimbólum) : {t['ideas_all_symbols']:>7,d}")
    print(f"  Ebből BTC                        : {t['ideas_btc']:>7,d}  "
          f"({_pct(t['ideas_btc'], t['ideas_all_symbols'])})")
    print(f"  Használható BTC call (long/short): {t['usable_btc_calls']:>7,d}  "
          f"({_pct(t['usable_btc_calls'], t['ideas_btc'])} a BTC-ből)")
    print(f"  Szerző BTC callal                : {t['authors_with_btc_calls']:>7,d}")
    print(f"  Ebből >= {t['min_calls_threshold']} call               : "
          f"{t['authors_above_min']:>7,d}   <-- ezeken mérhető egyáltalán")

    print("\n## Irány-eloszlás (BTC)")
    for label, count in sorted(report["direction_btc"].items(), key=lambda x: -x[1]):
        print(f"  {label:12s} {count:>7,d}  {_pct(count, t['ideas_btc'])}")

    print("\n## Call / szerző eloszlás (BTC, long+short)")
    q = report["calls_per_author"]
    if q:
        print("  " + "  ".join(f"{k}={v}" for k, v in q.items()))
    print("\n  Legtöbb callal:")
    for author, count in report["top_authors"]:
        print(f"    {author:28s} {count:>5,d}")

    print("\n## Időbeli lefedettség (használható BTC callok)")
    span = report["date_span"]
    print(f"  {span['first']}  ..  {span['last']}")
    for year, count in report["calls_by_year"].items():
        bar = "#" * min(50, count // max(1, max(report["calls_by_year"].values()) // 50 or 1))
        print(f"    {year}  {count:>6,d}  {bar}")

    print("\n## Szerző által megadott timeframe (interval)")
    for interval, count in report["interval_btc"].items():
        print(f"    {str(interval):>6s}  {count:>6,d}")

    print("\n## Tőzsde-névtér (BTC ideák)")
    for exchange, count in report["exchanges_btc"].items():
        print(f"    {str(exchange):>14s}  {count:>6,d}")

    print(f"\n## Mező-kitöltöttség (n={n:,d})")
    for field, filled in report["field_fill"].items():
        flag = "OK " if filled == n else "   "
        print(f"  {flag}{field:20s} {filled:>7,d}/{n:,d}  {_pct(filled, n)}")

    print("\n## Ami NINCS az adatban")
    print("  - belépő ár (entry)        : nincs mező az API-ban")
    print("  - célár (target) / stop    : nincs mező az API-ban")
    print("  - pozícióméret / R-multiple: nincs")
    print("  A mérés ezért direction + ts_utc alapú, ahogy a plan.md")
    print("  `call` absztrakciója eleve feltételezi.")
    print("=" * 72 + "\n")


# %% CLI

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", metavar="PATH", default=None,
                        help="Also write the report as JSON to PATH.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    root = raw_dir()
    ideas = load_ideas(root)
    if not ideas:
        logger.error("Ures raw reteg: %s", root)
        return 1

    report = build_report(ideas)
    print_report(report)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=1)
        logger.info("JSON riport: %s", args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
