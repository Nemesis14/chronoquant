"""TradingView Ideas fetcher for the market-sentiment report.

Not the epic_051 backfill machinery (which writes the ``calls`` table and drops
the text). This is a lightweight tool: every run fetches the freshest ideas and
writes a JSON that Claude reads directly. The quality/content analysis is done
by the chat; this module only collects the raw data.

Usage (repo root)::

    uv run python -m sentiment.tv_report.fetch --hours 24 --pages 3

Outputs (under ``data/tv_report/``):
  1. ``runs/<run_ts>/BTCUSD_p*.json`` - raw API pages (history, never deleted).
  2. ``latest.json``                   - cleaned list for Claude (latest run).
  3. ``state.json``                    - ids seen by the previous run (dedup).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from data_handling.collectors.tradingview.api import IdeasClient
from data_handling.collectors.tradingview.normalize import SYMBOL_WHITELIST, interval_to_minutes
from sentiment.tv_report import paths

logger = logging.getLogger(__name__)

#: symbol.direction code -> human-readable label
_DIRECTION_LABEL: dict[Any, str] = {1: "long", 2: "short"}
_SEEN_IDS_KEEP = 5000


def make_client(raw_dir: str) -> IdeasClient:
    """Create the HTTP ideas client (single seam so tests can swap in a fake)."""
    return IdeasClient(raw_dir=raw_dir)


# %% State (dedup against the previous run)

def _load_state() -> dict[str, Any]:
    path = paths.state_path()
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"last_run_ts": None, "seen_ids": []}


def _save_state(state: dict[str, Any]) -> None:
    path = paths.state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


# %% Cleaning

def _age_hours(created_at: str | None) -> float | None:
    if not created_at:
        return None
    ts = datetime.fromisoformat(created_at).astimezone(UTC)
    return round((datetime.now(UTC) - ts).total_seconds() / 3600, 1)


def clean_idea(idea: dict[str, Any], *, seen_ids: set[Any]) -> dict[str, Any] | None:
    """Reduce a raw API record to the compact report shape.

    Returns None when the symbol is not a BTC price series (see
    ``normalize.SYMBOL_WHITELIST``; e.g. BTC.D or ETHBTC are excluded).
    """
    symbol = idea.get("symbol") or {}
    short_name = symbol.get("short_name")
    if short_name not in SYMBOL_WHITELIST:
        return None

    user = idea.get("user") or {}
    created_at = idea.get("created_at")
    return {
        "id": idea.get("id"),
        "uuid": idea.get("image_url"),
        "chart_url": idea.get("chart_url"),
        "title": idea.get("name"),
        "author": user.get("username"),
        "author_id": user.get("id"),
        "created_at_utc": created_at,
        "age_hours": _age_hours(created_at),
        "is_new_since_last_run": idea.get("id") not in seen_ids,
        "direction_label": _DIRECTION_LABEL.get(symbol.get("direction")),  # None = author did not label
        "horizon_hint_min": interval_to_minutes(symbol.get("interval")),
        "horizon_hint_raw": symbol.get("interval"),
        "is_education": bool(idea.get("is_education")),
        "is_video": bool(idea.get("is_video")),
        "is_script": bool(idea.get("is_script")),
        "likes_count": idea.get("likes_count"),
        "comments_count": idea.get("comments_count"),
        "views_count": idea.get("views_count"),
        "native_symbol": symbol.get("full_name") or short_name,
        "description": idea.get("description"),
    }


# %% Main flow

def run_fetch(
    symbol: str = paths.DEFAULT_SYMBOL,
    pages: int = 1,
    hours: float | None = None,
    out_path: Path | None = None,
    *,
    log: logging.Logger | None = None,
) -> dict[str, Any]:
    """Fetch, clean and persist one report; return the report dict."""
    log = log or logger
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    raw_dir = paths.runs_dir() / run_ts
    client = make_client(str(raw_dir))

    state = _load_state()
    seen_ids = set(state.get("seen_ids") or [])

    all_raw: list[dict[str, Any]] = []
    for page in range(1, pages + 1):
        payload = client.fetch_page(f"{symbol}_p{page}.json", symbol=symbol, page=page)
        results = payload.get("results") or []
        log.info("page %d: %d ideas", page, len(results))
        all_raw.extend(results)
        if page >= int(payload.get("page_count") or 0) or not results:
            break

    cleaned = [c for c in (clean_idea(i, seen_ids=seen_ids) for i in all_raw) if c is not None]
    if hours is not None:
        cleaned = [c for c in cleaned if c["age_hours"] is not None and c["age_hours"] <= hours]
    cleaned.sort(key=lambda c: c["created_at_utc"] or "", reverse=True)

    trade_ideas = [c for c in cleaned if not c["is_education"] and not c["is_script"]]
    report: dict[str, Any] = {
        "run_ts_utc": run_ts,
        "symbol": symbol,
        "requested_hours_window": hours,
        "n_fetched_raw": len(all_raw),
        "n_after_symbol_filter": len(cleaned),
        "n_trade_ideas": len(trade_ideas),
        "n_new_since_last_run": sum(1 for c in cleaned if c["is_new_since_last_run"]),
        "n_directed": sum(1 for c in trade_ideas if c["direction_label"] is not None),
        "n_long": sum(1 for c in trade_ideas if c["direction_label"] == "long"),
        "n_short": sum(1 for c in trade_ideas if c["direction_label"] == "short"),
        "ideas": cleaned,
    }

    target = out_path or (paths.runs_dir() / "report_latest_run.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, indent=2, ensure_ascii=False)
    target.write_text(text, encoding="utf-8")
    paths.latest_path().parent.mkdir(parents=True, exist_ok=True)
    paths.latest_path().write_text(text, encoding="utf-8")

    _save_state({
        "last_run_ts": run_ts,
        "seen_ids": sorted(seen_ids | {c["id"] for c in cleaned})[-_SEEN_IDS_KEEP:],
    })
    log.info(
        "report %s: raw=%d btc=%d trade=%d directed=%d (long=%d short=%d) new=%d",
        run_ts, report["n_fetched_raw"], report["n_after_symbol_filter"], report["n_trade_ideas"],
        report["n_directed"], report["n_long"], report["n_short"], report["n_new_since_last_run"],
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", default=paths.DEFAULT_SYMBOL)
    parser.add_argument("--pages", type=int, default=1, help="Number of 100-idea pages (default 1).")
    parser.add_argument("--hours", type=float, default=None, help="Keep only ideas newer than N hours.")
    parser.add_argument("--out", type=Path, default=None, help="Report copy path (default runs/report_latest_run.json).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    run_fetch(args.symbol, args.pages, args.hours, args.out)
    logger.info("latest: %s", paths.latest_path())
    return 0


if __name__ == "__main__":
    sys.exit(main())
