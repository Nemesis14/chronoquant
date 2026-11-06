"""Compact digest of the latest TV report for the ``sentiment_latest`` MCP tool."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sentiment.tv_report import paths

_HEADER_KEYS = (
    "run_ts_utc", "symbol", "requested_hours_window", "n_fetched_raw", "n_after_symbol_filter",
    "n_trade_ideas", "n_new_since_last_run", "n_directed", "n_long", "n_short",
)
_IDEA_KEYS = (
    "id", "chart_url", "title", "author", "created_at_utc", "age_hours", "direction_label",
    "horizon_hint_raw", "is_video", "likes_count", "comments_count", "views_count",
)


def _report_age_hours(run_ts: str | None) -> float | None:
    if not run_ts:
        return None
    ts = datetime.strptime(run_ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    return round((datetime.now(UTC) - ts).total_seconds() / 3600, 1)


def latest_digest(max_ideas: int = 20, desc_chars: int = 400) -> dict[str, Any]:
    """Header counts plus the newest trade ideas (descriptions cut to ``desc_chars``).

    Raises FileNotFoundError when no report exists yet (run ``sentiment_refresh`` first).
    Education and script ideas are excluded from ``ideas`` (they stay in the full file).
    """
    path = paths.latest_path()
    if not path.exists():
        raise FileNotFoundError("no TV report yet; run sentiment_refresh first")
    report = json.loads(path.read_text(encoding="utf-8"))

    digest: dict[str, Any] = {k: report.get(k) for k in _HEADER_KEYS}
    digest["report_age_hours"] = _report_age_hours(report.get("run_ts_utc"))
    digest["full_report_path"] = str(path)

    trade = [i for i in report.get("ideas", []) if not i.get("is_education") and not i.get("is_script")]
    ideas: list[dict[str, Any]] = []
    for idea in trade[:max_ideas]:
        row = {k: idea.get(k) for k in _IDEA_KEYS}
        text = (idea.get("description") or "").strip()
        row["description"] = text[:desc_chars] + ("..." if len(text) > desc_chars else "")
        ideas.append(row)
    digest["ideas"] = ideas
    digest["ideas_shown"] = len(ideas)
    digest["ideas_total_trade"] = len(trade)
    return digest
