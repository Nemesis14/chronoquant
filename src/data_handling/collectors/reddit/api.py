#!/usr/bin/env python3
"""HTTP client for the Arctic Shift Reddit archive API (epic_053 / t202).

Raw-layer collector for Reddit comments and submissions, mirroring the shape
of the TradingView collector (src/data_handling/collectors/tradingview/api.py).
Every page is persisted verbatim as JSON under the raw layer with our own
fetched_at_utc, so re-running an ingest never re-requests a page already on
disk and an interrupted run is resumable at zero request cost.

Endpoint: https://arctic-shift.photon-reddit.com -- a free, community-run
mirror of the Reddit dumps (no auth). Docs: github.com/ArthurHeitmann/arctic_shift.

Field selection (`fields=`) is used deliberately narrow: id, created_utc,
author, and body/title/selftext only. The `score`, `num_comments` and
`upvote_ratio` fields are never requested -- they reflect the crawl-time
value, not the value at creation, which is a lookahead leak (see the
leak-rule in epic_053/todo_t202_reddit_dump_ingest.md).

Pagination is a created_utc cursor: results come back sorted ascending
(sort=asc), and the next page's `after` is the last row's created_utc + 1s.
There is no server-side pagination token.

Rate policy: single-threaded, small fixed delay between requests -- this is
a free, community-hosted service, not a commercial API. Exponential backoff
on 5xx / query-timeout responses. Hard stop on 429.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

BASE_URL = "https://arctic-shift.photon-reddit.com/api"

#: No contact e-mail here on purpose -- it would be sent to a third-party
#: service on every request. Add one only if you decide you want to.
_USER_AGENT = "chronoquant-research/0.1 (non-commercial research)"

_DELAY_S = 2.0
_MAX_RETRIES = 4
_BACKOFF_BASE_S = 15.0
_TIMEOUT_S = 60

#: Response page size. 100 is the documented per-request maximum for search.
PAGE_LIMIT = 100

#: Deliberately narrow field sets -- no score/num_comments/upvote_ratio.
COMMENT_FIELDS = "id,created_utc,author,body"
SUBMISSION_FIELDS = "id,created_utc,author,title,selftext"

#: our kind name -> Arctic Shift URL segment
KIND_ENDPOINT = {"comment": "comments", "submission": "posts"}


class RateLimitStop(RuntimeError):
    """Raised on HTTP 429 -- the run must stop, not retry."""


# %% Client

class ArcticShiftClient:
    """Single-threaded, resumable client for the Arctic Shift search endpoints.

    Every page is written to raw_dir and recorded in _manifest.jsonl with our
    own fetched_at_utc. A page already present on disk is never re-requested.

    Args:
        raw_dir : Root of the raw layer, e.g.
                  data/analyst_signal/raw/source=reddit.
        dry_run : If True, no HTTP is performed; cached pages are still served.
    """

    def __init__(self, raw_dir: str, dry_run: bool = False) -> None:
        self.raw_dir = raw_dir
        self.dry_run = dry_run
        self.manifest_path = os.path.join(raw_dir, "_manifest.jsonl")
        self.request_count = 0
        self._last_request_at: float | None = None
        os.makedirs(raw_dir, exist_ok=True)

    # --- internals ---

    def _sleep(self) -> None:
        """Fixed inter-request delay, measured from the previous request."""
        if self._last_request_at is None:
            return
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < _DELAY_S:
            time.sleep(_DELAY_S - elapsed)

    def _record(self, entry: dict) -> None:
        with open(self.manifest_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def _http_get(self, url: str) -> dict:
        """One GET with the rate policy applied. Raises RateLimitStop on 429."""
        for attempt in range(1, _MAX_RETRIES + 1):
            self._sleep()
            self._last_request_at = time.monotonic()
            request = urllib.request.Request(url, headers={
                "User-Agent": _USER_AGENT,
                "Accept": "application/json",
            })
            try:
                with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:
                    body = response.read().decode("utf-8")
                self.request_count += 1
            except urllib.error.HTTPError as exc:
                self.request_count += 1
                if exc.code == 429:
                    logger.error("Arctic Shift 429 -- futas leallitva: %s", url)
                    self._record({
                        "url": url, "fetched_at_utc": _utc_now(),
                        "status": 429, "error": "rate_limited",
                    })
                    raise RateLimitStop(
                        f"HTTP 429 -- a futas leallt. Varj, majd inditsd ujra "
                        f"(a mar letoltott lapok cache-bol jonnek): {url}"
                    ) from exc
                # 422 observed to be transient on this service (a retried,
                # byte-identical request has succeeded) -- treat it like a
                # 5xx rather than a genuine bad-request. Other 4xx are a real
                # bug in our own query and fail fast.
                retryable = exc.code >= 500 or exc.code == 422
                if not retryable or attempt == _MAX_RETRIES:
                    logger.warning("HTTP %d (%d. proba): %s", exc.code, attempt, url)
                    self._record({"url": url, "fetched_at_utc": _utc_now(), "status": exc.code})
                    raise
                backoff = _BACKOFF_BASE_S * (2 ** (attempt - 1))
                logger.warning("HTTP %d -- backoff %.0fs (%d/%d)",
                               exc.code, backoff, attempt, _MAX_RETRIES)
                time.sleep(backoff)
                continue
            except urllib.error.URLError as exc:
                if attempt == _MAX_RETRIES:
                    raise
                backoff = _BACKOFF_BASE_S * (2 ** (attempt - 1))
                logger.warning("Halozati hiba (%s) -- backoff %.0fs (%d/%d)",
                               exc.reason, backoff, attempt, _MAX_RETRIES)
                time.sleep(backoff)
                continue

            payload = json.loads(body)
            if payload.get("error"):
                # Server-side query timeout ("Timeout. Maybe slow down a bit")
                # -- not a transport error, the query needs a narrower window.
                if attempt == _MAX_RETRIES:
                    raise RuntimeError(f"Arctic Shift query error: {payload['error']} ({url})")
                backoff = _BACKOFF_BASE_S * attempt
                logger.warning("Arctic Shift query hiba (%s) -- backoff %.0fs (%d/%d)",
                               payload["error"], backoff, attempt, _MAX_RETRIES)
                time.sleep(backoff)
                continue
            return payload
        raise RuntimeError("unreachable")

    # --- public ---

    def fetch_window(
        self,
        kind: str,
        subreddit: str,
        after_utc: datetime,
        before_utc: datetime,
        rel_prefix: str,
    ) -> list[dict]:
        """Fetch every record in [after_utc, before_utc) for one subreddit/kind.

        Pages by a created_utc cursor (sort=asc, limit=PAGE_LIMIT). Each page
        is cached to raw_dir/{rel_prefix}_p{n}.json and skipped on re-run.

        Args:
            kind       : "comment" or "submission".
            subreddit  : Subreddit name, no r/ prefix.
            after_utc  : Inclusive window start (UTC, tz-aware).
            before_utc : Exclusive window end (UTC, tz-aware).
            rel_prefix : Destination prefix inside raw_dir, e.g.
                         "subreddit=BitcoinMarkets/kind=comment/2025-01".

        Returns:
            All rows in the window, across every page, in ascending created_utc order.
        """
        endpoint = KIND_ENDPOINT[kind]
        fields = COMMENT_FIELDS if kind == "comment" else SUBMISSION_FIELDS
        cursor = after_utc
        page_no = 1
        rows: list[dict] = []
        seen_ids: set[str] = set()

        while cursor < before_utc:
            dest = os.path.join(self.raw_dir, f"{rel_prefix}_p{page_no}.json")
            if os.path.exists(dest):
                with open(dest, encoding="utf-8") as fh:
                    page_rows = json.load(fh)
            else:
                if self.dry_run:
                    logger.info("[dry-run] %s p%d", rel_prefix, page_no)
                    break
                url = (
                    f"{BASE_URL}/{endpoint}/search"
                    f"?subreddit={subreddit}"
                    f"&after={cursor.strftime('%Y-%m-%dT%H:%M:%S')}"
                    f"&before={before_utc.strftime('%Y-%m-%dT%H:%M:%S')}"
                    f"&limit={PAGE_LIMIT}&sort=asc&fields={fields}"
                )
                payload = self._http_get(url)
                page_rows = payload.get("data") or []
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with open(dest, "w", encoding="utf-8") as fh:
                    json.dump(page_rows, fh, ensure_ascii=False)
                self._record({
                    "url": url,
                    "rel_path": f"{rel_prefix}_p{page_no}.json",
                    "fetched_at_utc": _utc_now(),
                    "status": 200,
                    "rows": len(page_rows),
                })

            # The cursor is re-queried at (not past) the last-seen second, so
            # a page boundary that lands mid-second never silently drops
            # rows -- de-dupe by id instead, across the whole call.
            new_rows = [r for r in page_rows if r.get("id") not in seen_ids]
            seen_ids.update(r["id"] for r in new_rows if r.get("id") is not None)
            rows.extend(new_rows)

            if len(page_rows) < PAGE_LIMIT:
                break

            last_ts = max(int(r["created_utc"]) for r in page_rows)
            next_cursor = datetime.fromtimestamp(last_ts, tz=UTC)
            if not new_rows or next_cursor <= cursor:
                # Every id in this page was already seen, or the cursor made
                # no progress: this second alone holds >= PAGE_LIMIT items,
                # which the created_utc cursor cannot page past. Skip one
                # second and accept the (logged) gap rather than loop forever.
                logger.warning(
                    "Kurzor nem halad -- lehetseges adatvesztes 1 masodpercben "
                    "(%s, page %d, t=%s, >= %d elem/mp): %s",
                    rel_prefix, page_no, cursor.isoformat(), PAGE_LIMIT, subreddit,
                )
                next_cursor = cursor + timedelta(seconds=1)
            cursor = next_cursor
            page_no += 1

        return rows


# %% Helpers

def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")
