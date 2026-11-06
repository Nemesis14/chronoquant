#!/usr/bin/env python3
"""HTTP client for the TradingView /api/v1/ideas/ endpoint.

Raw-layer collector. Responses are persisted verbatim as JSON — no parsing,
no normalisation. Decoding direction, filtering to BTC and building call_id
belong to the normalisation step (t3), so that a parser defect never costs a
re-crawl.

Rate policy (see _doc_/research/tradingview_poc.md):
  - 4-8s randomised delay; a fixed interval is a bot fingerprint
  - single threaded, cookie jar + keep-alive, browser User-Agent
  - 429/403 -> immediate hard stop, no retry grinding
  - 5xx -> exponential backoff, at most _MAX_RETRIES attempts
"""

import json
import logging
import os
import random
import time
from datetime import UTC, datetime
from http.cookiejar import CookieJar
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener

logger = logging.getLogger(__name__)

_ENDPOINT = "https://www.tradingview.com/api/v1/ideas/"

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

_DELAY_MIN_S = 4.0
_DELAY_MAX_S = 8.0
_MAX_RETRIES = 3
_BACKOFF_BASE_S = 30.0
_TIMEOUT_S = 60

#: Response page size. 100 is the server-side maximum.
PER_PAGE = 100

#: Hard cap the API applies per distinct query (count never exceeds this).
QUERY_RESULT_CAP = 1000


class RateLimitStop(RuntimeError):
    """Raised on 403/429 — the run must stop, not retry."""


# %% Client

class IdeasClient:
    """Single-threaded, resumable client for the ideas endpoint.

    Every response is written to raw_dir and recorded in _manifest.jsonl with
    our own fetched_at_utc. A page already present on disk is never re-requested,
    which makes an interrupted run resumable at zero request cost.

    Args:
        raw_dir  : Root of the raw layer, e.g.
                   data/analyst_signal/raw/source=tradingview.
        dry_run  : If True, no HTTP is performed; cached pages are still served.
    """

    def __init__(self, raw_dir: str, dry_run: bool = False) -> None:
        self.raw_dir = raw_dir
        self.dry_run = dry_run
        self.manifest_path = os.path.join(raw_dir, "_manifest.jsonl")
        self.request_count = 0
        self._last_request_at: float | None = None
        self._opener = build_opener(HTTPCookieProcessor(CookieJar()))
        self._opener.addheaders = [
            ("User-Agent", _USER_AGENT),
            ("Accept", "application/json"),
            ("Accept-Language", "en-US,en;q=0.9"),
        ]
        os.makedirs(raw_dir, exist_ok=True)

    # --- internals ---

    def _sleep(self) -> None:
        """Randomised inter-request delay, measured from the previous request."""
        if self._last_request_at is None:
            return
        target = random.uniform(_DELAY_MIN_S, _DELAY_MAX_S)
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < target:
            time.sleep(target - elapsed)

    def _record(self, entry: dict) -> None:
        with open(self.manifest_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def _http_get(self, url: str) -> str:
        """One GET with the rate policy applied. Raises RateLimitStop on 403/429."""
        for attempt in range(1, _MAX_RETRIES + 1):
            self._sleep()
            self._last_request_at = time.monotonic()
            try:
                with self._opener.open(Request(url), timeout=_TIMEOUT_S) as response:
                    body = response.read().decode("utf-8")
                self.request_count += 1
                return body
            except HTTPError as exc:
                if exc.code in (403, 429):
                    logger.error("TradingView %d — futas leallitva: %s", exc.code, url)
                    self._record({
                        "url": url,
                        "fetched_at_utc": _utc_now(),
                        "status": exc.code,
                        "error": "rate_limited",
                    })
                    raise RateLimitStop(
                        f"HTTP {exc.code} — a futas leallt. Varj, majd inditsd ujra "
                        f"(a mar letoltott lapok cache-bol jonnek)."
                    ) from exc
                if exc.code < 500 or attempt == _MAX_RETRIES:
                    logger.warning("HTTP %d (%d. proba): %s", exc.code, attempt, url)
                    self._record({
                        "url": url,
                        "fetched_at_utc": _utc_now(),
                        "status": exc.code,
                    })
                    raise
                backoff = _BACKOFF_BASE_S * (2 ** (attempt - 1))
                logger.warning("HTTP %d — backoff %.0fs (%d/%d)",
                               exc.code, backoff, attempt, _MAX_RETRIES)
                time.sleep(backoff)
            except URLError as exc:
                if attempt == _MAX_RETRIES:
                    raise
                backoff = _BACKOFF_BASE_S * (2 ** (attempt - 1))
                logger.warning("Halozati hiba (%s) — backoff %.0fs (%d/%d)",
                               exc.reason, backoff, attempt, _MAX_RETRIES)
                time.sleep(backoff)
        raise RuntimeError("unreachable")

    # --- public ---

    def fetch_page(
        self,
        rel_path: str,
        *,
        symbol: str | None = None,
        by: str | None = None,
        page: int = 1,
        per_page: int = PER_PAGE,
    ) -> dict:
        """Fetch one ideas page, or return it from the raw layer if already stored.

        Args:
            rel_path : Destination inside raw_dir, e.g. "discover/BTCUSD_p1.json".
            symbol   : Symbol filter. Plain (BTCUSD) or qualified (BITSTAMP:BTCUSD).
            by       : Author username filter. Returns that author's own history,
                       across all symbols, capped at QUERY_RESULT_CAP.
            page     : 1-based page index.
            per_page : Page size, at most PER_PAGE.

        Returns:
            The decoded response: count, page_size, page_count, next, results.
        """
        dest = os.path.join(self.raw_dir, rel_path)
        if os.path.exists(dest):
            with open(dest, encoding="utf-8") as fh:
                return json.load(fh)

        params: dict[str, str | int] = {"page": page, "per_page": per_page}
        if symbol:
            params["symbol"] = symbol
        if by:
            params["by"] = by
        url = _ENDPOINT + "?" + urlencode(params)

        if self.dry_run:
            logger.info("[dry-run] %s", url)
            return {"count": 0, "page_count": 0, "results": []}

        body = self._http_get(url)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "w", encoding="utf-8") as fh:
            fh.write(body)
        self._record({
            "url": url,
            "rel_path": rel_path,
            "fetched_at_utc": _utc_now(),
            "status": 200,
            "bytes": len(body),
        })
        return json.loads(body)

    def fetch_all_pages(
        self,
        rel_prefix: str,
        *,
        symbol: str | None = None,
        by: str | None = None,
        max_pages: int | None = None,
    ) -> list[dict]:
        """Fetch every page of a query, stopping at page_count or max_pages.

        Args:
            rel_prefix : Path prefix inside raw_dir; "_p{n}.json" is appended.
            symbol     : Symbol filter, forwarded to fetch_page.
            by         : Author filter, forwarded to fetch_page.
            max_pages  : Optional cap on pages fetched.

        Returns:
            The response payloads in page order.
        """
        pages: list[dict] = []
        page = 1
        while True:
            payload = self.fetch_page(
                f"{rel_prefix}_p{page}.json", symbol=symbol, by=by, page=page,
            )
            pages.append(payload)
            total_pages = int(payload.get("page_count") or 0)
            if max_pages is not None:
                total_pages = min(total_pages, max_pages)
            if page >= total_pages or not payload.get("results"):
                break
            page += 1
        return pages


# %% Helpers

def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")
