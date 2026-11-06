"""Semi-automated TradingView author track-record backtest (crypto-focused, research CLI).

Pipeline:
  1. discover - pull a broad pool per crypto symbol, build an author candidate list
                (must have posted within the recency window)
  2. fetch    - pull each candidate author's full post history, keep the tracked
                crypto symbols, skip the author unless crypto-focused
  3. extract  - regex-based TP/SL/direction extraction from free text
  4. backtest - evaluate each measurable post against cached Binance 1h OHLCV
  5. report   - per-author summary (``report.json``)

Never writes to config/ or database/; all files live under ``data/tv_report/runs/``.

Usage (repo root)::

    uv run python -m sentiment.tv_report.author_backtest discover 5
    uv run python -m sentiment.tv_report.author_backtest run 3
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from data_handling.collectors.tradingview.api import IdeasClient
from sentiment.tv_report import paths

logger = logging.getLogger(__name__)

CRYPTO_SYMBOLS: dict[str, str] = {
    "BTCUSD": "BTCUSDT",
    "ETHUSD": "ETHUSDT",
    "SOLUSD": "SOLUSDT",
    "XRPUSD": "XRPUSDT",
    "DOGEUSD": "DOGEUSDT",
    "ADAUSD": "ADAUSDT",
    "BNBUSD": "BNBUSDT",
    "LTCUSD": "LTCUSDT",
    "LINKUSD": "LINKUSDT",
    "AVAXUSD": "AVAXUSDT",
}

RECENCY_DAYS = 90

Candles = list[list[Any]]


def make_client() -> IdeasClient:
    """Create the ideas client writing raw author pages under the backtest dir."""
    raw_dir = paths.backtest_dir()
    raw_dir.mkdir(parents=True, exist_ok=True)
    return IdeasClient(raw_dir=str(raw_dir), dry_run=False)


# ---------- Stage 1: discover ----------

def discover(client: IdeasClient, pages_per_symbol: int = 5) -> dict[str, dict[str, Any]]:
    """Author candidates: ``author -> {count, min_age_hours, symbols}``."""
    candidates: dict[str, dict[str, Any]] = {}
    now = datetime.now(UTC)
    for tv_sym in CRYPTO_SYMBOLS:
        pages = client.fetch_all_pages(f"discover_{tv_sym}", symbol=tv_sym, max_pages=pages_per_symbol)
        items: list[dict[str, Any]] = []
        for p in pages:
            items.extend(p.get("results", []))
        logger.info("%s: %d raw ideas fetched", tv_sym, len(items))
        for it in items:
            if it.get("is_education") or it.get("is_video") or it.get("is_script"):
                continue
            sym = (it.get("symbol") or {}).get("short_name")
            if sym != tv_sym:
                continue
            if (it.get("symbol") or {}).get("direction") not in (1, 2):
                continue
            author = it.get("user", {}).get("username") if isinstance(it.get("user"), dict) else it.get("author")
            if not author:
                continue
            ts = it.get("date_timestamp")
            if not ts:
                continue
            age_hours = (now - datetime.fromtimestamp(ts, tz=UTC)).total_seconds() / 3600
            rec = candidates.setdefault(author, {"count": 0, "min_age_hours": age_hours, "symbols": set()})
            rec["count"] += 1
            rec["min_age_hours"] = min(rec["min_age_hours"], age_hours)
            rec["symbols"].add(tv_sym)
    return candidates


# ---------- Stage 2: fetch per-author crypto history ----------

def fetch_author_history(client: IdeasClient, author: str) -> tuple[int, list[dict[str, Any]]]:
    """All posts of ``author`` -> ``(total, crypto_posts)``."""
    pages = client.fetch_all_pages(f"hist_{author}", by=author)
    items: list[dict[str, Any]] = []
    for p in pages:
        items.extend(p.get("results", []))
    crypto = [it for it in items if (it.get("symbol") or {}).get("short_name") in CRYPTO_SYMBOLS]
    return len(items), crypto


# ---------- Stage 3: TP/SL extraction ----------

_NUM = r"\$?([\d,]+\.?\d*)"
_SEP = r"\s*[:=\-]?\s*[\*\s]*"
_FILLER = r"(?:\s*(?:is\s*set\s*at|at|around|near))?"

# label-before-value patterns ("Stop Loss: 82,319.07", "SL: Below $X")
PAT_SL = re.compile(
    r"(?:Stop\s*Loss(?:\s*\(SL\))?|SL)" + _FILLER + _SEP + r"(?:Below|Above)?\s*[\*\s]*" + _NUM, re.I)
PAT_TP = re.compile(
    r"(?:TP\s*1?\b|Target\s*(?:Level\s*)?1?\b|Take\s*Profit\s*1?\b|Fair\s*TP)" + _FILLER + _SEP + _NUM, re.I)

# "target of X and Y ... with stoploss of Z" (old FrankAgbo-style template)
PAT_OLD_TEMPLATE = re.compile(
    r"target of ([\d,]+\.?\d*)\s*(?:and\s*([\d,]+\.?\d*))?.{0,80}?(?:stoploss of ([\d,]+\.?\d*))?",
    re.I | re.S,
)

# value-before-label dash format: "$65,448.34 - invalidation area" / "$82,843 - ... breakout / target"
PAT_VAL_DASH_SL = re.compile(_NUM + r"\s*(?:—|-{1,2})\s*[^\n]{0,40}?invalidat", re.I)
PAT_VAL_DASH_TP = re.compile(_NUM + r"\s*(?:—|-{1,2})\s*[^\n]{0,40}?(?:target|resistance|breakout)", re.I)


def _plausible(target: float, stop: float) -> bool:
    """Reject mis-extractions where a label's own digit (e.g. "Target 1 (BSL 1):")
    got captured as the value instead of the real price. A genuine target/stop pair
    for the same instrument sits within a modest multiple of each other."""
    if target <= 0 or stop <= 0:
        return False
    lo, hi = min(target, stop), max(target, stop)
    if lo < 0.0001:
        return False
    return (hi / lo) <= 5.0


def _pair(target_raw: str, stop_raw: str) -> tuple[float, float] | None:
    try:
        target, stop = float(target_raw.replace(",", "")), float(stop_raw.replace(",", ""))
    except ValueError:
        return None
    return (target, stop) if _plausible(target, stop) else None


def extract_tp_sl(desc: str) -> tuple[float, float] | None:
    """Best-effort ``(target, stop)`` extraction. None if not confidently found."""
    m_sl, m_tp = PAT_SL.search(desc), PAT_TP.search(desc)
    if m_sl and m_tp and (pair := _pair(m_tp.group(1), m_sl.group(1))):
        return pair

    if "target of" in desc.lower():
        m = PAT_OLD_TEMPLATE.search(desc)
        if m and m.group(3) and (pair := _pair(m.group(1), m.group(3))):
            return pair

    m_sl2, m_tp2 = PAT_VAL_DASH_SL.search(desc), PAT_VAL_DASH_TP.search(desc)
    if m_sl2 and m_tp2 and (pair := _pair(m_tp2.group(1), m_sl2.group(1))):
        return pair
    return None


# ---------- Stage 4: OHLCV cache + backtest ----------

def _ts(ms: float) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def load_or_fetch_ohlcv(binance_symbol: str, need_from: datetime) -> Candles:
    """Ensure hourly klines are cached from ``need_from`` to now; return the candle list."""
    cache_dir = paths.ohlcv_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{binance_symbol}_1h.json"
    existing: Candles = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    have_from = _ts(existing[0][0]) if existing else None
    have_to = _ts(existing[-1][0]) if existing else None

    now = datetime.now(UTC)
    fetch_ranges: list[tuple[datetime, datetime]] = []
    if have_from is None or have_to is None:
        fetch_ranges.append((need_from, now))
    else:
        if need_from < have_from:
            fetch_ranges.append((need_from, have_from))
        if have_to < now - timedelta(hours=2):
            fetch_ranges.append((have_to, now))

    new_klines: Candles = []
    for start, end in fetch_ranges:
        cur = start
        while cur < end:
            start_ms = int(cur.timestamp() * 1000)
            url = (f"https://api.binance.com/api/v3/klines?symbol={binance_symbol}"
                   f"&interval=1h&startTime={start_ms}&limit=1000")
            with urllib.request.urlopen(url, timeout=20) as r:  # noqa: S310 - fixed https Binance endpoint
                data: Candles = json.loads(r.read())
            if not data:
                break
            new_klines.extend(data)
            nxt = _ts(data[-1][0]) + timedelta(hours=1)
            if nxt <= cur:
                break
            cur = nxt
            time.sleep(0.2)

    if new_klines:
        combined = {k[0]: k for k in existing}
        combined.update({k[0]: k for k in new_klines})
        existing = [combined[t] for t in sorted(combined)]
        path.write_text(json.dumps(existing), encoding="utf-8")
    return existing


def evaluate(
    candles: Candles, date_iso: str, direction: str, target: float, stop: float, horizon_days: int = 21,
) -> str:
    """Outcome ``TP`` / ``SL`` / ``inconclusive`` / ``inconclusive_recent`` / ``no_data`` (SL wins ties)."""
    t0 = datetime.fromisoformat(date_iso)
    if t0.tzinfo is None:
        t0 = t0.replace(tzinfo=UTC)
    horizon_end = t0 + timedelta(days=horizon_days)
    fwd = [c for c in candles if _ts(c[0]) >= t0]
    if not fwd:
        return "no_data"
    data_end_ts = fwd[-1][0]
    for c in fwd:
        if _ts(c[0]) > horizon_end:
            break
        high, low = float(c[2]), float(c[3])
        if direction == "long":
            hit_tp, hit_sl = high >= target, low <= stop
        else:
            hit_tp, hit_sl = low <= target, high >= stop
        if hit_sl:
            return "SL"
        if hit_tp:
            return "TP"
    return "inconclusive" if _ts(data_end_ts) >= horizon_end else "inconclusive_recent"


# ---------- Commands ----------

def cmd_discover(pages_per_symbol: int = 5) -> None:
    """Write ``candidates.json`` (active authors only)."""
    cands = discover(make_client(), pages_per_symbol=pages_per_symbol)
    active = {a: v for a, v in cands.items() if v["min_age_hours"] <= RECENCY_DAYS * 24}
    logger.info("total distinct authors: %d, active (<= %dd): %d", len(cands), RECENCY_DAYS, len(active))
    out = {
        a: {"count": v["count"], "min_age_hours": round(v["min_age_hours"], 1), "symbols": sorted(v["symbols"])}
        for a, v in active.items()
    }
    (paths.backtest_dir() / "candidates.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    for a, v in sorted(out.items(), key=lambda x: -x[1]["count"])[:60]:
        logger.info("%-30s count=%3d min_age_h=%8.1f symbols=%s", a, v["count"], v["min_age_hours"], v["symbols"])


def _measurable_posts(crypto: list[dict[str, Any]]) -> list[dict[str, Any]]:
    posts: list[dict[str, Any]] = []
    for it in crypto:
        if it.get("is_education") or it.get("is_video") or it.get("is_script"):
            continue
        dirn_code = (it.get("symbol") or {}).get("direction")
        if dirn_code not in (1, 2):
            continue
        tp_sl = extract_tp_sl(it.get("description") or "")
        ts = it.get("date_timestamp")
        if not tp_sl or not ts:
            continue
        posts.append({
            "id": it.get("id"), "symbol": (it.get("symbol") or {}).get("short_name"), "date_ts": ts,
            "direction": "long" if dirn_code == 1 else "short", "target": tp_sl[0], "stop": tp_sl[1],
        })
    return posts


def cmd_run(min_count: int = 3) -> None:
    """Full pipeline over ``candidates.json`` -> ``report.json``."""
    client = make_client()
    cands = json.loads((paths.backtest_dir() / "candidates.json").read_text(encoding="utf-8"))
    shortlist = [a for a, v in cands.items() if v["count"] >= min_count]
    logger.info("shortlist (%d+ candidate posts): %d authors", min_count, len(shortlist))

    report: dict[str, Any] = {}
    for author in shortlist:
        total, crypto = fetch_author_history(client, author)
        if total == 0:
            continue
        crypto_share = len(crypto) / total
        if crypto_share < 0.4 and len(crypto) < 10:
            report[author] = {"skip": "not_crypto_focused", "total": total, "crypto": len(crypto)}
            continue

        measurable = _measurable_posts(crypto)
        outcomes = {"TP": 0, "SL": 0, "other": 0}
        rows: list[dict[str, Any]] = []
        for m in measurable:
            posted = datetime.fromtimestamp(m["date_ts"], tz=UTC)
            candles = load_or_fetch_ohlcv(CRYPTO_SYMBOLS[m["symbol"]], posted - timedelta(days=1))
            date_iso = posted.isoformat()
            outcome = evaluate(candles, date_iso, m["direction"], m["target"], m["stop"])
            outcomes[outcome if outcome in ("TP", "SL") else "other"] += 1
            rows.append({**m, "date": date_iso, "outcome": outcome})

        denom = outcomes["TP"] + outcomes["SL"]
        hitrate = outcomes["TP"] / denom if denom else None
        report[author] = {
            "total_posts": total, "crypto_posts": len(crypto), "crypto_share": round(crypto_share, 2),
            "measurable": len(measurable), "TP": outcomes["TP"], "SL": outcomes["SL"],
            "other": outcomes["other"], "hitrate": round(hitrate, 3) if hitrate is not None else None,
            "rows": rows,
        }
        hr_str = f"{hitrate * 100:.0f}%" if hitrate is not None else "n/a"
        logger.info("%-28s crypto=%4d measurable=%3d TP=%2d SL=%2d other=%2d hitrate=%s",
                    author, len(crypto), len(measurable), outcomes["TP"], outcomes["SL"], outcomes["other"], hr_str)

    out_path = paths.backtest_dir() / "report.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("wrote %s", out_path)


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    cmd = args[0] if args else "discover"
    if cmd == "discover":
        cmd_discover(int(args[1]) if len(args) > 1 else 5)
    elif cmd == "run":
        cmd_run(int(args[1]) if len(args) > 1 else 3)
    else:
        logger.error("unknown command %r (expected: discover [pages] | run [min_count])", cmd)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
