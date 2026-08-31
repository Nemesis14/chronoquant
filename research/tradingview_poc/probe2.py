"""T1b — backfill-tervezési próba (eldobható, lásd probe.py fejlécét).

Stage A: paginálás-mélység, szerzői oldal, detail-fetch a nem-`Updated` kártyákra.
Stage B: szerzői oldal paginálása + related[] mélység (Stage A eredménye alapján).

    python probe2.py a      # ~10 kérés, 6s szünettel
    python probe2.py b      # ~5 kérés
"""

from __future__ import annotations

import json
import random
import re
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import HTTPCookieProcessor, Request, build_opener
from http.cookiejar import CookieJar

HERE = Path(__file__).parent
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
DELAY_S = 6.0

# Böngésző-szerű session: cookie jar + keep-alive.
_opener = build_opener(HTTPCookieProcessor(CookieJar()))
_opener.addheaders = [
    ("User-Agent", UA),
    ("Accept-Language", "en-US,en;q=0.9"),
    ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
]

_log: list[dict] = []
LOG = OUT / "probe2_log.json"


def get(name: str, url: str, *, first: bool = False) -> str | None:
    """Egy kérés, cache-elve. 429/403 -> teljes leállás (lásd README rate-policy)."""
    dest = OUT / name
    if dest.exists():                       # cache: soha ne kérjünk le kétszer
        print(f"  cached   {name}")
        return dest.read_text(encoding="utf-8", errors="replace")
    if not first:
        time.sleep(DELAY_S)
    try:
        with _opener.open(Request(url), timeout=60) as r:
            body = r.read().decode("utf-8", errors="replace")
            status = r.status
    except HTTPError as e:
        print(f"  HTTP {e.code}  {name}")
        _log.append({"name": name, "url": url, "status": e.code})
        LOG.write_text(json.dumps(_log, indent=1), encoding="utf-8")
        if e.code in (403, 429):
            print("\n!!! 403/429 — LEÁLLÁS. Nincs retry. !!!")
            sys.exit(2)
        return None
    except URLError as e:
        print(f"  ERR {e}  {name}")
        return None
    dest.write_text(body, encoding="utf-8")
    print(f"  {status}  {len(body):>8,d} B  {name}")
    _log.append({"name": name, "url": url, "status": status, "bytes": len(body)})
    LOG.write_text(json.dumps(_log, indent=1), encoding="utf-8")
    return body


def n_articles(html: str | None) -> int:
    return html.count("<article") if html else -1


def stage_a() -> None:
    print("=== 1. Szimbólum-paginálás mélysége (BTCUSD) ===")
    depth = {}
    for i, p in enumerate([10, 50, 200, 500]):
        h = get(f"btcusd_p{p}.html",
                f"https://www.tradingview.com/symbols/BTCUSD/ideas/page-{p}/",
                first=(i == 0))
        depth[p] = n_articles(h)
        print(f"      page-{p:<4d} -> {depth[p]} <article>")

    print("\n=== 2. Szerzői oldal ===")
    h = get("author_XAUApex.html", "https://www.tradingview.com/u/XAUApex/")
    if h:
        times = re.findall(r'dateTime="(20[^"]+)"', h)
        print(f"      <article>: {n_articles(h)}   dateTime db: {len(times)}")
        if times:
            print(f"      tartomány: {min(times)[:10]} .. {max(times)[:10]}")
            print(f"      doc-order csökkenő? {times == sorted(times, reverse=True)}")
        for pat in [r'href="(/u/[^"]*page-\d+[^"]*)"', r'href="(/u/[^"]*/(?:ideas|published)[^"]*)"']:
            found = sorted(set(re.findall(pat, h)))[:6]
            if found:
                print(f"      link-minta {pat[:28]}: {found}")

    print("\n=== 3. Detail fetch — nem-`Updated` kártyák (kártya-dátum == created_at?) ===")
    p1 = (OUT / "btcusd_p1.html").read_text(encoding="utf-8")
    cards = re.findall(r"<article\b.*?</article>", p1, re.S)
    picked = []
    for c in cards:
        label = re.search(r'publicationDate-[^"]*"[^>]*>(.*?)</time>', c, re.S)
        if label and label.group(1).strip():      # 'Updated' -> kihagyjuk
            continue
        url = re.search(r'href="(https://www\.tradingview\.com/chart/[^"]+)"', c)
        ts = re.search(r'dateTime="(20[^"]+)"', c)
        uuid = re.search(r"/chart/[^/]+/([A-Za-z0-9]+)-", c)
        if url and ts and uuid:
            picked.append((uuid.group(1), ts.group(1), url.group(1)))
    picked = picked[:5]
    json.dump(picked, open(OUT / "probe2_picked.json", "w"), indent=1)
    for uuid, ts, url in picked:
        get(f"detail_{uuid}.html", url)
        print(f"      {uuid}  kártya_ts={ts[:19]}")
    print("\nStage A kész. Futtasd: python probe2.py b")


def stage_b() -> None:
    """Szerzői oldal paginálás + related[] mélység egy régebbi ideán."""
    print("=== 4. Szerzői oldal paginálása ===")
    for i, p in enumerate([2, 5, 20]):
        h = get(f"author_XAUApex_p{p}.html",
                f"https://www.tradingview.com/u/XAUApex/page-{p}/", first=(i == 0))
        times = re.findall(r'dateTime="(20[^"]+)"', h) if h else []
        rng = f"{min(times)[:10]}..{max(times)[:10]}" if times else "-"
        print(f"      page-{p:<3d} -> {n_articles(h)} <article>  {rng}")


if __name__ == "__main__":
    {"a": stage_a, "b": stage_b}[sys.argv[1] if len(sys.argv) > 1 else "a"]()
