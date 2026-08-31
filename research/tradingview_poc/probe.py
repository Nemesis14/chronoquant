"""TradingView Ideas scraper PoC — eldobható próbadarab (_jira_ t1).

NEM termelési kód. Egyetlen célja, hogy a t1 ticket G1–G5 gate-kérdéseire
választ adjon. A T2 backfill runner ezt NEM importálja — az `src/` alá kerül,
saját implementációval, ennek a fájlnak a README.md-ben rögzített
megállapításai alapján.

Használat:
    python probe.py fetch     # ~5 kérés, >=5s szünettel  -> out/
    python probe.py parse     # offline elemzés az out/ tartalmán
"""

from __future__ import annotations

import json
import re
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen

OUT = Path(__file__).parent / "out"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# robots.txt: a `*` blokkban nincs Crawl-delay, de bingbot 5s / Yandex 10s.
# Ez adja a mércét — lásd README "0a" szakasz.
DELAY_S = 6.0

TARGETS = {
    "robots.txt": "https://www.tradingview.com/robots.txt",
    "sitemap-ideas.xml": "https://www.tradingview.com/sitemaps/www_tradingview_com/sitemap-ideas.xml",
    "btcusd_p1.html": "https://www.tradingview.com/symbols/BTCUSD/ideas/",
    "btcusd_p2.html": "https://www.tradingview.com/symbols/BTCUSD/ideas/page-2/",
    "idea_detail.html": ("https://www.tradingview.com/chart/BTCUSD/"
                         "e2LXn8NQ-BTCUSD-Liquidity-Above-Structure-Below/"),
}


def fetch() -> None:
    OUT.mkdir(exist_ok=True)
    for i, (name, url) in enumerate(TARGETS.items()):
        if i:
            time.sleep(DELAY_S)
        req = Request(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
        with urlopen(req, timeout=60) as r:
            body = r.read()
        (OUT / name).write_bytes(body)
        print(f"{r.status}  {len(body):>9,d} B  {name}")


# --- parsing -----------------------------------------------------------------

BACKSLASH = chr(92)


def _obj_at(txt: str, start: int) -> str | None:
    """Brace-match egy JSON objektumot txt[start] == '{' pozíciótól."""
    depth = 0
    instr = esc = False
    for i in range(start, len(txt)):
        c = txt[i]
        if instr:
            if esc:
                esc = False
            elif c == BACKSLASH:
                esc = True
            elif c == '"':
                instr = False
        elif c == '"':
            instr = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return txt[start:i + 1]
    return None


def parse_listing(html: str) -> list[dict]:
    """A szimbólum-oldal <article> kártyáit bontja mezőkre."""
    out = []
    for card in re.findall(r"<article\b.*?</article>", html, re.S):
        def g(pat, default=None):
            m = re.search(pat, card, re.S)
            return m.group(1) if m else default
        out.append({
            "uuid": g(r"/chart/[^/]+/([A-Za-z0-9]+)-"),
            "symbol_url": g(r"/chart/([^/]+)/"),
            "symbol_full": g(r'logoIcon[^>]*title="([^"]+)"'),
            "author": g(r'href="/u/([^/"]+)/"'),
            "direction": g(r'visuallyHiddenLabel-[^"]*">(\w+)</span>'),
            "card_ts": g(r'dateTime="(20[^"]+)"'),
            "card_ts_label": (g(r'publicationDate-[^"]*"[^>]*>(.*?)</time>', "") or "").strip(),
            "comments": g(r'aria-label="(\d+) comments?"'),
            "boosts": g(r'aria-label="(\d+) boosts?"'),
        })
    return out


def parse_detail(html: str) -> list[dict]:
    """A detail oldalba ágyazott idea-rekordokat parse-olja."""
    recs = {}
    for m in re.finditer(r'\{"id":\d+,"uuid":"', html):
        raw = _obj_at(html, m.start())
        if not raw:
            continue
        try:
            o = json.loads(raw)
        except ValueError:
            continue
        if "uuid" in o and "created_at" in o:
            recs[o["uuid"]] = o
    return list(recs.values())


def parse() -> None:
    p1 = (OUT / "btcusd_p1.html").read_text(encoding="utf-8")
    p2 = (OUT / "btcusd_p2.html").read_text(encoding="utf-8")
    detail = (OUT / "idea_detail.html").read_text(encoding="utf-8")

    cards = parse_listing(p1)
    print(f"=== G2/G3: listing kártyák (p1) — n={len(cards)} ===")
    for k in ("uuid", "author", "direction", "card_ts", "comments", "boosts"):
        filled = sum(1 for c in cards if c[k])
        print(f"  {k:14s} {filled:2d}/{len(cards)}  {100 * filled // len(cards):3d}%")
    print("  direction:", dict(Counter(c["direction"] for c in cards)))
    print("  ts_label :", dict(Counter(c["card_ts_label"] for c in cards)))

    print(f"\n=== G4: paginálás ===")
    u1 = {c['uuid'] for c in cards}
    u2 = {c['uuid'] for c in parse_listing(p2)}
    print(f"  p1={len(u1)}  p2={len(u2)}  overlap={len(u1 & u2)}")
    d1 = sorted(c["card_ts"] for c in cards if c["card_ts"])
    print(f"  p1 dátumtartomány: {d1[0][:10]} .. {d1[-1][:10]}")
    doc_order = [c["card_ts"] for c in cards if c["card_ts"]]
    print(f"  kronologikus-e a sorrend? {doc_order == sorted(doc_order, reverse=True)}")

    recs = parse_detail(detail)
    print(f"\n=== G1: created_at vs updated_at — n={len(recs)} ===")
    for o in recs:
        lag = (datetime.fromisoformat(o["updated_at"])
               - datetime.fromisoformat(o["created_at"])).total_seconds() / 3600
        print(f"  {o['uuid']:10s} dir={o['direction']} tf={str(o['interval']):>4s} "
              f"created={o['created_at'][:19]} lag={lag:7.1f}h "
              f"updates={len(o.get('updates') or [])}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "parse"
    {"fetch": fetch, "parse": parse}[cmd]()
