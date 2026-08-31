"""T1c — ideas API endpoint vadászat a JS bundle-ökben (eldobható).

A szerzői oldal SPA; az idea-listát XHR-rel tölti. A cél megtalálni az
endpointot. Statikus CDN fájlokat töltünk (static.tradingview.com), nem
tartalmi endpointot hívunk — a ban-kockázat gyakorlatilag nulla, ezért
rövidebb szünet is elég.

    python probe3.py fetch    # bundle-ök letöltése -> out/js/
    python probe3.py scan     # offline grep az endpointokra
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

HERE = Path(__file__).parent
OUT = HERE / "out"
JS = OUT / "js"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
DELAY_S = 2.0   # statikus CDN asset


def bundles() -> list[str]:
    """A szerzői oldal + a symbol ideas oldal összes script srcje."""
    urls: set[str] = set()
    for page in ("author_XAUApex.html", "btcusd_p1.html"):
        f = OUT / page
        if f.exists():
            html = f.read_text(encoding="utf-8", errors="replace")
            urls |= set(re.findall(r'<script[^>]+src="(https://static\.tradingview\.com[^"]+\.js)"', html))
    return sorted(urls)


def fetch() -> None:
    JS.mkdir(parents=True, exist_ok=True)
    urls = bundles()
    print(f"{len(urls)} bundle")
    for i, u in enumerate(urls):
        name = u.rsplit("/", 1)[-1]
        dest = JS / name
        if dest.exists():
            continue
        if i:
            time.sleep(DELAY_S)
        try:
            with urlopen(Request(u, headers={"User-Agent": UA}), timeout=60) as r:
                dest.write_bytes(r.read())
            print(f"  {r.status}  {dest.stat().st_size:>9,d} B  {name}")
        except Exception as e:
            print(f"  ERR {name}: {e}")


PATTERNS = {
    "api_path":      r'["\'`](/api/v\d+/[^"\'`\s]{3,90})["\'`]',
    "ideas_path":    r'["\'`](/[a-zA-Z0-9/_.${}-]*ideas[a-zA-Z0-9/_.${}-]*)["\'`]',
    "pubchart":      r'["\'`](/[a-zA-Z0-9/_.${}-]*(?:pubchart|publication|charts?_list)[a-zA-Z0-9/_.${}-]*)["\'`]',
    "feed_host":     r'["\'`](https://[a-z0-9.-]+\.tradingview\.com/[a-zA-Z0-9/_.${}-]*(?:idea|feed|chart)[^"\'`\s]{0,60})["\'`]',
}


def scan() -> None:
    files = sorted(JS.glob("*.js"))
    print(f"{len(files)} bundle beolvasva\n")
    hits: dict[str, dict[str, set]] = {}
    for f in files:
        s = f.read_text(encoding="utf-8", errors="replace")
        for label, pat in PATTERNS.items():
            found = set(re.findall(pat, s))
            found = {x for x in found if not x.startswith("/api/v1/script_packages")}
            if found:
                hits.setdefault(f.name, {}).setdefault(label, set()).update(found)
    if not hits:
        print("!!! nincs találat")
        return
    for name, groups in sorted(hits.items()):
        print(f"=== {name} ===")
        for label, vals in groups.items():
            for v in sorted(vals)[:25]:
                print(f"   [{label}] {v}")
        print()


if __name__ == "__main__":
    {"fetch": fetch, "scan": scan}[sys.argv[1] if len(sys.argv) > 1 else "scan"]()
