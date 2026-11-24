---
epic: epic_076
id: t948
title: Chat gomb átfedés a Scout panelen + MCP scout_signals since-validáció
assignee: ui_agent
status: done
blocks: [t950]
blocked_by: []
---

## Goal
1) A lebegő chat gomb kitakarja a Scout panel szabály-lábjegyzetének végét és az R2_03 oszlop jobb szélét — javítsd (pl. panel alsó padding / gomb pozíció), más panelekre is nézd meg. 2) Az MCP `scout_signals` tool érvénytelen `since`-re nyers ValueError-t dob → barátságos hibaüzenet + teszt. `npm run build` (dist frissül; API restart nélkül a statikus dist kiszolgálás azonnal frissül-e — jelezd).

## Acceptance Criteria
- [x] Nincs átfedés (screenshot)
- [x] MCP hibakezelés tesztelve

## Notes

**1) Chat gomb átfedés**
- Reprodukálva (headless Chrome, futó :8600): 1366×768-on a gomb a Scout panel jobb alsó sarkát (R2_03 oszlop + szabály-lábjegyzet) takarta, lap végén a „Utolsó trade-ek (Binance)" kártyát (SOL és ETH dashboardon is), és a Reports oldalon is volt átfedés. 1920 px-en a 1800 px-es `max-w` margója miatt nem volt átfedés.
- Általános javítás: `frontend/src/index.css` `.cq-dock-clear` a Layout `<main>`-jén: jobb gutter = `clamp(1rem, 84px - (100vw-1800px)/2, 76px)` (a gomb 68 px-es sávja + 8 px; ~1936 px felett nincs többlet), valamint `padding-bottom: 80px`, így az utolsó kártya a gomb fölé görgethető. Így minden oldalon (dashboard, reports) és görgetés közben is szabad a gomb területe. A `ChatDock.tsx` kommentje rögzíti a kettő összetartozását.
- Ellenőrzés (DOM-mérés, a gomb téglalapjával metsző `main section`): előtte 1366-on `["Impulse Scout"]` / `["Utolsó trade-ek …"]`, utána minden lapon és mindkét felbontáson `[]`.
- Képek: `C:\Users\eross\AppData\Local\Temp\claude\6e2e8d785be8bd2e\7be3e7df-ec00-4fac-89c7-d9bfaf3386d1\scratchpad\t948\` — `before_*.png` / `after_*.png` (`{1920,1366}_{solusdt,ethusdt}_{scoutbottom,pageend}`, `{…}_reports_pageend`).

**Dist kiszolgálás restart nélkül:** a futó API kérésenként lemezről olvassa a `frontend/dist`-et, tehát az `npm run build` azonnal kiszolgálódik. DE az `index.html`-nek nem volt `Cache-Control` headere, ezért a Chrome heurisztikusan cache-elte: sima navigációra a régi shell (régi CSS) jött, F5 (reload) után az új. Javítás: `src/api/main.py` SPA fallback `index.html` → `Cache-Control: no-cache` (+ assert a `test_spa_fallback`-ban). Ez csak a következő API restart után él; addig a nyitott fülön egy F5 kell (hard refresh nem szükséges).

**2) MCP `scout_signals` since-validáció**
- `src/cq_mcp/tools_scout.py` `_parse_since`: érvénytelen `since` → `ValueError("invalid since: 'yesterday'; expected ISO-8601, e.g. 2026-11-10T00:00:00Z")` (a többi tool `invalid owner/slug: …` stílusában), a nyers `fromisoformat` hiba helyett.
- Teszt: `src/cq_mcp/tests/test_tools.py::test_scout_signals_invalid_since` (szöveges és érvénytelen dátum).

**Validáció:** `npx tsc --noEmit` OK, `npm run build` OK, `uv run pytest src/cq_mcp src/api -q` → 79 passed, `ruff check src/cq_mcp src/api` OK, pyright (érintett fájlok) 0 hiba.
**Doc:** 8350 (`.cq-dock-clear`, dist/no-cache), 8320 (`since` hiba), 8300 (SPA no-cache).
**Mellékes megfigyelés (nem t948):** a „before" képeken a Szinkron panel DuckDB `IOException … being used by another process` hibát mutatott (PID 4768) — ez valószínűleg a t947/t949 (database_agent) területe.

**Validáció (validator_agent, 2026-11-24): PASS → done.**
- CSS logika ellenőrizve: a tartalom jobb széle és a viewport jobb éle közti távolság ≥1800 px felett `84 − scrollbar/2` (~75,5 px Windows-scrollbarral), alatta 76 px → mindig ≥ a gomb 68 px-es sávja; alsó 80 px ≥ 68 px. Képek (1366/1920, SOL/ETH Scout-alja + lap vége, Reports) átnézve: nincs átfedés, nincs layout-regresszió.
- **Javítva (kis fix):** a `/chat` oldalon a dock rejtett, de a `.cq-dock-clear` ott is élt → a `ChatPage` `h-[calc(100vh-110px)]` + 80 px alsó padding miatt ~36 px felesleges oldalgörgetés és 76 px üres jobb sáv. `Layout.tsx`: az osztály csak akkor kerül a `<main>`-re, ha a dock látszik (`!pathname.startsWith('/chat')`, ugyanaz a szabály, mint a `ChatDock`-ban); `index.css` komment + 8350 doc frissítve.
- Mobil-szélesség (megjegyzés, nem blokkoló): 375 px-en a 76 px-es jobb gutter ~283 px tartalomszélességet hagy — az app desktop-orientált, elfogadható; ha kell, később media query-vel csökkenthető.
- `index.html` no-cache csak az SPA shellre (a hash-elt `/assets` StaticFiles mount cache-elhető marad) — helyes; teszt assert a `test_spa_fallback`-ban. API restart nélkül csak a következő restart után él (a futó :8600-hoz nem nyúltam).
- `_parse_since` barátságos `ValueError` + `test_scout_signals_invalid_since` OK. Doc 8350/8320/8300 pontos.
- Futtatva: `npx tsc --noEmit` OK, `npm run build` OK (a fix után, dist frissült), `uv run pytest src/cq_mcp src/api -q` → 79 passed, `ruff check src/cq_mcp src/api` OK, pyright (main.py, tools_scout.py, 2 tesztfájl) 0 hiba.
