---
epic: epic_074
id: t943
title: Dashboard: Scout chart-réteg + panel
assignee: ui_agent
status: done
blocks: []
blocked_by: [t942]
---

## Goal
Árchart-réteg: jelölő t0-n (mélypont) és t_c-n (megerősítés), P* és stop szaggatott vonal a H=8h ablakra, kis címke P(impulzus)-szal; kimenet színezés a lezárt eseményeknél. Panel: legutóbbi események listája, 3-osztályos valószínűség-sáv, percentilis, top-20% badge, kereskedési terv árszintekkel (két szabály), outcome. Asset-váltóval együttműködik (SOL/BTC/ETH). Rétegkapcsoló a charton. Feltűnő „kísérleti – nem kereskedési jel” jelölés.

## Scope
`frontend/` (charts, dashboard komponensek, api kliens, típusok)

## Acceptance Criteria
- [x] Build + lint OK, screenshot a user vizuális ellenőrzéséhez (⛔ user jóváhagyás hátra)
- [x] Doksi: 8350 (+ 8300 candles `end`)

## Notes

**2026-11-23 (ui_agent) — kész, ⛔ user vizuális ellenőrzésre + validálásra vár.** (Nem commitolva; worker / :8600 API nem lett újraindítva.)

### Chart-réteg (`PredictionPriceChart` + `lib/scout.ts` `scoutOverlay`)
- A fókuszablakot átfedő események: ▲ lila (Scout-szín `#b37feb`) a mélyponton (`t0`, `low_t0`), ● a megerősítésnél (`t_c`, `close_tc`)
  állapotszínnel (nyitott kék; lezárt: impulzus zöld / hamis indulás piros / oldalazás szürke; érvénytelen szürke), t0→t_c pontozott összekötő,
  pontozott P* (zöld) + stop (piros) vonal és halvány sáv `t_c → label_end`, címke „Scout 16.0% · 63. pc · [top 20%] · állapot” (kimenet ikonnal: ✓/✗/◷/∅).
- A látható események P*/stop/low szintje bővíti az ár-tengelyt (különben a P* vonal levágódna).
- Kapcsoló: „Scout réteg” gomb a chart fejlécében (localStorage `cq_scout_layer`, alap be), csak `available` asseten.

### Panel (`components/dashboard/ScoutPanel.tsx`, a SyncPanel alatt)
- Feltűnő „⚠ KÍSÉRLETI – NEM KERESKEDÉSI JEL” badge (`meta.experimental` / `no_go` státusz) + meta sor (model_id, OOF AUC, live_start, „nem része a quant modellnek”), számlálók.
- Nyitott esemény(ek) kiemelve (kék keret): P(impulzus), percentilis, TOP 20%/10% badge + küszöbök, 3-osztályos valószínűség-sáv (címkézett értékekkel),
  t0/t_c/entry/ablak vége („X óra múlva” / „lezárulás folyamatban”), kereskedési terv mindkét szabályra (Alap P*/mélypont; R2_03 +3σ/−1,5σ):
  TP/SL ár + %-távolság az entrytől, kimenet, nettó bp.
- Legutóbbi 12 esemény táblája (t_c, P(imp) + top badge, pc, kimenet, Alap / R2_03 nettó bp); kattintás → részletek + esemény-grafikon.
- BCH (`available=false`): „nem elérhető (modellen kívüli asset; Scout: BTC / ETH / SOL)”, réteg-kapcsoló rejtve. Régi API (404 `/api/scout`): „API újraindítás után aktív”.

### Esemény-grafikon (`components/charts/ScoutEventChart.tsx`)
- A 24h dashboard-ablakon kívül eső eseményekhez (pl. SOL 11-21, BTC top-20 11-06): `[t0 − 2h, label_end + 1h]`, 5m gyertyák a Scout réteggel; nyitottnál „most” vonal + 60 s frissítés.
- Ehhez kis read-only API bővítés: `GET /api/market/candles?...&end=` (ISO, naiv = UTC; alap és felső korlát a legutolsó bár) —
  `services/market.py`, `routers/market.py`, teszt `test_api_smoke.py::test_candles_end`. Kliens: `api/dashboard.ts` `getCandles`, típus `Candle`/`CandlesResponse`.
- `vite.config.ts`: proxy cél `CQ_API` env-vel felülírható (alap :8600 változatlan).

### Ellenőrzés
- `npx tsc --noEmit` ✔ (a `tsconfig.node.json` is), `npm run build` ✔; frontend lint script / tesztek nincsenek a projektben.
- `uv run pytest src/api -q`: **61 passed**; ruff + pyright (market service/router, smoke teszt): tiszta.
- Vizuális teszt: külön API (:8601, read-only) + Vite (:5174, `CQ_API`), headless Chromium (Playwright). Screenshotok (`SC` = C:\Users\eross\AppData\Local\Temp\claude\6e2e8d785be8bd2e\7be3e7df-ec00-4fac-89c7-d9bfaf3386d1\scratchpad\t943):
  - `SC\01_sol_dashboard_open_event.png` — SOL 24h, nyitott esemény (11-23 06:42) a charton + panel
  - `SC\02_sol_nov21_event.png` — SOL 11-21 10:17 esemény (oldalazás, ◷ lejárt, +372 bp) esemény-grafikonon + részletek
  - `SC\03_sol_layer_off.png` — réteg kikapcsolva
  - `SC\04_btc_top20_event.png` — BTC 11-06 03:10 TOP 20% (20.2%, 87. pc, oldalazás, −38 bp)
  - `SC\05_eth_top20_event.png` — ETH 11-20 23:18 TOP 20% (hamis indulás, ✗ SL)
  - `SC\06_bch_not_available.png` — BCH: „nem elérhető”
- Megfigyelés: a `scout_signals` utolsó írása 07:33 (a régi worker még nem futtat `sync_scout`-ot) → a SOL 11-23 esemény még „nyitott”,
  bár az ablaka 14:42-kor lejárt és az ár közben elérte a P*-ot; a worker újraindítása után a következő futás feloldja.
- ⚠ Mellékhatás: a :8601 teszt API read-only DuckDB kapcsolata Windows-on fájl-lock ütközést okozott a worker írásaival — néhány `sync_full`
  job ~19:20–19:26 között „Cannot open file … being used by another process (PID = teszt API)” hibával bukott. A teszt API + Vite leállítva;
  utána csak Binance hálózati timeout hibák (független). A következő auto-sync pótolja a kimaradt bárokat. Tanulság: futó worker mellett második API példányt csak röviden.
- `npm run build` felülírta a `frontend/dist`-et, amit a futó :8600 szolgál ki: a Scout panel az API újraindításáig „API újraindítás után aktív” üzenetet mutat, a réteg-kapcsoló rejtett; más nem változik.

### Aktiváláshoz
- **API újraindítás** (`/api/scout/*`, candles `end`) + **worker újraindítás** (t942 `sync_scout`), user döntés.

### Fájlok
`frontend/src/{lib/scout.ts (új), components/dashboard/ScoutPanel.tsx (új), components/charts/ScoutEventChart.tsx (új),
components/charts/PredictionPriceChart.tsx, pages/DashboardPage.tsx, api/dashboard.ts, types/api.ts}`, `frontend/vite.config.ts`,
`src/api/{services/market.py, routers/market.py, tests/test_api_smoke.py}`; doksi: `8350_frontend.md`, `8300_api.md`.

### [validator] Validálva — 2026-11-23 → done
- Ellenőrzés: `npx tsc --noEmit` (tsconfig + tsconfig.node) ✔, `npm run build` ✔ (dist újraépítve), `uv run pytest src/api -q` **61 passed** (tmp DB, élő DB-t nem érint), ruff + pyright (`services/market.py`, `routers/market.py`, `tests/test_api_smoke.py`) tiszta.
- Kód-review: a TS típusok 1:1 egyeznek a t942 `schemas.py` Scout modellekkel; nyitott eseménynél a null kimenet-mezők (`outcome`, `net_bps`, `rules[k]` hiánya) mindenhol `—`-ként kezelve; `oof_percentile` 0–100 skála (runtime) → „22. pc” helyes; UTC ISO-Z idők konzisztensen a meglévő charttal; localStorage try/catch-ben; BCH `available=false` és 404 („API újraindítás után”) ág rendben; kísérleti badge panelen + esemény-grafikon fejlécen + hoverben; állapot mindig szöveggel/ikonnal is (nem csak szín). Asset-váltás: `DashboardPage key={asset}` remount → nincs keresztbe szivárgó Scout adat. Candles `end`: asset a `get_conn`/`AssetDep`-en át, tz-aware → UTC naiv, felső korlát a legutolsó bár; nyitott eseménynél a levágott `end` miatt az ablak visszafelé bővül, a t0−2h így is lefedett.
- Screenshotok: SOL 11-21 10:17 — P(imp) 9.6%, 22. pc, Alap TP 71.33 / SL 64.83, ◷ lejárt, +372 bp ✔; BTC 11-06 TOP 20% 20.2% / 87. pc / −38 bp ✔; BCH „nem elérhető”, kapcsoló rejtve ✔; meglévő predikciós panelek, entry vonalak nem sérültek.
- Kis javítás: „Scout réteg” kapcsoló `aria-pressed` + állapotot jelző `title` (be/ki) — korábban az állapot csak színnel látszott.
- Nem blokkoló megjegyzések: (1) a lebegő chat gomb (korábbi epic) a jobb oszlop alján kitakarja a Scout panel szabály-sorát / R2_03 oszlop végét — külön UI follow-up; (2) a chart magasság 700→480 és a signal-markerek ár-panel szélére tétele a working tree-ben van, nem t943-ban dokumentált, de nem regresszió; (3) ⛔ a user vizuális jóváhagyása továbbra is hátra van; aktiváláshoz API + worker újraindítás kell (user döntés).
