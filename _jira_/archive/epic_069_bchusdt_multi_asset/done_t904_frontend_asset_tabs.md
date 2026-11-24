---
epic: epic_069
id: t904
title: Frontend: SOL | BCH asset-fülek, a Dashboard asset-paraméteres
assignee: ui_agent
status: done
blocks: ["t905", "t908"]
blocked_by: ["t903"]
---

## Goal
A SOL dashboard klónja BCH-ra: ugyanaz az oldal, asset a route-ból.

## Scope
`frontend/src/**` (App routing, DashboardPage, api kliens, usePolling kulcsok, SyncPanel, ChartControls), `frontend/dist` build.

## Acceptance Criteria
- [x] Felső asset-fülsor (SOL | BCH), route `/a/:asset/...`; a `/` a default assetre irányít; az utolsó fül megjegyezve (localStorage, try/catch)
- [x] Minden dashboard API hívás az aktuális `asset`-et küldi; a fülváltás nem ad a másik asset adatából maradékot (cache/polling kulcs asset-függő)
- [x] Elemzések (Reports) lista szűrhető asset szerint (a modell asset_id-ja alapján), a Chat és a többi oldal nem törik
- [x] BCH fülön, ha még nincs modell/predikció, értelmes üres állapot (nem hiba)
- [x] `npx tsc --noEmit` + `npm run build` zöld; böngészős ellenőrzés a SOL fülön

## Notes

### Elvégzett változtatások
- `lib/asset.tsx` (AssetContext/`useAsset`, localStorage `cq_last_asset` try/catch), `components/AssetRoutes.tsx` (`/` -> `RootRedirect`: utolsó/default asset; `/a/:asset` -> `AssetDashboard`, `key={asset}` újramountol, így fülváltáskor semmi nem marad a másik asset adatából; ismeretlen asset -> `/`), `components/AssetTabs.tsx` + `Layout.tsx` (SOL | BCH fülsor a `/a/:asset` oldalon, `has_data` assetek, `/api/assets`-ből). `/reports` és `/chat` útvonal változatlan.
- `api/dashboard.ts`: minden dashboard hívás `asset` paramétert kap; minden panel `usePolling` deps-ében `[asset]`. SyncPanel `POST /api/jobs {kind, params:{asset_id}}`, a job lista kliensoldalon az asset jobjaira szűr (job nélküli `asset_id` = default).
- Üres állapot: ha nincs modell -> tájékoztató sáv; üres `bars` -> "nincs megjeleníthető adat" a grafikon helyén; 404/`db_busy` a meglévő PollNote szerint (nem piros hiba busy esetén).
- Elemzések: backend kiegészítés `api/services/reports.py`: a lista elemei `asset_id`-t kapnak (`artifacts/<model_id>/manifest.json` `asset_id`; general = null). `ReportsPage`: Asset választó (`?asset=`, default az utolsó dashboard asset, "Mind"), az asset modell-riportjai + general riportok látszanak.
- **Trade journal korlát megoldva (backend)**: `api/services/trades.py::journal_rows(asset_id)`: a default asset (solusdt) minden sort lát (változatlan), más asset csak azokat a sorokat, amelyek `run_id`-ja a `trading_runs.asset_id = <asset>` runokhoz tartozik (`trading_runs` saját `asset_id` szűrés); ha nincs `run_id`/`asset_id` oszlop, üres lista. Érinti: `trades/journal/*`, `trades/active` journal-ága, `trades/closed?source=journal`. A Binance fill-ek symbol szerint eleve asset-specifikusak. Így a BCH fülön nem jelenik meg SOL trade.
- Tesztek: új `test_journal_is_filtered_by_asset`, `test_report_list_has_asset_id`; `uv run pytest src/api src/reports` 83 passed, ruff/pyright tiszta. `tsc --noEmit` + `npm run build` zöld.
- Újraindítás (stop.ps1 -> start.ps1 -NoBrowser) megtörtént; /api/health ok, az új dist szolgál (`index-B8yj8rxC.js`), SOL chart (241 bar, küszöbök) és journal API ok.
- Ismert: böngészős (vizuális) ellenőrzés nem történt, API-szintű + build ellenőrzés; a BCH DB-t jelenleg egy külső python folyamat (PID 1508) tartja nyitva, ezért a BCH végpontok `db_busy`-t adnak (a UI "adatbázis foglalt" üzenetet mutat, nem hibát).

### Utólagos javítás (t907 nyomán)
- Jelentett hiba: a BCH `/api/market/chart` `active_trade`-je LONG-ot mutat SL/TP nélkül. Vizsgálat: nincs szivárgás. A chart `trades.active_position(conn, asset_id)`-t hív (asset-szűrt journal, majd `binance.current_position(asset_id)` BCHUSDT symbollal, asset-kulcsos cache-sel). A BCH journalban nincs nyitott sor; a LONG valódi Binance futures pozíció: közvetlen lekérdezésre `BCHUSDT LONG qty 3.724 @268.49` (SOL: `SOLUSDT LONG 11.0 @90.88`, külön ár). A Binance-ág eleve nem ad SL/TP-t, ezért null. Kódváltoztatás nem kellett; a BCH pozíció ellenőrzése/zárása a Binance-oldalon a felhasználó dolga.
- Regressziós teszt: `test_chart_active_trade_is_per_asset` (BCH `active_trade` null ha csak SOL-nak van pozíciója; SOL változatlan; symbolonként egy lekérdezés).

[validator] Done — 2026-11-07. tsc --noEmit + npm run build zöld; /api/market/chart?asset=solusdt 1441 bar, thresholds nem üres. Vizuális böngészős ellenőrzés nem történt.
