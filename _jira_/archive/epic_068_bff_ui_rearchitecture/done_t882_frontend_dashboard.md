---
epic: epic_068
id: t882
title: Frontend váz (React+Vite+Tailwind) + dashboard oldalak
assignee: ui_agent
status: done
blocks: [t889]
blocked_by: [t880]
---

## Goal
`frontend/` a life_plan felépítése szerint (pages/components/api/lib). Portold a Streamlit dashboard nézeteit: predikciók/jelek, equity, trade journal, sync státusz. Charthoz recharts vagy lightweight-charts (OHLCV-hez).

## Scope
`frontend/**` (új); referencia: `src/ui/main.py`, `components/charts.py`, `trade_panel.py`, `log_panel.py`.

## Acceptance Criteria
- [x] Minden jelenlegi Streamlit nézet elérhető az új UI-n
- [x] Sync futás közben a UI reszponzív
- [x] `tsc` + `vite build` hibamentes

## Notes

### Elkészült (ui_agent)
- `frontend/` (React 18 + Vite 5 + TS + Tailwind 3), a scaffold elsőként készült: `package.json`, vite/tsconfig/tailwind/postcss, `src/main.tsx`, `App.tsx` (routing: `/` Dashboard, `/reports` Elemzések, `/chat` Chat), `components/Layout.tsx`, STUB `pages/ChatPage.tsx` (t883) és `pages/ReportsPage.tsx` (t884), `api/client.ts` (fetch + `ApiError{status,code}`, `api.get/post/put/del`). Függőségek előre felvéve a blueprint szerint (react-markdown, remark-gfm, plotly.js-dist-min, react-plotly.js, lucide-react) — t883/t884 nem kell új npm install-t futtasson.
- Közös, t883/t884 által is használható: `components/charts/PlotlyFigure.tsx` (lazy, külön chunk, `BASE_LAYOUT/CONFIG`, `CQ_COLORS`), `lib/usePolling.ts` (db_busy-barát polling, utolsó adat megmarad), `lib/format.ts`, `components/dashboard/ui.tsx` (Card, DataTable, PollNote). (A `lib/sse.ts` NEM készült — az t883-é; a jobs SSE saját `lib/jobStream.ts`, EventSource.)
- Dashboard (`pages/DashboardPage.tsx` + `components/dashboard/*`): PredictionPriceChart (a `charts.py` portja: 3 soros long pct/gyertya/short pct, küszöbvonalak, long/short jelölők, aktív trade overlay, 24h/8h/4h ablak, kliens-oldali resample 5m/2m/1m), Equity (long/short) + összegzés, Zárt trade-ek (strat), Trade journal (positions/signals/orders/errors/runs, CSAK olvasható), Log (api/worker), Szinkron panel (worker állapot, Sync gomb, auto-sync kapcsoló + countdown, job lista, job-log SSE a `/api/jobs/{id}/events`-en, queued job törlése), Aktív trade, Aktív stratégia kártyák, Modellek és adat (frissesség), Binance fillek. Live trading vezérlés NINCS.
- Aszinkronitás: minden hívás `usePolling` (AbortController, hidden tab szünet, saját intervallum), loading/hiba állapotok; `503 db_busy` -> sárga "szinkron folyamatban" üzenet, az utolsó adat látszik; `worker_down` -> magyar üzenet, Sync gomb letiltva ha a worker nem él; `binance_unavailable` -> szürke jelzés.
- Vite proxy: `/api` és `/mcp` -> `http://127.0.0.1:8600`, rewrite nélkül (dev port 5173).

### Parancsok / ellenőrzés
- `cd frontend && npm install` ; `npx tsc --noEmit` (0 hiba) ; `npm run build` (OK; a Plotly külön lazy chunk ~4.7 MB, `chunkSizeWarningLimit` megemelve).
- BFF: `uv run uvicorn --app-dir src api.main:create_app --factory --host 127.0.0.1 --port 8600`; valós adattal headless Chrome screenshot a `http://127.0.0.1:8600/` (a BFF szolgálja a `frontend/dist`-et): chart, equity, stratégia, modellek, Binance fillek, sync panel betöltött. Dev: `cd frontend && npm run dev`.

### Nyitott pontok
1. **Lassú Binance fallback**: `GET /api/market/chart` ~6 s, `/api/trades/active` ~4 s, `/api/trades/binance` ~3 s, ha nincs journal pozíció. A UI emiatt nem blokkol (a chart/aktív trade/fillek külön pollok, az utolsó adat marad, "frissítés…" jelzés), de javaslat t880/t889-re: a `market/chart` kapjon `include_trade=false` paramétert vagy az active trade kapjon rövid (30–60 s) szerveroldali cache-t; ekkor az `ActiveTradeCard` külön hívása elég.
2. A worker nem futott a teszt alatt (`worker.alive=false`): a Sync gomb letiltva jelent meg; a job SSE log és az auto-sync kapcsoló élő worker mellett nincs végigpróbálva (csak típus/build szinten) -> t881/t889 integrációs próba.
3. `/api/stream` (globális SSE) nincs a backendben: a státusz 4 mp-es pollinggal frissül; ha lesz, a `DashboardPage` `usePolling(getStatus)` cserélhető.
4. A journal/`trading_positions` üres, a `stop_loss/take_profit` a journalból null (t880 Notes 8. pont) — az Aktív trade kártya ilyenkor csak entry-t mutat.
5. `src/ui/` érintetlen (t889 törli); backend nem módosult.

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
Headless Chrome (1500x1000) a dashboardról: chart, szinkron panel (worker él, job log), aktív trade/stratégia, chat-dock gomb renderel; üres/hibás állapot nincs, a konzolon csak egy Plotly 'clear() called with no buffers' perf-figyelmeztetés.
