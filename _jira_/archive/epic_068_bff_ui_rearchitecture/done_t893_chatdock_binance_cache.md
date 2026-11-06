---
epic: epic_068
id: t893
title: ChatDock a Layoutban + Binance open-position cache és include_trade a market/chart-on
assignee: ui_agent
status: done
blocks: []
blocked_by: []
---

## Goal
1. A `<ChatDock />` (floating chat panel, `/chat` útvonalon elrejtve) bekerül a `Layout`-ba, hogy minden oldalon elérhető legyen.
2. A `/api/trades/active` és a `/api/market/chart` nem journal-pozíció esetén a Binance-fallback miatt 4-6 s-ig tartott pollonként. Szerveroldali rövid TTL-es cache a Binance-hívás eredményére, és `include_trade` query param a `market/chart`-ra, hogy a trade overlay kihagyható legyen.

## Scope
- `frontend/src/components/Layout.tsx` — `ChatDock` import + `<ChatDock />` a `<main>` után.
- `src/api/services/binance.py` — `current_position()` TTL-cache (`POSITION_CACHE_TTL_S = 45.0`, `threading.Lock`, `clear_position_cache()`); a korábbi lookup `_fetch_current_position()` lett. Hiba és "nincs pozíció" eredmény is cache-elődik.
- `src/api/services/market.py` — `chart(conn, window, include_trade=True)`; `include_trade=False` esetén nincs active-trade lookup, `active_trade: null`.
- `src/api/routers/market.py` — `include_trade: bool = True` query param a `/chart`-on.
- `src/api/tests/test_binance_cache.py` — új teszt modul (monkeypatch-elt Binance kliens, szabályozott órajel).
- Válasz-séma nem változott.

## Acceptance Criteria
- [x] `Layout.tsx`-ben a `<ChatDock />` megjelenik minden route-on (a `/chat`-on a komponens maga rejti el magát)
- [x] `cd frontend && npx tsc --noEmit && npm run build` zöld
- [x] Binance open-position lookup TTL-en belül nem hívódik újra (teszt)
- [x] TTL lejárta után újra lekérdez (teszt)
- [x] Hiba / üres eredmény is cache-elődik (teszt)
- [x] A visszaadott dict másolat, külső módosítás nem szennyezi a cache-t (teszt)
- [x] `/api/market/chart?include_trade=false` → `active_trade: null`, a payload alakja változatlan (teszt)
- [x] `uv run ruff check src/api --fix`, `uv run pyright src/api` (0 errors), `uv run pytest src/api -v` (47 passed) zöld

## Notes
- A cache a `binance.current_position()`-ben van, így mindkét endpoint (`/trades/active` és `/market/chart`) ugyanazt a memoizált eredményt használja. A journal-ág nincs cache-elve (olcsó DuckDB olvasás).
- A cache processzen belüli (in-memory), a FastAPI worker-enként külön.
- A `_monotonic` modul-szintű alias a teszteléshez van (órajel-mock).
- Nem nyúltam: `src/data_handling`, `analyst/`, `src/api/routers/reports.py`, ReportsPage.

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
