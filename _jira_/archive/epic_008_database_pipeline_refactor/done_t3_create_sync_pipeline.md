---
epic: epic_008
id: t3
title: sync_pipeline.py létrehozása, régi scriptek törlése
assignee: database_agent
status: done
blocked_by: [t1, t2]
blocks: [t4, t5]
---

## Goal
A jelenlegi `02_sync_ohlcv.py` és `03_rebuild_derived.py` összevonása egyetlen `sync_pipeline.py` scriptbe. Ez lesz az egyetlen belépési pont a teljes adatbázis szinkronizáláshoz — legyen szó 1 perces live frissítésről vagy teljes historikus backfill-ről.

## Scope
- `src/database/sync_pipeline.py` — új fájl létrehozása
- `src/database/02_sync_ohlcv.py` — törlés
- `src/database/03_rebuild_derived.py` — törlés

## Paraméterei (CLI)

```
--asset-id          Asset key (default: config default)
--start             UTC start time (default: earliest OHLCV)
--end               UTC end time (default: latest OHLCV)
--chunk-months N    Feldolgozási chunk méret (default: 3)
--skip-ohlcv        Ne hívja a Binance sync-et (csak derived táblák)
--tables            Vesszővel elválasztott lista: ohlcv,targets,features,predictions
                    (default: mind)
```

## Dependency logika (a scriptben explicit)

```
ohlcv     → mindig első, ha kérték
targets   → ohlcv után (full range, nem chunkolt)
features  → ohlcv után (chunkolt)
predictions → features után (chunkolt)
```

Ha csak `--tables features,predictions`: targets nem fut, ohlcv nem fut.
Ha csak `--tables predictions`: features nem fut újra, de predictions igen.

## Acceptance Criteria
- [x] `sync_pipeline.py` egyetlen CLI-vel lefedi az összes korábbi use case-t
- [x] File logging megmarad (mint `02_sync_ohlcv.py`-ban volt)
- [x] `--skip-ohlcv` flag működik (belső re-számítás Binance nélkül)
- [x] `--tables` flag szabályozza melyik sync függvény hívódik meg
- [x] Chunking logika egységes, nem duplikált
- [x] `02_sync_ohlcv.py` és `03_rebuild_derived.py` törölve

## Notes
A chunking helper (`_monthly_chunks`) a scriptben marad, nem kell store-ba tenni — ez pipeline-szintű logika.
`sync_ohlcv` Binance API-t hív, a többi belső számítás — ezt dokumentálni a script docstring-jében.

---
**2026-06-15 — database_agent**

`src/database/sync_pipeline.py` létrehozva. A két régi script (`02_sync_ohlcv.py`, `03_rebuild_derived.py`) törölve.

Implementációs döntések:
- File logging: `database/<asset>/logs/sync_pipeline_<timestamp>.log` (02-es script mintájára, stdout handler is mindig aktív)
- `_monthly_chunks` helper a scriptben maradt (pipeline-szintű logika)
- `_resolve_tables()` validálja a `--tables` argumentumot és `--skip-ohlcv` esetén elveszi az `ohlcv`-t a setből
- `need_derived` guard: derived range (`_earliest_ohlcv_date` / `_latest_ohlcv_date`) csak akkor hívódik, ha van legalább egy derived tábla a kérésben
- Dependency order enforced: ohlcv → targets → features → predictions sorrend mindig érvényesül, független a `--tables` sorrendjétől
- `LOOKBACK_BARS = 2880` (mint `03_rebuild_derived.py`-ban), `DEFAULT_CHUNK_MONTHS = 3`
- `INIT_START_DATE = "2017-01-01 00:00:00"` (mint `02_sync_ohlcv.py`-ban) — ohlcv `--start` default

[validator] done — 2026-06-15
Fixes applied to `src/database/02_sync_pipeline.py`:
1. `chunks: list[tuple[str, str]] = []` inicializálva a `need_derived` blokk előtt (possibly unbound fix)
2. `_resolve_tables` return type: `tables: set[str]` type annotation hozzáadva (pyright invariance fix)
3. `pd.Timestamp(...).timestamp()` → `.value // 1_000_000` (NaT.timestamp AttributeError fix)
ruff: 3 auto-fix (clean after). pyright: 0 errors. Tests: 112/112 pass.
New smoke tests: `src/database/tests/sync_pipeline/smoke/test_sync_pipeline_helpers.py`
(10 tests for `_monthly_chunks` + `_resolve_tables`).
