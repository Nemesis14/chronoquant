---
epic: epic_008
id: t5
title: Teljes re-sync futtatása az új pipeline-nal
assignee: database_agent
status: done
blocked_by: [t3, t4]
---

## Goal
Az új `sync_pipeline.py` end-to-end futtatása a teljes historikus rangre-en, hogy minden tábla az új kódbázisból legyen feltöltve és konzisztens legyen.

## Scope
- `ohlcv` — nem kell újrafuttatni (Binance szinkron, a tábla tartalma nem változott a refactortól)
- `targets` — teljes rebuild (full range)
- `features` — teljes rebuild (chunked)
- `predictions` — teljes rebuild (chunked)

## Futtatási parancs

```
uv run python src/database/sync_pipeline.py --skip-ohlcv --start "2017-01-01 00:00:00"
```

## Acceptance Criteria
- [x] `sync_pipeline.py` végigfut hiba nélkül
- [x] `01_validate_stats.py` smoke report: mind a 4 tábla `status=OK`
- [x] `uv run pytest src/database/tests/ -v` zöld
- [x] Row count és time range konzisztens a rebuild előtti állapottal

## Notes
Ha az OHLCV tábla érintetlen maradt a refactortól, a Binance re-sync kihagyható (`--skip-ohlcv`).
Ha bármelyik teszt piros: először a smoke report-ot nézni (row count, null ratio), utána a teszt logot.

[orchestrator] 2026-06-15 — Elvégezve
- Parancs: `uv run python src/database/02_sync_pipeline.py --skip-ohlcv --start "2017-01-01 00:00:00"`
- Futási idő: 897.5s (targets + 38 features chunk + 38 predictions chunk)
- validate_stats: ohlcv=OK, target=OK, feat_ohlcv_quant=OK, predictions=OK — mind 3,022,861 sor, range=2020-09-14..2026-06-14
- pytest: 112/112 passed (19.97s)
- Bug fix: `01_validate_stats.py` `data_dir` → `db_path` (KeyError volt)

[validator] 2026-06-15 — Validálva, done-ra zárva
- ruff check src/database/ --fix → All checks passed (0 issue)
- uv run pyright src/database/ → 0 errors, 0 warnings, 0 informations
- Bugfix ellenőrzés: `asset_cfg["database"]["db_path"]` helyes — `load_asset_config()` pontosan ezt a struktúrát adja vissza
- Tesztek és validate_stats az orchestrator által futtatva, zöldek (nem futtattuk újra)
- `data_pipeline` → `sync_tables` mappa rename: NEM elvégezve — sandbox permission restriction (CodexSandboxUsers M+DC jogok, rename nem engedélyezett). A benne lévő importok már helyesek (`database.sync_tables.*`), pytest --import-mode=importlib miatt a mappanév nem befolyásolja a tesztfutást. Fix: következő session, manuálisan vagy admin jogokkal.
