---
epic: epic_033
id: t38
title: 2xxx/3xxx/4xxx/7xxx/8xxx audit + infra CLI kód-ref
assignee: code_doc_agent
status: done
blocks: [t39]
blocked_by: []
---

## Goal
A maradék meglévő kód-ref oldalak auditja a tényleges kód alapján, és a hiányzó infra/CLI fájlok
kód-referenciája.

## Scope
- Audit: 2100/2200 (features) vs `sync_features.py`/`_features_polars.py`; 3100 (targets);
  4100 (quant_train); 7110–7150 (trading: run_service, service, journal, exchange, state/strategy);
  8110–8150 (ui: main, data, sync, runners, components)
- Új infra kód-ref: `store/migrations.py`, `store/registry_validator.py`,
  `sync_tables/sync_quant_train.py`, `04_backfill_predictions.py`, `06_trigger_deploy.py`

## Acceptance Criteria
- [ ] A meglévő 2/3/4/7/8xxx oldalak a tényleges kódot tükrözik (drift javítva)
- [ ] Az új infra fájlokhoz kód-ref Overview diagrammal + függvény-szintű leírás
- [ ] Felfelé link a meglévő methodology-ra (4000, 7000/7100, 8000/8100); nincs duplikáció
- [ ] Konzisztens a 0003/0004 deploy/cutover narratívával

## Notes
Csak database_and_code_doc szerkeszthető; más zóna csak link.

### Elvégzett munka (2026-06-22)

**Drift javítások (meglévő fájlok):**
- `2100_sync_features.md` — path javítva `src/database/` → `src/data_handling/`; Overview flowchart + methodology uplink hozzáadva; Kapcsolódó dokumentumok szekció
- `3100_sync_targets.md` — path javítva; Overview flowchart + methodology uplink hozzáadva
- `4100_quant_train.md` — CLI példa javítva (`src/database/` → `src/data_handling/`); pontosított CLI flags
- `7110–7150_trading_*.md` — methodology uplinkek hozzáadva (7000+7100); Kapcsolódó dokumentumok szekciók; `7110` CLI szekció bővítve
- `8110–8150_ui_*.md` — methodology uplinkek hozzáadva (8000+8100); Kapcsolódó dokumentumok szekciók
- `1000_database.md`, `1001_database_module.md`, `1200_sync_tables.md` — maradék `src/database/` path referenciák javítva

**Új fájlok:**
- `1520_registry_validator.md` — `store/registry_validator.py` kód-ref (ValidationResult, validate_registry, CLI)
- `1240_backfill_predictions.md` — `04_backfill_predictions.py` CLI kód-ref (gap detection, monthly chunks, main)
- `1530_trigger_deploy.md` — `06_trigger_deploy.py` CLI kód-ref (trigger_deploy, registry-intent, rollback)
- `4110_sync_quant_train.md` — `sync_tables/sync_quant_train.py` függvény-szintű kód-ref

**Nem létrehozott (már fedett):**
- `1160_migrations.md` — a `migrations.py` már teljes mélységben dokumentálva: `1510_registry_code.md`-ben

**Acceptance Criteria:**
- [x] A meglévő 2/3/4/7/8xxx oldalak a tényleges kódot tükrözik (drift javítva)
- [x] Az új infra fájlokhoz kód-ref Overview diagrammal + függvény-szintű leírás
- [x] Felfelé link a meglévő methodology-ra (4000, 7000/7100, 8000/8100); nincs duplikáció
- [x] Konzisztens a 0003/0004 deploy/cutover narratívával
