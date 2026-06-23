---
epic: epic_033
id: t34
title: 0002–0004 cross-domain audit + törött link fix (helyben)
assignee: code_doc_agent
status: done
blocks: [t39]
blocked_by: []
---

## Goal
A három cross-domain arch ref (0002 data_architecture, 0003 runtime_flow, 0004 model_lifecycle)
tartalom-ellenőrzése a tényleges kód alapján, és a 14 törött `methodology_doc/...` link javítása.
A fájlok HELYBEN maradnak a database_and_code_doc-ban.

## Scope
- `_doc_/database_and_code_doc/0002_data_architecture.md`
- `_doc_/database_and_code_doc/0003_runtime_flow.md`
- `_doc_/database_and_code_doc/0004_model_lifecycle.md`
- Linkfix: `methodology_doc/...` → `../methodology_doc/...` (14 db: 0002=5, 0003=3, 0004=6)
- Tartalom-ellenőrzés: a leírt folyamat egyezzen a kóddal — sync_predictions cutover
  (`sync_tables/sync_predictions.py`), 06_trigger_deploy, snapshots/registry (`store/`).

## Acceptance Criteria
- [x] Mind a 14 methodology_doc link létező célfájlra mutat (`../methodology_doc/`)
- [x] A 0002↔0003↔0004 egymás közti linkjei élnek
- [x] A diagramok/folyamatleírás egyezik a tényleges kóddal (cutover, snapshot, registry)
- [x] Más zóna tartalma NINCS módosítva (csak link)

## Notes
A linkek azért törtek el, mert gyökér-relatívként íródtak, de a fájl a database_and_code_doc-ban van.

### Elvégzett változtatások

**0002_data_architecture.md** — 5 link javítva + 1 display-text korrekció:
- `methodology_doc/1400_snapshots.md` → `../methodology_doc/1400_snapshots.md` (3 előfordulás)
- `methodology_doc/1500_registry.md` → `../methodology_doc/1500_registry.md` (2 előfordulás)
- `methodology_doc/4000_quant_train.md` → `../methodology_doc/4000_quant_train.md` + display text rövidítve

**0003_runtime_flow.md** — 3 link javítva + 1 tartalmi hiba javítva:
- `methodology_doc/1500_registry.md` → `../methodology_doc/1500_registry.md`
- `methodology_doc/1400_snapshots.md` → `../methodology_doc/1400_snapshots.md`
- `methodology_doc/7100_live_trading.md` → `../methodology_doc/7100_live_trading.md`
- Tartalmi: `pred_long, pred_short` → `long_pred, short_pred` (egyezés a kód `_LONG_PRED_COL`/`_SHORT_PRED_COL` konstansokkal)

**0004_model_lifecycle.md** — 6 link javítva:
- `methodology_doc/1400_snapshots.md` → `../methodology_doc/1400_snapshots.md` (2 előfordulás)
- `methodology_doc/1500_registry.md` → `../methodology_doc/1500_registry.md`
- `methodology_doc/5400_sampling.md` → `../methodology_doc/5400_sampling.md`
- `methodology_doc/5500_hyper_param_search.md` → `../methodology_doc/5500_hyper_param_search.md`
- `methodology_doc/6000_strategy.md` → `../methodology_doc/6000_strategy.md`

**Tartalom-ellenőrzés eredménye:**
- cutover flow (detect → execute → activate) egyezik a kóddal
- `_activate_deployment`: pending→active + régi→archived logika egyezik
- `06_trigger_deploy.py`: INSERT pending sor a registrybe — egyezik
- `snapshots.py`: CTAS + `content_sha256`/`feature_set_hash` — egyezik
- `registry.py`: 8 tábla, `STATUS_LIFECYCLE`, `open_crud_connection` — egyezik
- follow-up validáció: a korábbi `model.__train_input` narratíva drift volt; t39+t40 során
  `snap ⋈ model.__sample` JOIN-ra javítva a lifecycle leírás

Nincs Entry Gate blocker (minden hivatkozott methodology_doc fájl létezik).
