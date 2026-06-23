# Epic 033: database_and_code_doc zóna rendrakása (kódbázis-doksi)

## Goal
A `_doc_/database_and_code_doc/` (kód-referencia zóna) átfogó rendrakása a tényleges kód alapján:
tartalom-ellenőrzés, anomáliák/ellentmondások feloldása, redundancia eliminálása, lefedettségi
hézagok pótlása. A `_plans_` registry-architektúra implementálva van — a doksinak ezt kell tükröznie.

## Hatókör-korlát (FONTOS)
- **Kizárólag a `database_and_code_doc/` zóna szerkeszthető.**
- A többi mappa (`methodology_doc/`, `models_doc/`, `_doc_/` gyökér 0000/0001, `analyst/`) **csak
  linkként** használható — TILOS a tartalmukat módosítani.
- A 0002/0003/0004 **helyben marad** a database_and_code_doc-ban (a korábbi gyökér-mozgatás
  elhalasztva); csak a törött linkjeik javítandók `../methodology_doc/`-ra.
- Entry Gate: ha egy kód-refhez hiányzik a methodology X100, NE írd meg — nyiss `todo_` ticketet a
  methodology_agent-nek, és linkelj a meglévő methodology overview-ra (5000, 6000, stb.).

## Tasks (mind code_doc_agent, kivéve validálás)
- t34: 0002–0004 cross-domain audit + 14 törött link fix (helyben) [no dep]
- t35: 1xxx database/store/sync/tests + snapshots/registry kód-ref audit [no dep]
- t36: modeling 5xxx — sampling audit + training/search/pipeline/predict/provenance kód-ref [no dep]
- t37: strategy 6xxx kód-ref (új) [no dep]
- t38: 2xxx/3xxx/4xxx/7xxx/8xxx audit + infra CLI kód-ref (migrations, validator, sync_quant_train, 04/06) [no dep]
- t39: validálás — dead-link scan, konzisztencia (validator_agent) [blocked_by t34–t38]
- t40: modeling follow-up — feature_engineering sample-scope korrekció (modeling_agent) [no dep, scope extension]

> A dev taskok fájl-diszjunktak (külön szám-tartományok) → biztonságos párhuzamos spawn (Mód B, max 3).

## Key Decisions
- Fókusz leszűkítve a database_and_code_doc zónára (user-kérés); más zónák csak linkek.
- 0002–0004 helyben marad; gyökér-mozgatás külön, későbbi döntés.

## Audit findings
- 14 törött `methodology_doc/...` link a 0002/0003/0004-ben (hiányzó `../`).
- Lefedettségi hézag: modeling (training/search/pipeline/predict/provenance), strategy (6xxx teljes),
  infra (migrations, registry_validator, sync_quant_train, 04_backfill_predictions, 06_trigger_deploy).
- Drift: 0000 Repository Layout elavult — DE 0000 a gyökérben van, NEM ebben a hatókörben (külön jegyezve).
- Redundancia: 0002 vs methodology 1400/1500 — a database_and_code_doc oldalon link, nem másolat.
- Új follow-up finding: a `01_feature_engineering.ipynb` jelenleg csak a `model.__sample`
  időhatárát használja, de a tényleges feature-szelekció a teljes `quant_train`
  időablakon fut, nem a konkrét model-fejlesztési mintán / snapshot-projekción.
  Ez modell-scope inkonzisztencia, külön javítandó.
