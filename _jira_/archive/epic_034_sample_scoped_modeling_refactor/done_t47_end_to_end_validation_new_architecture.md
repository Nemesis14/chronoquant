---
epic: epic_034
id: t47
title: Vegso end to end validacio az uj architekturara
assignee: validator_agent
status: done
blocks: []
blocked_by: [t41, t42, t43, t44, t45, t46]
---

## Goal
Lezáró validáció arra, hogy az új sample-scoped architektúra konzisztens, és a
pipeline ténylegesen végigment rajta.

## Scope
- statikus ellenőrzések
- releváns tesztek
- artifact és registry konzisztencia
- dokumentációs konzisztencia

## Acceptance Criteria
- [x] A releváns ellenőrzések és tesztek átmennek
- [x] Nincs nyilvánvaló architekturális ellentmondás a modeling láncban
- [x] A docs, artifactok és registry állapot összehangban vannak
- [x] A teljes újrafuttatás eredménye elfogadható és dokumentált

## Notes
Ha itt bukik ki rejtett legacy-függés, azt vissza kell dobni a megfelelő upstream taskra.

[validator_agent] Lezárva — 2026-06-22
- ruff check src/modeling/ src/strategy/ — All checks passed (1 pre-existing B017 javítva: test_walk_forward_config.py FrozenInstanceError)
- pyright src/modeling/ src/strategy/ — 0 errors, 0 warnings
- Uj smoke teszt irva: src/modeling/tests/smoke/test_pipeline_fe_step.py (4 teszt, t42 snapshot_id propagalas ellenorzese)
- src/modeling/ teljes suite: 112/112 passed
- src/strategy/tests/: 17/17 passed
- model."lgbm_solusdt_l_fw60_2101_2605__pred" letezik: 2846880 sor
- artifacts/lgbm_solusdt_l_fw60_2101_2605/feature_engineering/feature_set.json letezik, provenance.source_contract megvan, sample_rows == joined_rows == 47448 (I1 OK)
