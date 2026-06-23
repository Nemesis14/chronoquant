---
epic: epic_034
id: t45
title: Dokumentacio es provenance frissites az uj architekturara
assignee: code_doc_agent
status: pr
blocks: [t47]
blocked_by: [t41, t42, t43, t44]
---

## Goal
A dokumentáció és a provenance-leírások tükrözzék az új sample-scoped architektúrát.

## Scope
- `_doc_/methodology_doc/`
- `_doc_/database_and_code_doc/`
- manifest / provenance szerződések leírásai

## Acceptance Criteria
- [ ] A docs nem írnak olyat, hogy a FE csak időablakra szűkített teljes `quant_train`-on fut
- [ ] A modeling lánc mindenhol egységesen van leírva
- [ ] A predikciós és strategy input-output szerződés tiszta marad

## Notes
Ez a task csak az implementációs döntések lezárása után futhat értelmesen.

[code_doc_agent] Elvégezve — 2026-06-22

### Frissített fájlok (database_and_code_doc zóna)

1. **`4100_quant_train.md`**
   - Az "Áttekintés" flowchart és szöveg javítva: a `live.quant_train` most már
     helyesen csak a snapshot forrása; a FE/search/train a `snap ⋈ model.__sample`
     path-on fut.
   - Hozzáadva: "Snap-native sample handoff" szekció a `model.__sample` sémájával.
   - A "Yearly sample artifact handoff" átnevezve "Yearly parquet artifact (legacy)" -re.
   - Hozzáadva: **I1-I7 invariáns összefoglaló táblázat** kód-referencia szinten.
   - Kapcsolódó dokumentumok bővítve: `5300_create_sample.md`, `0004_model_lifecycle.md`.

2. **`5300_create_sample.md`**
   - Átstrukturálva: két sampling path elkülönítve (snap-native aktív vs. yearly legacy).
   - Hozzáadva: `create_model_sample` és `create_snapshot_sample` teljes kód-referencia
     (sequenceDiagram + flowchart + paraméter táblázat).
   - A `create_yearly_sample` LEGACY jelölést kapott.
   - Kapcsolódó fájlok bővítve: `1510_registry_code.md`, `5530_pipeline_predict_provenance.md`.

3. **`5510_training.md`**
   - `load_modeling_dataset` LEGACY jelölést és megjegyzést kapott: az aktív pipeline
     nem hívja ezt (snap-native path van érvényben); t43 auditálja.

4. **`5530_pipeline_predict_provenance.md`**
   - `step_feature_engineering` szekció: hozzáadva az I1 kikényszerítés leírása és a
     `feature_set.json["provenance"]` mezők táblázata.
   - `predict_offline` szekció: hozzáadva az I3 invariáns és a predict scope
     asszimmetria explicit leírása.
   - Kapcsolódó fájlok bővítve.

5. **`5520_search.md`**
   - `_load_search_dataset` szekció: hozzáadva I2 invariáns megjegyzés (logging szint).
   - Kapcsolódó fájlok bővítve.

6. **`5200_sampling_artifacts.md`**
   - `sample_train_valid.parquet` sémánál megjegyzés: ez a legacy yearly parquet
     formátum; az aktív `model.__sample` DuckDB táblában nincs `feat_*`.
   - Kapcsolódó fájlok frissítve.

7. **`0004_model_lifecycle.md`**
   - Sample step: I5, I6 garantálva megjegyzések hozzáadva; invariáns cross-reference.
   - Feature Engineering step: I1 és I7 kikényszerítés leírása hozzáadva.
   - Kapcsolódó dokumentumok bővítve.

### Megnyitott ticket methodology_agent-nek

- **t48** `todo_t48_methodology_invariants_provenance.md` — az I1-I7 invariánsok
  módszertani háttere a `methodology_doc/` zónában (5000_modelling.md, 5400_sampling.md)

### Acceptance criteria teljesítése

- [x] A docs nem írnak olyat, hogy a FE csak időablakra szűkített teljes `quant_train`-on fut
- [x] A modeling lánc mindenhol egységesen van leírva (snap ⋈ model.__sample path)
- [x] A predikciós és strategy input-output szerződés tiszta (predict = teljes snap, sample scope = FE/search/train only)
