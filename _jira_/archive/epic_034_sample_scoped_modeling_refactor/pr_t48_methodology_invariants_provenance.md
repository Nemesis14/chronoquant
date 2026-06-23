---
epic: epic_034
id: t48
title: Methodology doc frissítése — I1-I7 invariánsok és provenance mezők
assignee: methodology_agent
status: pr
blocks: []
blocked_by: [t45]
---

## Goal

A `_doc_/methodology_doc/` zónában dokumentálni kell az új sample-scoped architektúra
invariánsait (I1-I7) és a `feature_set.json["provenance"]` szerződést. Jelenleg ezek
csak az arch_spec_t41.md-ben és a kód-referencia zónában (4100_quant_train.md)
szerepelnek összefoglalóan — a módszertani indoklás, döntés és kockázatok a
methodology zónában hiányoznak.

## Scope

- `_doc_/methodology_doc/5000_modelling.md` — a "Validációs elvek" szekció kibővítése
  az I1-I7 invariánsokkal és a sample scope döntés indoklásával
- `_doc_/methodology_doc/5400_sampling.md` — a snap-native sampling döntés (A vs B)
  és az I1, I2, I5 invariáns módszertani háttere
- Esetleg `_doc_/methodology_doc/5600_model_training.md` — I2 invariáns (search/train
  rowcount == sample rowcount) módszertani kontextusa

Az arch spec forrás: `_jira_/epic_034_sample_scoped_modeling_refactor/arch_spec_t41.md`

## Acceptance Criteria

- [x] Az I1-I7 invariánsok megjelennek a methodology zónában (a "miért fontos" szinten,
      nem kód-szinten) — 5000_modelling.md "I1-I7 invariánsok — módszertani szint" táblázat
- [x] A `feature_set.json["provenance"]` szerződés módszertani indoklása megjelenik
      — 5000_modelling.md "Provenance szerződés: miért szükséges a `source_contract` mező?" szekció
- [x] A predict step scope aszimmetria (teljes snapshot range, nem sample) módszertani
      indoklással szerepel — 5000_modelling.md "Predict step scope aszimmetria" szekció
- [x] A kód-referencia zóna (`4100_quant_train.md`) invariáns táblázata linkjei
      felfelé mutatnak az új methodology doc oldalakra — 5000, 5400, 5600 linkek hozzáadva

## Notes

### Elvégzett változtatások

**`_doc_/methodology_doc/5000_modelling.md`:**
- Új szekció: "Sample-scope döntés és pipeline invariánsok"
  - A vs B döntés módszertani összehasonlító táblázat (miért INNER JOIN, nem MIN/MAX időablak)
  - I1-I7 invariánsok módszertani szintű táblázata (mit garantál, miért fontos)
  - "Provenance szerződés" alfejezet: `source_contract` mező szerepe és indoklása
  - "Predict step scope aszimmetria" alfejezet: miért helyes a teljes snapshot score-olása
- "Validacios elvek" szekció frissítve: hivatkozás az új invariáns-szekcióra és a 4100 kód-ref-re

**`_doc_/methodology_doc/5400_sampling.md`:**
- Új szekció: "A snap-native scope mint modell-szintű szerződés: I1, I2, I5"
  - A vs B döntés sampling kontextusban (sorpontos scope vs. időablakos szűkítés)
  - I1, I2, I5 invariánsok sampling nézőpontból: hogyan kapcsolódnak a `model.__sample`-hez
  - Hivatkozás a 5000_modelling.md teljes rationale-ra

**`_doc_/methodology_doc/5600_model_training.md`:**
- Új alfejezet: "I2 invariáns a training lépésben: miért kritikus a sorpontos match?"
  - I1 vs I2 különbség értelmezése
  - I2 és I3 kapcsolata (snapshot immutability mint előfeltétel)
  - Hivatkozás a 5000_modelling.md-re

**`_doc_/database_and_code_doc/4100_quant_train.md`:**
- Invariáns-táblázat alatti metodológiai link kibővítve: 5000, 5400, 5600 oldalakra mutat
  (korábban csak 5000-re mutatott)
