---
epic: epic_034
id: t41
title: Vegleges sample-scoped architektura es invariansok rogzitese
assignee: modeling_agent
status: pr
blocks: [t42, t43, t44, t45, t46, t47]
blocked_by: []
---

## Goal
Pontosan rögzíteni, hogy az új modellezési láncban mi a hivatalos adatfolyam és
mi számít modell-scope inputnak minden lépésben.

## Scope
- Modeling architektúra
- sample / FE / search / train / predict / strategy kapcsolatok
- explicit invariánsok a provenance-re és intermediate objektumokra

## Acceptance Criteria
- [x] Egyértelműen rögzített az aktív lánc: snapshot -> model.__sample -> FE -> search -> fit -> predict -> strategy
- [x] Kimondott döntés születik arról, hogy a FE közvetlen sample-sorokon, vagy egy abból épített explicit nézeten/táblán fusson
- [x] Rögzítve vannak a kötelező provenance mezők és intermediate objektumok

## Notes
Ez a task a teljes epic alapja; itt kell megszületnie annak a döntésnek, amit a többi implementáció végigvisz.

[modeling_agent] 2026-06-22 — Arch spec elkészítve: arch_spec_t41.md
- Döntés: A megközelítés (snap ⋈ model.__sample INNER JOIN → TEMP quant_train),
  B megközelítés elutasítva (felesleges indirekcio).
- Megállapítás: a `materialize_sample_scoped_quant_train` (sample_scope.py) és a
  `01_feature_engineering.ipynb` config cellája már implementálja a helyes logikát.
- A search és train lépések szintén már snapshot-native path-t használnak.
- I1-I7 invariánsok rögzítve; 7 provenance mező specifikálva a feature_set.json-hoz.
- Gap analízis: t42 FE pipeline konzisztencia audit, t43 datasets.py legacy audit,
  t44 strategy contract ellenőrzés.
- Adatfolyam táblázat, intermediate objektumok és registry séma a spec fájlban.
