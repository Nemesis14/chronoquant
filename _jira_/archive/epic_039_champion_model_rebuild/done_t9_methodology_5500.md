---
epic: epic_039
id: t9
title: 5500_hyper_param_search.md frissítés — CV → train/valid split
assignee: methodology_agent
status: pr
blocks: []
blocked_by: []
---

## Goal

A `5500_hyper_param_search.md` metodológiai dokumentum frissítése: a walk-forward
CV fold-stability objektív lecserélése az egyszerű valid top10_lift megközelítésre,
Optuna patience-alapú stopping dokumentálása, best trial selection logika leírása.

## Scope

- `_doc_/methodology_doc/5500_hyper_param_search.md`

## Acceptance Criteria

- [x] CV fold-logika és fold-stability penalty eltávolítva a dokumentumból
- [x] Új objective: valid top10_lift (egyszerű, nincs std penalty)
- [x] Train top10_lift szerepe: diagnosztika, nem optimized
- [x] Optuna stopping dokumentálva: patience=20, epsilon=0.001, max 100 trial
- [x] Best trial selection: valid max + train-valid gap mint másodlagos szűrő
- [x] Comparison table frissítve (régi CV megközelítés: kivezetett; új: aktív)
- [x] Mermaid diagramok aktuálisak

## Notes

Stílus: `_doc_/methodology_doc/5400_sampling.md` — metodológia és indoklás,
nem kód-leírás. A kód-referencia a `5520_search.md`-ben lesz (t10 task).

---

[methodology_agent] Elvégezve — 2026-06-23

**Változtatások összefoglalója:**

- **CV / fold-stability eltávolítva:** A korábbi "Fold-stabilitás" szekció
  (4 fold átlag − lambda*std képlettel) teljesen kivezetésre került. A comparison
  table kivezetett státuszban tartalmazza az okot (epic_038 finding: train mask
  nem tartalmazott felső időhatárt → jövőbeli szivárgás a CV-ben).

- **Új objective dokumentálva:** Valid top10_lift egyszerű mechanizmusa leírva
  (top decile target átlag − teljes minta átlag). Mermaid flowchart illusztrálja
  a számítás lépéseit.

- **Train top10_lift diagnosztikai szerepe:** Külön szekció tisztázza, hogy a
  train lift párhuzamosan riportált de nem optimized; a train-valid gap
  detektálásához szükséges. Mermaid diagram mutatja a trial-szintű adatáramlást.

- **Optuna stopping dokumentálva:** stateDiagram-v2 illusztrálja a patience
  mechanizmust (patience=20, epsilon=0.001, max=100 trial). Szöveges magyarázat
  mindkét leállási feltételre.

- **Best trial selection leírva:** Flowchart mutatja a rangsorolás → gap
  ellenőrzés → best trial kiválasztás lépéseit. Tisztázva: a gap másodlagos
  szűrő, nem kemény küszöb.

- **Diagramok:** 5 Mermaid diagram a fájlban (overview flowchart + időbeli
  elrendezés, alternatívák összehasonlítás LR, top10_lift számítás TD, train
  diagnosztika graph TD, Optuna stopping stateDiagram-v2, best trial selection TD).

- **Validációs checklist bővítve:** 9 ellenőrzési pont, a CV-specifikus tételek
  (purge foldhatárokon, foldonkénti lift) lecserélve az új megközelítés
  specifikus ellenőrzéseire (stopping feltételek aktívak, gap riportálás, trial
  log tartalom).
