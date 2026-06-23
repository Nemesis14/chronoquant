---
epic: epic_033
id: t40
title: modeling follow-up — feature_engineering sample-scope korrekció
assignee: modeling_agent
status: done
blocks: []
blocked_by: []
---

## Goal
A feature engineering lépés ténylegesen az adott modell fejlesztési mintáján
fusson, ne csak a `model.__sample` időablakára szűkített teljes `quant_train`
tartományon.

## Scope
- Auditálni a `src/modeling/01_feature_engineering.ipynb` és a kapcsolódó
  `src/modeling/feature_engineering/` csomag működését.
- Megszüntetni azt az inkonzisztenciát, hogy a notebook jelenleg:
  1. beolvassa a `model.__sample` minimum és maximum idejét,
  2. majd erre az időablakra egy teljes `quant_train` részhalmazon futtat FE-t,
  3. tehát nem a konkrét sample-sorokon és nem közvetlen snapshot-projekción dolgozik.
- A kívánt célállapot:
  - a FE az adott modellhez tartozó fejlesztési mintát használja,
  - a feature-választás ugyanarra a snapshot/sample szerződésre épüljön, mint a
    search, train és predict,
  - a provenance-ben egyértelmű legyen, melyik sample/snapshot alapján jött létre
    a `feature_set.json`.

## Acceptance Criteria
- [x] A feature engineering inputja nem pusztán időablakra szűkített `quant_train`
      másolat, hanem az adott modell mintájához kötött adatnézet
- [x] A FE kiválasztott feature-listája ugyanarra a modell-scope-ra épül, mint a
      későbbi search/train/predict lépések
- [x] A változás nem töri el a meglévő FE artifactokat (`feature_set.json`, notebook, html)
- [x] A methodology és code-doc anyagok ezután már nem állítanak olyat, hogy a FE
      teljes időablakon fut, ha ténylegesen modell-mintán dolgozik

## Notes
- Konkrét eltérés a jelenlegi kódban:
  `src/modeling/01_feature_engineering.ipynb` a `model."<MODEL_ID>__sample"` táblából
  csak `MIN(open_time)` és `MAX(open_time)` értéket vesz át, majd
  `CREATE TABLE quant_train AS SELECT * FROM _src.quant_train WHERE open_time >= ? AND open_time <= ?`
  logikával dolgozik.
- Ez azt jelenti, hogy a sample-időablak teljes tartományán fut a FE, nem a
  tényleges sample-sorokon.
- A projekt elvi célja ezzel szemben a snapshot → sample → FE → search → fit →
  strategy zárt lánc.

### Megvalósítás (2026-06-22)

- Új helper: `src/modeling/feature_engineering/sample_scope.py`
  `materialize_sample_scoped_quant_train(conn, model_id, snapshot_id)`
- A notebook most a `sampling.snapshot_id`-t configból vagy `reg.models`-ből oldja fel,
  majd `snap."<snapshot_id>" ⋈ model."<model_id>__sample"` JOIN-ból hoz létre
  lokális `quant_train` temp táblát
- A `feature_set.json` új `provenance` blokkot kap:
  `snapshot_id`, `sample_table`, `sample_rows`, `joined_rows`, `min_open_time`,
  `max_open_time`, `source_contract`
- Frissítve a kapcsolódó módszertani és kód-ref doksik:
  `2010_feature_engineering.md`, `5000_modelling.md`,
  `0004_model_lifecycle.md`, `5530_pipeline_predict_provenance.md`
- Teszt lefedés hozzáadva a helperhez; smoke + provenance tesztek zöldek
