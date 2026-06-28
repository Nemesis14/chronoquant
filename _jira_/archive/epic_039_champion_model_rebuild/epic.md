# Epic 039: Champion Model Rebuild — Train/Valid Split

## Goal

A jelenlegi walk-forward CV-alapú sampling és hyperparameter search teljes lecserélése
egyszerű train/valid split megközelítésre. A champion modelleket (long + short) újra
kell futtatni az új pipeline-on, majd új strategy session és deploy szükséges.
Az élő kereskedés teljes modell- és artifact-csere után indul újra.

Módszertani háttér: `_doc_/methodology_doc/5400_sampling.md` (frissítve epic_038-ban).

## Scope

- `src/modeling/sampling/create_sample.py` — új `train_valid_split` sampling mode
- `src/modeling/search/lgbm_search.py` — CV eltávolítása, valid top10_lift objective, patience stopping
- `config/models.json` — champion modellek sampling config frissítése
- Pipeline újrafuttatás: sample → search → train → predict (l + s champion modellek)
- `src/strategy/00_run_strategy_session.py` — új strategy session új predikciókkal
- `src/data_handling/06_trigger_deploy.py` — deploy trigger + artifact csere
- `_doc_/methodology_doc/5500_hyper_param_search.md` — search metodológia frissítés
- `_doc_/database_and_code_doc/5300_create_sample.md` — code doc frissítés
- `_doc_/database_and_code_doc/5520_search.md` — code doc frissítés
- `_doc_/database_and_code_doc/0004_model_lifecycle.md` — lifecycle doc frissítés
- Analyst notebook: search vizualizáció (train/valid top10_lift per trial, l + s)

## Tasks

- t2: Sampling kód refaktor + config (modeling_agent)
- t3: Search kód refaktor (modeling_agent)
- t4: Pipeline rerun — sample + search (l + s) (modeling_agent)
- t5: Pipeline rerun — train + predict (l + s) (modeling_agent)
- t6: Strategy session — calibrate + grid search (modeling_agent)
- t7: Deploy trigger — artifact aktiválás (database_agent)
- t8: Analyst notebook — search vizualizáció (analyst_agent)
- t9: 5500_hyper_param_search.md frissítés (methodology_agent)
- t10: Code docs frissítés — 5300 + 5520 + 0004 (code_doc_agent)

## Execution order

Wave 1 (párhuzamos): t2, t9
Wave 2: t3 (blocked_by t2)
Wave 3 (párhuzamos): t4, t10 (mindkettő blocked_by t2 + t3)
Wave 4 (párhuzamos): t5, t8 (t5 blocked_by t4; t8 blocked_by t4)
Wave 5: t6 (blocked_by t5)
Wave 6: t7 (blocked_by t6)

## Key Decisions

- Walk-forward CV kivezetva: epic_038 audit megmutatta, hogy a train mask felső időhatár
  nélkül volt, jövőbeli adatszivárgással. Az egyszerű split módszertanilag tisztább és
  a kalibráció time window-jával konzisztens.
- Valid periódus = 2025-05 – 2026-05: szándékos összhang a strategy calibrációval,
  a legfrissebb piaci rezsimet képviseli.
- Search objective: valid top10_lift maximalizálása (fold-stability penalty eltávolítva).
- Stopping: patience=20 trial, epsilon=0.001; max 100 trial.
- Best trial selection: top valid top10_lift, ahol train-valid gap minimális.
- Long ÉS short champion modell újrafut; strategy és deploy is frissül.
