---
epic: epic_039
id: t3
title: Search kód refaktor — CV eltávolítása, valid top10_lift objective
assignee: modeling_agent
status: pr
blocks: [t4, t10]
blocked_by: [t2]
---

## Goal

A hyperparameter search kód átírása: walk-forward CV fold-logika eltávolítása,
helyette egyszerű train/valid evaluáció. Új objective: valid top10_lift
maximalizálása. Optuna patience-alapú early stopping bevezetése.

Módszertani háttér: `_doc_/methodology_doc/5500_hyper_param_search.md` (frissítve t9-ben)

## Scope

- `src/modeling/search/lgbm_search.py`

## Acceptance Criteria

- [ ] CV fold-logika (`_fold_split_walk_forward`) eltávolítva vagy kikapcsolva
- [ ] Minden trial: modell tanul a train seten, kiértékel train ÉS valid seten
- [ ] Objective = `valid_top10_lift` (fold-stability penalty eltávolítva)
- [ ] Train top10_lift is rögzítve minden trialhoz (diagnosztika + analyst notebook)
- [ ] Optuna patience-alapú early stopping: patience=20, epsilon=0.001
- [ ] Max 100 trial
- [ ] Best trial kiválasztás: legmagasabb valid top10_lift, ahol train-valid gap minimális
- [ ] Search artifacts megmaradnak: `search_best.json`, `search_trials.jsonl`
- [ ] `reg.search_runs` bejegyzés az új formátummal
- [ ] `ruff check` és `pyright` tisztán fut

## Notes

A `search_trials.jsonl`-ben minden trial tartalmaz `train_top10_lift` és
`valid_top10_lift` mezőt — az analyst notebook (t8) ezekre épít.

---

[modeling_agent] Implementálva — 2026-06-23

### Elvégzett változtatások

**Eltávolítva:**
- `_fold_split_walk_forward` és `_fold_split_4fold` fold-split logika
- `_load_model_sample_meta` és `_anchor_year_from_meta` (CV fold metadata)
- `generate_walk_forward_folds` import a sampling modulból
- `_LIFT_LAMBDA` konstans (fold-stability penalty)
- Fold loop a `_run_one_trial`-ból; `fold_ids`, `purge_minutes`, `fold_week_assignments`, `fold_time_windows` paraméterek

**Hozzáadva / átírva:**
- `_SearchDataset` dataclass: `train: DatasetSplit`, `train_n`, `valid_n` — az egyszerű split col (0=train, 1=valid) alapján épül fel
- `_load_search_dataset` új implementáció: `split` oszlopot olvas a `model.__sample` táblából; `split==0` → train, `split==1` → valid; `ValueError` ha a `split` col hiányzik
- `_run_one_trial` átírva: egy modell, train + valid kiértékelés; `train_top10_lift` és `valid_top10_lift` mindkét szett kimenete
- `_compute_objective`: `objective_score = -valid_top10_lift` (Optuna minimize), fold-stability penalty eltávolítva
- `_check_patience`: patience=20, epsilon=0.001; `study.stop()` Optuna-ban és `break` a random search-ben
- `_select_best_trial`: top-5 pool valid_top10_lift szerint csökkenő, majd a legkisebb gap kerül kiválasztásra; nem kemény küszöb
- `_update_best` frissítve: `_select_best_trial`-t hív minden trial után
- `search_trials.jsonl` compact record tartalmaz: `valid_top10_lift`, `train_top10_lift`, `train_valid_gap`
- `run_search` signature: `fold_limit` paraméter eltávolítva; `n_trials` default = `_MAX_TRIALS` (100)
- `_print_final_summary`: `valid_top10_lift` alapján rangsorol (nem objective_score)
- `ruff check --fix` és `pyright`: 0 hiba, 0 figyelmeztetés
