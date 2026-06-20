---
epic: epic_025
id: t9
title: Validation és acceptance audit az új walk-forward rerunra
assignee: validator_agent
status: done
blocks: []
blocked_by: [t5, t6, t7, t8]
---

## Goal

Az új metodológia implementációját, a rerun artifactokat és a dokumentáció
konzisztenciáját végigellenőrizni.

## Scope

- fold idősor helyesség
- search objective helyesség
- rerun artifactok integritása
- archiválás és új analysis output konzisztenciája

## Acceptance Criteria

- [x] A foldok időrendje és gap szabálya validált
- [x] A search valóban a Top10 Lift objective alapján választ modellt
- [x] A 5600 rerun outputjai konzisztens állapotban vannak
- [x] A régi 5600 anyag archív, az új anyag aktív referencia
- [x] A `_doc_` dokumentáció és a notebook ugyanazt a metodológiát írja le

## Notes

[validator] Accepted — 2026-06-20
Ruff: 1 SIM108 hiba javítva (if/else → ternary, lgbm_search.py:124).
Pyright: 36 hiba javítva (spearmanr typed index, decile_monotonicity .to_numpy(), list comprehension explicit float cast, type: ignore szükséges helyeken).
Pytest: 57/57 passed (sampling + training smoke tests, beleértve walk_forward_folds tesztjeit).
Artifact: metadata.json tartalmaz fold_time_windows (4 fold, 2021-10..2022-09), sampling_mode=walk_forward, nincs fold_week_assignments. fold_id=0 (train-only): 6552 sor, fold 1-4: 2160-2208 sor.
Temporal ordering: minden foldban train_end < valid_start (1 nap gap >= 240 min purge). Nem-átfedő valid ablakok.
Top10 Lift: _compute_top10_lift = mean(y_true | top10% score) - mean(y_true). objective = mean(lift) - 0.5*std(lift). objective_score = -objective (Optuna minimize). Helyes.
search_best.json tartalmazza: mean_top10_lift, mean_spearman_rho, mean_decile_monotonicity kulcsokat.
Backward compat: create_model_sample(), create_yearly_sample(), assign_fold_ids(), select_hourly_observations() változatlanok. Legacy fold_week_assignments path megmarad a search-ben.
_doc_/5600: aktív .html + .ipynb létezik, ARCHIVE fájl is megvan.
_doc_/5010_sampling_yearly.md: Walk-forward CV szekció megvan.
_doc_/5500_hyper_param_search.md: Top10 Lift + Rank Audit szekció megvan.
