---
epic: epic_025
id: t3
title: Search refactor Top10 Lift objective-re és rank auditokra
assignee: modeling_agent
status: todo
blocks: [t4, t7]
blocked_by: [t1, t2]
---

## Goal

A `lgbm_search.py` objective-jét és riportolását átalakítani úgy, hogy az
elsődleges modellválasztási metrika a `Top10 Lift` legyen fold-stability
penaltyvel.

## Scope

- `src/modeling/search/lgbm_search.py`
- search artifactok
- fold metric struktúra
- rank audit outputok

## Design

Primary metric:

```text
top10_lift = mean(y_true | score in top 10%) - mean(y_true | full validation)
objective = mean(top10_lift_folds) - 0.5 * std(top10_lift_folds)
```

Kötelező audit metrikák:

- `Spearman rank correlation`
- `decile monotonicity`

Opcionális kiegészítők:

- top-bottom spread
- top10 trade count / coverage

## Acceptance Criteria

- [ ] A search primary objective-je Top10 Lift fold-stability penaltyvel
- [ ] `lambda = 0.5` defaultként szerepel
- [ ] Foldonként mentésre kerül a top10_lift, Spearman és monotonicity audit
- [ ] A search summary-ból egyértelműen kiolvasható a rank-alapú döntés

## Notes

### Implementált változások (`src/modeling/search/lgbm_search.py`)

**Import:** `from scipy.stats import spearmanr` hozzáadva.

**Régi konstansok eltávolítva:** `_ALLOWED_GAP`, `_STAB_W`, `_GAP_W`
**Új konstans:** `_LIFT_LAMBDA = 0.5`

**Új funkciók:**
- `_fold_split_walk_forward(sd, fold_k, fold_time_windows, purge_minutes)` — time-window alapú train/valid split, double-sided purge
- `_fold_label_walk_forward(fold_k, fold_time_windows)` — human-readable fold label walk-forward esetén
- `_compute_top10_lift(y_true, y_score)` — top decile lift
- `_compute_decile_monotonicity(y_true, y_score)` — szomszédos decile párok monoton aránya

**`_compute_objective()` teljesen újraírva:**
- Elsődleges metrika: `mean_top10_lift - 0.5 * std_top10_lift`
- `objective_score = -objective` (lower is better, Optuna minimize)
- Visszaad: `objective_score`, `mean_top10_lift`, `std_top10_lift`, `mean_spearman_rho`, `mean_decile_monotonicity`, + RMSE/MAE informatív célból

**`_run_one_trial()` módosítva:**
- `fold_time_windows` opcionális paraméter → ha megvan, `_fold_split_walk_forward` hívódik
- Per-fold: `top10_lift`, `spearman_rho`, `decile_monotonicity` számítva és tárolva
- `fold_week_assignments` → `dict | None` (backward compatible)

**`run_search()`:** detektálja `fold_time_windows` vs `fold_week_assignments` a metadata-ban, átadja mindkét search backendjének.

**`_search_random()` és `_search_optuna()`:** `fold_time_windows=None` opcionális paraméter hozzáadva, átadják `_run_one_trial()`-nak.

**`_persist_completed()`:** compact rekordba bekerül: `mean_top10_lift`, `std_top10_lift`, `mean_spearman_rho`, `mean_decile_monotonicity`.

**Logging frissítve:** trial szinten mutja a lift/spearman/mono értékeket; summary a top-10-et lift szerint rangsorolja.

**Backward compatibility:** a legacy weekly CV (`fold_week_assignments`) teljes mértékben működik — a dispatch `fold_time_windows is not None` feltétel alapján történik.

