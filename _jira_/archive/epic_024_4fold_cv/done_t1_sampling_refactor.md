---
epic: epic_024
id: t1
title: 4-fold CV sampling refactor
assignee: modeling_agent
status: todo
blocks: [t2, t4]
---

## Goal

A yearly sampling logikát átírni 12-fold (train/valid/purge segment) struktúráról
4-fold stratifikált CV struktúrára. Minden hétnek lesz fold_id 1-4, nincs statikus
segment oszlop.

## Scope

- `src/modeling/sampling/yearly_sampler.py`
- `src/modeling/sampling/create_sample.py`
- `src/modeling/sampling/config.py`
- `src/modeling/sampling/__init__.py`
- `src/modeling/sampling/artifacts.py` (docstring only)

## Design

### Régi struktúra (eltávolítani)
- `select_monthly_validation_weeks`: 1 hetet választ/hónap → 12 valid hét
- `assign_segments`: minden sorhoz `segment` (train/valid/purge) + `fold_id` (0-11)
- sample parquet: `open_time | target | segment | fold_id | feat_*`

### Új struktúra (implementálni)
- `assign_fold_ids(hourly_df, year, seed, n_folds=4)`:
  - Minden hónapban a hétfőket (Monday-start weeks) random shuffle-öli seed-del
  - Ciklikusan rendel fold ID-t 1-4: `fold_id = (i % n_folds) + 1`
  - Minden sor kap fold_id-t (Int8, 1-4) aszerint melyik hétbe esik
  - Visszaad: `(df_with_fold_id, fold_week_assignments_dict)`
  - `fold_week_assignments`: `{1: [{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}, ...], 2: [...], 3: [...], 4: [...]}`
  - Minden foldban ~12 hét (1 / hónap), egyes 5-hetes hónapokban 2 hét kerülhet egy foldba
  - Hét-határon kívüli sorok (pl. jan 1-3 ha a hét dec 28-án indult): fill_null(1)

- sample parquet: `open_time | target | fold_id (Int8) | feat_*` (NO segment oszlop)
- metadata.json változások:
  - `selected_valid_weeks` → `fold_week_assignments` (dict, kulcsok: "1","2","3","4")
  - `row_counts` (segment-alapú) → `fold_row_counts` (dict: {"1": N, "2": N, ...})
  - `n_folds: 4` mező hozzáadva
  - `purge_minutes: 240` marad (search használja)

### config.py változás
- `n_folds: int = 4` mező hozzáadva a `YearlySamplingConfig`-hoz

### __init__.py változás
- `select_monthly_validation_weeks`, `assign_segments` eltávolítva
- `assign_fold_ids` hozzáadva

## Acceptance Criteria
- [ ] `assign_fold_ids` függvény létezik yearly_sampler.py-ban
- [ ] `select_monthly_validation_weeks` és `assign_segments` eltávolítva
- [ ] sample parquet tartalmaz `fold_id` (Int8, 1-4) oszlopot, NEM tartalmaz `segment` oszlopot
- [ ] metadata.json tartalmaz: `fold_week_assignments`, `fold_row_counts`, `n_folds`
- [ ] Minden foldban ~2,016 sor (12 hét × 168 óra)
- [ ] Reprodukálható: azonos seed + év → azonos fold assignment

## Notes
