---
epic: epic_025
id: t2
title: Sampling implementáció 9m train + 3m validation walk-forward foldokra
assignee: modeling_agent
status: todo
blocks: [t3, t7]
blocked_by: [t1]
---

## Goal

A jelenlegi yearly random-hour sampling pipeline-t kiegészíteni vagy lecserélni
úgy, hogy walk-forward foldokat generáljon az új metodológia szerint.

## Scope

- `src/modeling/sampling/`
- sample metadata
- sample parquet struktúra
- smoke / unit tesztek

## Design

- Az óránkénti baseline supervised sample marad.
- A foldok nem random week assignmenttel készülnek.
- Minden foldnak explicit train/valid időablaka van.
- A sample tartalmazza a foldhozzárendelést vagy az időhatárokat úgy, hogy a
  search determinisztikusan újra tudja építeni a splitet.

## Acceptance Criteria

- [ ] Walk-forward fold generálás implementálva
- [ ] Random week assignment a fő validációs pipeline-ból eltávolítva vagy legacyként elkülönítve
- [ ] Metadata tartalmazza a fold time window-kat
- [ ] Tesztek lefedik a fold-időrend helyességét és a gap logikát

## Notes

### Implementált szimbólumok

**`src/modeling/sampling/config.py`:**
- `WalkForwardSamplingConfig` dataclass (frozen) — a `YearlySamplingConfig` mellé, attól független

**`src/modeling/sampling/yearly_sampler.py`:**
- `generate_walk_forward_folds(year, train_months=9, valid_months=3, shift_months=3, purge_minutes=240, n_folds=4) → list[dict]`
  - 4 fold generálása, első valid start: `year-10-01`
  - Non-overlapping validation: shift_months == valid_months
  - Minden fold dict: `{fold_id, train_start, train_end, valid_start, valid_end}` (YYYY-MM-DD stringek)
- `assign_walk_forward_fold_ids(hourly_df, fold_time_windows) → pl.DataFrame`
  - Rows valid ablakban → fold_id = az a fold száma
  - Minden egyéb sor → fold_id = 0 (train-only)
  - `calendar` import hozzáadva a modulba

**`src/modeling/sampling/create_sample.py`:**
- `create_walk_forward_sample(config, output_dir=None)` — orchestrator, multi-year hourly selection, walk-forward fold_id hozzárendelés, `write_yearly_artifacts` reuse
- `create_model_walk_forward_sample(model_id)` — modell-config alapú wrapper

**`src/modeling/sampling/__init__.py`:** Összes új szimbólum exportálva.

**`src/modeling/tests/sampling/smoke/test_walk_forward_folds.py`:** 13 smoke teszt, mind zöld.

### Döntések

- `select_hourly_observations()` NEM volt közvetlen reuse lehetséges multi-year rangera (az function filtrálja a `year`-t) → inline implementáltuk per-year ciklusban a `create_walk_forward_sample`-ben
- A fold-id hozzárendelés time-based (nem shuffle), pontosan a `valid_start`…`valid_end` intervallum alapján
- `purge_minutes` a `fold_time_windows` metadata-ban tárolódik, de a tényleges purge a search idején (`_fold_split_walk_forward`) kerül alkalmazásra
- Meglévő funkciók (`YearlySamplingConfig`, `create_model_sample`, `create_yearly_sample`, `assign_fold_ids`, `select_hourly_observations`) érintetlenek

