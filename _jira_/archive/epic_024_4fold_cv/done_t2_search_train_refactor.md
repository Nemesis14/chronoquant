---
epic: epic_024
id: t2
title: Search + train kód refaktor 4-fold CV-re
assignee: modeling_agent
status: todo
blocks: [t4]
blocked_by: [t1]
---

## Goal

A search (lgbm_search.py) és train (fit_lgbm.py) kódot átírni az új 4-fold
struktúrára: fold_id alapú split, purge dinamikusan számolva, final fit minden soron.

## Scope

- `src/modeling/search/lgbm_search.py`
- `src/modeling/training/fit_lgbm.py`
- `src/modeling/` tesztek ellenőrzése, szükség esetén frissítése

## Design

### lgbm_search.py változások

**`_SearchDataset` dataclass:**
```python
@dataclass(frozen=True)
class _SearchDataset:
    dataset:  ModelingDataset
    fold_ids: pd.Series   # Int8, 1-4 — replaces 'segments'
```

**`_load_search_dataset`:**
- `fold_id` oszlopot tölt be `segment` helyett
- `_SearchDataset(dataset=dataset, fold_ids=pd.Series(merged["fold_id"].astype("int8")))`

**`_fold_split_4fold(sd, fold_k, fold_week_assignments, purge_minutes)` — REPLACE `_fold_split_yearly`:**
```python
valid_mask = sd.fold_ids == fold_k
delta = pd.Timedelta(minutes=purge_minutes)
purge_mask = pd.Series(False, index=sd.dataset.open_time.index)
weeks = fold_week_assignments.get(fold_k, fold_week_assignments.get(str(fold_k), []))
for week in weeks:
    vs = pd.Timestamp(week["start"])
    ve = pd.Timestamp(week["end"]) + pd.Timedelta(hours=23, minutes=59)
    pre  = (sd.dataset.open_time >= vs - delta) & (sd.dataset.open_time < vs)
    post = (sd.dataset.open_time > ve) & (sd.dataset.open_time <= ve + delta)
    purge_mask = purge_mask | pre | post
train_mask = ~valid_mask & ~purge_mask
```

**`run_search`:**
- `fold_week_assignments = sample_meta["fold_week_assignments"]`
- `purge_minutes = sample_meta.get("purge_minutes", 240)`
- `n_folds = sample_meta.get("n_folds", 4)`
- `all_fold_ids = list(range(1, n_folds + 1))`  → [1, 2, 3, 4]
- `fold_ids_to_run = all_fold_ids[:fold_limit] if fold_limit else all_fold_ids`
- Logging: `f"folds={len(fold_ids_to_run)}/{n_folds}"`

**Stage defaults (`_apply_stage_defaults`):**
- smoke: fold_limit → `2` (volt 2 → marad)
- explore: fold_limit → `None` (mind a 4)
- refine: fold_limit → `None`

**`_run_one_trial` szignatúra:**
```python
def _run_one_trial(trial_no, params, sd, fold_ids, fold_week_assignments, purge_minutes, search_dir)
```
- Iterál: `for fold_k in fold_ids: split = _fold_split_4fold(sd, fold_k, fold_week_assignments, purge_minutes)`
- fold_results["fold"] = fold_k (1-4)

**`fold_summary` struktúra** a search_best.json-ban marad ugyanaz, de 4 entry lesz.

### fit_lgbm.py változások

**`_load_train_data`:**
```python
# RÉGI (eltávolítani):
train_valid = sample_df[sample_df["segment"].isin(["train", "valid"])].copy()
# ÚJ: minden sor részt vesz a final fitnél
train_valid = sample_df.copy()
```

**`_add_predictions_to_sample`:**
- Predict ALL rows (no segment filter)
- Új oszloprend: `["open_time", target_name, pred_col, "fold_id"] + feat_cols`

**`n_estimators` számítás** — marad ugyanaz, `search_best["fold_summary"]` alapján (most 4 fold).

## Acceptance Criteria
- [ ] `_fold_split_4fold` függvény létezik, `_fold_split_yearly` eltávolítva
- [ ] `_SearchDataset.fold_ids` létezik, `.segments` eltávolítva
- [ ] smoke stage: 2 fold, explore/refine: 4 fold
- [ ] `_load_train_data` NEM filterez segment alapján
- [ ] sample parquet oszloprend: `open_time | target | pred_{dir} | fold_id | feat_*`
- [ ] Nincs referencia `segment` oszlopra fit_lgbm.py-ban

## Notes
