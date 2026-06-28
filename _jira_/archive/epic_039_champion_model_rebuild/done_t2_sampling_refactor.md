---
epic: epic_039
id: t2
title: Sampling kód refaktor — train_valid_split mode + config
assignee: modeling_agent
status: pr
blocks: [t3, t4, t10]
blocked_by: []
---

## Goal

A jelenlegi walk-forward CV-alapú sampling kód lecserélése egyszerű train/valid
split megközelítésre. Új `sampling_mode: "train_valid_split"` bevezetése.
A champion modellek config-ja frissül az új paraméterekkel.

Módszertani háttér: `_doc_/methodology_doc/5400_sampling.md`

## Scope

- `src/modeling/sampling/create_sample.py` — új `train_valid_split` branch
- `config/models.json` — champion modellek (`lgbm_solusdt_l_fw60_2101_2605`,
  `lgbm_solusdt_s_fw60_2101_2605`) sampling szekciója

## Acceptance Criteria

- [x] Új `sampling_mode: "train_valid_split"` működik a pipeline `sample` lépésén
- [x] Train: 2021-01-01 – 2025-04-30, Valid: 2025-05-01 – 2026-05-31
- [x] Feature lookback embargo: 240 perc a train elején kizárva
- [x] Target purge: utolsó 60 perc a train végéről kizárva
- [x] Valid elején nincs embargo
- [x] Óránkénti véletlenszerű perc mintavétel megmarad (seed=42)
- [x] `model.__sample` tábla tartalmaz split indicator oszlopot (0=train, 1=valid)
- [x] Ugyanaz a snapshot + seed reprodukálható mintát ad
- [x] `config/models.json` champion modellek frissítve
- [x] `ruff check` és `pyright` tisztán fut

## Notes

Kapcsolódó finding: epic_038 story_s1 — walk-forward CV train mask bug.

[modeling_agent] Elvégzett változtatások — 2026-06-23

### Módosított fájlok

**`src/modeling/sampling/config.py`**
- Új `TrainValidSplitConfig` dataclass hozzáadva (`frozen=True`).
- Mezők: `sample_id`, `asset_id`, `train_start`, `train_end`, `valid_start`,
  `valid_end`, `seed` (default 42), `feature_lookback_embargo_minutes` (default 240),
  `target_purge_minutes` (default 60), `target_cols`.

**`src/modeling/sampling/snapshot_sampler.py`**
- `build_train_valid_split_select_sql()` — új SQL builder: óránkénti QUALIFY +
  train/valid WHERE filter + `split` TINYINT (0=train, 1=valid) CASE expression.
  Feature lookback embargo (`>= train_start + N minutes`) és target purge
  (`<= train_end - N minutes`) SQL szinten alkalmazva; valid oldalon nincs embargo.
- `build_train_valid_split_ctas_sql()` — wrapper: `CREATE OR REPLACE TABLE ... AS SELECT`.

**`src/modeling/sampling/create_sample.py`**
- `create_model_sample()` — módosítva: `sampling_mode` dispatch alapján hív
  `create_snapshot_sample_train_valid_split()` vagy `create_snapshot_sample()`.
  Ismeretlen `sampling_mode` esetén `ValueError`.
- `create_snapshot_sample_train_valid_split()` — új publikus függvény: snapshot
  ellenőrzés, CTAS futtatás, `split_row_counts`, registry `feature_sets` és
  `models` upsert, summary dict visszaadás.
- `_split_counts()` — új belső helper: `SELECT split, COUNT(*)` a sample táblán.
- Import: `build_train_valid_split_ctas_sql` hozzáadva.

**`config/models.json`**
- Mindkét champion modell (`lgbm_solusdt_l_fw60_2101_2605`,
  `lgbm_solusdt_s_fw60_2101_2605`) sampling szekciója frissítve:
  - `sampling_mode`: `"walk_forward"` → `"train_valid_split"`
  - Törölt: `row_stride`, `year`, `train_months`, `valid_months`, `shift_months`, `n_folds`
  - Hozzáadott: `seed: 42`, `train_start`, `train_end`, `valid_start`, `valid_end`,
    `feature_lookback_embargo_minutes: 240`, `target_purge_minutes: 60`
  - `description` szöveg frissítve.

### Teszt eredmények

```
ruff check src/modeling/sampling/ --fix  →  All checks passed!
uv run pyright src/modeling/sampling/    →  0 errors, 0 warnings, 0 informations
```
