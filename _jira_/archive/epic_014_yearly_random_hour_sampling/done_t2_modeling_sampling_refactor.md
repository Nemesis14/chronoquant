---
epic: epic_014
id: t2
title: Sampling modul teljes refaktor — régi logika kidobása, új yearly_sampler, CLI átírás
assignee: modeling_agent
status: pr
blocks: [t3, t5]
blocked_by: []
---

## Goal

A régi expanding-window CV sampling logikát teljesen lecserélni egy éves, random-hour
alapú sampling modulra.

## Scope

### Törölve
- `src/modeling/quantitative/sampling/splits.py`
- `src/modeling/quantitative/sampling/config.py` — régi `SamplingConfig` eltávolítva
- `database/solusdt/samples/solusdt_fw60_2010_2605/` — régi sample directory
- `src/modeling/quantitative/tests/sampling/smoke/test_splits.py`
- `src/modeling/quantitative/tests/sampling/smoke/test_config.py`
- `src/modeling/tests/sampling/smoke/test_write_sample_parquet.py`

### Létrehozva / módosítva
- `src/modeling/quantitative/sampling/yearly_sampler.py` (új)
- `src/modeling/quantitative/sampling/config.py` (teljesen újraírva — `YearlySamplingConfig`)
- `src/modeling/quantitative/sampling/artifacts.py` (yearly write/load hozzáadva, legacy load/validate megmarad)
- `src/modeling/quantitative/sampling/create_sample.py` (teljesen újraírva — `create_yearly_sample`)
- `src/modeling/quantitative/sampling/__init__.py` (exportok frissítve)
- `src/modeling/quantitative/00_create_sample.py` (CLI teljesen újraírva)

## Acceptance Criteria

- [x] `splits.py` törölve
- [x] Régi `SamplingConfig` törölve, helyén `YearlySamplingConfig`
- [x] `database/solusdt/samples/solusdt_fw60_2010_2605/` törölve
- [x] `yearly_sampler.py` létezik: hourly selection, monthly valid week selection, segment assignment
- [x] Minden selected hour: pontosan 1 sor (max 1 obs/hour constraint teljesül)
- [x] Fixed seed: azonos seed → azonos kiválasztás (hash-based, row-order independent)
- [x] Purge logika: ±240 perc a valid week határain, purge nem érinti a valid set-et
- [x] `sample.parquet` tartalmaz: open_time, segment, long_mfe_fw60, short_mfe_fw60
- [x] `metadata.json` tartalmaz: year, seed, selected_valid_weeks (12 db), row_counts by segment
- [x] CLI: `--year 2021 --asset-id solusdt` futtatható, kész sample dir-t hoz létre
- [x] `__init__.py` exportjai konzisztensek az új API-val
- [ ] Pyright és ruff clean — validator_agent feladata (t4)

## Notes

**Legacy backward compat:** `load_sample_definition` és `validate_sample_definition` megmaradtak
az `artifacts.py`-ban és `__init__.py`-ban, mert `lightgbm_model.py` és `lgbm_search.py`
hivatkoznak rájuk. Ezek a fájlok a t2 scope-ján kívül esnek — a modell training pipeline
frissítése külön task lesz.

**`write_sample_artifacts`** is megmarad a legacy tesztek (`test_artifacts.py`) miatt.

**Seed stratégia:** `seed = 42 + year` (pl. 2021 → 2063). CLI-ben default, felülírható `--seed`-del.

**Hash-based selection:** `open_time.cast(Int64).hash(seed=seed, seed2=seed+1)` — reproducible
függetlenül az input row ordering-től.
