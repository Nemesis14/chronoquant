---
epic: epic_047
id: t3
title: Long model 4 notebook template-esítése
assignee: analyst_agent
status: todo
blocks: [t4]
blocked_by: [t2]
---

## Goal
A long model 4 analysis notebookját (`01_sampling`–`04_strategy`) template-síteni:
- Hardcoded `MODEL_ID`, `TARGET`, `LAB_DB` lecserélése fejléc paraméter cellára
- Notebook-specifikus src kód lecserélése `analyst.lib` importokra (t2 után)
- Az artifact notebook csak eredményeket és minimális wiring-et tartalmazzon

## Scope
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/01_sampling.ipynb`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/02_feature_engineering.ipynb`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/03_hyperparameter_search.ipynb`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/04_strategy.ipynb`

## Acceptance Criteria
- [ ] Minden notebookban van egy dedikált "Parameters" cella a tetején (MODEL_ID, TARGET, stb.)
- [ ] Nincs hardcoded model-specifikus string a kód cellákon kívül a params cellán
- [ ] Közös kód (colors, setup, loaders) `analyst.lib`-ből importálva
- [ ] A long model HTML-ek újrarenderelhetők a template-esített notebookokból
- [ ] A notebooks tartalma (kimenet) nem változik — csak a kód szerkezete

## Notes
