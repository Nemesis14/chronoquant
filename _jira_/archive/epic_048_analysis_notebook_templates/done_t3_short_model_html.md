---
epic: epic_048
id: t3
title: Short modell HTML-ek generálása
assignee: modeling_agent
status: pr
blocks: [t4]
blocked_by: [t1, t2]
---

## Goal
Az `lgbm_solusdt_s_fw60_2101_2605` short modellre futtatni az `--step analyze`-t,
hogy a hiányzó HTML reportok (`01_sampling.html`, `03_hyperparameter_search.html`,
`04_strategy.html`) is elkészüljenek.

## Scope
- `artifacts/lgbm_solusdt_s_fw60_2101_2605/` — HTML fájlok generálása

## Végrehajtás

```bash
uv run python src/modeling/pipeline.py \
  --model lgbm_solusdt_s_fw60_2101_2605 \
  --step analyze
```

## Elvárt output
```
artifacts/lgbm_solusdt_s_fw60_2101_2605/
  01_sampling.html             ← új
  02_feature_engineering.html  ← már megvan, felülírja
  03_hyperparameter_search.html ← új
  04_strategy.html             ← új
  analysis/
    01_sampling.ipynb          ← felülírja template-ből
    02_feature_engineering.ipynb
    03_hyperparameter_search.ipynb
    04_strategy.ipynb
```

## Acceptance Criteria
- [ ] `01_sampling.html` létezik és megnyitható
- [ ] `03_hyperparameter_search.html` létezik és megnyitható
- [ ] `04_strategy.html` létezik és megnyitható
- [ ] `analysis/*.ipynb` fájlok frissültek (template-ből példányosítva)

## Notes
- 4 HTML elkészült: `01_sampling.html`, `02_feature_engineering.html`, `03_hyperparameter_search.html`, `04_strategy.html`
- Elérési út: `artifacts/lgbm_solusdt_s_fw60_2101_2605/*.html`
- Papermill OK mind a 4 notebooknál; Quarto OK mind a 4-nél
- `analysis/` mappában a végrehajtott `.ipynb` fájlok megmaradnak
