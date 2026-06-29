# Epic 048: Analysis Notebook Template Rendszer

## Goal
Az `artifacts/<model_id>/analysis/` mappa 4 numbered notebookjának (`01_sampling`,
`02_feature_engineering`, `03_hyperparameter_search`, `04_strategy`) legyen
újrahasználható template-je, amelyből a pipeline automatikusan tudja példányosítani
bármely (long vagy short) modellhez.

## Scope
- `analyst/notebooks/` — 4 új template notebook
- `src/modeling/pipeline.py` — `--step analyze` (automatikus predict után)
- `artifacts/lgbm_solusdt_s_fw60_2101_2605/` — short modell HTML-jeinek generálása

## Status: DONE

## Tasks
- t1: Template notebookok létrehozása (modeling_agent) — done
- t2: `pipeline.py --step analyze` implementálása (modeling_agent) — done
- t3: Short modell HTML-ek generálása `--step analyze`-zal (modeling_agent) — done
- t4: Validáció (validator_agent) — done

## Key Decisions
- Kétfázisú build: nbformat placeholder-csere a raw frontmatter cellában → papermill execution
- Placeholder szintaxis: `{{MODEL_ID}}`, `{{DIRECTION_LABEL}}`, `{{DATE}}`
- DIRECTION egységesítve: "long"/"short" mindenhol (01_sampling-ban volt "l"/"s")
- `--step analyze` automatikusan fut a `predict` step után
- Skip logika: ha egy notebook prereq. artifactja hiányzik → skip + warning (nem fatal)
- HTML output: `artifacts/<model_id>/0N_xxx.html` (root szinten, nem `analysis/`-ban)
- `04_strategy` dátumok: `VALID_START`/`VALID_END` paraméter, default = strategy_artifact `fit_period`-ból
