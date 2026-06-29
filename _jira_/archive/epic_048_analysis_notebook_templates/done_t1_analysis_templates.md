---
epic: epic_048
id: t1
title: 4 analysis template notebook létrehozása
assignee: modeling_agent
status: pr
blocks: [t2, t3]
blocked_by: []
---

## Goal
Létrehozni a `src/modeling/analysis/` mappában a 4 template notebookot, amelyekből
a pipeline majd példányosítja a modell-specifikus analysis notebookokat.
A meglévő long modell notebookjaiból kiindulva, de paraméterezhető formában.

## Scope
- `analyst/notebooks/01_sampling.ipynb` (új)
- `analyst/notebooks/02_feature_engineering.ipynb` (új — vagy a meglévő `src/modeling/01_feature_engineering.ipynb` másolata + adaptáció)
- `analyst/notebooks/03_hyperparameter_search.ipynb` (új)
- `analyst/notebooks/04_strategy.ipynb` (új)

## Template követelmények

### Raw frontmatter cell (Cell 0) — Quarto YAML
Placeholdereket tartalmaz, amelyeket a builder cserél ki nbformat-tal:
- `{{MODEL_ID}}` → pl. `lgbm_solusdt_s_fw60_2101_2605`
- `{{DIRECTION_LABEL}}` → `Long` vagy `Short`
- `{{DATE}}` → build dátuma (ISO format)

Példa:
```yaml
---
title: "Sampling Elemzés – {{DIRECTION_LABEL}} Champion Modell"
subtitle: "{{MODEL_ID}} | train/valid periódus vizsgálat"
date: "{{DATE}}"
format:
  html:
    theme: cosmo
    css: ../../../analyst/quarto/chronoquant_analysis.css
    ...
---
```

### Parameters cell — papermill injekció
Minden notebooknak legyen egy `parameters` tagged cellája (Cell 1):

**01_sampling:**
```python
# parameters
MODEL_ID  = "lgbm_solusdt_l_fw60_2101_2605"
DIRECTION = "long"   # "long" or "short"
TARGET    = "long_mfe_fw60"
```

**02_feature_engineering:**
```python
# parameters
ARTIFACT_DIR = ""
SAMPLE_DIR   = ""
MODEL_ID     = "lgbm_solusdt_l_fw60_2101_2605"
SNAPSHOT_ID  = ""
```
(Ez a notebook már papermill-kész az artifacts-ban; forrás: `src/modeling/01_feature_engineering.ipynb`)

**03_hyperparameter_search:**
```python
# parameters
model_id = "lgbm_solusdt_l_fw60_2101_2605"
```

**04_strategy:**
```python
# parameters
MODEL_ID      = "lgbm_solusdt_l_fw60_2101_2605"
DIRECTION     = "long"    # "long" or "short"
STRATEGY_DIR  = ""        # default: artifacts/<MODEL_ID>/strategy
VALID_START   = ""        # default: strategy_artifact["fit_period"]["valid_start"]
VALID_END     = ""        # default: strategy_artifact["fit_period"]["valid_end"]
```

### DIRECTION egységesítés
Az eredeti `01_sampling` "l"/"s"-t használt. A templateban **egységesítjük "long"/"short"-ra**.
A notebook belső logikája adaptálódik (TARGET deriválása: `"long"` → `"long_mfe_fw60"` stb.).

### 04_strategy dátumok
A `Cell 3`-ban hardcoded dátumok (`'2025-06-09'`, `'2025-05-01'`) cserélnek:
- Ha `VALID_START`/`VALID_END` paraméter meg van adva: azt használja
- Ha üres: `strategy_artifact.json` → `fit_period.valid_start`/`fit_period.valid_end`
- Ha az artifact sem létezik: skip a dátumfüggő cellákban

## Forrás notebookok
A templatek alapja a meglévő long modell notebookjai:
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/01_sampling.ipynb`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/02_feature_engineering.ipynb`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/03_hyperparameter_search.ipynb`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/04_strategy.ipynb`

## Acceptance Criteria
- [ ] `analyst/notebooks/` mappában 4 template `.ipynb` fájl létezik
- [ ] Minden notebooknak van `parameters` tagged cellája
- [ ] Raw frontmatter cellában `{{MODEL_ID}}`, `{{DIRECTION_LABEL}}`, `{{DATE}}` placeholder-ek
- [ ] `04_strategy` nem használ hardcoded dátumokat, VALID_START/VALID_END-ből derivál
- [ ] DIRECTION = "long"/"short" mindenhol (nem "l"/"s")

## Notes
- 4 template létrehozva: `analyst/notebooks/01_sampling.ipynb`, `02_feature_engineering.ipynb`, `03_hyperparameter_search.ipynb`, `04_strategy.ipynb`
- Raw Cell 0 frontmatterben `{{MODEL_ID}}`, `{{DIRECTION_LABEL}}`, `{{DATE}}` (kivéve `02_fe`: csak `{{DATE}}`)
- `03_hyperparameter_search`: `valid_ratio_p925` → `valid_top10_lift` metrika frissítve
- `04_strategy`: direction-aware TP_LOG/entry_condition/PnL, VALID_START/VALID_END paraméterek
- Long modell `analysis/` mappa visszaállítva git-ből + s→l adaptációval
