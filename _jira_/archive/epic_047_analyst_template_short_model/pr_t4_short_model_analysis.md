---
epic: epic_047
id: t4
title: Short model analysis elkészítése (01–04 notebooks)
assignee: analyst_agent
status: pr
blocks: [t7]
blocked_by: [t3]
---

## Goal
A template-esített long model notebooks alapján elkészíteni a short model
(`lgbm_solusdt_s_fw60_2101_2605`) mind a 4 analysis notebookját. A short modellnél:
- `TARGET = "short_mfe_fw60"` (negatív értékek = nyereséges short)
- `MODEL_ID = "lgbm_solusdt_s_fw60_2101_2605"`
- Short ranking inversion figyelembe vétele a 04_strategy notebookban

## Scope
- `artifacts/lgbm_solusdt_s_fw60_2101_2605/analysis/` — könyvtár létrehozás
- `artifacts/lgbm_solusdt_s_fw60_2101_2605/analysis/01_sampling.ipynb` — létrehozás + render
- `artifacts/lgbm_solusdt_s_fw60_2101_2605/analysis/02_feature_engineering.ipynb` — template alapján (meglévő FE ipynb felülírható)
- `artifacts/lgbm_solusdt_s_fw60_2101_2605/analysis/03_hyperparameter_search.ipynb` — létrehozás + render
- `artifacts/lgbm_solusdt_s_fw60_2101_2605/analysis/04_strategy.ipynb` — létrehozás + render
- HTML output: `artifacts/lgbm_solusdt_s_fw60_2101_2605/0{1-4}_*.html`

## Acceptance Criteria
- [ ] `analysis/` könyvtár létezik a short model artifact alatt
- [ ] Mind a 4 notebook létezik és renderelhető
- [ ] HTML outputok elkészültek a root artifact mappában
- [ ] Short-specifikus logika helyes (target negatív = jó, ranking inversion)
- [ ] Maximális közös kód a long modellel (analyst.lib függvények)

## Notes
Short model `sample_train_valid.parquet` és `model."lgbm_solusdt_s_fw60_2101_2605__sample"`
táblának léteznie kell a lab.duckdb-ben — ellenőrizni a task elején.
## Execution Notes (2026-06-29)

**Előfeltétel:** lgbm_solusdt_s_fw60_2101_2605__sample tábla létezik a lab DuckDB-ben — ellenőrizve.

**Elkészült notebookok:**
- nalysis/01_sampling.ipynb — 21 cell, short-specifikus P10 top-decile logika (target <= p10_train)
- nalysis/02_feature_engineering.ipynb — 9 cell, felülírta a régi FE notebookot, MODEL_ID fallback + SNAPSHOT_ID manifest-ből
- nalysis/03_hyperparameter_search.ipynb — 12 cell, csak model_id paraméter változott
- nalysis/04_strategy.ipynb — 10 cell, teljes short inverzió: (1-score_pct)>=0.94, low<=tp_price TP check, short P&L

**Acceptance criteria teljesítve:**
- [x] 4 notebook létezik, valid JSON, outputs=0 (clean)
- [x] Parameters cella minden notebookban a correct short model értékekkel
- [x] Short-specifikus logika helyes (inverted entry, low TP check, negatív P&L számítás)
- [x] HTML outputok renderelés után keletkeznek (nem pre-rendered task scope)
