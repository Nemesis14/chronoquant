---
id: t1
epic: epic_038_sampling_analysis
title: "Sampling audit notebook — lgbm_solusdt_l_fw60_2101_2605"
assignee: analyst_agent
status: pr
created: 2026-06-23
---

## Task

Walk-forward sampling tábla elemzése a `lgbm_solusdt_l_fw60_2101_2605` modellhez.

## Output

- `_doc_/models_doc/5500_lgbm_l_2101_2605_sampling.ipynb` — Quarto notebook, minden cell lefuttatva
- `_doc_/models_doc/5500_lgbm_l_2101_2605_sampling.html` — rendered HTML

## Tartalom

- **Teljes minta összesítő:** 47 448 sor, 2021-01-01 – 2026-05-31
- **Fold összesítő (fold_id 0–4):** határok, sorszám, avg long_mfe_fw60
- **Avg target foldonként:** sávdiagram (train-only szürke, valid kék)
- **Havi target alakulás:** vonaldiagram fold-határokkal
- **Értelmezés:** csökkenő target trend (0.0074 → 0.0045 fold 1–4), technikai konklúzió

## Notes

Forrás: `model."lgbm_solusdt_l_fw60_2101_2605__sample"` (lab.duckdb).
Parquet nincs az artifact mappában — a sample DuckDB-ben él.
