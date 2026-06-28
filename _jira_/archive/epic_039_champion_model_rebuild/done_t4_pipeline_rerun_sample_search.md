---
epic: epic_039
id: t4
title: Pipeline újrafuttatás — sample + search (long + short)
assignee: modeling_agent
status: pr
blocks: [t5, t8]
blocked_by: [t2, t3]
---

## Goal

A két champion modell sample és search lépéseinek újrafuttatása az átírt kóddal.
Az eredmény: új `model.__sample` táblák és új search artifacts mindkét modellhez.

## Scope

- `uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2605 --step sample`
- `uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2605 --step search`
- `uv run python src/modeling/pipeline.py --model lgbm_solusdt_s_fw60_2101_2605 --step sample`
- `uv run python src/modeling/pipeline.py --model lgbm_solusdt_s_fw60_2101_2605 --step search`

## Acceptance Criteria

- [x] `model."lgbm_solusdt_l_fw60_2101_2605__sample"` létezik, train/valid split helyes
- [x] `model."lgbm_solusdt_s_fw60_2101_2605__sample"` létezik, train/valid split helyes
- [x] Train: 2021-01-01 – 2025-04-30 (240 perc embargo + 60 perc purge alkalmazva)
- [x] Valid: 2025-05-01 – 2026-05-31 (nincs embargo)
- [x] `artifacts/lgbm_solusdt_l_fw60_2101_2605/search/search_best.json` frissítve
- [x] `artifacts/lgbm_solusdt_s_fw60_2101_2605/search/search_best.json` frissítve
- [x] `search_trials.jsonl` mindkét modellhez tartalmaz `train_top10_lift` + `valid_top10_lift` mezőt
- [x] `reg.search_runs` bejegyzések mentve

## Notes

**pipeline.py bugfix (orchestrátor):** `pipeline.py:131-135` — `fold_row_counts` → `split_row_counts`
kompatibilitás javítva az orchestrátor által, mielőtt a pipeline újrafutott.

**Long modell eredmények:**
- Sample: 37,939 train sor (split=0), 9,504 valid sor (split=1), összesen 47,443
- Search: best valid_top10_lift=0.003994, train_top10_lift=0.011206, gap=0.007211

**Short modell eredmények:**
- Sample: 37,939 train sor (split=0), 9,504 valid sor (split=1), összesen 47,443
- Search: best valid_top10_lift=0.002579, train_top10_lift=0.004799, gap=0.002220

Streamlit UI (PID 11652) ideiglenesen le lett állítva a short search futtatásához
(DuckDB file lock), majd kézzel újraindítandó.
